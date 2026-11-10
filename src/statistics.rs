// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{ScalarValue, Statistics};
use iceberg::scan::FileScanTask;
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

pub(crate) fn file_bounds(
    statistics: &mut Statistics,
    output: &Schema,
    schema: &IcebergSchema,
    tasks: &[FileScanTask],
) {
    if tasks.is_empty() {
        return;
    }
    for (column, field) in statistics.column_statistics.iter_mut().zip(output.fields()) {
        let Some(source) = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id| id.parse().ok())
            .and_then(|id| schema.as_struct().field_by_id(id))
        else {
            continue;
        };
        let integer_type = matches!(
            (source.field_type.as_ref(), field.data_type()),
            (Type::Primitive(PrimitiveType::Int), DataType::Int32)
                | (Type::Primitive(PrimitiveType::Long), DataType::Int64)
        );
        if !integer_type || field.metadata().contains_key("ARROW:extension:name") {
            continue;
        }
        let bounds = tasks.iter().try_fold(None, |range, task| {
            if task.record_count() == Some(0) {
                return Some(range);
            }
            let lower = integer_bound(task.lower_bounds().get(&source.id)?, field.data_type())?;
            let upper = integer_bound(task.upper_bounds().get(&source.id)?, field.data_type())?;
            if lower > upper {
                return None;
            }
            Some(Some(match range {
                Some((minimum, maximum)) => (i64::min(minimum, lower), i64::max(maximum, upper)),
                None => (lower, upper),
            }))
        });
        if let Some(Some((lower, upper))) = bounds {
            let value = |value| match field.data_type() {
                DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                _ => ScalarValue::Int64(Some(value)),
            };
            column.min_value = Precision::Inexact(value(lower));
            column.max_value = Precision::Inexact(value(upper));
        }
    }
}

fn integer_bound(datum: &Datum, target: &DataType) -> Option<i64> {
    match (target, datum.data_type(), datum.literal()) {
        (DataType::Int32 | DataType::Int64, PrimitiveType::Int, PrimitiveLiteral::Int(value)) => {
            Some(i64::from(*value))
        }
        (DataType::Int64, PrimitiveType::Long, PrimitiveLiteral::Long(value)) => Some(*value),
        _ => None,
    }
}

pub(crate) fn output_bytes(statistics: &mut Statistics, schema: &Schema) {
    let rows = statistics.num_rows.get_value().copied();
    let mut total = rows.map(|_| 0usize);
    for (column, field) in statistics.column_statistics.iter_mut().zip(schema.fields()) {
        let bytes = rows.and_then(|rows| {
            if rows == 0 {
                return Some(0);
            }
            let values = match field.data_type() {
                DataType::Boolean => rows.div_ceil(8),
                DataType::FixedSizeBinary(width) => {
                    rows.checked_mul(usize::try_from(*width).ok()?)?
                }
                data_type => rows.checked_mul(data_type.primitive_width()?)?,
            };
            values.checked_add(if field.is_nullable() {
                rows.div_ceil(8)
            } else {
                0
            })
        });
        column.byte_size = bytes.map_or(Precision::Absent, Precision::Inexact);
        total = total
            .zip(bytes)
            .and_then(|(total, bytes)| total.checked_add(bytes));
    }
    statistics.total_byte_size = total.map_or(Precision::Absent, Precision::Inexact);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Field;
    use datafusion::common::stats::Precision::{Absent, Exact, Inexact};
    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::spec::{DataFileFormat, NestedField};

    use super::*;

    fn integer_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    7,
                    "renamed",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "item",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap()
    }

    fn task(
        schema: &IcebergSchema,
        lower: HashMap<i32, Datum>,
        upper: HashMap<i32, Datum>,
        rows: Option<u64>,
    ) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(100)
            .with_start(0)
            .with_length(100)
            .with_record_count(rows)
            .with_data_file_path("part.parquet".to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::new(schema.clone()))
            .with_project_field_ids(vec![7, 9])
            .with_case_sensitive(true)
            .with_lower_bounds(Arc::new(lower))
            .with_upper_bounds(Arc::new(upper))
            .build()
            .unwrap()
    }

    fn ranges(statistics: &Statistics) -> Vec<(Precision<ScalarValue>, Precision<ScalarValue>)> {
        statistics
            .column_statistics
            .iter()
            .map(|column| (column.min_value.clone(), column.max_value.clone()))
            .collect()
    }

    #[test]
    fn file_bounds_follow_ids_projection_and_integer_promotion() {
        let schema = integer_schema();
        let output = schema_to_arrow_schema(&schema)
            .unwrap()
            .project(&[1, 0])
            .unwrap();
        let output = Schema::new(vec![
            output.field(0).clone(),
            output.field(1).clone().with_name("alias"),
        ]);
        let tasks = vec![
            task(
                &schema,
                HashMap::from([(7, Datum::int(-9)), (9, Datum::int(i32::MIN))]),
                HashMap::from([(7, Datum::int(5)), (9, Datum::int(4))]),
                Some(2),
            ),
            task(
                &schema,
                HashMap::from([(7, Datum::long(i64::MIN)), (9, Datum::int(1))]),
                HashMap::from([(7, Datum::long(i64::MAX)), (9, Datum::int(i32::MAX))]),
                Some(2),
            ),
        ];
        let mut statistics = Statistics::new_unknown(&output).with_num_rows(Exact(4));
        file_bounds(&mut statistics, &output, &schema, &tasks);
        assert_eq!(ranges(&statistics), vec![
            (
                Inexact(ScalarValue::Int32(Some(i32::MIN))),
                Inexact(ScalarValue::Int32(Some(i32::MAX)))
            ),
            (
                Inexact(ScalarValue::Int64(Some(i64::MIN))),
                Inexact(ScalarValue::Int64(Some(i64::MAX)))
            ),
        ]);
        assert_eq!(statistics.num_rows, Exact(4));
        assert!(
            statistics
                .column_statistics
                .iter()
                .all(|column| column.distinct_count == Absent && column.null_count == Absent)
        );
    }

    #[test]
    fn file_bounds_require_complete_compatible_endpoints() {
        let schema = integer_schema();
        let output = schema_to_arrow_schema(&schema).unwrap();
        let good = task(
            &schema,
            HashMap::from([(7, Datum::int(1))]),
            HashMap::from([(7, Datum::int(5))]),
            Some(2),
        );
        for (lower, upper) in [
            (HashMap::new(), HashMap::from([(7, Datum::int(5))])),
            (HashMap::from([(7, Datum::int(1))]), HashMap::new()),
            (
                HashMap::from([(7, Datum::int(9))]),
                HashMap::from([(7, Datum::int(5))]),
            ),
            (
                HashMap::from([(7, Datum::date(1))]),
                HashMap::from([(7, Datum::date(5))]),
            ),
        ] {
            let tasks = [good.clone(), task(&schema, lower, upper, None)];
            let mut statistics = Statistics::new_unknown(&output);
            file_bounds(&mut statistics, &output, &schema, &tasks);
            assert_eq!(ranges(&statistics), vec![
                (Absent, Absent),
                (Absent, Absent)
            ]);
        }
        let tasks = [task(
            &schema,
            HashMap::from([(9, Datum::long(i64::MIN))]),
            HashMap::from([(9, Datum::long(i64::MAX))]),
            Some(2),
        )];
        let mut statistics = Statistics::new_unknown(&output);
        file_bounds(&mut statistics, &output, &schema, &tasks);
        assert_eq!(ranges(&statistics)[1], (Absent, Absent));
        let mut statistics = Statistics::new_unknown(&output);
        file_bounds(&mut statistics, &output, &schema, &[]);
        assert_eq!(ranges(&statistics), vec![
            (Absent, Absent),
            (Absent, Absent)
        ]);
    }

    #[test]
    fn file_bounds_cover_byte_ranges_and_ignore_only_known_empty_files() {
        let schema = integer_schema();
        let output = schema_to_arrow_schema(&schema).unwrap();
        let bounded = task(
            &schema,
            HashMap::from([(7, Datum::int(-5))]),
            HashMap::from([(7, Datum::int(9))]),
            Some(4),
        )
        .with_byte_range(20, 50)
        .unwrap();
        assert_eq!(bounded.record_count(), None);
        let empty = task(&schema, HashMap::new(), HashMap::new(), Some(0));
        let mut statistics = Statistics::new_unknown(&output).with_num_rows(Inexact(2));
        file_bounds(&mut statistics, &output, &schema, &[bounded, empty]);
        assert_eq!(ranges(&statistics), vec![
            (
                Inexact(ScalarValue::Int64(Some(-5))),
                Inexact(ScalarValue::Int64(Some(9)))
            ),
            (Absent, Absent)
        ]);
        assert_eq!(statistics.num_rows, Inexact(2));
    }

    fn byte_sizes(statistics: &Statistics) -> (Precision<usize>, Vec<Precision<usize>>) {
        (
            statistics.total_byte_size,
            statistics
                .column_statistics
                .iter()
                .map(|column| column.byte_size)
                .collect(),
        )
    }

    #[test]
    fn projections_estimate_values_and_nullable_bitmaps() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int32, true),
            Field::new("flag", DataType::Boolean, false),
            Field::new("optional_flag", DataType::Boolean, true),
            Field::new("uuid", DataType::FixedSizeBinary(16), false),
        ]);
        let mut statistics = Statistics::new_unknown(&schema).with_num_rows(Exact(9));
        output_bytes(&mut statistics, &schema);
        assert_eq!(
            byte_sizes(&statistics),
            (Inexact(260), vec![
                Inexact(72),
                Inexact(38),
                Inexact(2),
                Inexact(4),
                Inexact(144)
            ])
        );
        let projection = schema.project(&[3, 1]).unwrap();
        let mut projected = Statistics::new_unknown(&projection).with_num_rows(Exact(9));
        output_bytes(&mut projected, &projection);
        assert_eq!(
            (projected.num_rows, byte_sizes(&projected)),
            (Exact(9), (Inexact(42), vec![Inexact(4), Inexact(38)]))
        );
    }

    #[test]
    fn unknown_columns_and_row_counts_keep_byte_estimates_unknown() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("nested", DataType::Struct(Default::default()), true),
        ]);
        let mut statistics = Statistics::new_unknown(&schema)
            .with_num_rows(Inexact(4))
            .with_total_byte_size(Exact(12345));
        output_bytes(&mut statistics, &schema);
        assert_eq!(
            (statistics.num_rows, byte_sizes(&statistics)),
            (Inexact(4), (Absent, vec![Inexact(32), Absent, Absent]))
        );
        statistics.num_rows = Absent;
        output_bytes(&mut statistics, &schema);
        assert_eq!(byte_sizes(&statistics), (Absent, vec![Absent; 3]));
        statistics.num_rows = Exact(0);
        output_bytes(&mut statistics, &schema);
        assert_eq!(byte_sizes(&statistics), (Inexact(0), vec![Inexact(0); 3]));
    }

    #[test]
    fn overflowing_columns_and_totals_stay_unknown() {
        for (fields, expected) in [
            (vec![Field::new("wide", DataType::Int64, false)], vec![
                Absent,
            ]),
            (
                vec![
                    Field::new("left", DataType::Int8, false),
                    Field::new("right", DataType::Int8, false),
                ],
                vec![Inexact(usize::MAX), Inexact(usize::MAX)],
            ),
            (vec![Field::new("nullable", DataType::Int8, true)], vec![
                Absent,
            ]),
        ] {
            let schema = Schema::new(fields);
            let mut statistics = Statistics::new_unknown(&schema).with_num_rows(Exact(usize::MAX));
            output_bytes(&mut statistics, &schema);
            assert_eq!(byte_sizes(&statistics), (Absent, expected));
        }
    }
}
