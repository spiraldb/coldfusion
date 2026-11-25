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

//! Query-local Parquet readers over shared, footer-described segments.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema_reader::Schema as ReaderSchema;
use arrow_schema_reader::ffi::FFI_ArrowSchema as ReaderFFIArrowSchema;
use datafusion::arrow::array::ffi::FFI_ArrowSchema;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use parquet_reader::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use vortex::error::{VortexResult, vortex_ensure, vortex_err};
use vortex::layout::segments::{SegmentId, SegmentSource};
use vortex::layout::{LayoutReaderRef, LayoutRef};
use vortex::scan::DataSourceRef;
use vortex::session::VortexSession;
use vortex_parquet::file::FileHandle;
use vortex_parquet::hierarchy::ParquetColumnChunk;
use vortex_parquet::layout::ParquetField;
use vortex_parquet::segments::SegmentTable;
use vortex_parquet::{ParquetFormat, file_metadata};

pub(super) struct ParquetFile {
    pub reader: LayoutReaderRef,
    pub source: DataSourceRef,
    pub schema: SchemaRef,
    layout: LayoutRef,
    segments: Arc<dyn SegmentSource>,
}

impl ParquetFile {
    pub async fn open(format: &ParquetFormat, handle: FileHandle) -> VortexResult<Self> {
        let opened = format.open_file(handle.clone()).await?;
        let mut ranges = BTreeMap::new();
        let mut context = None;
        for layout in opened.layout.depth_first_traversal() {
            let layout = layout?;
            if let Some(field) = layout.as_opt::<ParquetField>() {
                let data = field.data();
                context.get_or_insert_with(|| data.ctx.clone());
                let end = data
                    .byte_offset
                    .checked_add(data.byte_length)
                    .ok_or_else(|| vortex_err!("Parquet column byte range overflow"))?;
                let ids = field.segment_ids();
                vortex_ensure!(ids.len() == 1, "Expected one logical column segment");
                add_range(&mut ranges, ids[0], data.byte_offset..end)?;
                for index in &data.offset_indexes {
                    add_range(&mut ranges, index.id, index.range.clone())?;
                }
                for index in &data.column_indexes {
                    add_range(&mut ranges, index.id, index.range.clone())?;
                }
                for bloom in &data.bloom_filters {
                    add_range(&mut ranges, bloom.id, bloom.range.clone())?;
                }
            }
            if let Some(column) = layout.as_opt::<ParquetColumnChunk>() {
                let ids = column.segment_ids();
                vortex_ensure!(ids.len() == 1, "Expected one physical column segment");
                add_range(&mut ranges, ids[0], column.data().byte_range.clone())?;
            }
        }
        let (metadata, schema) = if let Some(context) = context {
            (
                context.metadata.clone(),
                import_schema(context.arrow_metadata.schema())?,
            )
        } else {
            // Files without column chunks have no layout-owned footer to reuse.
            let metadata = file_metadata(&handle).await?;
            let arrow =
                ArrowReaderMetadata::try_new(metadata.clone(), ArrowReaderOptions::default())
                    .map_err(|error| vortex_err!("Parquet schema conversion failed: {error}"))?;
            (metadata, import_schema(arrow.schema())?)
        };
        let source = format.open_data_source_with_metadata(handle.clone(), metadata)?;
        let mut table = SegmentTable::new();
        for (expected, range) in ranges {
            let actual = table.push(range)?;
            vortex_ensure!(
                usize::try_from(*actual)? == expected,
                "Noncontiguous Parquet segment IDs"
            );
        }
        Ok(Self {
            reader: opened.reader,
            source,
            schema: Arc::new(schema),
            layout: opened.layout,
            segments: handle.segment_source(table)?,
        })
    }

    pub fn new_reader(&self, session: &VortexSession) -> VortexResult<LayoutReaderRef> {
        self.layout.new_reader(
            "parquet".into(),
            self.segments.clone(),
            session,
            &Default::default(),
        )
    }
}

fn add_range(
    ranges: &mut BTreeMap<usize, Range<u64>>,
    id: SegmentId,
    range: Range<u64>,
) -> VortexResult<()> {
    if let Some(previous) = ranges.insert(usize::try_from(*id)?, range.clone()) {
        vortex_ensure!(previous == range, "Conflicting Parquet segment ranges");
    }
    Ok(())
}

fn import_schema(schema: &ReaderSchema) -> VortexResult<Schema> {
    let mut schema = ReaderFFIArrowSchema::try_from(schema)
        .map_err(|error| vortex_err!("Arrow schema export failed: {error}"))?;
    // SAFETY: both Arrow versions implement the stable ArrowSchema ABI. This moves
    // ownership and clears the source's release callback, retaining all field metadata.
    let schema = unsafe { FFI_ArrowSchema::from_raw((&raw mut schema).cast()) };
    Ok(Schema::try_from(&schema)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::fs::File;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use vortex::VortexSessionDefault;
    use vortex::session::VortexSession;
    use vortex_parquet::ParquetFormat;
    use vortex_parquet::file::{file_handle, local_filesystem};

    use super::ParquetFile;

    #[tokio::test]
    async fn empty_parquet_preserves_footer_field_metadata() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("empty.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_owned(),
                "7".to_owned(),
            )])),
        ]));
        ArrowWriter::try_new(File::create(&path)?, schema.clone(), None)?.close()?;
        let session = VortexSession::default();
        let fs = local_filesystem(&session);
        let handle = file_handle(path.to_str().unwrap(), fs.as_ref(), &session).await?;
        let file = ParquetFile::open(&ParquetFormat::new(), handle).await?;
        assert_eq!(file.schema, schema);
        assert_eq!(file.new_reader(&session)?.row_count(), 0);
        Ok(())
    }
}
