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

//! Shared Iceberg storage handles for format readers.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use futures::FutureExt;
use futures::future::BoxFuture;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{FileIO, FileRead};
use iceberg::spec::{DataFileFormat, Schema};
use iceberg::{Error, ErrorKind, Result, Runtime};
use moka::future::Cache;
use vortex::VortexSessionDefault;
use vortex::array::buffer::BufferHandle;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::error::{VortexResult, vortex_err};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::VortexReadAt;
use vortex::layout::LayoutReaderRef;
use vortex::scan::DataSourceRef;
use vortex::session::VortexSession;
use vortex_parquet::file::FileHandle;
use vortex_parquet::{OpenedParquetFile, ParquetFormat};

use super::to_iceberg_error;

#[derive(Clone)]
pub(crate) struct OpenedFile {
    pub reader: LayoutReaderRef,
    pub source: DataSourceRef,
    pub schema: SchemaRef,
    format: OpenedFormat,
}

#[derive(Clone)]
enum OpenedFormat {
    Vortex(Arc<VortexFile>),
    Parquet(OpenedParquetFile),
}
impl OpenedFile {
    pub(super) fn for_scan(&self, session: &VortexSession) -> Result<Arc<Self>> {
        let reader = match &self.format {
            OpenedFormat::Vortex(file) => file.layout_reader(),
            OpenedFormat::Parquet(file) => file.new_reader(session),
        }
        .map_err(to_iceberg_error)?;
        Ok(Arc::new(Self {
            reader,
            ..self.clone()
        }))
    }
}

#[derive(Clone)]
pub(crate) struct FileCache {
    io: FileIO,
    runtime: Runtime,
    session: VortexSession,
    parquet: ParquetFormat,
    files: Cache<(String, u64, String), Arc<OpenedFile>>,
}
impl fmt::Debug for FileCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCache")
            .field("files", &self.files.entry_count())
            .finish()
    }
}
impl FileCache {
    pub(crate) async fn new(io: FileIO, runtime: Runtime) -> Result<Self> {
        let session = runtime
            .io()
            .spawn(async { VortexSession::default() })
            .await?;
        Ok(Self {
            io,
            runtime,
            session,
            parquet: ParquetFormat::new(),
            files: Cache::new(128),
        })
    }
    pub(crate) fn session(&self) -> &VortexSession {
        &self.session
    }
    pub(crate) async fn open(
        &self,
        path: &str,
        size: u64,
        format: DataFileFormat,
    ) -> Result<Arc<OpenedFile>> {
        if !matches!(format, DataFileFormat::Vortex | DataFileFormat::Parquet) {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported Iceberg data format {format}"),
            ));
        }
        self.files
            .try_get_with((path.to_owned(), size, format.to_string()), async {
                let path = path.to_owned();
                let io = self.io.clone();
                let session = self.session.clone();
                let parquet = self.parquet.clone();
                self.runtime
                    .io()
                    .spawn(async move {
                        let input = io.new_input(&path)?;
                        let size = if size == 0 {
                            input.metadata().await?.size
                        } else {
                            size
                        };
                        let read = Arc::new(StorageRead {
                            reader: Arc::from(input.reader().await?),
                            size,
                            uri: Arc::from(path),
                        });
                        let opened = match format {
                            DataFileFormat::Parquet => {
                                let file = parquet
                                    .open_file(FileHandle::new(read, size, session.clone()))
                                    .await
                                    .map_err(to_iceberg_error)?;
                                OpenedFile {
                                    schema: file.arrow_schema().clone(),
                                    reader: file.reader.clone(),
                                    source: file.data_source(session).map_err(to_iceberg_error)?,
                                    format: OpenedFormat::Parquet(file),
                                }
                            }
                            DataFileFormat::Vortex => {
                                let file = session
                                    .open_options()
                                    .with_file_size(size)
                                    .with_include_metadata(true)
                                    .open(read)
                                    .await
                                    .map_err(to_iceberg_error)?;
                                let schema = match file.metadata_segment("iceberg.schema") {
                                    Some(bytes) => Arc::new(schema_to_arrow_schema(
                                        &serde_json::from_slice::<Schema>(bytes.as_slice())?,
                                    )?),
                                    None => Arc::new(
                                        session
                                            .arrow()
                                            .to_arrow_schema(file.dtype())
                                            .map_err(to_iceberg_error)?,
                                    ),
                                };
                                OpenedFile {
                                    reader: file.layout_reader().map_err(to_iceberg_error)?,
                                    source: file.data_source().map_err(to_iceberg_error)?,
                                    format: OpenedFormat::Vortex(Arc::new(file)),
                                    schema,
                                }
                            }
                            _ => unreachable!(),
                        };
                        Ok::<_, Error>(Arc::new(opened))
                    })
                    .await?
            })
            .await
            .map_err(|error| {
                Error::new(error.kind(), "Cannot open Iceberg data file").with_source(error)
            })
    }
}

struct StorageRead {
    reader: Arc<dyn FileRead>,
    size: u64,
    uri: Arc<str>,
}
impl VortexReadAt for StorageRead {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }
    fn concurrency(&self) -> usize {
        32
    }
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        futures::future::ready(Ok(self.size)).boxed()
    }
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let reader = Arc::clone(&self.reader);
        async move {
            let end = offset
                .checked_add(length as u64)
                .ok_or_else(|| vortex_err!("File range overflow"))?;
            let bytes = reader
                .read(Range { start: offset, end })
                .await
                .map_err(|error| vortex_err!("Iceberg storage read failed: {error}"))?;
            if bytes.len() != length {
                return Err(vortex_err!("Short read from Iceberg storage"));
            }
            Ok(BufferHandle::new_host(
                ByteBuffer::from(bytes).aligned(alignment),
            ))
        }
        .boxed()
    }
}
