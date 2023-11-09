use crate::{CollectError, Datatype, Dim, MetaDatatype, Partition, Table, FileFormat, ParseError, U256Type, ColumnEncoding, BlockChunk, cluster_datatypes};
use std::collections::{HashMap, HashSet};

/// Query
#[derive(Clone)]
pub struct Query {
    /// MetaDatatype
    pub datatypes: Vec<MetaDatatype>,
    /// Schemas for each subdatatype
    pub schemas: HashMap<Datatype, Table>,
    /// Time dimension
    pub time_dimension: TimeDimension,
    /// MetaChunks
    pub partitions: Vec<Partition>,
    /// Partitioning
    pub partitioned_by: Vec<Dim>,
    /// Exclude failed
    pub exclude_failed: bool,
    /// Javascript tracer
    pub js_tracer: Option<String>,
    /// Labels (these are non-functional)
    pub labels: QueryLabels,
}

/// query labels (non-functional)
#[derive(Clone)]
pub struct QueryLabels {
    /// align
    pub align: bool,
    /// reorg buffer
    pub reorg_buffer: u64,
}

impl Query {
    /// total number of tasks needed to perform query
    pub fn n_tasks(&self) -> usize {
        self.datatypes.len() * self.partitions.len()
    }

    /// total number of outputs of query
    pub fn n_outputs(&self) -> usize {
        self.datatypes.iter().map(|x| x.datatypes().len()).sum::<usize>() * self.partitions.len()
    }

    /// check that query is valid
    pub fn is_valid(&self) -> Result<(), CollectError> {
        // check that required parameters are present
        let mut all_datatypes = std::collections::HashSet::new();
        for datatype in self.datatypes.iter() {
            all_datatypes.extend(datatype.datatypes())
        }
        let mut requirements: HashSet<Dim> = HashSet::new();
        for datatype in all_datatypes.iter() {
            for dim in datatype.required_parameters() {
                requirements.insert(dim);
            }
        }
        for partition in self.partitions.iter() {
            let partition_dims = partition.dims().into_iter().collect();
            if !requirements.is_subset(&partition_dims) {
                let missing: Vec<_> =
                    requirements.difference(&partition_dims).map(|x| x.to_string()).collect();
                return Err(CollectError::CollectError(format!(
                    "need to specify {}",
                    missing.join(", ")
                )))
            }
        }
        Ok(())
    }
}

/// Time dimension for queries
#[derive(Clone)]
pub enum TimeDimension {
    /// Blocks
    Blocks,
    /// Transactions
    Transactions,
}

/// Builder pattern for queries
pub struct QueryBuilder {
  block_numbers: Vec<u64>,
  output_format: FileFormat,
  align_chunk_boundaries: bool,
  exclude_failed: bool,
  js_tracer: Option<String>,
}

impl Default for QueryBuilder {
  fn default() -> Self {
      QueryBuilder {
          block_numbers: vec![],
          output_format: FileFormat::Parquet,
          align_chunk_boundaries: false,
          exclude_failed: false,
          js_tracer: Some("tracer".to_string()),
      }
  }
}

impl QueryBuilder {

  /// Create new query builder
  pub fn new() -> Self {
      QueryBuilder {
          ..Default::default()
      }
  }

  /// Set block numbers
  pub fn block_numbers(mut self, block_numbers: Vec<u64>) -> Self {
      self.block_numbers = block_numbers;
      self
  }

  /// Set chunk boundaries
  pub fn align_chunk_boundaries(mut self, align_chunk_boundaries: bool) -> Self {
      self.align_chunk_boundaries = align_chunk_boundaries;
      self
  }

  /// Set exclude failed
  pub fn exclude_failed(mut self, exclude_failed: bool) -> Self {
      self.exclude_failed = exclude_failed;
      self
  }

  ///  Set js tracer
  pub fn output_format(mut self, output_format: FileFormat) -> Self {
      self.output_format = output_format;
      self
  }

  /// Build new query
  pub async fn build(self) -> Result<Query, ParseError> {

      let block_numbers = self.block_numbers;
      let align_chunk_boundaries = self.align_chunk_boundaries;
      let exclude_failed = self.exclude_failed;
      let js_tracer = self.js_tracer;

      // eq to parse_datatypes
      let mut datatypes = vec![Datatype::Blocks];

      // generate schemas
      let sort: HashMap<Datatype, Option<Vec<String>>> = HashMap::from_iter(
          datatypes
              .iter()
              .map(|datatype| (*datatype, Some(datatype.default_sort()))),
      );

      let u256_types =
          HashSet::from_iter(vec![U256Type::Binary, U256Type::String, U256Type::F64]);
      let binary_column_format = ColumnEncoding::Binary;
      let include_columns: Option<Vec<String>> = None;
      let exclude_columns: Option<Vec<String>> = None;
      let columns: Option<Vec<String>> = None;

      let schemas: Result<HashMap<Datatype, Table>, ParseError> = datatypes
          .iter()
          .map(|datatype| {
              datatype
                  .table_schema(
                      &u256_types,
                      &binary_column_format,
                      &include_columns,
                      &exclude_columns,
                      &columns,
                      sort[datatype].clone(),
                      None,
                  )
                  .map(|schema| (*datatype, schema))
                  .map_err(|e| {
                      ParseError::ParseError(format!(
                          "Failed to get schema for datatype: {:?}, {:?}",
                          datatype, e
                      ))
                  })
          })
          .collect();

      // generate partitions
      let time_dimension = TimeDimension::Blocks;

      let chunk = BlockChunk::Numbers(block_numbers);
      let chunk = Some(vec![chunk]);
      let partition = Partition {
          block_numbers: chunk,
          ..Default::default()
      };
      let partitions = vec![partition];
      let partitioned_by = vec![Dim::BlockNumber];

      // Reorg buffer, save blocks only when this old,
      // can be a number of blocks
      let reorg_buffer = 0;
      let labels = QueryLabels {
          align: align_chunk_boundaries,
          reorg_buffer,
      };

      let datatypes = cluster_datatypes(datatypes);

      let schemas = schemas.unwrap();

      let query = Query {
          datatypes,
          schemas: schemas,
          time_dimension,
          partitions,
          partitioned_by,
          exclude_failed,
          js_tracer,
          labels,
      };

      Ok(query)
  }
}