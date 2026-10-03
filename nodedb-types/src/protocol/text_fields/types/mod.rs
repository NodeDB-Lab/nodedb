// SPDX-License-Identifier: Apache-2.0

//! [`TextFields`] struct definition and field-count helper.
//!
//! # Wire format
//!
//! TextFields is encoded as a MsgPack **map** whose keys are `u16` numeric
//! field IDs starting at 1. Fields whose value is `None` are **omitted**
//! entirely (compact encoding). The decoder ignores unknown keys, so new
//! fields can be added to newer servers without breaking older clients
//! (forward compatibility).
//!
//! # Field ID table
//!
//! ```text
//!  1  auth
//!  2  sql
//!  3  key
//!  4  value
//!  5  collection
//!  6  document_id
//!  7  data
//!  8  query_vector
//!  9  top_k
//! 10  field
//! 11  limit
//! 12  delta
//! 13  peer_id
//! 14  vector_top_k
//! 15  edge_labels
//! 16  direction
//! 17  expansion_depth
//! 18  final_top_k
//! 19  vector_k
//! 20  graph_k
//! 21  vector_field
//! 22  start_node
//! 23  end_node
//! 24  depth
//! 25  from_node
//! 26  to_node
//! 27  edge_type
//! 28  properties
//! 29  query_text
//! 30  vector_weight
//! 31  fuzzy
//! 32  ef_search
//! 33  field_name
//! 34  lower_bound
//! 35  upper_bound
//! 36  mutation_id
//! 37  vectors
//! 38  documents
//! 39  query_geometry
//! 40  spatial_predicate
//! 41  distance_meters
//! 42  payload
//! 43  format
//! 44  time_range_start
//! 45  time_range_end
//! 46  bucket_interval
//! 47  ttl_ms
//! 48  cursor
//! 49  match_pattern
//! 50  keys
//! 51  entries
//! 52  fields
//! 53  incr_delta
//! 54  incr_float_delta
//! 55  expected
//! 56  new_value
//! 57  index_name
//! 58  sort_columns
//! 59  key_column
//! 60  window_type
//! 61  window_timestamp_column
//! 62  window_start_ms
//! 63  window_end_ms
//! 64  top_k_count
//! 65  score_min
//! 66  score_max
//! 67  updates
//! 68  filters
//! 69  vector
//! 70  vector_id
//! 71  policy
//! 72  algorithm
//! 73  match_query
//! 74  algo_params
//! 75  index_paths
//! 76  source_collection
//! 77  field_position
//! 78  backfill
//! 79  m
//! 80  ef_construction
//! 81  metric
//! 82  index_type
//! 83  database
//! 84  sql_params
//! 85  list_path
//! 86  list_index
//! 87  list_from_index
//! 88  list_to_index
//! 89  list_fields_json
//! 90  vector_dim
//! 91  text_mode
//! 92  allowed_ids
//! ```

mod field_count;
mod text_fields;

pub use text_fields::TextFields;
