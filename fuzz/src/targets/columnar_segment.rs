use std::sync::Arc;

use nodedb_columnar::memtable::ColumnData;
use nodedb_columnar::writer::PROFILE_PLAIN;
use nodedb_columnar::{
    DeleteBitmap, OwnedSegmentReader, ScanPredicate, SegmentReader, SegmentWriter,
};
use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::{DatabaseId, TenantId};
use nodedb_wal::crypto::WalEncryptionKey;

const INPUT_PREFIX_BYTES: usize = 64;
const MAX_SEGMENT_INPUT_BYTES: usize = 1024 * 1024;
const MAX_GENERATED_SEGMENT_BYTES: usize = 256 * 1024;
const MAX_MUTATION_PREFIX_BYTES: usize = 64 * 1024;
const MAX_TRUNCATION_BYTES: usize = 64;
const ROW_COUNT: usize = 1_025;
const MAX_STRING_BYTES: usize = 5;
const MAX_BINARY_VALUE_BYTES: usize = 16;
const COLUMN_COUNT: usize = 5;
const FUZZ_KEK_BYTES: [u8; 32] = [0x5a; 32];
const FUZZ_KEK_EPOCH: [u8; 4] = [0; 4];

/// A memory scope whose governor ceiling covers every engine's limit — the
/// same shape the integration tests use. Segment writes now take mandatory
/// ScopedMemory (refactor(mem): make ScopedMemory budgeting mandatory).
fn fuzz_memory() -> ScopedMemory {
    let per_engine = usize::MAX / EngineId::ALL.len();
    let governor = Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: per_engine * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(per_engine),
        })
        .expect("fuzz governor"),
    );
    ScopedMemory::new(
        governor,
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Columnar,
    )
}

fn exercise_reader(reader: &SegmentReader<'_>) {
    let count = reader.column_count().min(COLUMN_COUNT);
    for index in 0..count {
        let _ = reader.read_column(index);
    }

    let predicates = [ScanPredicate::gt_i64(0, 2_302)];
    for index in 0..count {
        let _ = reader.read_column_filtered(index, &predicates);
    }
    let _ = reader.read_column_filtered(2, &[ScanPredicate::str_eq(2, "north")]);
    let _ = reader.read_column_filtered(3, &[ScanPredicate::str_eq(3, "north")]);
    let _ = reader.read_columns(&[0, 1, 2, 3, 4], &predicates);

    let mut deletes = DeleteBitmap::new();
    deletes.mark_deleted_batch(&[0, 5, (ROW_COUNT - 1) as u32]);
    for index in 0..count {
        let _ = reader.read_column_with_deletes(index, &predicates, &deletes);
    }
    let _ = reader.read_columns_with_deletes(&[0, 1, 2, 3, 4], &predicates, &deletes);
}

fn deterministic_kek() -> WalEncryptionKey {
    WalEncryptionKey::with_epoch(&FUZZ_KEK_BYTES, FUZZ_KEK_EPOCH)
}

fn exercise_owned_reader(segment: &[u8], kek: &WalEncryptionKey) {
    if let Ok(reader) = OwnedSegmentReader::open_with_kek(segment, None) {
        exercise_reader(&reader.reader());
    }
    if let Ok(reader) = OwnedSegmentReader::open_with_kek(segment, Some(kek)) {
        exercise_reader(&reader.reader());
    }
}

fn exercise_segment_paths(segment: &[u8], kek: &WalEncryptionKey) {
    if let Ok(reader) = SegmentReader::open(segment) {
        exercise_reader(&reader);
    }
    exercise_owned_reader(segment, kek);
}

fn exercise_generated_mutations(segment: &[u8], seed: &[u8], kek: &WalEncryptionKey) {
    if seed.is_empty() || segment.len() > MAX_GENERATED_SEGMENT_BYTES {
        return;
    }

    let mutation_prefix_len = segment.len().min(MAX_MUTATION_PREFIX_BYTES);
    if mutation_prefix_len == 0 {
        return;
    }

    let header_index = usize::from(seed[0]) % mutation_prefix_len.min(7);
    let header_xor = seed[1 % seed.len()] | 1;
    let mut header_variant = segment.to_vec();
    if let Some(byte) = header_variant.get_mut(header_index) {
        *byte ^= header_xor;
        exercise_segment_paths(&header_variant, kek);
    }

    let body_or_footer_index = if segment.len() <= MAX_MUTATION_PREFIX_BYTES {
        segment.len() - 1
    } else {
        7 + (usize::from(seed[2 % seed.len()]) % (mutation_prefix_len - 7))
    };
    let body_or_footer_xor = seed[3 % seed.len()] | 1;
    let mut body_or_footer_variant = segment.to_vec();
    if let Some(byte) = body_or_footer_variant.get_mut(body_or_footer_index) {
        *byte ^= body_or_footer_xor;
        exercise_segment_paths(&body_or_footer_variant, kek);
    }

    let max_truncation = segment.len().min(MAX_TRUNCATION_BYTES);
    let truncated_len = segment.len() - 1 - (usize::from(seed[4 % seed.len()]) % max_truncation);
    exercise_segment_paths(&segment[..truncated_len], kek);
}

fn generated_segment(seed: &[u8], kek: Option<&WalEncryptionKey>) -> Option<Vec<u8>> {
    if seed.is_empty() {
        return None;
    }

    let mut integers = Vec::with_capacity(ROW_COUNT);
    let mut floats = Vec::with_capacity(ROW_COUNT);
    let mut float_valid = Vec::with_capacity(ROW_COUNT);
    let mut string_data = Vec::with_capacity(ROW_COUNT * MAX_STRING_BYTES);
    let mut string_offsets = Vec::with_capacity(ROW_COUNT + 1);
    let mut string_valid = Vec::with_capacity(ROW_COUNT);
    let mut bytes_data = Vec::with_capacity(ROW_COUNT * MAX_BINARY_VALUE_BYTES);
    let mut bytes_offsets = Vec::with_capacity(ROW_COUNT + 1);
    let mut bytes_valid = Vec::with_capacity(ROW_COUNT);
    string_offsets.push(0);
    bytes_offsets.push(0);

    for row in 0..ROW_COUNT {
        let byte = seed[row % seed.len()];
        integers.push((row as i64) * 2 + i64::from(byte));
        floats.push((f64::from(byte) + row as f64) / 3.0);
        float_valid.push(row % 5 != 0);

        let label = match byte & 3 {
            0 => b"north".as_slice(),
            1 => b"south".as_slice(),
            2 => b"east".as_slice(),
            _ => b"west".as_slice(),
        };
        string_data.extend_from_slice(label);
        string_offsets.push(string_data.len() as u32);
        string_valid.push(row % 7 != 0);

        let value_len = 1 + usize::from(byte & 15);
        for offset in 0..value_len {
            bytes_data.push(seed[(row + offset) % seed.len()]);
        }
        bytes_offsets.push(bytes_data.len() as u32);
        bytes_valid.push(row % 3 != 0);
    }

    let string_column = ColumnData::String {
        data: string_data,
        offsets: string_offsets,
        valid: Some(string_valid),
    };
    let dictionary_column = ColumnData::try_dict_encode(&string_column, 4)?;
    let schema = ColumnarSchema {
        columns: vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::nullable("metric", ColumnType::Float64),
            ColumnDef::nullable("region", ColumnType::String),
            ColumnDef::nullable("region_dictionary", ColumnType::String),
            ColumnDef::nullable("payload", ColumnType::Bytes),
        ],
        version: 1,
    };
    let columns = [
        ColumnData::Int64 {
            values: integers,
            valid: None,
        },
        ColumnData::Float64 {
            values: floats,
            valid: Some(float_valid),
        },
        string_column,
        dictionary_column,
        ColumnData::Bytes {
            data: bytes_data,
            offsets: bytes_offsets,
            valid: Some(bytes_valid),
        },
    ];

    SegmentWriter::new(PROFILE_PLAIN, fuzz_memory())
        .write_segment(&schema, &columns, ROW_COUNT, kek)
        .ok()
}

pub fn run(data: &[u8]) {
    let bounded_segment = &data[..data.len().min(MAX_SEGMENT_INPUT_BYTES)];
    if let Ok(reader) = SegmentReader::open(bounded_segment) {
        exercise_reader(&reader);
    }

    let kek = deterministic_kek();
    exercise_owned_reader(bounded_segment, &kek);

    let seed = &data[..data.len().min(INPUT_PREFIX_BYTES)];
    if let Some(plaintext_segment) = generated_segment(seed, None) {
        exercise_segment_paths(&plaintext_segment, &kek);
        exercise_generated_mutations(&plaintext_segment, seed, &kek);
    }
    if let Some(encrypted_segment) = generated_segment(seed, Some(&kek)) {
        exercise_segment_paths(&encrypted_segment, &kek);
        exercise_generated_mutations(&encrypted_segment, seed, &kek);
    }
}
