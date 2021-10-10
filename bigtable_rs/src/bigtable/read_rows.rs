use crate::bigtable::{Error, Result, RowCell, RowKey};
use crate::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use crate::google::bigtable::v2::ReadRowsResponse;
use log::{trace, warn};
use std::time::{Duration, Instant};
use tonic::Streaming;

/// As each `CellChunk` could be only part of a cell, this method reorganize multiple `CellChunk`
/// from multiple `ReadRowsResponse` into a `Vec<(RowKey, Vec<RowCell>)>`.
pub async fn decode_read_rows_response(
    timeout: &Option<Duration>,
    mut rrr: Streaming<ReadRowsResponse>,
) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
    let mut rows: Vec<(RowKey, Vec<RowCell>)> = vec![];

    let mut row_key = None;
    let mut row_data: Vec<RowCell> = vec![];

    let mut cell_family_name = None;
    let mut cell_name = None;
    let mut cell_timestamp = 0;
    let mut cell_value = vec![];

    let started = Instant::now();

    while let Some(res) = rrr.message().await? {
        if let Some(timeout) = timeout.as_ref() {
            if Instant::now().duration_since(started) > *timeout {
                return Err(Error::TimeoutError(timeout.as_secs()));
            }
        }
        for (i, mut chunk) in res.chunks.into_iter().enumerate() {
            // The comments for `read_rows_response::CellChunk` provide essential details for
            // understanding how the below decoding works...
            trace!("chunk {}: {:?}", i, chunk.value);

            // Starting a new row?
            if !chunk.row_key.is_empty() {
                row_key = Some(chunk.row_key);
            }

            // Starting a new cell? A new cell will have a qualifier and a family
            if let Some(chunk_qualifier) = chunk.qualifier {
                // New cell begins. Check whether previous cell_name exist, if so then it means
                // the cell_value is not empty and previous cell is not closed up. So close up the previous cell.
                if let Some(cell_name) = cell_name {
                    let row_cell = RowCell {
                        family_name: cell_family_name.take().unwrap_or("".to_owned()),
                        qualifier: cell_name,
                        value: cell_value,
                        timestamp_micros: cell_timestamp,
                    };
                    row_data.push(row_cell);
                    cell_value = vec![];
                }
                cell_name = Some(chunk_qualifier);
                cell_family_name = chunk.family_name;
                cell_timestamp = chunk.timestamp_micros;
            }
            cell_value.append(&mut chunk.value);

            // End of a row?
            match chunk.row_status {
                None => {
                    // more for this row, don't push to row_data or rows vector, let the next
                    // chunk close up those vectors.
                }
                Some(RowStatus::CommitRow(_)) => {
                    // End of a row, closing up the cell, then close this row
                    if let Some(cell_name) = cell_name.take() {
                        let row_cell = RowCell {
                            family_name: cell_family_name.take().unwrap_or("".to_owned()),
                            qualifier: cell_name,
                            value: cell_value,
                            timestamp_micros: cell_timestamp,
                        };
                        row_data.push(row_cell);
                        cell_value = vec![];
                    } else {
                        warn!("Row ended with cell_name=None. This should not happen.")
                    }

                    if let Some(row_key) = row_key.take() {
                        rows.push((row_key, row_data));
                        row_data = vec![];
                    }
                }
                Some(RowStatus::ResetRow(_)) => {
                    // ResetRow indicates that the client should drop all previous chunks for
                    // `row_key`, as it will be re-read from the beginning.
                    row_key = None;
                    row_data = vec![];
                }
            }
        }
    }
    Ok(rows)
}
