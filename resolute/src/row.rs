//! Typed row abstraction over raw wire protocol DataRow.

use std::sync::Arc;

use pg_wired::protocol::types::RawRow;

use crate::decode::{Decode, DecodeText};
use crate::error::TypedError;

/// Per-result-set schema (column names, type OIDs, formats). Shared across
/// every `Row` in a result via `Arc` so we pay the metadata allocations once
/// per query rather than once per row.
#[derive(Debug)]
pub(crate) struct RowSchema {
    pub(crate) columns: Vec<String>,
    pub(crate) type_oids: Vec<u32>,
    pub(crate) formats: Vec<i16>,
}

impl RowSchema {
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
            type_oids: Vec::new(),
            formats: Vec::new(),
        }
    }
}

/// A row from a query result with typed column access.
#[derive(Debug, Clone)]
pub struct Row {
    pub(crate) schema: Arc<RowSchema>,
    /// Raw column data shared with the wire-protocol body. `RawRow::cell`
    /// returns `None` for SQL NULL.
    pub(crate) data: RawRow,
}

impl Row {
    /// Get a column value by index (binary decode).
    pub fn get<T: Decode + DecodeText>(&self, idx: usize) -> Result<T, TypedError> {
        let cell = self.data.try_cell(idx).ok_or(TypedError::Decode {
            column: idx,
            message: format!("column index {idx} out of range (have {})", self.data.len()),
        })?;

        let bytes = cell.ok_or(TypedError::UnexpectedNull(idx))?;

        let format = self.schema.formats.get(idx).copied().unwrap_or(0);
        if format == 1 {
            // Binary format.
            T::decode(bytes)
        } else {
            // Text format — parse the string.
            let s = std::str::from_utf8(bytes).map_err(|e| TypedError::Decode {
                column: idx,
                message: format!("invalid UTF-8: {e}"),
            })?;
            T::decode_text(s)
        }
    }

    /// Get a possibly-NULL column value by index.
    pub fn get_opt<T: Decode + DecodeText>(&self, idx: usize) -> Result<Option<T>, TypedError> {
        let cell = self.data.try_cell(idx).ok_or(TypedError::Decode {
            column: idx,
            message: format!("column index {idx} out of range"),
        })?;

        match cell {
            None => Ok(None),
            Some(bytes) => {
                let format = self.schema.formats.get(idx).copied().unwrap_or(0);
                if format == 1 {
                    Ok(Some(T::decode(bytes)?))
                } else {
                    let s = std::str::from_utf8(bytes).map_err(|e| TypedError::Decode {
                        column: idx,
                        message: format!("invalid UTF-8: {e}"),
                    })?;
                    Ok(Some(T::decode_text(s)?))
                }
            }
        }
    }

    /// Get a column value by name.
    pub fn get_by_name<T: Decode + DecodeText>(&self, name: &str) -> Result<T, TypedError> {
        let idx = self.column_index(name)?;
        self.get(idx)
    }

    /// Get a possibly-NULL column value by name.
    pub fn get_opt_by_name<T: Decode + DecodeText>(
        &self,
        name: &str,
    ) -> Result<Option<T>, TypedError> {
        let idx = self.column_index(name)?;
        self.get_opt(idx)
    }

    /// Check whether a column with the given name exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.schema.columns.iter().any(|c| c == name)
    }

    /// Look up column index by name.
    fn column_index(&self, name: &str) -> Result<usize, TypedError> {
        self.schema
            .columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| TypedError::ColumnNotFound(name.to_string()))
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether this row has zero columns.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the raw bytes for a column (None = NULL or out-of-range).
    pub fn raw(&self, idx: usize) -> Option<&[u8]> {
        self.data.cell(idx)
    }

    /// Get the column name at an index.
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.schema.columns.get(idx).map(|s| s.as_str())
    }

    /// Get the type OID for a column.
    pub fn column_type_oid(&self, idx: usize) -> Option<u32> {
        self.schema.type_oids.get(idx).copied()
    }
}

/// Trait for types that can be constructed from a Row.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self, TypedError>;
}
