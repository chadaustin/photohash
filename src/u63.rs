use rusqlite::types::FromSql;
use rusqlite::types::FromSqlError;
use rusqlite::types::FromSqlResult;
use rusqlite::types::ToSql;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::Value;
use rusqlite::types::ValueRef;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct U63(i64);

impl U63 {
    pub const MAX: Self = Self(i64::MAX);
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> u64 {
        self.0 as u64
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct U63OutOfRange {
    value: u128,
}

impl fmt::Display for U63OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} is outside the u63 range", self.value)
    }
}

impl std::error::Error for U63OutOfRange {}

impl fmt::Display for U63 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<u64> for U63 {
    type Error = U63OutOfRange;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        i64::try_from(value).map(Self).map_err(|_| U63OutOfRange {
            value: value.into(),
        })
    }
}

impl TryFrom<usize> for U63 {
    type Error = U63OutOfRange;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        i64::try_from(value).map(Self).map_err(|_| U63OutOfRange {
            value: value as u128,
        })
    }
}

impl From<u32> for U63 {
    fn from(value: u32) -> Self {
        Self(value.into())
    }
}

impl From<U63> for u64 {
    fn from(value: U63) -> Self {
        value.get()
    }
}

impl serde::Serialize for U63 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.get())
    }
}

impl FromSql for U63 {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let value = i64::column_result(value)?;
        if value >= 0 {
            Ok(Self(value))
        } else {
            Err(FromSqlError::OutOfRange(value))
        }
    }
}

impl ToSql for U63 {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use rusqlite::Error;

    #[test]
    fn try_from_u64_accepts_u63_values() {
        assert_eq!(0, U63::try_from(0_u64).unwrap().get());
        assert_eq!(U63::MAX, U63::try_from(U63::MAX.get()).unwrap());
    }

    #[test]
    fn try_from_u64_rejects_values_above_i64_max() {
        assert!(U63::try_from(U63::MAX.get() + 1).is_err());
        assert!(U63::try_from(u64::MAX).is_err());
    }

    #[test]
    fn try_from_usize_accepts_u63_values() {
        assert_eq!(0, U63::try_from(0_usize).unwrap().get());

        if let Ok(max) = usize::try_from(U63::MAX.get()) {
            assert_eq!(U63::MAX, U63::try_from(max).unwrap());
        }
    }

    #[test]
    fn try_from_usize_rejects_values_above_i64_max_when_representable() {
        if let Ok(too_large) = usize::try_from(U63::MAX.get() + 1) {
            assert!(U63::try_from(too_large).is_err());
        }
    }

    #[test]
    fn displays_inner_value() {
        assert_eq!("123", U63::from(123_u32).to_string());
    }

    #[test]
    fn serializes_as_json_number() {
        assert_eq!("123", serde_json::to_string(&U63::from(123_u32)).unwrap());
    }

    #[test]
    fn stores_as_sqlite_integer() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        let value = U63::MAX;
        let stored: i64 = conn.query_row("SELECT ?", (value,), |row| row.get(0))?;

        assert_eq!(i64::MAX, stored);
        Ok(())
    }

    #[test]
    fn loads_from_non_negative_sqlite_integer() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        let loaded: U63 = conn.query_row("SELECT ?", (i64::MAX,), |row| row.get(0))?;

        assert_eq!(U63::MAX, loaded);
        Ok(())
    }

    #[test]
    fn rejects_negative_sqlite_integer() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        let error = conn
            .query_row("SELECT -1", (), |row| row.get::<_, U63>(0))
            .unwrap_err();

        assert!(matches!(error, Error::IntegralValueOutOfRange(0, -1)));
        Ok(())
    }
}
