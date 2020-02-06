use diesel::deserialize::{self, FromSql, FromSqlRow, Queryable, QueryableByName};
use diesel::row::{NamedRow, Row};
use diesel::{backend::Backend, QueryId, SqlType};
use std::iter::FromIterator;
use std::ops::Index;

#[derive(Debug, Clone, Copy, Default, QueryId, SqlType)]
#[postgres(oid = "0", array_oid = "0")]
#[sqlite_type = "Integer"]
pub struct Any;

#[cfg(feature = "mysql")]
impl diesel::sql_types::HasSqlType<Any> for diesel::mysql::Mysql {
    fn metadata(_lookup: &Self::MetadataLookup) -> Self::TypeMetadata {
        None
    }
}

#[derive(Debug, Clone)]
pub struct DynamicRow<I> {
    values: Vec<I>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedField<I> {
    pub name: String,
    pub value: I,
}

impl<I> FromIterator<I> for DynamicRow<I> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = I>,
    {
        DynamicRow {
            values: iter.into_iter().collect(),
        }
    }
}

impl<I> DynamicRow<I> {
    pub fn get(&self, index: usize) -> Option<&I> {
        self.values.get(index)
    }
}

impl<I> DynamicRow<NamedField<I>> {
    pub fn get_by_name<S: AsRef<str>>(&self, name: S) -> Option<&I> {
        self.values
            .iter()
            .find(|f| f.name == name.as_ref())
            .map(|f| &f.value)
    }
}

#[cfg(feature = "postgres")]
impl<I> FromSqlRow<Any, diesel::pg::Pg> for DynamicRow<I>
where
    I: FromSql<Any, diesel::pg::Pg>,
{
    const FIELDS_NEEDED: usize = 1;

    fn build_from_row<T: Row<diesel::pg::Pg>>(row: &mut T) -> deserialize::Result<Self> {
        (0..row.column_count())
            .map(|_| I::from_sql(row.take()))
            .collect::<deserialize::Result<_>>()
    }
}

#[cfg(feature = "sqlite")]
impl<I> FromSqlRow<Any, diesel::sqlite::Sqlite> for DynamicRow<I>
where
    I: FromSql<Any, diesel::sqlite::Sqlite>,
{
    const FIELDS_NEEDED: usize = 1;

    fn build_from_row<T: Row<diesel::sqlite::Sqlite>>(row: &mut T) -> deserialize::Result<Self> {
        (0..row.column_count())
            .map(|_| I::from_sql(row.take()))
            .collect::<deserialize::Result<_>>()
    }
}

#[cfg(feature = "mysql")]
impl<I> FromSqlRow<Any, diesel::mysql::Mysql> for DynamicRow<I>
where
    I: FromSql<Any, diesel::mysql::Mysql>,
{
    const FIELDS_NEEDED: usize = 1;

    fn build_from_row<T: Row<diesel::mysql::Mysql>>(row: &mut T) -> deserialize::Result<Self> {
        (0..row.column_count())
            .map(|_| I::from_sql(row.take()))
            .collect::<deserialize::Result<_>>()
    }
}

impl<I, DB> Queryable<Any, DB> for DynamicRow<I>
where
    DB: Backend,
    Self: FromSqlRow<Any, DB>,
{
    type Row = DynamicRow<I>;

    fn build(row: Self::Row) -> Self {
        row
    }
}

impl<I, DB> QueryableByName<DB> for DynamicRow<NamedField<I>>
where
    DB: Backend,
    I: FromSql<Any, DB>,
{
    fn build<R: NamedRow<DB>>(row: &R) -> deserialize::Result<Self> {
        row.field_names()
            .into_iter()
            .map(|name| {
                Ok(NamedField {
                    name: name.to_owned(),
                    value: row.get::<Any, I>(name)?,
                })
            })
            .collect()
    }
}

#[cfg(feature = "postgres")]
impl<I> QueryableByName<diesel::pg::Pg> for DynamicRow<I>
where
    I: FromSql<Any, diesel::pg::Pg>,
{
    fn build<R: NamedRow<diesel::pg::Pg>>(row: &R) -> deserialize::Result<Self> {
        row.field_names()
            .into_iter()
            .map(|name| row.get::<Any, I>(name))
            .collect()
    }
}

#[cfg(feature = "sqlite")]
impl<I> QueryableByName<diesel::sqlite::Sqlite> for DynamicRow<I>
where
    I: FromSql<Any, diesel::sqlite::Sqlite>,
{
    fn build<R: NamedRow<diesel::sqlite::Sqlite>>(row: &R) -> deserialize::Result<Self> {
        row.field_names()
            .into_iter()
            .map(|name| row.get::<Any, I>(name))
            .collect()
    }
}

#[cfg(feature = "mysql")]
impl<I> QueryableByName<diesel::mysql::Mysql> for DynamicRow<I>
where
    I: FromSql<Any, diesel::mysql::Mysql>,
{
    fn build<R: NamedRow<diesel::mysql::Mysql>>(row: &R) -> deserialize::Result<Self> {
        row.field_names()
            .into_iter()
            .map(|name| row.get::<Any, I>(name))
            .collect()
    }
}

impl<I, DB> FromSqlRow<Any, DB> for DynamicRow<NamedField<I>>
where
    DB: Backend,
    I: FromSql<Any, DB>,
{
    const FIELDS_NEEDED: usize = 1;

    fn build_from_row<T: Row<DB>>(row: &mut T) -> deserialize::Result<Self> {
        Ok(DynamicRow {
            values: (0..row.column_count())
                .map(|_| {
                    let name = row
                        .column_name()
                        .ok_or_else(|| "Request name for an unnamed column")?
                        .into();
                    Ok(NamedField {
                        name,
                        value: I::from_sql(row.take())?,
                    })
                })
                .collect::<deserialize::Result<_>>()?,
        })
    }
}

impl<I> Index<usize> for DynamicRow<I> {
    type Output = I;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl<'a, I> Index<&'a str> for DynamicRow<NamedField<I>> {
    type Output = I;

    fn index(&self, field_name: &'a str) -> &Self::Output {
        self.values
            .iter()
            .find(|f| f.name == field_name)
            .map(|f| &f.value)
            .expect("Field not found")
    }
}

impl<V> IntoIterator for DynamicRow<V> {
    type Item = V;
    type IntoIter = <Vec<V> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<'a, V> IntoIterator for &'a DynamicRow<V> {
    type Item = &'a V;
    type IntoIter = <&'a Vec<V> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&self.values).into_iter()
    }
}
