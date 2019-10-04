use diesel::deserialize::{self, FromSql, FromSqlRow, Queryable};
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::row::Row;
use diesel::*;
use diesel::{QueryId, SqlType};
use std::ops::Index;

#[derive(Debug, Clone, Copy, Default, QueryId, SqlType)]
#[postgres(oid = "0", array_oid = "0")]
pub struct Any;

#[derive(Debug)]
pub struct DynamicRow<I> {
    values: Vec<I>,
}

#[derive(Debug, PartialEq)]
pub struct NamedField<I> {
    pub name: String,
    pub value: I,
}

impl<I> DynamicRow<I> {
    fn get(&self, index: usize) -> Option<&I> {
        self.values.get(index)
    }
}

impl<I> DynamicRow<NamedField<I>> {
    fn get_by_name<S: AsRef<str>>(&self, name: S) -> Option<&I> {
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
        Ok(DynamicRow {
            values: (0..row.column_count())
                .map(|_| I::from_sql(row.take()))
                .collect::<deserialize::Result<_>>()?,
        })
    }
}

#[cfg(feature = "postgres")]
impl<I> Queryable<Any, diesel::pg::Pg> for DynamicRow<I>
where
    Self: FromSqlRow<Any, diesel::pg::Pg>,
{
    type Row = DynamicRow<I>;

    fn build(row: Self::Row) -> Self {
        row
    }
}

#[cfg(feature = "postgres")]
impl<I> FromSqlRow<Any, diesel::pg::Pg> for DynamicRow<NamedField<I>>
where
    I: FromSql<Any, diesel::pg::Pg>,
{
    const FIELDS_NEEDED: usize = 1;

    fn build_from_row<T: Row<diesel::pg::Pg>>(row: &mut T) -> deserialize::Result<Self> {
        Ok(DynamicRow {
            values: (0..row.column_count())
                .map(|_| {
                    let name = row.column_name().into();
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
