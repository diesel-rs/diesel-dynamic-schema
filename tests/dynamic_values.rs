use diesel::deserialize::*;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;
use diesel_dynamic_schema::dynamic_select::DynamicSelectClause;
use diesel_dynamic_schema::dynamic_value::*;

#[derive(PartialEq, Debug)]
enum MyDynamicValue {
    String(String),
    Integer(i32),
    Null,
}

#[cfg(feature = "postgres")]
impl FromSql<Any, diesel::pg::Pg> for MyDynamicValue {
    fn from_sql(value: Option<diesel::pg::PgValue>) -> Result<Self> {
        use diesel::pg::Pg;
        use std::num::NonZeroU32;

        const VARCHAR_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1043) };
        const TEXT_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(25) };
        const INTEGER_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(23) };

        if let Some(value) = value {
            match value.get_oid() {
                VARCHAR_OID | TEXT_OID => {
                    <String as FromSql<diesel::sql_types::Text, Pg>>::from_sql(Some(value))
                        .map(MyDynamicValue::String)
                }
                INTEGER_OID => {
                    <i32 as FromSql<diesel::sql_types::Integer, Pg>>::from_sql(Some(value))
                        .map(MyDynamicValue::Integer)
                }
                e => Err(format!("Unknown type: {}", e).into()),
            }
        } else {
            Ok(MyDynamicValue::Null)
        }
    }
}

#[cfg(feature = "sqlite")]
impl FromSql<Any, diesel::sqlite::Sqlite> for MyDynamicValue {
    fn from_sql(value: Option<&diesel::sqlite::SqliteValue>) -> Result<Self> {
        use diesel::sqlite::{Sqlite, SqliteType};
        if let Some(value) = value {
            match value.value_type() {
                Some(SqliteType::Text) => {
                    <String as FromSql<diesel::sql_types::Text, Sqlite>>::from_sql(Some(value))
                        .map(MyDynamicValue::String)
                }
                Some(SqliteType::Long) => {
                    <i32 as FromSql<diesel::sql_types::Integer, Sqlite>>::from_sql(Some(value))
                        .map(MyDynamicValue::Integer)
                }
                _ => Err("Unknown data type".into()),
            }
        } else {
            Ok(MyDynamicValue::Null)
        }
    }
}

#[cfg(feature = "mysql")]
impl FromSql<Any, diesel::mysql::Mysql> for MyDynamicValue {
    fn from_sql(value: Option<diesel::mysql::MysqlValue>) -> Result<Self> {
        use diesel::mysql::{Mysql, MysqlType, MysqlTypeMetadata};
        if let Some(value) = value {
            match value.value_type() {
                MysqlTypeMetadata {
                    data_type: MysqlType::String,
                    ..
                }
                | MysqlTypeMetadata {
                    data_type: MysqlType::Blob,
                    ..
                } => <String as FromSql<diesel::sql_types::Text, Mysql>>::from_sql(Some(value))
                    .map(MyDynamicValue::String),
                MysqlTypeMetadata {
                    data_type: MysqlType::Long,
                    is_unsigned: false,
                } => <i32 as FromSql<diesel::sql_types::Integer, Mysql>>::from_sql(Some(value))
                    .map(MyDynamicValue::Integer),
                e => Err(format!("Unknown data type: {:?}", e).into()),
            }
        } else {
            Ok(MyDynamicValue::Null)
        }
    }
}

#[test]
fn dynamic_query() {
    let connection = super::establish_connection();
    super::create_user_table(&connection);
    sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
        .execute(&connection)
        .unwrap();

    let users = diesel_dynamic_schema::table("users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");
    let hair_color = users.column::<Nullable<Text>, _>("hair_color");

    let mut select = DynamicSelectClause::new();

    select.add_field(id);
    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<NamedField<MyDynamicValue>>> =
        users.select(select).load(&connection).unwrap();

    assert_eq!(
        actual_data[0]["name"],
        MyDynamicValue::String("Sean".into())
    );
    assert_eq!(
        actual_data[0][1],
        NamedField {
            name: "name".into(),
            value: MyDynamicValue::String("Sean".into())
        }
    );
    assert_eq!(
        actual_data[1]["name"],
        MyDynamicValue::String("Tess".into())
    );
    assert_eq!(
        actual_data[1][1],
        NamedField {
            name: "name".into(),
            value: MyDynamicValue::String("Tess".into())
        }
    );
    assert_eq!(actual_data[0]["hair_color"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[0][2],
        NamedField {
            name: "hair_color".into(),
            value: MyDynamicValue::Null
        }
    );
    assert_eq!(actual_data[1]["hair_color"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[1][2],
        NamedField {
            name: "hair_color".into(),
            value: MyDynamicValue::Null
        }
    );

    let mut select = DynamicSelectClause::new();

    select.add_field(id);
    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<MyDynamicValue>> =
        users.select(select).load(&connection).unwrap();

    assert_eq!(actual_data[0][1], MyDynamicValue::String("Sean".into()));
    assert_eq!(actual_data[1][1], MyDynamicValue::String("Tess".into()));
    assert_eq!(actual_data[0][2], MyDynamicValue::Null);
    assert_eq!(actual_data[1][2], MyDynamicValue::Null);
}

#[test]
fn dynamic_query_2() {
    let connection = super::establish_connection();
    super::create_user_table(&connection);
    sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
        .execute(&connection)
        .unwrap();

    let actual_data: Vec<DynamicRow<NamedField<MyDynamicValue>>> =
        sql_query("SELECT id, name, hair_color FROM users")
            .load(&connection)
        .unwrap();

    dbg!(&actual_data);

    assert_eq!(
        actual_data[0]["name"],
        MyDynamicValue::String("Sean".into())
    );
    assert_eq!(
        actual_data[0][1],
        NamedField {
            name: "name".into(),
            value: MyDynamicValue::String("Sean".into())
        }
    );
    assert_eq!(
        actual_data[1]["name"],
        MyDynamicValue::String("Tess".into())
    );
    assert_eq!(
        actual_data[1][1],
        NamedField {
            name: "name".into(),
            value: MyDynamicValue::String("Tess".into())
        }
    );
    assert_eq!(actual_data[0]["hair_color"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[0][2],
        NamedField {
            name: "hair_color".into(),
            value: MyDynamicValue::Null
        }
    );
    assert_eq!(actual_data[1]["hair_color"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[1][2],
        NamedField {
            name: "hair_color".into(),
            value: MyDynamicValue::Null
        }
    );

    let actual_data: Vec<DynamicRow<MyDynamicValue>> =
        sql_query("SELECT id, name, hair_color FROM users")
            .load(&connection)
            .unwrap();

    assert_eq!(actual_data[0][1], MyDynamicValue::String("Sean".into()));
    assert_eq!(actual_data[1][1], MyDynamicValue::String("Tess".into()));
    assert_eq!(actual_data[0][2], MyDynamicValue::Null);
    assert_eq!(actual_data[1][2], MyDynamicValue::Null);
}
