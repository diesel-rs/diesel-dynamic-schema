use diesel::deserialize::*;
use diesel::pg::{Pg, PgValue};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;
use diesel_dynamic_schema::dynamic_select::DynamicSelectClause;
use diesel_dynamic_schema::dynamic_value::*;
use std::num::NonZeroU32;

#[derive(PartialEq, Debug)]
enum MyDynamicValue {
    String(String),
    Integer(i32),
    Unknown(Vec<u8>, NonZeroU32),
    Null,
}

impl FromSql<Any, Pg> for MyDynamicValue {
    fn from_sql(value: Option<PgValue>) -> Result<Self> {
        const VARCHAR_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1043) };
        const TEXT_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(25) };
        const INTEGER_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(16) };

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
                e => Ok(MyDynamicValue::Unknown(value.as_bytes().to_owned(), e)),
            }
        } else {
            Ok(MyDynamicValue::Null)
        }
    }
}

// #[test]
// fn dynamic_query1() {
//     use diesel::pg::types::any::*;
//     use schema::users::dsl::*;

//     let connection = connection();
//     connection
//         .execute("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
//         .unwrap();

//     let actual_data: Vec<DynamicRow<Option<OwnedPgValue>>> = users
//         .select(DynamicSelectClause::new(vec![
//             Box::new(name),
//             Box::new(hair_color),
//         ]))
//         .load(&connection)
//         .unwrap();

//     println!("{:?}", actual_data);
//     println!("{:?}", actual_data[0][0]);
//     println!("{:?}", actual_data[0][1]);
//     panic!();
// }

// #[test]
// fn dynamic_query2() {
//     use diesel::pg::types::any::*;
//     use schema::users::dsl::*;

//     let connection = connection();
//     connection
//         .execute("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
//         .unwrap();

//     let actual_data: Vec<DynamicRow<NamedField<Option<OwnedPgValue>>>> = users
//         .select(DynamicSelectClause::new(vec![
//             Box::new(name),
//             Box::new(hair_color),
//         ]))
//         .load(&connection)
//         .unwrap();

//     println!("{:?}", actual_data);
//     println!("{:?}", actual_data[0]["name"]);
//     println!("{:?}", actual_data[0][0]);
//     println!("{:?}", actual_data[0]["hair_color"]);
//     println!("{:?}", actual_data[0][1]);
//     panic!();
// }

#[test]
fn dynamic_query3() {
    let connection = super::establish_connection();
    sql_query(
        "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, hair_color TEXT)",
    )
    .execute(&connection)
    .unwrap();
    sql_query("INSERT INTO test_users (name) VALUES ('Sean'), ('Tess')")
        .execute(&connection)
        .unwrap();

    let users = diesel_dynamic_schema::table("test_users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");
    let hair_color = users.column::<Nullable<Text>, _>("hair_color");

    let mut select = DynamicSelectClause::new();

    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<NamedField<MyDynamicValue>>> =
        users.select(select).load(&connection).unwrap();

    assert_eq!(
        actual_data[0]["name"],
        MyDynamicValue::String("Sean".into())
    );
    assert_eq!(
        actual_data[0][0],
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
        actual_data[1][0],
        NamedField {
            name: "name".into(),
            value: MyDynamicValue::String("Tess".into())
        }
    );
    assert_eq!(actual_data[0]["hair_color"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[0][1],
        NamedField {
            name: "hair_color".into(),
            value: MyDynamicValue::Null
        }
    );
    assert_eq!(actual_data[1]["hair_color"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[1][1],
        NamedField {
            name: "hair_color".into(),
            value: MyDynamicValue::Null
        }
    );

    let mut select = DynamicSelectClause::new();

    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<MyDynamicValue>> =
        users.select(select).load(&connection).unwrap();

    assert_eq!(actual_data[0][0], MyDynamicValue::String("Sean".into()));
    assert_eq!(actual_data[1][0], MyDynamicValue::String("Tess".into()));
    assert_eq!(actual_data[0][1], MyDynamicValue::Null);
    assert_eq!(actual_data[1][1], MyDynamicValue::Null);
}
