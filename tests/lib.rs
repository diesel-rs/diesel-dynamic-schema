extern crate diesel;
extern crate diesel_dynamic_schema;

use diesel::sql_types::*;
use diesel::sqlite::Sqlite;
use diesel::*;
use diesel_dynamic_schema::{schema, table};

#[test]
fn querying_basic_schemas() {
    let conn = establish_connection();
    sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        .execute(&conn)
        .unwrap();
    sql_query("INSERT INTO users DEFAULT VALUES")
        .execute(&conn)
        .unwrap();

    let users = table("users");
    let id = users.column::<Integer, _>("id");
    let ids = users.select(id).load::<i32>(&conn);
    assert_eq!(Ok(vec![1]), ids);
}

#[test]
fn querying_multiple_types() {
    let conn = establish_connection();
    sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
        .execute(&conn)
        .unwrap();
    sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
        .execute(&conn)
        .unwrap();

    let users = table("users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");
    let users = users.select((id, name)).load::<(i32, String)>(&conn);
    assert_eq!(Ok(vec![(1, "Sean".into()), (2, "Tess".into())]), users);
}

#[test]
fn columns_used_in_where_clause() {
    let conn = establish_connection();
    sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
        .execute(&conn)
        .unwrap();
    sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
        .execute(&conn)
        .unwrap();

    let users = table("users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");
    let users = users
        .select((id, name))
        .filter(name.eq("Sean"))
        .load::<(i32, String)>(&conn);
    assert_eq!(Ok(vec![(1, "Sean".into())]), users);
}

#[test]
fn providing_custom_schema_name() {
    let table = schema("information_schema").table("users");
    let sql = debug_query::<Sqlite, _>(&table);
    assert_eq!("`information_schema`.`users` -- binds: []", sql.to_string());
}

fn establish_connection() -> SqliteConnection {
    SqliteConnection::establish(":memory:").unwrap()
}

#[test]
fn display_for_columns_and_tables() {
    let users = table("users");
    let id = users.column::<Integer, _>("id");

    assert_eq!(format!("{}", users), "users");
    assert_eq!(format!("{}", id), "id");
}
