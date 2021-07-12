extern crate diesel;
extern crate diesel_dynamic_schema;

use diesel::sql_types::*;
use diesel::*;
use diesel_dynamic_schema::table;

mod dynamic_values;

#[cfg(feature = "postgres")]
fn create_user_table(conn: &PgConnection) {
    sql_query("CREATE TABLE users (id Serial PRIMARY KEY, name TEXT NOT NULL DEFAULT '', hair_color TEXT)")
        .execute(conn)
        .unwrap();
}

#[cfg(feature = "sqlite")]
fn create_user_table(conn: &SqliteConnection) {
    sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL DEFAULT '', hair_color TEXT)")
        .execute(conn)
        .unwrap();
}

#[cfg(feature = "mysql")]
fn create_user_table(conn: &MysqlConnection) {
    sql_query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT NOT NULL DEFAULT '', hair_color TEXT)")
        .execute(conn)
        .unwrap();
    sql_query("DELETE FROM users").execute(conn).unwrap();
}

#[cfg(feature = "sqlite")]
fn establish_connection() -> SqliteConnection {
    SqliteConnection::establish(":memory:").unwrap()
}

#[cfg(feature = "postgres")]
fn establish_connection() -> PgConnection {
    let conn = PgConnection::establish(
        &dotenv::var("DATABASE_URL")
            .or_else(|_| dotenv::var("PG_DATABASE_URL"))
            .expect("Set either `DATABASE_URL` or `PG_DATABASE_URL`"),
    )
    .unwrap();

    conn.begin_test_transaction().unwrap();
    conn
}

#[cfg(feature = "mysql")]
fn establish_connection() -> MysqlConnection {
    let conn = MysqlConnection::establish(
        &dotenv::var("DATABASE_URL")
            .or_else(|_| dotenv::var("MYSQL_DATABASE_URL"))
            .expect("Set either `DATABASE_URL` or `MYSQL_DATABASE_URL`"),
    )
    .unwrap();

    conn.begin_test_transaction().unwrap();

    conn
}

#[test]
fn querying_basic_schemas() {
    let conn = establish_connection();
    create_user_table(&conn);
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
    create_user_table(&conn);
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
    create_user_table(&conn);
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
#[cfg(feature = "sqlite")]
fn providing_custom_schema_name() {
    use diesel_dynamic_schema::schema;
    let table = schema("information_schema").table("users");
    let sql = debug_query::<diesel::sqlite::Sqlite, _>(&table);
    assert_eq!("`information_schema`.`users` -- binds: []", sql.to_string());
}
