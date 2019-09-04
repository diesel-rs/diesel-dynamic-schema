//! Example: columns used in where clause
//!
//! To run this:
//!
//! ```sh
//! cargo run --example columns_used_in_where_clause --features="diesel/sqlite"
//! ```
extern crate diesel;
extern crate diesel_dynamic_schema;

use diesel::*;
use diesel::sql_types::{Integer, Text};
use diesel::sqlite::SqliteConnection;
use diesel_dynamic_schema::table;

fn main() {
    let conn = SqliteConnection::establish(":memory:").unwrap();

    // Create some example data by using typical SQL statements.
    sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)").execute(&conn).unwrap();
    sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')").execute(&conn).unwrap();

    // Use diesel-dynamic-schema to create a table and a column.
    let users = table("users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");

    // Use typical Diesel syntax to get some data.
    // This uses a filter on name equal to "Sean",
    // which generates a SQL `where` clause.
    let users = users
        .select((id, name))
        .filter(name.eq("Sean"))
        .load::<(i32, String)>(&conn);

    // Print the results.
    // The `users` are type `std::result::Result<std::vec::Vec<(i32, std::string::String)>, diesel::result::Error>`
    let users = users.unwrap();
    for user in users {
        let (x, y) = user;
        println!("user id:{} name:{}", x, y);
    }

}