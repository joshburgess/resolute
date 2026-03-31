//! Custom PostgreSQL types: PgEnum, PgComposite, PgDomain.
//!
//! Run: cargo run -p stalwart --example custom_types

use stalwart::{Decode, Encode, PgComposite, PgDomain, PgEnum};

#[derive(Debug, PartialEq, PgEnum)]
#[pg_type(rename_all = "snake_case")]
enum Status {
    Active,
    Inactive,
    #[pg_type(rename = "pending-review")]
    PendingReview,
}

#[derive(Debug, PartialEq, PgComposite)]
struct Address {
    street: String,
    city: String,
    zip: Option<String>,
}

#[derive(Debug, PartialEq, PgDomain)]
struct Email(String);

fn main() {
    // Enum encode/decode
    let mut buf = bytes::BytesMut::new();
    Status::PendingReview.encode(&mut buf);
    println!("PendingReview encodes as: {:?}", std::str::from_utf8(&buf).unwrap());
    let decoded = Status::decode(&buf).unwrap();
    assert_eq!(decoded, Status::PendingReview);

    // Composite encode/decode
    let addr = Address {
        street: "123 Main St".into(),
        city: "Springfield".into(),
        zip: Some("62704".into()),
    };
    let mut buf = bytes::BytesMut::new();
    addr.encode(&mut buf);
    let decoded = Address::decode(&buf).unwrap();
    println!("Address roundtrip: {decoded:?}");
    assert_eq!(decoded, addr);

    // Domain encode/decode
    let email = Email("user@example.com".into());
    let mut buf = bytes::BytesMut::new();
    email.encode(&mut buf);
    let decoded = Email::decode(&buf).unwrap();
    println!("Email roundtrip: {decoded:?}");
    assert_eq!(decoded, email);

    println!("All custom types OK");
}
