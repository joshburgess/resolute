// Should fail: PgDomain requires exactly one field.
#[derive(stalwart::PgDomain)]
struct Bad(String, i32);

fn main() {}
