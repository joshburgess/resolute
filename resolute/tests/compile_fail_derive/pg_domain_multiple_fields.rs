// Should fail: PgDomain requires exactly one field.
#[derive(resolute::PgDomain)]
struct Bad(String, i32);

fn main() {}
