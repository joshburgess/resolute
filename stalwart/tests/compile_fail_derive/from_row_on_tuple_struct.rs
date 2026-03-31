// Should fail: FromRow only supports structs with named fields.
#[derive(stalwart::FromRow)]
struct Bad(i32, String);

fn main() {}
