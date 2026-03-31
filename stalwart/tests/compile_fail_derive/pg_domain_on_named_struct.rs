// Should fail: PgDomain requires a tuple struct with exactly one field.
#[derive(stalwart::PgDomain)]
struct Bad {
    value: String,
}

fn main() {}
