// Should fail: PgDomain requires a tuple struct with exactly one field.
#[derive(resolute::PgDomain)]
struct Bad {
    value: String,
}

fn main() {}
