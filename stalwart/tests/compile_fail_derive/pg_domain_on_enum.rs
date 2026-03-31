// Should fail: PgDomain only supports tuple structs, not enums.
#[derive(stalwart::PgDomain)]
enum Bad {
    A,
    B,
}

fn main() {}
