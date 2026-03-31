// Should fail: PgDomain only supports tuple structs, not enums.
#[derive(resolute::PgDomain)]
enum Bad {
    A,
    B,
}

fn main() {}
