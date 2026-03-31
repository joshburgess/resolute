// Should fail: integer-backed enums must have explicit discriminants.
#[derive(resolute::PgEnum)]
#[repr(i32)]
enum Bad {
    A,
    B,
    C,
}

fn main() {}
