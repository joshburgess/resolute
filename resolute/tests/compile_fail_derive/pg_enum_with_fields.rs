// Should fail: PgEnum only supports unit variants (no fields).
#[derive(resolute::PgEnum)]
enum Bad {
    A,
    B(i32),
}

fn main() {}
