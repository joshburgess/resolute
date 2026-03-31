// Should fail: PgEnum only supports unit variants (no fields).
#[derive(stalwart::PgEnum)]
enum Bad {
    A,
    B(i32),
}

fn main() {}
