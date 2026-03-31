// Should fail: FromRow only supports structs, not enums.
#[derive(stalwart::FromRow)]
enum Bad {
    A,
    B,
}

fn main() {}
