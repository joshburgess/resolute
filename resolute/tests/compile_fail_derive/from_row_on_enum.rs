// Should fail: FromRow only supports structs, not enums.
#[derive(resolute::FromRow)]
enum Bad {
    A,
    B,
}

fn main() {}
