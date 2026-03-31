// Should fail: skip cannot be combined with default.
#[derive(resolute::FromRow)]
struct Bad {
    #[from_row(skip, default)]
    field: i32,
}

fn main() {}
