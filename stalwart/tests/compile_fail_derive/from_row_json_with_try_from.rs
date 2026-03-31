// Should fail: json cannot be combined with try_from.
#[derive(stalwart::FromRow)]
struct Bad {
    #[from_row(json, try_from = "String")]
    field: String,
}

fn main() {}
