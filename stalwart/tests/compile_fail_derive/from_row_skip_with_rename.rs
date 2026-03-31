// Should fail: skip cannot be combined with rename.
#[derive(stalwart::FromRow)]
struct Bad {
    #[from_row(skip, rename = "col")]
    field: String,
}

fn main() {}
