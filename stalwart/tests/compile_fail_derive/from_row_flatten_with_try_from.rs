// Should fail: flatten cannot be combined with try_from.
#[derive(stalwart::FromRow)]
struct Inner {
    name: String,
}

#[derive(stalwart::FromRow)]
struct Bad {
    #[from_row(flatten, try_from = "Inner")]
    inner: Inner,
}

fn main() {}
