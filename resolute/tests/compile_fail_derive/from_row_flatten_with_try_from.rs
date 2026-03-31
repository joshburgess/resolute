// Should fail: flatten cannot be combined with try_from.
#[derive(resolute::FromRow)]
struct Inner {
    name: String,
}

#[derive(resolute::FromRow)]
struct Bad {
    #[from_row(flatten, try_from = "Inner")]
    inner: Inner,
}

fn main() {}
