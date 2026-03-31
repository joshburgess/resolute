// Should fail: flatten cannot be combined with json.
#[derive(stalwart::FromRow)]
struct Inner {
    name: String,
}

#[derive(stalwart::FromRow)]
struct Bad {
    #[from_row(flatten, json)]
    inner: Inner,
}

fn main() {}
