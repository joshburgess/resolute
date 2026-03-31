// Should fail: flatten cannot be combined with json.
#[derive(resolute::FromRow)]
struct Inner {
    name: String,
}

#[derive(resolute::FromRow)]
struct Bad {
    #[from_row(flatten, json)]
    inner: Inner,
}

fn main() {}
