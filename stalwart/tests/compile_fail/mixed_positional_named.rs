// Should fail: cannot mix positional and named parameters.
fn main() {
    let _ = stalwart::query!("SELECT $1::int4, :name::text", 1i32, name = "hello");
}
