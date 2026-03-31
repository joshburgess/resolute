// Should fail: query expects 1 parameter, but 2 are given.
fn main() {
    let _ = resolute::query!("SELECT $1::int4 AS n", 1i32, 2i32);
}
