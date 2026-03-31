//! Compile-time error tests using trybuild.
//!
//! These verify that the query!() macro produces helpful compile errors.
//! Requires DATABASE_URL to be set (macros connect at compile time).

#[test]
fn compile_fail_tests() {
    if std::env::var("DATABASE_URL").is_err() {
        eprintln!("Skipping trybuild tests: DATABASE_URL not set");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
