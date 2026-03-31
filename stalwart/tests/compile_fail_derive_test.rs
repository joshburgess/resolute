//! Compile-time error tests for derive macros using trybuild.
//!
//! These verify that derive macros produce helpful compile errors
//! for invalid inputs. No DATABASE_URL required.

#[test]
fn derive_compile_fail_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail_derive/*.rs");
}
