fn main() {
    napi_build::setup();

    // Unit tests in this crate link the napi v2 rlib target, but the `napi_*`
    // symbols are normally supplied by the host Node process at dlopen time.
    // For a pure Rust test binary there's no host, so tell the linker not to
    // fail on unresolved references to those symbols. The napi-rs Drop impls
    // have a fast path for Rust-constructed values that never reaches the
    // FFI, so the test binary runs fine in practice.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("apple") {
        println!("cargo:rustc-link-arg=-undefined");
        println!("cargo:rustc-link-arg=dynamic_lookup");
    } else if target.contains("linux") {
        println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-all");
    }
}
