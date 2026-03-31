// Should fail: PgEnum can only be derived for enums, not structs.
#[derive(stalwart::PgEnum)]
struct Bad {
    field: String,
}

fn main() {}
