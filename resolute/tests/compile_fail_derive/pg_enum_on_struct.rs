// Should fail: PgEnum can only be derived for enums, not structs.
#[derive(resolute::PgEnum)]
struct Bad {
    field: String,
}

fn main() {}
