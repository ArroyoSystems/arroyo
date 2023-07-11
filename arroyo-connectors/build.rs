fn main() -> Result<(), String> {
    println!("cargo:rerun-if-changed=../connector-schemas");
    Ok(())
}
