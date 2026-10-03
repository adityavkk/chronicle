fn main() {
    let include = std::env::var("DEP_SQLITE3_INCLUDE")
        .expect("libsqlite3-sys must provide the header matching its linked bundled SQLite");
    cc::Build::new()
        .file("src/fault_vfs.c")
        .include(include)
        .warnings(true)
        .compile("chronicle_fault_vfs");
    println!("cargo:rerun-if-changed=src/fault_vfs.c");
}
