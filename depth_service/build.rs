fn main() {
    let proto = "../proto/event.proto";
    prost_build::Config::new()
        .out_dir("src")
        .compile_protos(&[proto], &["../proto"])
        .unwrap();
}
