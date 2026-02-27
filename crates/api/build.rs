fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&["proto/exchange_lab.proto"], &["proto"])
        .expect("compile proto");
}
