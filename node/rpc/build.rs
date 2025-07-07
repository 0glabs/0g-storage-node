fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile proto/my_service.proto
    tonic_build::configure()
        .file_descriptor_set_path("proto/zgs_grpc_descriptor.bin")
        .compile(&["proto/zgs_grpc.proto"], &["proto"])?;
    Ok(())
}
