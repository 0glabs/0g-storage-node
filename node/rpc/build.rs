fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile proto/my_service.proto
    tonic_build::compile_protos("proto/zgs_grpc.proto")?;
    Ok(())
}
