# Optional gRPC/protobuf transport, enabled with -DKVDB_ENABLE_GRPC=ON.
# Not exercised by the default build (no gRPC toolchain assumed present);
# wires proto/kvdb.proto and proto/raft.proto into a kvdb_grpc target that
# the server/client link against when this option is on.
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/kvdb.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto
)

add_library(kvdb_grpc_proto ${PROTO_FILES})
target_link_libraries(kvdb_grpc_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(kvdb_grpc_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate(TARGET kvdb_grpc_proto LANGUAGE cpp)
protobuf_generate(
  TARGET kvdb_grpc_proto
  LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
)

target_compile_definitions(kvdb_core PUBLIC KVDB_ENABLE_GRPC=1)
target_link_libraries(kvdb_core PUBLIC kvdb_grpc_proto)
