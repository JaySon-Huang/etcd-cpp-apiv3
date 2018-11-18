
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
message(STATUS "proto files: ${PROTO_FILES}")
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

find_program(GRPC_CPP_PLUGIN_PATH NAMES grpc_cpp_plugin)
set(GRPC_SRCS "rpc.grpc.pb.cc")
set(GRPC_HDRS "rpc.grpc.pb.h")
add_custom_command(
        OUTPUT "${GRPC_SRCS}" "${GRPC_HDRS}"
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_CURRENT_SOURCE_DIR}"
        --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_PATH}"
        "rpc.proto"
        DEPENDS "${PROTO_FILES}")

add_library(${PROJECT_NAME}-protocol-etcd STATIC ${GRPC_SRCS} ${PROTO_SRCS})
target_link_libraries(${PROJECT_NAME}-protocol-etcd protobuf)
set_property(TARGET ${PROJECT_NAME}-protocol-etcd PROPERTY POSITION_INDEPENDENT_CODE ON)
add_custom_command(
        TARGET ${PROJECT_NAME}-protocol-etcd
        POST_BUILD
        COMMENT "copying etcd's protocol ... "
        COMMAND mkdir -p ${PROJECT_SOURCE_DIR}/include/etcd/v3/proto
        COMMAND cp -f ${GRPC_SRCS} ${GRPC_HDRS} ${PROTO_SRCS} ${PROTO_HDRS} ${PROJECT_SOURCE_DIR}/include/etcd/v3/proto
)

