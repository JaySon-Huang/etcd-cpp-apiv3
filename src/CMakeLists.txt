set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(CLIENT_HDR_ROOT ${HDR_ROOT}/etcd/)
set(V3_HDR_ROOT ${CLIENT_HDR_ROOT}/v3)
set(V3_SRC_ROOT ${SRC_ROOT}/v3)
set(V3_PROTO_ROOT ${V3_HDR_ROOT}/proto)

set(SOURCES ${SRC_ROOT}/Client.cpp
		${SRC_ROOT}/Response.cpp
		${SRC_ROOT}/Value.cpp
		${SRC_ROOT}/SyncClient.cpp
		${SRC_ROOT}/Watcher.cpp
		${SRC_ROOT}/Utils.cpp)

set(PROTO_SOURCES ${V3_PROTO_ROOT}/auth.pb.cc
		${V3_PROTO_ROOT}/kv.pb.cc
		${V3_PROTO_ROOT}/rpc.pb.cc
		${V3_PROTO_ROOT}/rpc.grpc.pb.cc)
set_source_files_properties(
        ${PROTO_SOURCES} ${PROTO_HDRS} PROPERTIES
        GENERATED TRUE)


set(V3_SOURCES ${V3_SRC_ROOT}/AsyncTxnResponse.cpp 
		 ${V3_SRC_ROOT}/AsyncRangeResponse.cpp
		 ${V3_SRC_ROOT}/Transaction.cpp
		 ${V3_SRC_ROOT}/action_constants.cpp
		 ${V3_SRC_ROOT}/AsyncSetAction.cpp
		 ${V3_SRC_ROOT}/AsyncCompareAndSwapAction.cpp
		 ${V3_SRC_ROOT}/AsyncUpdateAction.cpp
		 ${V3_SRC_ROOT}/AsyncGetAction.cpp
		 ${V3_SRC_ROOT}/AsyncDeleteAction.cpp
		 ${V3_SRC_ROOT}/AsyncCompareAndDeleteAction.cpp
		 ${V3_SRC_ROOT}/Action.cpp
		 ${V3_SRC_ROOT}/AsyncWatchAction.cpp
		 ${V3_SRC_ROOT}/V3Response.cpp
		 ${V3_SRC_ROOT}/AsyncDeleteRangeResponse.cpp
		 ${V3_SRC_ROOT}/AsyncWatchResponse.cpp
		 ${V3_SRC_ROOT}/AsyncLeaseGrantResponse.cpp
		 ${V3_SRC_ROOT}/AsyncLeaseGrantAction.cpp
		 ${V3_SRC_ROOT}/KeyValue.cpp
		 ${V3_SRC_ROOT}/V3Status.cpp)

add_library(${PROJECT_NAME} ${LIBRARY_TYPE} ${PROTO_SOURCES} ${V3_SOURCES} ${SOURCES})
add_dependencies(${PROJECT_NAME} ${PROJECT_NAME}-protocol-etcd)
target_link_libraries(${PROJECT_NAME} ${CPPREST_LIB} boost_system boost_chrono boost_thread ssl crypto protobuf grpc grpc++ pthread)

install(TARGETS ${PROJECT_NAME} DESTINATION lib)

install(FILES ${CLIENT_HDR_ROOT}/Client.hpp
               ${CLIENT_HDR_ROOT}/SyncClient.hpp
               ${CLIENT_HDR_ROOT}/Response.hpp
               ${CLIENT_HDR_ROOT}/Value.hpp
               ${CLIENT_HDR_ROOT}/Watcher.hpp
			   ${CLIENT_HDR_ROOT}/Utils.hpp
               DESTINATION include/etcd)

install(FILES ${V3_HDR_ROOT}/action_constants.hpp
		${V3_HDR_ROOT}/AsyncCompareAndSwapAction.hpp
		${V3_HDR_ROOT}/AsyncGetAction.hpp
		${V3_HDR_ROOT}/AsyncRangeResponse.hpp
		${V3_HDR_ROOT}/AsyncUpdateAction.hpp
		${V3_HDR_ROOT}/KeyValue.hpp
		${V3_HDR_ROOT}/Action.hpp	
		${V3_HDR_ROOT}/AsyncDeleteAction.hpp
		${V3_HDR_ROOT}/AsyncLeaseGrantAction.hpp
		${V3_HDR_ROOT}/AsyncSetAction.hpp
		${V3_HDR_ROOT}/AsyncWatchAction.hpp
		${V3_HDR_ROOT}/Transaction.hpp
		${V3_HDR_ROOT}/AsyncCompareAndDeleteAction.hpp
		${V3_HDR_ROOT}/AsyncDeleteRangeResponse.hpp
		${V3_HDR_ROOT}/AsyncLeaseGrantResponse.hpp
		${V3_HDR_ROOT}/AsyncTxnResponse.hpp
		${V3_HDR_ROOT}/AsyncWatchResponse.hpp
		${V3_HDR_ROOT}/V3Response.hpp
		${V3_HDR_ROOT}/V3Status.hpp
               DESTINATION include/etcd/v3/)

install(FILES ${V3_PROTO_ROOT}/auth.pb.h
		${V3_PROTO_ROOT}/etcdserver.pb.h
		${V3_PROTO_ROOT}/kv.pb.h
		${V3_PROTO_ROOT}/rpc.grpc.pb.h
		${V3_PROTO_ROOT}/rpc.pb.h
               DESTINATION include/etcd/v3/proto)
