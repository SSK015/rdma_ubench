cmake_minimum_required(VERSION 3.10.0)
project(rdma_ubench VERSION 0.1)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)
#set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(CMAKE_ENABLE_EXPORTS ON)
add_compile_options(-Wall -Wextra -pedantic)

# 使用pkg-config查找ZeroMQ
find_package(PkgConfig REQUIRED)
pkg_check_modules(ZMQ REQUIRED libzmq)

# 查找gflags
find_package(PkgConfig QUIET)
pkg_check_modules(GFLAGS QUIET gflags)
if(NOT GFLAGS_FOUND)
    find_package(gflags QUIET)
    if(NOT gflags_FOUND)
        message(WARNING "gflags not found via pkg-config or find_package")
    endif()
endif()

# 查找spdlog
find_package(PkgConfig QUIET)
pkg_check_modules(SPDLOG QUIET spdlog)
if(NOT SPDLOG_FOUND)
    find_package(spdlog QUIET)
    if(NOT spdlog_FOUND)
        message(WARNING "spdlog not found via pkg-config or find_package")
    endif()
endif()

# 查找ibverbs
find_package(PkgConfig QUIET)
pkg_check_modules(IBVERBS QUIET libibverbs)
if(NOT IBVERBS_FOUND)
    find_library(IBVERBS_LIBRARIES NAMES ibverbs)
    if(NOT IBVERBS_LIBRARIES)
        message(WARNING "ibverbs library not found")
    endif()
endif()

# 创建common库
add_library(common ib.h ib.cc log.h log.cc zmq_helper.h)

# 链接库到common
if(ZMQ_FOUND)
    target_include_directories(common PRIVATE ${ZMQ_INCLUDE_DIRS})
    target_link_libraries(common ${ZMQ_LIBRARIES})
endif()

if(IBVERBS_FOUND)
    target_include_directories(common PRIVATE ${IBVERBS_INCLUDE_DIRS})
    target_link_libraries(common ${IBVERBS_LIBRARIES})
elseif(IBVERBS_LIBRARIES)
    target_link_libraries(common ${IBVERBS_LIBRARIES})
endif()

if(SPDLOG_FOUND)
    target_include_directories(common PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_link_libraries(common ${SPDLOG_LIBRARIES})
elseif(spdlog_FOUND)
    target_link_libraries(common spdlog::spdlog)
endif()

# 创建可执行文件
add_executable(ubench ubench.cc server.h server.cc client.h client.cc)
target_link_libraries(ubench common)

# 链接gflags到可执行文件
if(GFLAGS_FOUND)
    target_include_directories(ubench PRIVATE ${GFLAGS_INCLUDE_DIRS})
    target_link_libraries(ubench ${GFLAGS_LIBRARIES})
elseif(gflags_FOUND)
    target_link_libraries(ubench gflags_static)
endif()
