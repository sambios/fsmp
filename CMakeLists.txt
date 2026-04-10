cmake_minimum_required(VERSION 3.20)
project(cppfsmp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(fsmp_runtime
  src/runtime.cpp
)

target_include_directories(fsmp_runtime
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(fsmp_runtime PUBLIC pthread)

add_executable(fsmp_tests
  tests/fsmp_tests.cpp
)

target_link_libraries(fsmp_tests PRIVATE fsmp_runtime pthread)

add_executable(echo_server
  examples/echo_server.cpp
)

target_link_libraries(echo_server PRIVATE fsmp_runtime pthread)

add_executable(echo_client
  examples/echo_client.cpp
)

target_link_libraries(echo_client PRIVATE fsmp_runtime pthread)
