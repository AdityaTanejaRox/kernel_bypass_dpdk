cmake_minimum_required(VERSION 3.16)
project(dpdk_kernel_bypass_lab LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED libdpdk)

add_executable(rx_only
  src/rx_only.cpp
)

# Includes + compile flags from pkg-config
target_include_directories(rx_only PRIVATE
  ${DPDK_INCLUDE_DIRS}
)

target_compile_options(rx_only PRIVATE
  ${DPDK_CFLAGS_OTHER}
  -O3
  -march=native
  -Wall
  -Wextra
  -Wpedantic
  # DPDK headers may trigger these depending on compiler/DPDK version
  -Wno-error=deprecated-declarations
  -Wno-error=volatile
)

# Link dirs before libs (more robust)
target_link_directories(rx_only PRIVATE
  ${DPDK_LIBRARY_DIRS}
)

target_link_libraries(rx_only PRIVATE
  ${DPDK_LIBRARIES}
  numa
)
