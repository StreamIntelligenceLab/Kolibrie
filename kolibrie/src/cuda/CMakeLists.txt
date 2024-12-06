cmake_minimum_required(VERSION 3.18)
project(CUDAJoin LANGUAGES CUDA CXX)

# Set CUDA source file
set(CUDA_SOURCE cuda_join.cu)

# Add shared library target
add_library(cudajoin SHARED ${CUDA_SOURCE})

# Check platform and configure CUDA_ARCHITECTURES
if(WIN32)
    message(STATUS "Configured build for Windows")
    set(CUDA_ARCHITECTURES "75;80;86") # Compatible with CUDA 12.6
    set_target_properties(cudajoin PROPERTIES
        OUTPUT_NAME cudajoin
    )
    target_compile_options(cudajoin PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/MD>)
elseif(UNIX)
    message(STATUS "Configured build for Linux")
    set(CUDA_ARCHITECTURES "52;60;61") # Compatible with CUDA 11.5
    set_target_properties(cudajoin PROPERTIES
        OUTPUT_NAME cudajoin
    )
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

# Apply architecture configuration
set_target_properties(cudajoin PROPERTIES
    CUDA_ARCHITECTURES "${CUDA_ARCHITECTURES}"
    POSITION_INDEPENDENT_CODE ON
)

# Specify output directories
set_target_properties(cudajoin PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output
)
