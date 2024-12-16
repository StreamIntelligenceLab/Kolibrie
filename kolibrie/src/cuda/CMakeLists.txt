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
    target_compile_options(cudajoin PRIVATE 
        $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/MD>
    )
    set_target_properties(cudajoin PROPERTIES
        CUDA_ARCHITECTURES "${CUDA_ARCHITECTURES}"
        CUDA_GENERATE_CODE "arch=compute_75,code=[compute_75,sm_75,lto_75];arch=compute_80,code=[compute_80,sm_80,lto_80];arch=compute_86,code=[compute_86,sm_86,lto_86]"
    )
elseif(UNIX)
    message(STATUS "Configured build for Linux")
    set(CUDA_ARCHITECTURES "52;60;61") # Compatible with CUDA 11.5
    set_target_properties(cudajoin PROPERTIES
        OUTPUT_NAME cudajoin
        CUDA_GENERATE_CODE "arch=compute_52,code=[compute_52,sm_52,lto_52];arch=compute_60,code=[compute_60,sm_60,lto_60];arch=compute_61,code=[compute_61,sm_61,lto_61]"
    )
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

# Add nvcc-specific flags
target_compile_options(cudajoin PRIVATE 
    $<$<COMPILE_LANGUAGE:CUDA>:-lineinfo>
    $<$<COMPILE_LANGUAGE:CUDA>:-Xptxas=-O3>
    $<$<COMPILE_LANGUAGE:CUDA>:-use_fast_math>
)

# Apply architecture configuration
set_target_properties(cudajoin PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Specify output directories
set_target_properties(cudajoin PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output
)
