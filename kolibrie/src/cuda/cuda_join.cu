/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

// hash_join.cu
#include <cuda_runtime.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#define EXPORT_SYMBOL __declspec(dllexport)
#else
#define EXPORT_SYMBOL __attribute__((visibility("default")))
#endif

extern "C" {

// CUDA kernel to perform hash join
__global__ void hash_join_kernel(
    const uint32_t* __restrict__ d_subjects,
    const uint32_t* __restrict__ d_predicates,
    const uint32_t* __restrict__ d_objects,
    uint32_t predicate_filter,
    uint32_t* d_indices,
    uint32_t num_triples,
    uint32_t* d_result_count)
{
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_triples) {
        if (d_predicates[idx] != predicate_filter) {
            return; // Skip non-matching predicate
        }

        // Atomically store the index of the matching triple
        uint32_t pos = atomicAdd(d_result_count, 1);
        d_indices[pos] = idx;
    }
}

// Host function to perform hash join using CUDA
EXPORT_SYMBOL void perform_hash_join_cuda(
    const uint32_t* h_subjects,
    const uint32_t* h_predicates,
    const uint32_t* h_objects,
    uint32_t num_triples,
    uint32_t predicate_filter,
    uint32_t* literal_filter, // Pass NULL if no filter
    uint32_t** h_indices,
    uint32_t* h_result_count)
{
    uint32_t *d_subjects, *d_predicates, *d_objects;
    uint32_t *d_indices, *d_result_count;

    // Allocate device memory
    cudaMalloc((void**)&d_subjects, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_predicates, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_objects, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_indices, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_result_count, sizeof(uint32_t));

    // Copy data to device
    cudaMemcpy(d_subjects, h_subjects, num_triples * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(d_predicates, h_predicates, num_triples * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(d_objects, h_objects, num_triples * sizeof(uint32_t), cudaMemcpyHostToDevice);

    uint32_t zero = 0;
    cudaMemcpy(d_result_count, &zero, sizeof(uint32_t), cudaMemcpyHostToDevice);

    // Query device properties
    cudaDeviceProp deviceProp;
    cudaGetDeviceProperties(&deviceProp, 0);

    // Configure kernel launch parameters
    int threadsPerBlock = deviceProp.maxThreadsPerBlock; // Maximum threads per block
    int blocksPerGrid = (num_triples + threadsPerBlock - 1) / threadsPerBlock;

    // Ensure we don't exceed the maximum number of blocks
    int maxBlocks = deviceProp.multiProcessorCount * deviceProp.maxThreadsPerMultiProcessor / threadsPerBlock;
    if (blocksPerGrid > maxBlocks) {
        blocksPerGrid = maxBlocks;
    }

    // Launch kernel
    hash_join_kernel<<<blocksPerGrid, threadsPerBlock>>>(
        d_subjects,
        d_predicates,
        d_objects,
        predicate_filter, // Pass scalar value
        d_indices,
        num_triples,
        d_result_count);

    // Synchronize device
    cudaDeviceSynchronize();

    // Copy result count back to host
    cudaMemcpy(h_result_count, d_result_count, sizeof(uint32_t), cudaMemcpyDeviceToHost);

    // Allocate host memory for indices
    *h_indices = (uint32_t*)malloc(*h_result_count * sizeof(uint32_t));

    // Copy indices back to host
    cudaMemcpy(*h_indices, d_indices, *h_result_count * sizeof(uint32_t), cudaMemcpyDeviceToHost);

    // Free device memory
    cudaFree(d_subjects);
    cudaFree(d_predicates);
    cudaFree(d_objects);
    cudaFree(d_indices);
    cudaFree(d_result_count);
}
}
