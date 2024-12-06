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
    const uint32_t* d_subjects,
    const uint32_t* d_predicates,
    const uint32_t* d_objects,
    const uint32_t* d_predicate_filter,
    const uint32_t* d_literal_filter,
    uint32_t* d_indices,
    uint32_t num_triples,
    uint32_t* d_result_count)
{
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_triples) {
        uint32_t predicate = d_predicates[idx];

        // Apply predicate filter
        if (predicate != *d_predicate_filter) {
            return;
        }

        // Apply literal filter if provided
        if (d_literal_filter != NULL) {
            uint32_t object = d_objects[idx];
            if (object != *d_literal_filter) {
                return;
            }
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
    uint32_t *d_indices, *d_predicate_filter, *d_literal_filter;
    uint32_t *d_result_count;

    // Allocate device memory
    cudaMalloc((void**)&d_subjects, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_predicates, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_objects, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_indices, num_triples * sizeof(uint32_t));
    cudaMalloc((void**)&d_predicate_filter, sizeof(uint32_t));
    cudaMalloc((void**)&d_result_count, sizeof(uint32_t));
    if (literal_filter != NULL) {
        cudaMalloc((void**)&d_literal_filter, sizeof(uint32_t));
    } else {
        d_literal_filter = NULL;
    }

    // Copy data to device
    cudaMemcpy(d_subjects, h_subjects, num_triples * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(d_predicates, h_predicates, num_triples * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(d_objects, h_objects, num_triples * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(d_predicate_filter, &predicate_filter, sizeof(uint32_t), cudaMemcpyHostToDevice);
    uint32_t zero = 0;
    cudaMemcpy(d_result_count, &zero, sizeof(uint32_t), cudaMemcpyHostToDevice);
    if (literal_filter != NULL) {
        cudaMemcpy(d_literal_filter, literal_filter, sizeof(uint32_t), cudaMemcpyHostToDevice);
    }

    // Launch kernel
    int threadsPerBlock = 256;
    int blocksPerGrid = (num_triples + threadsPerBlock - 1) / threadsPerBlock;
    hash_join_kernel<<<blocksPerGrid, threadsPerBlock>>>(
        d_subjects,
        d_predicates,
        d_objects,
        d_predicate_filter,
        d_literal_filter,
        d_indices,
        num_triples,
        d_result_count);

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
    cudaFree(d_predicate_filter);
    cudaFree(d_result_count);
    if (d_literal_filter != NULL) {
        cudaFree(d_literal_filter);
    }
}
}
