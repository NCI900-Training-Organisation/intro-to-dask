Streams
-------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn how target GPUs streams using Numba.

Streams are sequences of operations that are executed in order on the GPU. Operations in different 
streams can run concurrently, allowing for parallel execution and better utilization of GPU resources.
CUDA streams in Numba allow you to manage and execute multiple tasks concurrently on a GPU, enhancing 
performance by overlapping computation and data transfer operations. 

..  code-block:: python
    :linenos:

    from numba import cuda
    import numpy as np

    # Define a simple kernel function
    @cuda.jit
    def add_kernel(a, b, c):
        tx = cuda.threadIdx.x
        ty = cuda.blockIdx.x
        bw = cuda.blockDim.x

        pos = tx + ty * bw

        if pos < a.size:
            c[pos] = a[pos] + b[pos]

    # Create two streams
    stream1 = cuda.stream()
    stream2 = cuda.stream()

    # Initialize data
    size = 1000000
    a_cpu = np.arange(size, dtype=np.float32)
    b_cpu = np.arange(size, dtype=np.float32) * 2
    c_cpu = np.zeros(size, dtype=np.float32)

    # Allocate device memory
    a_gpu = cuda.to_device(a_cpu)
    b_gpu = cuda.to_device(b_cpu)
    c_gpu = cuda.device_array(size, dtype=np.float32)

    # Define block and grid dimensions
    threads_per_block = 256
    blocks_per_grid = (size + (threads_per_block - 1)) // threads_per_block

    # Launch kernels in different streams
    add_kernel[blocks_per_grid, threads_per_block](a_gpu, b_gpu, c_gpu, stream=stream1)
    add_kernel[blocks_per_grid, threads_per_block](b_gpu, c_gpu, a_gpu, stream=stream2)

    # Wait for the streams to complete
    stream1.synchronize()
    stream2.synchronize()

    # Copy result back to host
    c_cpu = c_gpu.copy_to_host()

.. admonition:: Key Points
   :class: hint

    #. Streams can be used to run concurrent operations in GPUs.