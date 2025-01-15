CUDA and Numba
--------------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 min

        **Objectives:**
            #. Learn how target GPUs using Numba.

Kernel Function
****************

A kernel function is a GPU function called from CPU code that cannot return values directly.
It also define how GPU threads hierarchy (threads, blocks and grids) is used. 

..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @cuda.jit
    def polar_to_cartesian(rho, theta):
        x = rho * math.cos(theta)
        y = rho * math.sin(theta)

Device Functions
****************

Device functions are used to perform computations on the GPU, and they can be invoked from within 
other device functions or kernels. Unlike a kernel function, a device function can return a value
like normal functions.


..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @cuda.jit(device=True) 
    def polar_to_cartesian(rho, theta):
        x = rho * math.cos(theta)
        y = rho * math.sin(theta)
        return x, y

`@vectorize` can also target GPU.

..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @cuda.jit(device=True)
    def polar_to_cartesian(rho, theta):
        x = rho * math.cos(theta)
        y = rho * math.sin(theta)
        return x, y  

    @vectorize(['float32(float32, float32, float32, float32)'], target='cuda')
    def polar_distance(rho1, theta1, rho2, theta2):
        x1, y1 = polar_to_cartesian(rho1, theta1)
        x2, y2 = polar_to_cartesian(rho2, theta2)

        return ((x1 - x2)**2 + (y1 - y2)**2)**0.5


Thread Indexing
****************

When launching a kernel, you should also specify the thread arrangements.

..  code-block:: python
    :linenos:

    @cuda.jit
    def increment_a_2D_array(an_array):
        x, y = cuda.grid(2)
        if x < an_array.shape[0] and y < an_array.shape[1]:
           an_array[x, y] += 1

    threadsperblock = (16, 16)
    blockspergrid_x = math.ceil(an_array.shape[0] / threadsperblock[0])
    blockspergrid_y = math.ceil(an_array.shape[1] / threadsperblock[1])
    blockspergrid = (blockspergrid_x, blockspergrid_y)
    increment_a_2D_array[blockspergrid, threadsperblock](an_array)

You can learn more about thread indexing in the tutorial 
`Introduction to Parallel Programming Using Python <https://intro-to-parallel-programming.readthedocs.io/en/latest>`_ .
    


.. admonition:: Key Points
   :class: hint

    #. `@vectorize` can target GPUs.
    #. Device functions can only be invoked from another device functions or kernel functions.