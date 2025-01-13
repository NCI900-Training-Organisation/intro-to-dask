Automatic Parallelisation
--------------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn how to automatically parallelize code in Numba.
            #. Learn how to parallelise loops in Numba.

Setting the `parallel` option for `jit()` enables Numba to automatically parallelize and optimize 
parts of a function, though it currently only works on CPUs. Instead of parallelizing each operation 
individually, which can lead to inefficiencies, Numba identifies and fuses adjacent parallelizable 
operations into kernels that run in parallel, improving performance.


..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @jit(nopython=True, parallel=True)
    def reduction_with_parallel(n):
        shp = (13, 17)
        result1 = 2 * np.ones(shp, np.int_)
        tmp = 2 * np.ones_like(result1)

        for i in prange(n):
            result1 *= tmp

        return result1

When `parallel=True`, Numba supports explicit parallel loops using `prange` instead of `range`. 
This allows you to specify that a loop can be parallelized, but you must ensure there are no 
dependencies between iteration, except for those allowed in reductions.

..  code-block:: python
    :emphasize-lines: 1, 3, 8
    :linenos:


    from numba import njit, prange


    @njit(parallel=True)
    def prange_test(A):
        s = 0
        # Without "parallel=True" in the jit-decorator
        # the prange statement is equivalent to range
        for i in prange(A.shape[0]):
            s += A[i]
        return s

Without `parallel=True` in the `jit` decorator, the `prange` statement behaves the same as `range`.

.. admonition:: Key Points
   :class: hint

    #. `@jit(nopython=True, parallel=True)` automatically parallelise functions.
    #. Numba supports explicit parallel by replacing `range` with `prange`. 
    #. This functionality only works for CPU.