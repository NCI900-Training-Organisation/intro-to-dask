Reduction in GPU
--------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 5 min

        **Objectives:**
            #. Learn how to perform reduction operations on GPUs.


Numba offers a `@reduce` decorator that transforms a simple binary operation into a reduction kernel.

..  code-block:: python
    :linenos:

    import numpy
    from numba import cuda

    @cuda.reduce
    def sum_reduce(a, b):
        return a + b

    A = (numpy.arange(1234, dtype=numpy.float64)) + 1
    normal_sum = A.sum()      # NumPy sum reduction
    gpu_sum = sum_reduce(A)   # cuda sum reduction
    assert normal_sum == gpu_sum



.. admonition:: Key Points
   :class: hint

    #. `@reduce` can convert a simple binary operation into a reduction kernel.