Vectorization in GPU
--------------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn how vectorize using GPUs.


The code below Python function adds a given scalar (`y`) to all elements of a one-dimensional array. 
The more interesting aspects lie in the function's declaration, which includes two key elements:

..  code-block:: python
    :linenos:

    from numba import guvectorize, int64
    import numpy as np
    
    @guvectorize([(int64[:], int64, int64[:])], '(n),()->(n)')
    def g(x, y, res):
        for i in range(x.shape[0]):
            res[i] = x[i] + y

*(n),()->(n)* tells NumPy that the function takes a n-element one-dimension array, a scalar, denoted 
by the empty *tuple ()*, and computes an *n-element one-dimension array*.

Unlike *vectorize()* functions, *guvectorize()* functions should not return any result.

.. admonition:: Key Points
   :class: hint

    #. `@guvectorize` can be used to vectorize the function in GPU. 