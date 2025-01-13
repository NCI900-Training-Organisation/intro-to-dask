Caching Compiled Functions
--------------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn file-based cache in Numba.

To avoid compilation each time you run a Python program, you can direct Numba to save the 
results of function compilation to a file-based cache by using the `cache=True` option.

When the function is first called, Numba compiles it and saves the compiled code and metadata to a 
cache directory on disk. For future executions, Numba checks if there is a valid cached version of 
the function available. If the cached version matches the current function signature and other 
conditions, Numba uses the cached compiled code instead of recompiling the function. This saves time
by avoiding repeated compilation of the same function, especially for large or complex functions.



..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @jit(nopython=True, cache=True)
    def f(x, y):
        return x + y


Cached functions on disk can still incur some overhead initially due to operations like type checking. 
However subsequent executions with the same type signature are significantly faster.


.. admonition:: Key Points
   :class: hint

    #. `@jit(nopython=True, cache=True)` saves the compiled function to file.
    #. This eliminates the need for repeated function compilations.