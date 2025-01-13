Compiler Modes in Python
------------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn the difference between Nopython and Object compilation.
            #. Learn how to use Nopython compilation.

Numba offers two compilation modes: nopython mode and object mode. 

- **Nopython Mode**: Generates code that avoids using the Python C API, resulting in the highest 
performance.

- **Object Mode**: Handles values as Python objects and relies on the Python C API, often leading 
to performance similar to standard Python code.

By default, Numba will fall back to object mode if nopython mode cannot be used. However, it might 
be preferable to generate an error rather than automatically falling back to object mode.

..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @jit(nopython=True) 
    def with_numba(a): 
        trace = 0.0
        for i in range(a.shape[0]):   
            trace += np.tanh(a[i, i]) 
        return a + trace 

Setting `nopython=True` will trigger an error if nopython mode fails to apply. 
You can also use `@jit(nopython=True)` interchangeably with `@njit`.

..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @njit 
    def with_numba(a): 
        trace = 0.0
        for i in range(a.shape[0]):   
            trace += np.tanh(a[i, i]) 
        return a + trace 

.. admonition:: Key Points
   :class: hint

    #. *nopython* mode gives the best performance.
    #. There is no fallback option in the *nopython* mode.