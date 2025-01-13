Working with Numba
------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn the how Numba works.


Numba was developed to address the inefficiencies in NumPy use cases. NumPy uses multi-dimensional arrays 
(ndarrays) for data storage, and operations on these arrays are implemented in C for efficiency. Before Numba, 
custom efficient computations required writing Python C extensions. Numba is a Just-in-Time (JIT) compiler for 
CPython that allows users to annotate compute-intensive Python functions for compilation without needing to 
rewrite the code in C.

.. image:: ../figs/performance.png


Just-in-Time (JIT) Compiler
---------------------------

A Just-in-Time (JIT) compiler in Numba works by dynamically compiling Python code into optimized machine code 
at runtime, rather than ahead of time. 

.. image:: ../figs/normal_working.png
.. image:: ../figs/jit_working.png


Here's a breakdown of how it functions:

1. **Annotation and Compilation**: When you use Numba's `@jit` decorator on a Python function, Numba 
first analyzes the function's code. This analysis determines how to compile the function to improve performance. 
You can also provide type hints to help Numba generate more efficient machine code.

2. **Type Inference**: Numba performs type inference on the function’s inputs and outputs. It determines the 
types of variables and ensures that operations are optimized for those types. For example, it might optimize
arithmetic operations for specific numerical types.

3. **Machine Code Generation**: Based on the type information and analysis, Numba generates machine code 
tailored to the function. This code is designed to run directly on the hardware, bypassing the overhead of the 
Python interpreter.

4. **Execution**: When the annotated function is called, Numba uses the compiled machine code to execute the 
function. This process can be significantly faster than executing the original Python code because the machine 
code is optimized for performance.

5. **Caching**: Numba caches the compiled code for future use. If the function is called again with the same 
types, Numba can reuse the cached machine code, avoiding recompilation and improving performance further.

6. **Fallback and Adaptation**: If Numba encounters code or data types it cannot optimize directly, it will 
fall back to executing the function in the normal Python interpreter. Numba can adapt over time as more functions
are annotated and optimized.

Overall, Numba's JIT compilation allows Python code, especially numerical and scientific computations, to run 
significantly faster by translating it into efficient machine code while maintaining the ease of writing in 
Python. 

..  code-block:: python
    :linenos:

    import numba
    from numba import jit, int32, prange, vectorize, float64, cuda


Decorating a function with `@jit` marks it for optimization by Numba's JIT compiler. 

..  code-block:: python
    :linenos:

    @jit
    def f(x, y):
        return x + y

.. image:: ../figs/numba_working.png


Compilation is deferred until the function is first executed

..  code-block:: python
    :linenos:

    f(2, 3)

and different function invocations may result in different compilations based on the input types.

..  code-block:: python
    :linenos:

    f('2', '3')


.. admonition:: Key Points
   :class: hint

    #. Numba uses simple annonations to parallelise code.