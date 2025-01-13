Eager Compilation in Numba
--------------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn the difference between eager and lazy compilation.
            #. Learn how to use eager compilation.

Typically, Numba operates in *lazy compilation* mode. 

..  code-block:: python
    :linenos:

    @jit
    def f(x, y):
        return x + y

In this mode, the compilation is postponed until the function is executed for the first time. 
During execution, Numba infers the argument types and generates optimized code based on this 
information. Additionally, Numba can create separate specialized versions of the code depending 
on the input types.

In *eager compilation* mode you can also tell Numba the function signature you are expecting.

..  code-block:: python
    :emphasize-lines: 1
    :linenos:

    @jit(int32(int32, int32))
    def f(x, y):
        return x + y

The function signature is `int32(int32, int32)`. In this case, the `@jit` decorator will 
compile the specific specialization for this signature, and no other specializations will 
be permitted. If you omit the return type and use `(int32, int32)` instead of `int32(int32, int32)`, 
Numba will infer the return type automatically.

.. admonition:: Key Points
   :class: hint

    #. Generally we go for lazy compilation in Numba.
    #. Eager compilation gives more control over types chosen by the compiler.