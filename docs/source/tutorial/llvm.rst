LLVM and Numba
------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn how to inspect IR code in Numba

LLVM Intermediate Representation (IR) is a low-level, typed programming language used as an 
intermediate step in the LLVM compiler framework. It serves as a common code representation for 
various compiler optimizations and code generation processes. Its primary purpose is to enable 
optimizations and transformations that improve the performance and efficiency of compiled code before 
it is translated into machine-specific instructions.

The code below is used to inspect and retrieve a dictionary of items related to the LLVM IR of the
Numba-compiled function f. This can be useful for debugging or understanding how Numba has 
translated Python code into low-level LLVM instructions, which can provide insights into 
the performance optimizations applied by Numba.

..  code-block:: python
    :linenos:

    @jit
    def f(x, y):
        return x + y

    for k, v in f.inspect_llvm().items():
        print(k, v)

In the code 

1. **f**: This represents a Numba-compiled function or a Numba function object. 

2. **inspect_llvm()**: This is a method provided by Numba that you can call on a Numba function
 object. When you call `inspect_llvm()`, it returns an object that contains the LLVM 
 (Low-Level Virtual Machine) Intermediate Representation (IR) for the compiled function. 
 LLVM IR is a low-level programming language used by the LLVM compiler framework to represent code during compilation.

3. **.items**: After calling `inspect_llvm()`, the returned object typically has an `items` 
attribute. This attribute is a dictionary-like object that contains information about the LLVM IR of the function. Each entry in this dictionary represents a part of the LLVM IR, and it may include details such as function signatures, LLVM IR instructions, and other relevant information.




.. admonition:: Key Points
   :class: hint

    #. Numba generated LLVM code can be inspected.