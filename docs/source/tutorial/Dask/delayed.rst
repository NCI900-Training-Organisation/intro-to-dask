Dask Delayed
---------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 min

        **Objectives:**
            - Learn about Dask Delayed.


Dask Delayed is a powerful tool within the Dask library that allows you to parallelize and optimize custom Python functions by transforming them into 
lazy, deferred computations. It allows you to build complex workflows by delaying the execution of operations and deferring the computation until 
explicitly requested (e.g., with `.compute()`), enabling you to perform operations in parallel without needing to modify your existing code significantly.

Key Concepts of Dask Delayed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Lazy Evaluation** : When you wrap a function with `dask.delayed()`, the function is not executed immediately. Instead, a task graph is built that represents the function and its dependencies.
The actual computation is triggered when you call `.compute()`, which runs the task graph.

2. **Task Graph** : Dask Delayed constructs a task graph that represents the sequence of operations. Each function call wrapped by dask.delayed() is turned into a node in the graph, with dependencies between tasks defined by their function calls and inputs.

3. **Parallel Execution** : Once the task graph is built, Dask can execute the operations in parallel across multiple CPU cores or distributed workers. Dask schedules and runs tasks based on the task graph, respecting the task dependencies.

4. **Minimal Overhead** : Dask Delayed introduces minimal overhead when applied to regular Python functions, making it a flexible tool to scale up existing Python workflows without requiring major changes

How to Use Dask Delayed?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Wrap Python Functions** : You can wrap normal Python functions (such as those performing I/O, mathematical computations, or data processing) in dask.delayed().
These functions remain unchanged but are delayed for execution until .compute() is called.

2. **Building a Task Graph** : You can chain multiple delayed functions together, which creates a series of tasks and their dependencies, forming a graph of computations.

3. **Compute the Result** : Once all tasks are defined, call .compute() to trigger the execution of the entire workflow. Dask will optimize the task execution, running independent tasks in parallel and handling dependencies

..  code-block:: python
    :linenos:

    import dask
    from dask import delayed

    # Define normal Python functions
    def add(x, y):
        return x + y

    def multiply(x, y):
        return x * y

    # Wrap the functions with dask.delayed()
    x = delayed(add)(10, 20)       # Add 10 and 20
    y = delayed(multiply)(x, 2)    # Multiply the result of 'add' by 2

    # Chain the tasks
    result = delayed(add)(y, 5)    # Add 5 to the result of the previous operation

    # Visualize the task graph (optional)
    result.visualize(filename="task_graph.png")

    # Compute the result
    final_result = result.compute()


Key Advantages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. **Minimal Changes to Code** : You can easily parallelize your existing code without requiring a major rewrite. Just decorate your functions with dask.delayed() and chain them together.

2. **Fine-Grained Control** : Unlike higher-level collections like Dask DataFrame or Array, Dask Delayed gives you fine-grained control over how your computations are parallelized. You can handle more custom workflows that don’t fit neatly into the other Dask collections.

3. **Optimized Scheduling** :  Dask can optimize the execution order of tasks based on their dependencies. This ensures efficient resource utilization by running independent tasks in parallel.


.. admonition:: Key Points
   :class: hint

        - Dask delayed is useful for workflows that involve custom, complex operations or tasks that don’t fit into Dask’s higher-level collections. 
        - It offers a simple way to parallelize Python functions using minimal changes to your code.