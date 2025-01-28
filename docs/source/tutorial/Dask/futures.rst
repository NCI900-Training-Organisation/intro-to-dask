Dask Futures
---------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 min

        **Objectives:**
            - Learn about Dask Futures.


Futures are a low-level abstraction that allows for fine-grained control over parallel and distributed computations. They represent a placeholder for 
a result that is being computed asynchronously. A future is created when you submit a task to be computed, and it will eventually hold the result 
once the computation completes. Futures allow you to execute computations in parallel while controlling when and how to access the results.

Key Concepts of Dask Futures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Asynchronous Computation** : Futures in Dask represent tasks that are being computed asynchronously. They don’t block the rest of your program, allowing you to continue with other work while waiting for the result. When you submit a task, Dask schedules it to run on available workers. A Future object is returned, which represents the eventual result of that task.

2. **Task Graph** : A task graph is constructed when you use Dask Futures, defining the sequence of computations to be done. Each task is represented as a node, and the edges represent data dependencies. Dask uses this graph to optimize scheduling and parallelism.

3. **Parallel Execution** : Dask can execute multiple futures in parallel, taking advantage of available CPU cores or distributed workers. Futures allow Dask to scale computation and distribute tasks across workers, making it suitable for both small and large-scale computations.

4. **Non-blocking** : Futures allow for non-blocking execution. You can submit multiple tasks (futures) and work with them as they finish. The .result() method on a future can be used to retrieve the result once it's available. If the result isn’t ready yet, the call to `.result()` will block until the computation is finished.

5. **Distributed and Local Executors** : Dask Futures can run on either a local thread pool or a distributed cluster. In a distributed cluster, tasks are sent to workers across multiple machines. In a local setup, tasks are executed on the local machine using a thread pool or multiple processes.

6. **Interaction with dask.delayed()** : Futures can be integrated with higher-level Dask abstractions like `dask.delayed()` to perform asynchronous computations. You can submit delayed tasks to a Dask scheduler and get back future objects that you can use to monitor or collect results when they are ready.

How to Use Dask Futures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Submitting Tasks:** To create a future, you can use the submit() function from Dask's Client. This function schedules a computation to be executed on a worker and returns a future representing the result.

2. **Accessing Results:** You can retrieve the result of a future using the .result() method. If the task is not yet completed, it will block until the result is available.

3. **Multiple Futures:** You can submit multiple tasks and then gather their results asynchronously, which allows you to manage parallel execution efficiently

..  code-block:: python
    :linenos:

    from dask.distributed import Client, as_completed

    # Connect to the Dask cluster (this can be a local or distributed cluster)
    client = Client()

    # Function to perform some computation
    def square(x):
        return x ** 2

    # Submit a task to the Dask cluster and get a future
    future = client.submit(square, 10)

    # Perform other work while waiting for the result
    print("Doing other work...")

    # Get the result once it's available
    result = future.result()

Key Methods with Dask Futures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. `submit()`: Schedules a task for execution on a Dask worker and returns a future.

2. `result()`: Blocks until the result of the computation is available and then returns the result.

3. `cancel()`: Cancels a future before the computation is finished. Useful if you no longer need the result or want to halt an expensive computation.

4. `done()`: Returns True if the future has completed (either successfully or with an error).

5. `as_completed()`: A utility that returns futures as they complete, allowing you to process results as soon as they are available, rather than waiting for all tasks to complete.


.. admonition:: Key Points
   :class: hint

        - Dask futures help submit tasks for execution, track their progress, and retrieve results asynchronously
        - Futures provide a flexible mechanism for parallelizing custom workflows and handling complex task dependencies.