Dask DataFrame
---------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 min

        **Objectives:**
            #. Learn about Dask DataFrame.


Dask DataFrame is a large-scale, parallelized version of the pandas DataFrame, designed to handle datasets that are larger than memory or that need 
to be distributed across multiple machines. It provides a similar API to **pandas**, making it easy for users familiar with pandas to scale their 
workflows to larger datasets.

Key Features of Dask DataFrame:
1. **Distributed Computation: ** Dask DataFrame splits data into partitions that can be processed in parallel across multiple CPU cores or machines in a distributed cluster.
Each partition is a pandas DataFrame, and operations are executed lazily on these partitions.

2. **Larger-than-memory: ** Dask DataFrame works efficiently with datasets that don’t fit into memory by breaking them down into smaller chunks (partitions). It allows operations on datasets that exceed the available system memory by reading and processing the data chunk-by-chunk.

3. **Lazy Evaluation: ** Like other Dask collections, Dask DataFrame uses lazy evaluation. When operations are performed on the DataFrame, Dask does not immediately execute them. Instead, it builds a task graph that describes the operations to be performed. Execution happens only when the `.compute()` method is called.

4. **Pandas-like API:** Dask DataFrame offers a pandas-like API, making it easier for pandas users to scale up their code without having to learn new syntax. Most pandas operations, such as `.groupby()`, `.mean()`, `.join()`, `.merge()`, and `.dropna()`, are available in Dask DataFrame.
However, not all pandas operations are supported, particularly those that are difficult to parallelize or that require a full scan of the data.

5. **Optimized for Parallelism: ** Dask DataFrame is optimized to execute operations in parallel across many partitions. For example, a groupby operation can be computed independently on each partition and then combined, leveraging parallelism.

6. **Distributed Computing Support:** Dask DataFrame can scale computations across a single machine or a cluster of machines. It integrates seamlessly with Dask’s distributed scheduler, allowing for fault tolerance, load balancing, and efficient resource utilization in a distributed environment.

..  code-block:: python
    :linenos:

    import dask.dataframe as dd

    # Create a Dask DataFrame from a set of CSV files
    ddf = dd.read_csv('data/*.csv')

    # Perform some operations (groupby, mean)
    result = df.groupby('column_name').mean()

    # Trigger computation and get the result
    result.compute()


Dask DataFrame vs. Pandas DataFrame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. **Scale:** Dask DataFrame can scale beyond memory by using partitions, while pandas DataFrame is limited by the available system memory.

2. **Lazy Evaluation:** Dask uses lazy evaluation, so computations are deferred until `.compute()` is called. Pandas executes operations immediately.

3. **Parallelism:** Dask DataFrame leverages parallel and distributed execution, making it more suitable for large-scale computations compared to pandas.

When to Use Dask DataFrame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. **Large Datasets: ** When working with datasets that are too large to fit into memory on a single machine.

2. **Parallel or Distributed Computing: ** When you need to scale computations to multiple CPU cores or distributed systems.

3. **Familiarity with pandas:** If you're already familiar with pandas, Dask DataFrame allows you to scale up your code with minimal changes.


.. admonition:: Key Points
   :class: hint

    #. Dask DataFrame is a powerful tool for handling large datasets in parallel.
    #. Dask DataFrame provides a pandas-like API that scales to larger-than-memory datasets and distributed environments.