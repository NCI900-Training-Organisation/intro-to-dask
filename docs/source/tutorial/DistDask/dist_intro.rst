
Dask Distributed
---------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 1 Hour

        **Objectives:**
            #. Learn about Dask Distributed.


Dask Distributed is a powerful extension of Dask that enables parallel and distributed computing across multiple machines or a cluster of machines. 
It allows users to scale their Dask computations beyond a single machine by distributing tasks across a cluster, utilizing multiple workers. 
Dask Distributed provides a **distributed scheduler**, which manages the execution of tasks, handles dependencies, and balances workloads efficiently. 
With Dask Distributed, users can easily scale from local machine execution to large-scale, high-performance clusters, making it ideal for large 
datasets, high computational workloads, and complex machine learning or data processing tasks. It integrates seamlessly with other Dask collections 
like `Dask DataFrame`, `Dask Array`, and `Dask Delayed`.

.. image:: ../../figs/dask_cluster.png

Users interact with the scheduler through a Python session by submitting tasks using `client.submit(function, *args, **kwargs)` for custom 
control or via Dask collections like `dask.array` and `dask.dataframe`. The dynamic asynchronous task scheduler that coordinates multiple worker processes across 
machines and handles requests  from multiple clients. The scheduler tracks tasks as a constantly evolving directed acyclic graph (DAG), where each 
task is a Python function operating on objects,  potentially the results of other tasks. This graph grows with user-submitted computations and 
shrinks as tasks are completed or discarded.


Local cluster
^^^^^^^^^^^^^^

A local cluster in Dask refers to running Dask's distributed computing framework on your local machine, using multiple processes or threads for 
parallelism without needing a distributed system or cluster of remote machines. It allows you to take advantage of multiple CPU cores on a 
single machine to parallelize tasks and scale computations for large datasets. 

..  code-block:: python
    :linenos:

    from dask.distributed import Client, LocalCluster

    # Set up a local cluster with a specific number of workers
    cluster = LocalCluster(n_workers=4)

    # Connect to the local cluster
    client = Client(cluster)

    # Submit a task to the local cluster
    future = client.submit(lambda x: x + 1, 10)

    # Get the result once the task is complete
    result = future.result()


PBS cluster
^^^^^^^^^^^^^^



    
.. admonition:: Key Points
   :class: hint

    #. Dask futures help submit tasks for execution, track their progress, and retrieve results asynchronously
    #. Futures provide a flexible mechanism for parallelizing custom workflows and handling complex task dependencies.