
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


..  code-block:: python
    :linenos:

    from dask.distributed import Client, as_completed

    # Connect to the Dask cluster (this can be a local or distributed cluster)
    client = Client()

    
.. admonition:: Key Points
   :class: hint

    #. Dask futures help submit tasks for execution, track their progress, and retrieve results asynchronously
    #. Futures provide a flexible mechanism for parallelizing custom workflows and handling complex task dependencies.