Dask Basics
------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 10 min

        **Objectives:**
            #. Learn about Dask.
            #. Learn about Dask collections.


Dask is a Python library for parallel and distributed computing that is easy to use and set up, as it functions just like any other Python library. 
It is a powerful tool for scaling data science and machine learning workloads across distributed hardware, unlocking the ability to handle complex 
algorithms and larger-than-memory computations efficiently. Designed with Python users in mind, Dask integrates seamlessly with popular libraries 
like NumPy, Pandas, and Scikit-Learn, offering parallelized versions of their APIs to enable distributed processing of large datasets while 
maintaining a familiar workflow.


Task Graphs
-----------

Task scheduling is a common approach to parallel execution, where programs are divided into medium-sized tasks represented as nodes in a graph, 
with edges indicating dependencies. A task scheduler executes this graph, respecting dependencies and maximizing parallelism by running 
independent tasks simultaneously. 

.. image:: ../figs/task_graph.png

Dask simplifies task scheduling in Python, using familiar constructs like dicts, tuples, and callables to 
encode computations with minimal complexity. A sample Dask graph is shown below

..  code-block:: python
    :linenos:

    def inc(i):
        return i + 1

    def add(a, b):
        return a + b

    x = 1
    y = inc(x)
    z = add(y, 10)

.. image:: ../figs/example_graph.png

The Dask library currently contains a few **schedulers** to execute these graphs.


.. admonition:: Key Points
   :class: hint

    #. Dask works by building task graphs.