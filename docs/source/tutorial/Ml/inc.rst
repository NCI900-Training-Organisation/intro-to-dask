Incremental Learning
----------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 Minutes

        **Objectives:**
            #. Learn how to do incremental learning in Dask.

Some estimators can be trained incrementally, processing data without needing to load the entire dataset at once. Typically, if you pass a Dask Array to 
an estimator that expects a NumPy array, the Dask Array will be converted into a single large NumPy array. On a single machine, this could lead to 
running out of RAM and causing the program to crash. On a distributed cluster, all workers would send their data to a single machine, potentially 
causing a crash. Dask-ML addresses this by wrapping the underlying estimator in an `Incremental` class, allowing each block of the Dask Array to be 
passed sequentially to the estimator.

Here we a first generate a small training data

..  code-block:: python
    :linenos:

    from dask_ml.datasets import make_classification
    from dask_ml.wrappers import Incremental    
    from sklearn.linear_model import SGDClassifier

    X, y = make_classification(chunks=25)
    estimator = SGDClassifier(random_state=10, max_iter=100)
    clf = Incremental(estimator)

    clf.fit(X, y, classes=[0, 1])


You instantiate the underlying estimator as usual, we can also use the regular `.fit` method. Dask-ML handles passing each block of the data to 
the underlying estimator automatically.


.. admonition:: Key Points
   :class: hint

    #. `Incremental` can be used for incremental learning in Dask.