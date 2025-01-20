Parallel Prediction
----------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 Minutes

        **Objectives:**
            #. Learn how to do parallel prediction in Dask.

`ParallelPostFit` in Dask-ML wraps around a scikit-learn model, enabling parallel prediction and transformation operations. While the model's 
training step remains on a single machine, the predict and transform methods are executed in parallel using Dask, which is especially useful for 
large datasets that exceed a single machine's memory.

Here we a first generate a small training data

..  code-block:: python
    :linenos:

    from sklearn.ensemble import GradientBoostingClassifier
    import sklearn.datasets
    import dask_ml.datasets
    from dask_ml.wrappers import ParallelPostFit

    X, y = sklearn.datasets.make_classification(n_samples=1000, random_state=0)

and then wrap the `GradientBoostingClassifier` model using `ParallelPostFit`. 

..  code-block:: python
    :linenos:

    clf = ParallelPostFit(estimator=GradientBoostingClassifier())
    clf.fit(X, y)

The training occurs on a single node, while the prediction is distributed across the cluster, returning a Dask array.

..  code-block:: python
    :linenos:

    X_big, _ = dask_ml.datasets.make_classification(n_samples=100000, chunks=10000, random_state=0)
    clf.predict(X_big)


.. admonition:: Key Points
   :class: hint

    #. `ParallelPostFit` can be used to parallelize prediction across a cluster.