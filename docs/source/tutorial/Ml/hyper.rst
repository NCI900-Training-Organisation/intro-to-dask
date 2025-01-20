Hyper Parameter Search
----------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 Minutes

        **Objectives:**
            #. Learn how to do hyper parameter searching in Dask.

Hyperparameter search is a critical step in machine learning, involving the optimization of model parameters to improve performance. 
These searches can be highly time-consuming, often taking days particularly when working with large datasets. Dask-ML enhances 
this process by scaling hyperparameter searches across large datasets and distributed systems, overcoming the limitations of traditional single-machine 
workflows in libraries like scikit-learn.

..  code-block:: python
    :linenos:

    from dask.distributed import Client
    client = Client()

Create a client to submit tasks to the Dask cluster.

..  code-block:: python
    :linenos:
    
    from dask_ml.datasets import make_classification
    from dask_ml.model_selection import train_test_split

    X, y = make_classification(chunks=20, random_state=0)
    X_train, X_test, y_train, y_test = train_test_split(X, y)

Generate synthetic classification datasets and splits datasets into training and testing sets.

..  code-block:: python
    :linenos:

    from sklearn.linear_model import SGDClassifier

    clf = SGDClassifier(tol=1e-3, penalty='elasticnet', random_state=0)


Create and instance of a linear classifier in scikit-learn that uses Stochastic Gradient Descent (SGD) for optimization. It's well-suited for large 
datasets and supports various loss functions and penalties, making it versatile for tasks like classification and regression.

..  code-block:: python
    :linenos:

    from scipy.stats import uniform, loguniform

    params = {'alpha': loguniform(1e-2, 1e0),  # or np.logspace
          'l1_ratio': uniform(0, 1)}  # or np.linspace

The dictionary, `params`, specifies the search space for hyperparameters. The hyperparemeters `alpha` and `l1_ratio` are two hyperparameters 
that belongs to SGDClassifier.


..  code-block:: python
    :linenos:

    from dask_ml.model_selection import HyperbandSearchCV

    search = HyperbandSearchCV(clf, params, max_iter=81, random_state=0)

    search.fit(X_train, y_train, classes=[0, 1])

    search.best_params_

`HyperbandSearchCV` efficiently searches for the best combination of hyperparameters by adaptively allocating resources to more promising candidates.
`search.fit()` Starts the hyperparameter tuning process using the provided training data (`X_train` and `y_train`). Finally, `search.best_params_`
will have the best hyperparameters for the model `clf`.


Implicit Usage in Dask-ML Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The client is not explicitly referenced after being created, but it is used under the hood by Dask-ML functions that leverage distributed computing.

* The generated dataset (X, y) is a Dask array, which divides the data into chunks (specified by chunks=20).
* These chunks can be processed in parallel across multiple workers managed by the Dask Client.
* The `train_test_split` function operates on the Dask arrays, which means the splitting process is distributed and parallelized across workers.
* `HyperbandSearchCV` uses Dask to parallelize the search for optimal hyperparameters across multiple workers.


.. admonition:: Key Points
   :class: hint

    #. Dask-ML can speedup hyperparameter search. 