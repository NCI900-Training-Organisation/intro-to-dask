Distributed Learning
----------------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 20 Minutes

        **Objectives:**
            - Learn how to do distributed learning in Dask.

Distributed learning in Dask splits both the data and computation across multiple machines or nodes in a cluster, leveraging parallelism for faster 
training. Incremental learning focuses on processing data in smaller chunks sequentially, updating the model after each chunk, and does not 
require the whole dataset in memory. While Distributed learning distributes the entire dataset across a cluster of workers, where each worker 
processes a portion of the data in parallel. 

Dask distributes the data and computation across the cluster, where each worker processes its portion of the data in parallel.
After each worker processes its chunk of the data, the results are aggregated to update the model. The model's parameters are synchronized across 
the workers, and training happens concurrently across the distributed setup.

..  code-block:: python
    :linenos:

    from dask.distributed import Client
    from dask_ml.datasets import make_classification
    from dask_ml.model_selection import train_test_split
    from sklearn.linear_model import SGDClassifier
    from dask_ml.model_selection import GridSearchCV
    from scipy.stats import uniform, loguniform

    # Step 1: Start a Dask client
    client = Client()

    # Step 2: Create a synthetic classification dataset with Dask
    X, y = make_classification(n_samples=5000, n_features=20, chunks=25, random_state=0)

    # Step 3: Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y)

    # Step 4: Initialize the estimator (SGDClassifier)
    estimator = SGDClassifier(random_state=10, max_iter=100)

    # Step 5: Define the parameter grid for hyperparameter tuning
    params = {
        'alpha': loguniform(1e-5, 1e-1),
        'l1_ratio': uniform(0, 1)
    }

    # Step 6: Set up GridSearchCV using Dask-ML to distribute hyperparameter search
    search = GridSearchCV(estimator, params, cv=3)

    # Step 7: Fit the model (this will distribute the computation across the Dask cluster)
    search.fit(X_train, y_train)

    # Step 8: Output the best hyperparameters and best score
    print("Best parameters:", search.best_params_)
    print("Best score:", search.best_score_)

    # Step 9: Evaluate the model on the test set
    test_score = search.score(X_test, y_test)
    print("Test score:", test_score)

    # Close the Dask client when done
    client.close()



.. admonition:: Key Points
   :class: hint

        - Distributed learning can help with large dataset in Dask..