Memory Management
-----------------

.. admonition:: Overview
   :class: Overview

    * **Tutorial:** 5 min

        **Objectives:**
            #. Learn how we can manually manage memory in Numba.

Although Numba can automatically transfer NumPy arrays to the device and back, we can prevent 
unnecessary transfers by manually controlling the transfer process.

Host to device copy:

..  code-block:: python
    :linenos:

    data_cpu = np.arange(10)
    data_gpu = cuda.to_device(data_cpu)

Device to host copy:

..  code-block:: python
    :linenos:

    data_cpu = data_gpu.copy_to_host()


or ...

..  code-block:: python
    :linenos:

    data_gpu.copy_to_host(data_cpu)



.. admonition:: Key Points
   :class: hint

    #. Data management is automatic in Numba, but we can also manage it manually.