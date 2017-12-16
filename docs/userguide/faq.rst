FAQ
========


#. How can I debug a Parsl script

    Parsl interfaces with the Python logger. To enable logging to stdout turn on the logger as follows. Alternatively, you can configure the stream logger to write to an output file. 

    .. code-block:: python
    
            parsl.set_stream_logger()


#. How can I view outputs and errors from Apps

    Parsl Apps include keyword arguments for capturing stderr and stdout in files. 

#. How can I make an App dependent on multiple inputs

    You can pass many futures in to a single App. The App will wait for all inputs to be satisfied before execution. 

#. Can I pass any Python object between Apps

    No. Unfortunately, only picklable objects can be passed between Apps. For objects that can't be pickled it is easiest to serialize the object into a file and use files to communicate between Apps.
 
#. How do I specify where Apps should be run. 

    Parsl's multi-site support allows you to define the site (including local threads) on which an App should be executed. 
