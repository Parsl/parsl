13. Example Parallel Patterns
=============================

Parsl can be used to implement a wide range of parallel programming patterns. These patterns help you structure your code to take advantage of parallel processing, making your programs run faster and more efficiently.

Bag of Tasks
------------

This is the simplest pattern, where you have a bunch of independent tasks that can be run in any order. Parsl will take care of running these tasks concurrently, utilizing the available resources.

.. code-block:: python

   from parsl import python_app
   parsl.load()

   @python_app
   def app_random():
       import random
       return random.random()

   results = []
   for i in range(0, 10):
       x = app_random()
       results.append(x)

   for r in results:
       print(r.result())

This script defines a task called `app_random` that generates a random number. It then creates 10 instances of this task and runs them in parallel. Finally, it prints the results as they become available.

Sequential Workflows
--------------------

Sometimes, tasks need to be run in a specific order, one after the other. Parsl can handle this too. You can create sequential workflows by passing the result of one task as input to the next task.

.. code-block:: python

   from parsl import python_app, bash_app
   parsl.load()

   # Generate a random number
   @python_app
   def generate(limit):
       from random import randint
       return randint(1, limit)

   # Write a message to a file
   @bash_app
   def save(message, outputs=[]):
       return 'echo {} &> {}'.format(message, outputs[0])

   message = generate(10)
   saved = save(message, outputs=['output.txt'])
   with open(saved.outputs[0].result(), 'r') as f:
       print(f.read())

In this example, the `generate` task produces a random number, which is then passed to the `save` task to be written to a file. The `save` task cannot start until the `generate` has finished, ensuring a sequential workflow.

Parallel Workflows
------------------

Parsl automatically runs tasks in parallel whenever possible. If tasks don't depend on each other's results, they can run at the same time.

.. code-block:: python

   from parsl import python_app
   parsl.load()

   @python_app
   def wait_sleep_double(x, foo_1, foo_2):
       import time
       time.sleep(2)  # Sleep for 2 seconds
       return x * 2

   # Launch two apps in parallel
   doubled_x = wait_sleep_double(10, None, None)
   doubled_y = wait_sleep_double(10, None, None)

   # This app depends on the first two
   doubled_z = wait_sleep_double(10, doubled_x, doubled_y)

   print(doubled_z.result())  # Result will be available in ~4 seconds

In this example, the first two calls to `wait_sleep_double` run in parallel because they don't depend on each other. The third call depends on the results of the first two, so it will wait for them to finish before starting.

Parallel Workflows with Loops
------------------------------

You can easily create many parallel tasks using loops.

.. code-block:: python

   from parsl import python_app
   parsl.load()

   @python_app
   def generate(limit):
       from random import randint
       return randint(1, limit)

   rand_nums = []
   for i in range(1, 5):
       rand_nums.append(generate(i))

   # Wait for all apps to finish and collect the results
   outputs = [r.result() for r in rand_nums]
   print(outputs)

This script creates five tasks that generate random numbers and runs them in parallel.

MapReduce
---------

MapReduce is a common pattern for processing large amounts of data. It has two steps:
- **Map**: Each data is processed separately.
- **Reduce**: The results from the map step are combined into a final result.

Parsl can easily implement MapReduce.

.. code-block:: python

   from parsl import python_app
   parsl.load()

   # Map function that doubles the input
   @python_app
   def app_double(x):
       return x * 2

   # Reduce function that sums the inputs
   @python_app
   def app_sum(inputs=[]):
       return sum(inputs)

   # Create a list of numbers
   items = range(0, 4)

   # Map phase: double each number
   mapped_results = []
   for i in items:
       x = app_double(i)
       mapped_results.append(x)

   # Reduce phase: sum the doubled numbers
   total = app_sum(inputs=mapped_results)
   print(total.result())

In this example, the `app_double` function doubles each number in the input list. The `app_sum` function then adds up all the doubled numbers.

Caching Expensive Initialization Between Tasks
----------------------------------------------

Some tasks need to do a lot of setup work before they can start. If you're running the same task multiple times, you can save time by doing the setup only once and then reusing it for each task. Parsl can help you do this.

Advanced Parsl Programming
--------------------------

Parsl offers advanced features that let you create more complex and efficient parallel workflows. These features include lifted operators, join apps, sub-workflows, and integration with other components. As your Parsl programs grow, it's important to structure them in a way that makes them easy to understand and maintain. One way to do this is to split your code into multiple files or modules. For example, you could have one file for your Parsl configuration, another file for your app definitions, and a third file for your main workflow logic.

Lifted Operators
----------------

Parsl allows you to use some Python operators (like `[]` and `.`) directly on futures. This can make your code more concise and easier to read.

- **Lifted `[]` Operator**: If a Parsl app returns a list or a dictionary, you can use the `[]` operator in the future to access an element of the list or dictionary. Parsl will automatically wait for the task to finish and then return the requested element.
- **Lifted `.` Operator**: Similarly, if a Parsl app returns an object, you can use the `.` operator on the future to access an attribute of the object.

Join Apps
---------

Join apps are a special type of Parsl app that allows you to create sub-workflows. A join app can launch other Parsl apps and wait for them to finish before returning a result. This can be useful for creating complex workflows with multiple stages.

Using Sub-Workflows
-------------------

Sub-workflows are a way to break down a large workflow into smaller, more manageable pieces. Each sub-workflow can be defined as a separate Parsl script, and you can use join apps to connect the sub-workflows together.

Integrating Futures from Other Components
-----------------------------------------

Parsl can also work with futures from other libraries or frameworks. This allows you to integrate Parsl with other tools and create hybrid workflows that combine different types of tasks.

Practical Example: Advanced Parsl Techniques
--------------------------------------------

.. code-block:: python

   import parsl
   from parsl import python_app, join_app
   parsl.load()

   @python_app
   def preprocess_data(data):
       # ... preprocessing logic ...
       return processed_data

   @python_app
   def train_model(processed_data):
       # ... model training logic ...
       return model

   @python_app
   def evaluate_model(model, test_data):
       # ... model evaluation logic ...
       return accuracy

   @join_app
   def run_experiment(data, test_data):
       processed_data = preprocess_data(data)
       model = train_model(processed_data)
       return evaluate_model(model, test_data)

   # Run the experiment
   result_future = run_experiment(training_data, testing_data)
   accuracy = result_future.result()
   print(f"Accuracy: {accuracy}")

In this example, we define a sub-workflow `run_experiment` that consists of three tasks: `preprocess_data`, `train_model`, and `evaluate_model`. The `join_app` decorator allows us to launch these tasks in sequence and wait for them to finish before returning the final result.
