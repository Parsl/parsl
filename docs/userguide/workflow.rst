.. _label-workflow:

Composing a workflow
====================

Workflows in Parsl are created implicitly based on the passing of control or data between apps. The flexibility of this model allows for the implementation of a wide range of workflow patterns from sequential through to complex nested, parallel workflows. 

Parsl is also designed to address broad execution requirements from workflows that run a large number of very small tasks to those that run few long running tasks. In each case, Parsl can be configured to optimize deployment towards performance or fault tolerance.

Below we illustrate a range of workflow patterns, however it is important to note that this set of examples is by no means comprehensive.


Procedural workflows
--------------------

Simple sequential or procedural workflows can be created by passing an AppFuture from one task to another. The following example shows one such workflow which first generates a random number and then writes it to a file. Note that this example demonstrates the use of both Python and Bash apps.

.. code-block:: python

      # Generate a random number
      @python_app
      def generate(limit):
            from random import randint
            """Generate a random integer and return it"""
            return randint(1,limit)

      # Write a message to a file
      @bash_app
      def save(message, outputs=[]):
            return 'echo {} &> {}'.format(message, outputs[0])

      message = generate(10)

      saved = save(message, outputs=['output.txt'])

      with open(saved.outputs[0].result(), 'r') as f:
            print(f.read())


Parallel workflows
------------------

Parallel execution occurs automatically in Parsl, respecting dependencies among app executions. The following example shows how a single app can be used with and without dependencies to demonstrate parallel execution.

.. code-block:: python

      @python_app
      def wait_sleep_double(x, foo_1, foo_2):
           import time
           time.sleep(2)   # Sleep for 2 seconds
           return x*2

      # Launch two apps, which will execute in parallel, since they do not have to
      # wait on any futures
      doubled_x = wait_sleep_double(10, None, None)
      doubled_y = wait_sleep_double(10, None, None)

      # The third depends on the first two:
      #    doubled_x   doubled_y     (2 s)
      #           \     /
      #           doublex_z          (2 s)
      doubled_z = wait_sleep_double(10, doubled_x, doubled_y)

      # doubled_z will be done in ~4s
      print(doubled_z.result())

Parallel workflows with loops
-----------------------------

One of the most common ways that Parsl apps are executed in parallel is via loops. The following example shows how a simple loop can be used to create many random numbers in parallel.

.. code-block:: python

    @python_app
    def generate(limit):
        from random import randint
        """Generate a random integer and return it"""
        return randint(1,limit)

    rand_nums = []
    for i in range(1,5):
        rand_nums.append(generate(i))

    # Wait for all apps to finish and collect the results
    outputs = [i.result() for i in rand_nums]



Parallel dataflows
------------------

Parallel dataflows can be developed by passing data between apps. In the following example a set of files, each with a random number, is created by the generate app. These files are then concatenated into a single file, which is subsequently used to compute the sum of all numbers. 

.. code-block:: python

      @bash_app
      def generate(outputs=[]):
          return 'echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) &> {}'.format(outputs[0])

      @bash_app
      def concat(inputs=[], outputs=[], stdout='stdout.txt', stderr='stderr.txt'):
          return 'cat {0} >> {1}'.format(' '.join(inputs), outputs[0])

      @python_app
      def total(inputs=[]):
          total = 0
          with open(inputs[0].filepath, 'r') as f:
              for l in f:
                  total += int(l)
          return total

      # Create 5 files with random numbers
      output_files = []
      for i in range (5):
           output_files.append(generate(outputs=['random-%s.txt' % i]))

      # Concatenate the files into a single file
      cc = concat(inputs=[i.outputs[0] for i in output_files], outputs=['all.txt'])

      # Calculate the average of the random numbers
      totals = total(inputs=[cc.outputs[0]])

      print(totals.result())
