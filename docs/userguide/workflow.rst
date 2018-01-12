.. _label-workflow:

Composing a workflow
====================

Workflows in Parsl are created implicitly based on the passing of control or data between ``Apps``. The flexibility of this model allows for the creation of a wide range of workflows from sequential through to complex nested, parallel workflows. As we will see below a range of workflows can be created by passing AppFutures and DataFutures between ``Apps``.

Parsl is also designed to address the requirements of a range of workflows from those that run a large number of very small tasks through to those that run few long running tasks. In each case, Parsl can be configured to optimize deployment towards performance or fault tolerance.

Below we illustrate a range of workflow patterns, however it is important to note that this set of examples is by no means comprehensive.


Procedural workflows
--------------------

Simple sequential or procedural workflows can be created by passing an AppFuture from one task to another. The following example shows one such workflow which first generates a random number and then writes it to a file. Note: in this case we combine a Pyhton and a Bash ``App`` seamlessly.

.. code-block:: python

      # Generate a random number
      @App('python', dfk)
      def generate(limit):
            from random import randint
            """Generate a random integer and return it"""
            return randint(1,limit)

      # write a message to a file
      @App('bash', dfk)
      def save(message, outputs=[]):
            return 'echo %s &> {outputs[0]}' % (message)

      message = generate(10)

      saved = save(message, outputs=['output.txt'])

      with open(saved.outputs[0].result(), 'r') as f:
            print(f.read())


Parallel workflows
------------------

Parallel execution occurs automatically in Parsl, respecting dependencies among ``App`` executions. The following example shows how a single ``App`` can be used with and without dependencies to demonstrate parallel execution.

.. code-block:: python

      @App('python', dfk)
      def wait_sleep_double(x, fu_1, fu_2):
           import time
           time.sleep(2)   # Sleep for 2 seconds
           return x*2

      # Launch two apps, which will execute in parallel, since they don't have to
      # wait on any futures
      doubled_x = wait_sleep_double(10, None, None)
      doubled_y = wait_sleep_double(10, None, None)

      # The third depends on the first two :
      #    doubled_x   doubled_y     (2 s)
      #           \     /
      #           doublex_z          (2 s)
      doubled_z = wait_sleep_double(10, doubled_x, doubled_y)

      # doubled_z will be done in ~4s
      print(doubled_z.result())

Parallel workflows with loops
-----------------------------

The most common way that Parsl ``Apps`` are executed in parallel is via looping. The following example shows how a simple loop can be used to create many random numbers in parallel.

.. code-block:: python

    @App('python', dfk)
    def generate(limit):
        from random import randint
        """Generate a random integer and return it"""
        return randint(1,limit)

    rand_nums = []
    for i in range(1,5):
        rand_nums.append(generate(i))

    # wait for all apps to finish and collect the results
    outputs = [i.result() for i in rand_nums]



Parallel dataflows
------------------

Parallel dataflows can be developed by passing data between ``Apps``. In this example we create a set of files, each with a random number, we then concatenate these files into a single file and compute the sum of all numbers in that file. In the first two ``Apps`` files are exchanged. The final ``App`` returns the sum as a Python integer.

.. code-block:: python

      @App('bash', dfk)
      def generate(outputs=[]):
          return 'echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) &> {outputs[0]}'

      @App('bash', dfk)
      def concat(inputs=[], outputs=[], stdout='stdout.txt', stderr='stderr.txt'):
          return 'cat {0} >> {1}'.format(' '.join(inputs), outputs[0])

      @App('python', dfk)
      def total(inputs=[]):
          total = 0
          with open(inputs[0], 'r') as f:
              for l in f:
                  total += int(l)
          return total

      # create 5 files with random numbers
      output_files = []
      for i in range (5):
           output_files.append(generate(outputs=['random-%s.txt' % i]))

      # concatenate the files into a single file
      cc = concat(inputs=[i.outputs[0] for i in output_files], outputs=['all.txt'])

      # calculate the average of the random numbers
      totals = total(inputs=[cc.outputs[0]])

      print (totals.result())
