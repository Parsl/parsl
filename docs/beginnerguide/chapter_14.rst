14. Structuring Parsl Programs
==============================

Introduction
------------

Parsl is a powerful tool for running tasks in parallel, but to use it effectively, you need to organize your code well. This chapter will teach you how to structure your Parsl projects so they are easy to understand, maintain, and scale.

Why Structure Matters
---------------------

Unstructured code can be messy and difficult to use. It's like having a toolbox with tools scattered everywhere – you can't find what you need when you need it. Structured code, on the other hand, is organized and easy to follow. It's like having a toolbox with everything in its place, making it easy to find the right tool for the job.

When your Parsl code is structured, it's easier to:

- **Read and understand**: You can quickly see how different parts of your code fit together.
- **Debug**: You can isolate problems easier.
- **Maintain**: You can modify your code without breaking other parts.
- **Share**: Others can understand and use your code easier.

The Basic Building Blocks
-------------------------

- **Apps**: These are the individual tasks you want to run in parallel. You define them as Python functions or Bash scripts.
- **Dependencies**: Sometimes, one task needs to wait for another to finish before it can start. You can specify these dependencies in Parsl.
- **Dataflow**: This is how your apps share information. Parsl can automatically move data between tasks, even if they are running on different computers.

Common Parsl Structures
------------------------

- **Bag of Tasks**: This is the simplest structure, where you have many independent tasks that can run in any order.
- **Sequential Workflows**: This is where tasks run one after the other, in a specific order.
- **Parallel Workflows**: This is where multiple tasks run at the same time.
- **Loops and MapReduce**: These are ways to handle situations where you need to repeat the same task many times with different data.

Modularizing Your Code
----------------------

To make your Parsl code more organized, you can split it into smaller parts. This is called modularization.

- **Divide and Conquer**: Instead of having all your code in one big file, you can break it into smaller files. This makes it easier to test and reuse parts of your code.
- **Configs and Apps**: You can separate the settings for your Parsl environment (like how many workers to use) from the code that defines your tasks. This makes it easy to change your settings without having to change your task code.

Real-World Examples
-------------------

We'll look at a real-world example of how to use Parsl to build a simple data pipeline. This pipeline will download data from the internet, process it, and then analyze it, all in parallel.

Here's a real-world example of a simple data pipeline using Parsl, explained in simple terms:

.. code-block:: python

   import parsl
   from parsl import python_app, bash_app

   # 1. Set up Parsl to run on your computer
   parsl.load()

   # 2. Define the tasks as Parsl apps
   @bash_app
   def download_webpage(url, outputs=[]):
       return f'wget -O {outputs[0]} {url}'

   @python_app
   def count_words(filename):
       with open(filename, 'r') as f:
           text = f.read()
       return len(text.split())

   @python_app
   def average_word_count(counts):
       return sum(counts) / len(counts)

   # 3. Create a list of websites
   websites = ['https://www.example1.com', 'https://www.example2.com', 'https://www.example3.com']

   # 4. Download the webpages in parallel
   downloaded_files = []
   for url in websites:
       output_file = f'webpage_{websites.index(url)}.html'
       future = download_webpage(url, outputs=[output_file])
       downloaded_files.append(future.outputs[0])  # Get the DataFuture for the output file

   # 5. Count the words in each webpage in parallel
   word_counts = []
   for file in downloaded_files:
       future = count_words(file)
       word_counts.append(future)

   # 6. Calculate the average word count
   average = average_word_count(word_counts)

   # 7. Get the final result
   print(f"Average word count: {average.result()}")

Explanation
-----------

1. **Import Parsl**: We start by importing the Parsl library.
2. **Load Parsl Configuration**: We tell Parsl to use our computer's resources by calling `parsl.load()`.
3. **Define Apps**:
   - `download_webpage`: This Bash app uses the `wget` command to download a webpage and save it to a file.
   - `count_words`: This Python app reads a file, splits it into words, and counts them.
   - `average_word_count`: This Python app calculates the average of a list of numbers.
4. **Create a List of Websites**: We have a list of website addresses that we want to process.
5. **Download Webpages in Parallel**: We use a loop to call the `download_webpage` app for each website. This creates multiple tasks that run in parallel, downloading the web pages simultaneously.
6. **Count Words in Parallel**: We use another loop to call the `count_words` app for each downloaded file. This also creates multiple tasks that run in parallel, counting the words in each web page simultaneously.
7. **Calculate Average**: We call the `average_word_count` app to calculate the average of the word counts.
8. **Get the Result**: Finally, we use `.result()` to wait for the average task to finish and then print the result.
