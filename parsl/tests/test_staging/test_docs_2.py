from parsl import App, File


@App('bash')
def cat(inputs=[], stdout='stdout.txt'):
    return 'cat %s' % (inputs[0])


def test():
    # create a test file
    open('/tmp/test.txt', 'w').write('Hello\n')

    # create the Parsl file
    parsl_file = File('file:///tmp/test.txt')

    # call the cat app with the Parsl file
    cat(inputs=[parsl_file])
