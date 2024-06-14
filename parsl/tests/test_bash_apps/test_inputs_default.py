from parsl import AUTO_LOGNAME, bash_app, python_app


@python_app
def identity(inp):
    return inp


@bash_app
def sum_inputs(inputs=[identity(1), identity(2)], stdout=AUTO_LOGNAME):
    calc = sum(inputs)
    return f"echo {calc}"


def test_default_inputs():
    fut = sum_inputs()
    fut.result()
    with open(fut.stdout, 'r') as f:
        assert int(f.read()) == 3
