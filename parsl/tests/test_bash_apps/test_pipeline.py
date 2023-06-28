import pytest

from parsl.app.app import bash_app
from parsl.data_provider.files import File
from parsl.app.futures import DataFuture


@bash_app
def increment(inputs=(), outputs=(), stdout=None, stderr=None):
    cmd_line = """
    if ! [ -f {inputs[0]} ] ; then exit 43 ; fi
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    """.format(inputs=inputs, outputs=outputs)
    return cmd_line


@bash_app
def slow_increment(dur, inputs=(), outputs=(), stdout=None, stderr=None):
    cmd_line = """
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    sleep {0}
    """.format(dur, inputs=inputs, outputs=outputs)
    return cmd_line


@pytest.mark.staging_required
def test_increment(tmpd_cwd, depth=5):
    """Test simple pipeline A->B...->N
    """
    fpath = tmpd_cwd / "test0.txt"
    fpath.write_text("0\n")

    prev = [File(str(fpath))]
    futs = []
    for i in range(1, depth):
        assert isinstance(prev[0], (DataFuture, File))
        output = File(str(tmpd_cwd / f"test{i}.txt"))
        f = increment(
            inputs=prev,
            outputs=[output],
            stdout=str(tmpd_cwd / f"incr{i}.out"),
            stderr=str(tmpd_cwd / f"incr{i}.err"),
        )
        prev = f.outputs
        futs.append((i, prev[0]))
        assert isinstance(prev[0], DataFuture)

    for key, f in futs:
        file = f.result()
        expected = str(tmpd_cwd / f"test{key}.txt")

        assert file.local_path is None, "File on local side has overridden local_path, file: {}".format(repr(file))
        assert file.filepath == expected, "Submit side filepath has not been preserved over execution"
        data = open(file.filepath).read().strip()
        assert data == str(key)


@pytest.mark.staging_required
def test_increment_slow(tmpd_cwd, depth=5, dur=0.01):
    """Test simple pipeline slow (sleep.5) A->B...->N
    """

    fpath = tmpd_cwd / "test0.txt"
    fpath.write_text("0\n")

    prev = [File(str(fpath))]
    futs = []
    for i in range(1, depth):
        output = File(str(tmpd_cwd / f"test{i}.txt"))
        f = slow_increment(
            dur,
            inputs=prev,
            outputs=[output],
            stdout=str(tmpd_cwd / f"incr{i}.out"),
            stderr=str(tmpd_cwd / f"incr{i}.err"),
        )
        prev = f.outputs
        futs.append((i, prev[0]))

    for key, f in futs:
        data = open(f.result().filepath).read().strip()
        assert data == str(key)
