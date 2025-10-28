import pytest

from parsl import File
from parsl.app.app import bash_app


@bash_app
def multiline(inputs=(), outputs=(), stderr=None, stdout=None):
    return """echo {inputs[0]} &> {outputs[0]}
    echo {inputs[1]} &> {outputs[1]}
    echo {inputs[2]} &> {outputs[2]}
    echo "Testing STDOUT"
    echo "Testing STDERR" 1>&2
    """.format(inputs=inputs, outputs=outputs)


@pytest.mark.shared_fs
def test_multiline(tmpd_cwd):
    so, se = tmpd_cwd / "std.out", tmpd_cwd / "std.err"
    f = multiline(
        inputs=["Hello", "This is", "Cat!"],
        outputs=[
            File(str(tmpd_cwd / "hello.txt")),
            File(str(tmpd_cwd / "this.txt")),
            File(str(tmpd_cwd / "cat.txt")),
        ],
        stdout=str(so),
        stderr=str(se),
    )
    f.result()

    flist = list(map(str, (f.name for f in tmpd_cwd.iterdir())))
    assert 'hello.txt' in flist, "hello.txt is missing"
    assert 'this.txt' in flist, "this.txt is missing"
    assert 'cat.txt' in flist, "cat.txt is missing"
    assert "Testing STDOUT" in so.read_text()
    assert "Testing STDERR" in se.read_text()
