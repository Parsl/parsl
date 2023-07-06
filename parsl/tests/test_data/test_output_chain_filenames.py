from concurrent.futures import Future

from parsl import File
from parsl.app.app import bash_app


@bash_app
def app1(inputs=(), outputs=(), stdout=None, stderr=None, mock=False):
    return f"echo 'test' > {outputs[0]}"


@bash_app
def app2(inputs=(), outputs=(), stdout=None, stderr=None, mock=False):
    return f"echo '{inputs[0]}' > {outputs[0]}"


def test_behavior(tmpd_cwd):
    expected_path = str(tmpd_cwd / "simple-out.txt")
    app1_future = app1(
        inputs=[],
        outputs=[File(expected_path)]
    )

    o = app1_future.outputs[0]
    assert isinstance(o, Future)

    app2_future = app2(
        inputs=[o],
        outputs=[File(str(tmpd_cwd / "simple-out2.txt"))]
    )
    app2_future.result()

    with open(app2_future.outputs[0].filepath, 'r') as f:
        name = f.read().strip()

    assert name == expected_path, "Filename mangled due to DataFuture handling"
