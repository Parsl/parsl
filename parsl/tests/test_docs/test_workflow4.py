import pytest

from parsl.app.app import bash_app, python_app
from parsl.data_provider.files import File


@bash_app
def generate(outputs=()):
    return "echo 1 &> {o}".format(o=outputs[0])


@bash_app
def concat(inputs=(), outputs=(), stdout=None, stderr=None):
    return "cat {0} >> {1}".format(" ".join(map(lambda x: x.filepath, inputs)), outputs[0])


@python_app
def total(inputs=()):
    with open(inputs[0].filepath, "r") as f:
        return sum(int(line) for line in f)


@pytest.mark.staging_required
@pytest.mark.parametrize("width", (5, 10, 15))
def test_parallel_dataflow(tmpd_cwd, width):
    """Test parallel dataflow from docs on Composing workflows
    """

    # create 5 files with random numbers
    output_files = [
        generate(outputs=[File(str(tmpd_cwd / f"random-{i}.txt"))])
        for i in range(width)
    ]

    # concatenate the files into a single file
    cc = concat(
        inputs=[i.outputs[0] for i in output_files],
        outputs=[File(str(tmpd_cwd / "all.txt"))]
    )

    # calculate the average of the random numbers
    totals = total(inputs=[cc.outputs[0]])
    assert totals.result() == len(output_files)
