import pytest

from parsl.app.app import bash_app
from parsl.data_provider.files import File


@bash_app
def cat(inputs=(), outputs=(), stdout=None, stderr=None):
    infiles = " ".join(i.filepath for i in inputs)
    return f"cat {infiles} &> {outputs[0]}"


@pytest.mark.staging_required
def test_regression_200(tmpd_cwd):
    """Regression test for #200. Pickleablility of Files"""
    opath = tmpd_cwd / "test_output.txt"
    fpath = tmpd_cwd / "test.txt"

    fpath.write_text("Hello World")
    f = cat(inputs=[File(fpath)], outputs=[File(opath)])

    f.result()
    with open(f.outputs[0].filepath) as f:
        data = f.read()
        assert "Hello World" == data
