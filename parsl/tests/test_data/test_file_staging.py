import pytest

from parsl.app.app import bash_app
from parsl.data_provider.files import File


@bash_app
def cat(inputs=(), outputs=(), stdout=None, stderr=None):
    infiles = " ".join(i.filepath for i in inputs)
    return f"cat {infiles} &> {outputs[0]}\n"


@pytest.mark.staging_required
def test_regression_200(tmp_path):
    """Regression test for #200. Pickleablility of Files"""
    opath = tmp_path / "test_output.txt"
    fpath = tmp_path / "test.txt"

    fpath.write_text("Hello World")
    f = cat(inputs=[File(str(fpath))], outputs=[File(str(opath))])

    f.result()
    with open(f.outputs[0].filepath) as f:
        data = f.read()
        assert "Hello World" == data
