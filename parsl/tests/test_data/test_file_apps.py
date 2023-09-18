import pytest

from parsl.app.app import bash_app
from parsl.data_provider.files import File


@bash_app
def cat(inputs=(), outputs=(), stdout=None, stderr=None):
    infiles = " ".join(i.filepath for i in inputs)
    return f"cat {infiles} &> {outputs[0]}"


@pytest.mark.staging_required
def test_files(setup_data):
    fs = sorted(setup_data / f for f in setup_data.iterdir())
    fs = list(map(File, fs))
    x = cat(
        inputs=fs,
        outputs=[File(setup_data / "cat_out.txt")],
        stdout=setup_data / "f_app.out",
        stderr=setup_data / "f_app.err",
    )
    x.result()
    d_x = x.outputs[0]
    data = open(d_x.filepath).read().strip()
    assert "1\n2" == data, "Per setup_data fixture"


@bash_app
def increment(inputs=(), outputs=(), stdout=None, stderr=None):
    return (
        f"x=$(cat {inputs[0]})\n"
        f"echo $(($x+1)) > {outputs[0]}"
    )


@pytest.mark.staging_required
def test_increment(tmp_path, depth=5):
    """Test simple pipeline A->B...->N"""
    # Test setup
    first_fpath = tmp_path / "test0.txt"
    first_fpath.write_text("0\n")

    prev = [File(first_fpath)]
    futs = []
    for i in range(1, depth):
        f = increment(
            inputs=prev,
            outputs=[File(tmp_path / f"test{i}.txt")],
            stdout=tmp_path / f"incr{i}.out",
            stderr=tmp_path / f"incr{i}.err",
        )
        prev = f.outputs
        futs.append((i, prev[0]))

    for i, f in futs:
        data = open(f.result().filepath).read().strip()
        assert data == str(i)
