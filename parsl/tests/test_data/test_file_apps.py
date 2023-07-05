import pytest

from parsl.app.app import bash_app
from parsl.data_provider.files import File


@bash_app
def cat(inputs=(), outputs=(), stdout=None, stderr=None):
    infiles = " ".join(i.filepath for i in inputs)
    return f"cat {infiles} &> {outputs[0]}"


@pytest.mark.staging_required
def test_files(setup_data):
    fs = sorted(str(setup_data / f) for f in setup_data.iterdir())
    fs = list(map(File, fs))
    x = cat(
        inputs=fs,
        outputs=[File(str(setup_data / "cat_out.txt"))],
        stdout=str(setup_data / "f_app.out"),
        stderr=str(setup_data / "f_app.err"),
    )
    x.result()
    d_x = x.outputs[0]
    data = open(d_x.filepath).read().strip()
    assert "1\n2" == data, "Per setup_data fixture"


@bash_app
def increment(inputs=(), outputs=(), stdout=None, stderr=None):
    # Place double braces to avoid python complaining about missing keys for {item = $1}
    return """
    x=$(cat {i})
    echo $(($x+1)) > {o}
    """.format(i=inputs[0], o=outputs[0])


@pytest.mark.staging_required
def test_increment(tmp_path, depth=5):
    """Test simple pipeline A->B...->N
    """
    # Test setup
    first_fpath = tmp_path / "test0.txt"
    first_fpath.write_text("0\n")

    prev = [File(str(first_fpath))]
    futs = []
    for i in range(1, depth):
        f = increment(
            inputs=prev,
            outputs=[File(str(tmp_path / f"test{i}.txt"))],
            stdout=str(tmp_path / f"incr{i}.out"),
            stderr=str(tmp_path / f"incr{i}.err"),
        )
        prev = f.outputs
        futs.append((i, prev[0]))

    for i, f in futs:
        data = open(f.result().filepath).read().strip()
        assert data == str(i)
