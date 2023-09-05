import pytest

from parsl.app.app import bash_app


@bash_app
def foo(z=2, stdout=None):
    return f"echo {z}"


@pytest.mark.issue363
def test_command_format_1(tmpd_cwd):
    """Testing command format for BashApps
    """

    stdout = tmpd_cwd / "std.out"
    for exp_value, z in (
        ("3", 3),
        ("4", 4),
        ("5", 5),
    ):
        app_fu = foo(z=z, stdout=str(stdout))
        assert app_fu.result() == 0, "BashApp had non-zero exit"

        so_content = stdout.read_text().strip()
        assert so_content == exp_value
        stdout.unlink()

    app_fu = foo(stdout=str(stdout))
    assert app_fu.result() == 0, "BashApp had non-zero exit"

    so_content = stdout.read_text().strip()
    assert so_content == "2"
