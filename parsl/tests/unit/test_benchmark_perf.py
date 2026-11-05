import concurrent.futures
import json
import re
import sys
from contextlib import nullcontext
from datetime import datetime, timezone

import pytest

from parsl.benchmark import perf

pytestmark = pytest.mark.local


@pytest.fixture
def benchmark_cli(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(perf, "load_dfk_from_config", lambda filename: nullcontext())

    def run(*options):
        monkeypatch.setattr(sys, "argv", [
            "parsl-perf", "--config", "config.py", "--iterate", "1,2", *options,
        ])
        perf.cli_run()

    def app(*args, **kwargs):
        future = concurrent.futures.Future()
        future.set_result(7)
        return future

    monkeypatch.setattr(perf, "app", app)
    return run


@pytest.mark.parametrize("directory", ["outputs", "custom/nested"])
def test_json_output(benchmark_cli, tmp_path, directory, capsys):
    options = [] if directory == "outputs" else ["--output-dir", directory]
    benchmark_cli("--json-output", *options)
    files = list((tmp_path / directory).glob("*.json"))
    assert len(files) == 1
    assert re.fullmatch(r"parsl_perf_\d{8}T\d{12}Z\.json", files[0].name)
    report = json.loads(files[0].read_text())
    assert report["status"] == "completed"
    assert report["schema_version"] == 1
    assert report["configuration"]["iteration_mode"] == [1, 2]
    assert report["parsl_version"] == perf.parsl.__version__
    assert report["started_at"] <= report["finished_at"]
    started_at_utc = datetime.fromisoformat(report["started_at"]).astimezone(timezone.utc)
    assert files[0].name == started_at_utc.strftime("parsl_perf_%Y%m%dT%H%M%S%fZ.json")
    assert [row["task_count"] for row in report["iterations"]] == [1, 2]
    for row in report["iterations"]:
        assert row["tasks_per_second"] == pytest.approx(row["task_count"] / row["runtime_seconds"])
        assert row["submission_tasks_per_second"] == pytest.approx(row["task_count"] / row["submission_seconds"])
    assert "Tasks per second:" in capsys.readouterr().out


@pytest.mark.parametrize("options", [[], ["--output-dir", "unused"]])
def test_no_json_without_flag(benchmark_cli, tmp_path, options):
    benchmark_cli(*options)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("error, status", [
    (RuntimeError("task failed"), "failed"),
    (KeyboardInterrupt(), "interrupted"),
])
def test_partial_results(benchmark_cli, monkeypatch, tmp_path, error, status):
    calls = 0

    def app(*args, **kwargs):
        nonlocal calls
        calls += 1
        future = concurrent.futures.Future()
        if calls == 2:
            future.set_exception(error)
        else:
            future.set_result(7)
        return future

    monkeypatch.setattr(perf, "app", app)
    with pytest.raises(type(error)) as raised:
        benchmark_cli("--json-output")
    assert raised.value is error
    report = json.loads(next((tmp_path / "outputs").glob("*.json")).read_text())
    assert report["status"] == status
    assert report["error"] == {"type": type(error).__name__, "message": str(error)}
    assert [row["task_count"] for row in report["iterations"]] == [1]


def test_configuration_failure(benchmark_cli, monkeypatch, tmp_path):
    def fail(filename):
        raise RuntimeError("configuration failed")

    monkeypatch.setattr(perf, "load_dfk_from_config", fail)
    with pytest.raises(RuntimeError, match="configuration failed"):
        benchmark_cli("--json-output")
    report = json.loads(next((tmp_path / "outputs").glob("*.json")).read_text())
    assert report["status"] == "failed"
    assert report["iterations"] == []


def test_invalid_output_directory(benchmark_cli, tmp_path):
    (tmp_path / "blocked").write_text("existing file")
    with pytest.raises(SystemExit) as raised:
        benchmark_cli("--json-output", "--output-dir", "blocked")
    assert raised.value.code == 2


@pytest.mark.parametrize("directory", ["outputs", "results/nested"])
def test_custom_output_filename(benchmark_cli, tmp_path, directory):
    options = [] if directory == "outputs" else ["--output-dir", directory]
    benchmark_cli("--json-output", "--output-file", "baseline.json", *options)
    path = tmp_path / directory / "baseline.json"
    assert json.loads(path.read_text())["status"] == "completed"
    assert list(path.parent.iterdir()) == [path]


def test_custom_output_file_not_overwritten(benchmark_cli, tmp_path):
    directory = tmp_path / "outputs"
    directory.mkdir()
    path = directory / "baseline.json"
    path.write_text("existing report")
    with pytest.raises(SystemExit) as raised:
        benchmark_cli("--json-output", "--output-file", "baseline.json")
    assert raised.value.code == 2
    assert path.read_text() == "existing report"


@pytest.mark.parametrize("filename", ["", ".", "..", "../baseline.json", "nested/baseline.json", "/tmp/baseline.json"])
def test_invalid_output_filename(benchmark_cli, tmp_path, filename):
    with pytest.raises(SystemExit) as raised:
        benchmark_cli("--json-output", "--output-file", filename)
    assert raised.value.code == 2
    assert list(tmp_path.iterdir()) == []


def test_output_filename_requires_json(benchmark_cli, tmp_path):
    with pytest.raises(SystemExit) as raised:
        benchmark_cli("--output-file", "baseline.json")
    assert raised.value.code == 2
    assert list(tmp_path.iterdir()) == []
