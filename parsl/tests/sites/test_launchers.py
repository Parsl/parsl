import os
import subprocess
import tempfile
from pathlib import Path

import pytest

from parsl.launchers import (
    AprunLauncher,
    GnuParallelLauncher,
    JsrunLauncher,
    MpiExecLauncher,
    MpiRunLauncher,
    SimpleLauncher,
    SingleNodeLauncher,
    SrunLauncher,
    SrunMPILauncher,
    WrappedLauncher,
)


def test_wrapped_launcher(caplog):
    w = WrappedLauncher('docker')
    assert w('test', 1, 1) == 'docker test'

    # Make sure it complains if you have > 1 node or task per node
    w('test', 2, 2)
    assert 'tasks per node' in caplog.text
    assert 'nodes per block' in caplog.text


def create_mock_script(script_content: str, mock_dir: Path) -> str:
    """Prepend mock function definitions to the launcher script."""

    # Mock functions for HPC commands
    mocks = f"""
# Mock HPC commands for testing
export PATH="{mock_dir}:$PATH"

srun() {{
    echo "MOCK_SRUN called with: $@" >&2
    # Look for bash and execute the script that follows
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "bash" ]]; then
            shift
            bash "$@"
            return $?
        fi
        shift
    done
    echo "ERROR: srun mock did not find bash command to execute" >&2
    return 1
}}

mpiexec() {{
    echo "MOCK_MPIEXEC called with: $@" >&2
    # Look for bash and execute the script that follows
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "bash" ]] || [[ "$1" == "/usr/bin/sh" ]]; then
            shift
            bash "$@"
            return $?
        fi
        shift
    done
    echo "ERROR: mpiexec mock did not find bash command to execute" >&2
    return 1
}}

mpirun() {{
    echo "MOCK_MPIRUN called with: $@" >&2
    # Look for bash and execute the script that follows
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "bash" ]] || [[ "$1" == *"/bash" ]]; then
            shift
            bash "$@"
            return $?
        fi
        shift
    done
    echo "ERROR: mpirun mock did not find bash command to execute" >&2
    return 1
}}

aprun() {{
    echo "MOCK_APRUN called with: $@" >&2
    # Look for bash and execute the script that follows
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "/bin/bash" ]] || [[ "$1" == "bash" ]]; then
            shift
            bash "$@"
            return $?
        fi
        shift
    done
    echo "ERROR: aprun mock did not find bash command to execute" >&2
    return 1
}}

jsrun() {{
    echo "MOCK_JSRUN called with: $@" >&2
    # Look for bash and execute the script that follows
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "/bin/bash" ]] || [[ "$1" == "bash" ]]; then
            shift
            bash "$@"
            return $?
        fi
        shift
    done
    echo "ERROR: jsrun mock did not find bash command to execute" >&2
    return 1
}}

parallel() {{
    echo "MOCK_PARALLEL called with: $@" >&2
    # For gnu parallel, read commands from stdin and execute them
    while IFS= read -r line; do
        eval "$line"
    done
}}

# Original script content follows
{script_content}
"""
    return mocks


@pytest.mark.local
@pytest.mark.parametrize("launcher_class,creates_cmd_script", [
    (SimpleLauncher, False),
    (SingleNodeLauncher, False),
    (GnuParallelLauncher, True),
    (MpiExecLauncher, True),
    (MpiRunLauncher, True),
    (SrunLauncher, True),
    (SrunMPILauncher, True),
    (AprunLauncher, True),
    (JsrunLauncher, True),
])
def test_launcher_script_execution(launcher_class, creates_cmd_script):
    """Test that launcher scripts can be generated and executed with mocked commands."""

    test_command = "echo 'Hello from worker'"
    launcher = launcher_class()
    name = launcher_class.__name__

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        mock_bin = tmppath / "mock_bin"
        mock_bin.mkdir()

        # Generate the launcher script
        script_dir = tmppath / f"scripts_{name}"
        script_dir.mkdir(exist_ok=True)

        # Call the launcher
        launcher_script = launcher(
            command=test_command,
            tasks_per_node=2,
            nodes_per_block=1,
            script_dir=str(script_dir)
        )

        # Verify we got a string back
        assert isinstance(launcher_script, str), f"{name} should return a string"
        assert len(launcher_script) > 0, f"{name} should return non-empty script"

        # Create a test script with mocks
        test_script = tmppath / f"test_{name}.sh"
        full_script = create_mock_script(launcher_script, mock_bin)
        test_script.write_text(full_script)
        test_script.chmod(0o755)

        # Set environment variables that scripts might expect
        env = os.environ.copy()
        env['JOBNAME'] = f'test_job_{name}'
        env['SLURM_JOB_NAME'] = f'test_job_{name}'
        env['SLURM_CPUS_ON_NODE'] = '4'  # Mock CPU count for SLURM launchers
        env['SLURM_JOB_NUM_NODES'] = '2'  # Mock node count for SLURM launchers
        env['PBS_NODEFILE'] = '/dev/null'

        # Execute the script
        result = subprocess.run(
            ['bash', str(test_script)],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
            cwd=tmpdir
        )

        # Check that script executed successfully
        assert result.returncode == 0, (
            f"{name} script failed with returncode {result.returncode}\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}\n"
            f"SCRIPT:\n{full_script}"
        )

        # Verify the command was actually executed by checking output
        assert "Hello from worker" in result.stdout, (
            f"{name} command did not produce expected output\n"
            f"Expected 'Hello from worker' in stdout\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}"
        )

        # Verify command script was created if launcher creates one
        if creates_cmd_script:
            cmd_scripts = list(script_dir.glob("cmd_*.sh"))
            assert len(cmd_scripts) > 0, f"{name} should create a cmd_*.sh file in {script_dir}"


@pytest.mark.local
@pytest.mark.parametrize("launcher_class,uses_script_dir", [
    (GnuParallelLauncher, True),
    (MpiExecLauncher, True),
    (MpiRunLauncher, True),
    (SrunLauncher, True),
    (SrunMPILauncher, True),
    (AprunLauncher, True),
    (JsrunLauncher, True),
])
def test_launcher_script_dir_parameter(launcher_class, uses_script_dir):
    """Test that script_dir parameter is properly set in generated scripts."""

    with tempfile.TemporaryDirectory() as tmpdir:
        script_dir = Path(tmpdir) / "my_custom_scripts"
        script_dir.mkdir()

        launcher = launcher_class()
        script = launcher(
            command="echo test",
            tasks_per_node=1,
            nodes_per_block=1,
            script_dir=str(script_dir)
        )

        # Verify the exact SCRIPT_DIR assignment appears in the script
        expected_line = f'SCRIPT_DIR="{script_dir}"'
        assert expected_line in script, (
            f"{launcher_class.__name__} should set SCRIPT_DIR to the provided path\n"
            f"Expected line: {expected_line}\n"
            f"Generated script:\n{script}"
        )

        # Verify the directory check logic is present
        assert "[[ -d $SCRIPT_DIR ]] || SCRIPT_DIR=\"\"" in script, (
            f"{launcher_class.__name__} should include directory existence check"
        )

        # Verify the script uses the conditional expansion pattern
        assert "${SCRIPT_DIR:+$SCRIPT_DIR/}" in script, (
            f"{launcher_class.__name__} should use bash parameter expansion for script_dir"
        )


@pytest.mark.local
@pytest.mark.parametrize("launcher_class,uses_script_dir", [
    (SimpleLauncher, False),
    (SingleNodeLauncher, False),
    (GnuParallelLauncher, True),
    (MpiExecLauncher, True),
    (MpiRunLauncher, True),
    (SrunLauncher, True),
    (SrunMPILauncher, True),
    (AprunLauncher, True),
    (JsrunLauncher, True),
])
def test_launcher_with_empty_script_dir(launcher_class, uses_script_dir):
    """Test that launchers handle empty script_dir gracefully."""

    launcher = launcher_class()
    script = launcher(
        command="echo test",
        tasks_per_node=1,
        nodes_per_block=1,
        script_dir=""
    )

    # Should still generate valid script
    assert isinstance(script, str)
    assert len(script) > 0

    # Launchers that create command scripts should properly handle empty script_dir
    if uses_script_dir:
        # Verify SCRIPT_DIR is set to empty string
        assert 'SCRIPT_DIR=""' in script, (
            f"{launcher_class.__name__} should set SCRIPT_DIR to empty string when script_dir is empty\n"
            f"Generated script:\n{script}"
        )

        # Verify the directory check logic is present
        assert "[[ -d $SCRIPT_DIR ]] || SCRIPT_DIR=\"\"" in script, (
            f"{launcher_class.__name__} should include directory existence check"
        )

        # Verify the parameter expansion pattern is used
        assert "${SCRIPT_DIR:+$SCRIPT_DIR/}" in script, (
            f"{launcher_class.__name__} should use bash parameter expansion for script_dir"
        )


@pytest.mark.local
@pytest.mark.parametrize("launcher_class", [
    SimpleLauncher,
    SingleNodeLauncher,
    GnuParallelLauncher,
    MpiExecLauncher,
    MpiRunLauncher,
    SrunLauncher,
    SrunMPILauncher,
    AprunLauncher,
    JsrunLauncher,
])
def test_launcher_bash_syntax(launcher_class):
    """Test that generated launcher scripts have valid bash syntax."""

    test_command = "echo test"
    launcher = launcher_class()
    script = launcher(test_command, 1, 1, "")

    # Check bash syntax without executing
    result = subprocess.run(
        ['bash', '-n'],  # -n flag checks syntax only
        input=script,
        capture_output=True,
        text=True
    )

    launcher_name = launcher_class.__name__
    assert result.returncode == 0, (
        f"Syntax error in {launcher_name}\n"
        f"Error: {result.stderr}\n"
        f"Script:\n{script}"
    )


@pytest.mark.local
def test_wrapped_launcher_bash_syntax():
    """Test that WrappedLauncher generates valid bash syntax."""

    test_command = "echo test"
    launcher = WrappedLauncher('echo')
    script = launcher(test_command, 1, 1, "")

    # Check bash syntax without executing
    result = subprocess.run(
        ['bash', '-n'],  # -n flag checks syntax only
        input=script,
        capture_output=True,
        text=True
    )

    assert result.returncode == 0, (
        f"Syntax error in WrappedLauncher\n"
        f"Error: {result.stderr}\n"
        f"Script:\n{script}"
    )
