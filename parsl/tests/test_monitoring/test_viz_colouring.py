import pytest

import parsl.monitoring.visualization.plots.default.workflow_plots as workflow_plots

from parsl.dataflow.states import States


@pytest.mark.local
def test_all_states_colored() -> None:
    """This checks that the coloring tables in parsl-visualize contain
    a color for each state defined in the task state enumeration.
    """
    for s in States:
        assert s.name in workflow_plots.gantt_colors
        assert s.name in workflow_plots.dag_state_colors
