import copy
from abc import *
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output

from parsl.monitoring.web_app.app import app


class BasePlot(metaclass=ABCMeta):
    def __init__(self, plot_id, plot_args):
        self._plot_args = copy.deepcopy(plot_args)

        if not self._plot_args[0]:
            raise ValueError('Must provide a dash Input() object to plot_args')

        self._plot_id = plot_id

        plot = self.plot
        plot_input_args = self._plot_args[0]
        plot_state_args = self._plot_args[1]

        @app.callback(Output(plot_id, 'figure'), plot_input_args, plot_state_args)
        def plot_callback(*args):
            return plot(*args)

    @abstractmethod
    def setup(self, *args):
        raise NotImplementedError('Must define setup() to use this base class')

    @abstractmethod
    def plot(self, *args):
        raise NotImplementedError('Must define plot() to use this base class')

    def html(self, run_id):
        return html.Div(className='plot_container',
                        children=[html.Div(className='plot_components',
                                           children=self.setup(run_id)),
                                  html.Div(className='plot_figure',
                                           children=dcc.Graph(id=self._plot_id))])
