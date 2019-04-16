from abc import *
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input

from parsl.monitoring.web_app.app import app


class BasePlot(metaclass=ABCMeta):
    def __init__(self, plot_id, setup_args, plot_args):
        if not setup_args:
            setup_args = ([Input('run_number_dropdown', 'value')], [])
        if not plot_args:
            plot_args = ([Input('run_number_dropdown', 'value')], [])

        self._plot_id = plot_id

        plot = self.plot
        setup = self.setup

        @app.callback(Output(plot_id + '_components', 'children'), setup_args[0], setup_args[1])
        def setup_callback(*args):
            return setup(*args)

        @app.callback(Output(plot_id, 'figure'), plot_args[0], plot_args[1])
        def plot_callback(*args):
            return plot(*args)

    @abstractmethod
    def setup(self, *args):
        raise NotImplementedError('Must define setup() to use this base class')

    @abstractmethod
    def plot(self, *args):
        raise NotImplementedError('Must define plot() to use this base class')

    @property
    def html(self):
        return html.Div(className='plot_container',
                        children=[html.Div(id=self._plot_id + '_components'), dcc.Graph(id=self._plot_id)])
