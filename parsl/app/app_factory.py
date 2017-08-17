''' AppFactoryFactory

Centralize app object creation.

'''
import logging
from inspect import signature
from parsl.app.app import BashApp
from parsl.app.app import PythonApp
from parsl.app.errors import InvalidAppTypeError

logger = logging.getLogger(__name__)

class AppFactory(object):
    ''' AppFactory streamlines creation of apps
    '''

    def __init__(self, app_class, executor, func, **kwargs):
        ''' Construct an AppFactory for a particular app_class

        Args:
            app_class(Class) : An app class
            executor(Executor) : An executor object which will handle app execution
            func(Function) : The function to execute

        Kwargs:
            walltime(int) : Walltime in seconds, default=60

        Returns:
            An AppFactory Object
        '''
        self.__name__ = func.__name__
        self.app_class = app_class
        self.executor = executor
        self.func = func
        self.status = 'created'
        self.walltime = kwargs.get('walltime', 60)
        self.sig = signature(func)

    def __call__(self, *args, **kwargs):
        ''' Create a new object of app_class with the args,
        execute the app_object and return the futures

        Args:
             Arbitrary args to the decorated function

        Kwargs:
             Arbitrary kwargs to the decorated function

        Returns:
            (App_Future, [Data_Futures...])

        The call is mostly pass through
        '''
        # Create and call the new App object
        app_obj = self.app_class(self.func,
                                 self.executor,
                                 walltime=self.walltime)
        return app_obj(*args, **kwargs)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return '<class %s"%s for %s>' %(self.app_class.__name__,
                                        self.__class__.__name__,
                                        self.__name__)


class AppFactoryFactory(object):
    ''' An instance AppFactoryFactory will be factory that creates object of a particular kind.
    AppFactoryFactory has the various apps registered with it, and it will return an AppFactory
    that constructs objects of a specific kind.


    '''
    def __init__(self, name):
        ''' Constructor

        Args:
             name(string) : Name for the appfactory

        Returns:
             object(AppFactory)
        '''
        self.name = name
        self.apps = {'bash' : BashApp,
                     'python' : PythonApp}

    def make(self, kind, executor, func, **kwargs):
        ''' Creates a new App of the kind specified

        Args:
            kind(string) : For now only(bash|python)
            executor(Executor) : An executor object which will handle app execution
            func(Function) : The function to execute

        Kwargs:
            Walltime(int) : Walltime in seconds
            Arbritrary kwargs passed onto the AppFactory

        Raises:
            InvalidAppTypeError

        Returns:
            An AppFactory object bound to the specific app_class kind

        '''
        if kind in self.apps:
            return AppFactory(self.apps[kind],
                              executor,
                              func,
                              **kwargs)

        else:
            logger.error("AppFactory:%s Invalid app kind requested : %s ",
                         self.name, kind)
            raise InvalidAppTypeError(
                "AppFactory:%s Invalid app kind requested : %s ",
                self.name, kind)
