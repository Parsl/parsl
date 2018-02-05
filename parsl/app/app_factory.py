''' AppFactoryFactory

Centralize app object creation.

'''
import logging
from inspect import getsource
from hashlib import md5
from inspect import signature
from parsl.app.bash_app import BashApp
from parsl.app.python_app import PythonApp
from parsl.app.errors import InvalidAppTypeError

logger = logging.getLogger(__name__)


class AppFactory(object):
    ''' AppFactory streamlines creation of apps
    '''

    def __init__(self, app_class, executor, func, cache=False, sites='all', walltime=60):
        ''' Construct an AppFactory for a particular app_class

        Args:
            - app_class(Class) : An app class
            - executor(Executor) : An executor object which will handle app execution
            - func(Function) : The function to execute

        Kwargs:
            - walltime(int) : Walltime in seconds, default=60
            - sites (str|list) : List of site names that this app could execute over. default is 'all'
            - cache (Bool) : Enable caching of app.

        Returns:
            An AppFactory Object
        '''
        self.__name__ = func.__name__
        self.app_class = app_class
        self.executor = executor
        self.func = func
        self.status = 'created'
        self.walltime = walltime
        self.sites = sites
        self.sig = signature(func)
        self.cache = cache
        # Function source hashing is done here to avoid redoing this every time
        # the app is called.
        if cache is True:
            try:
                fn_source = getsource(func)
            except OSError:
                logger.debug("Unable to get source code for AppCaching. Recommend creating module")
                fn_source = func.__name__

            self.func_hash = md5(fn_source.encode('utf-8')).hexdigest()
        else:
            self.func_hash = func.__name__

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
                                 sites=self.sites,
                                 walltime=self.walltime,
                                 cache=self.cache,
                                 fn_hash=self.func_hash)
        return app_obj(*args, **kwargs)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return '<class %s"%s for %s>' % (self.app_class.__name__,
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
        self.apps = {'bash': BashApp,
                     'python': PythonApp}

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
