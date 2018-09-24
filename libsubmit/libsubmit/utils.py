import inspect


def wtime_to_minutes(time_string):
    ''' wtime_to_minutes

    Convert standard wallclock time string to minutes.

    Args:
        - Time_string in HH:MM:SS format

    Returns:
        (int) minutes

    '''
    hours, mins, seconds = time_string.split(':')
    return int(hours) * 60 + int(mins) + 1


class RepresentationMixin(object):
    """A mixin class for adding a __repr__ method.

    The __repr__ method will return a string equivalent to the code used to instantiate
    the child class, with any defaults included explicitly. The __max_width__ class variable
    controls the maximum width of the representation string. If this width is exceeded,
    the representation string will be split up, with one argument or keyword argument per line.

    Any arguments or keyword arguments in the constructor must be defined as attributes, or
    an AttributeError will be raised.

    Examples
    --------
    >>> from libsubmit.utils import RepresentationMixin
    >>> class Foo(RepresentationMixin):
            def __init__(self, first, second, third='three', fourth='fourth'):
                self.first = first
                self.second = second
                self.third = third
                self.fourth = fourth
    >>> bar = Foo(1, 'two', fourth='baz')
    >>> bar
    Foo(1, 'two', third='three', fourth='baz')
    """
    __max_width__ = 80

    def __repr__(self):
        argspec = inspect.getargspec(self.__init__)
        if len(argspec.args) > 1:
            defaults = dict(zip(reversed(argspec.args), reversed(argspec.defaults)))
        else:
            defaults = []

        for arg in argspec.args[1:]:
            if not hasattr(self, arg):
                template = 'class {} uses {} in the constructor, but does not define it as an attribute'
                raise AttributeError(template.format(self.__class__.__name__, arg))

        args = [getattr(self, a) for a in argspec.args[1:-len(defaults)]]
        kwargs = {key: getattr(self, key) for key in defaults}

        def assemble_multiline(args, kwargs):
            def indent(text):
                lines = text.splitlines()
                if len(lines) <= 1:
                    return text
                return "\n".join("    " + l for l in lines).strip()
            args = ["\n    {},".format(indent(repr(a))) for a in args]
            kwargs = ["\n    {}={}".format(k, indent(repr(v)))
                      for k, v in sorted(kwargs.items())]

            info = "".join(args) + ", ".join(kwargs)
            return self.__class__.__name__ + "({}\n)".format(info)

        def assemble_line(args, kwargs):
            kwargs = ['{}={}'.format(k, repr(v)) for k, v in sorted(kwargs.items())]

            info = ", ".join([repr(a) for a in args] + kwargs)
            return self.__class__.__name__ + "({})".format(info)

        if len(assemble_line(args, kwargs)) <= self.__class__.__max_width__:
            return assemble_line(args, kwargs)
        else:
            return assemble_multiline(args, kwargs)
