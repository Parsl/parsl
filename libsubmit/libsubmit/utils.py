import inspect


class RepresentationMixin(object):
    """A mixin class for adding a __repr__ method.

    The __repr__ method will return a string equivalent to the code used to instantiate
    the child class.
    """
    def __repr__(self):
        return self.__representation

    def setup_representation(self, local_vars, max_width=80):
        """Setup the representation string.

        This method should be called in the constructor of any child class.

        Parameters
        ----------
        local_vars : dict
            Local variables from the child class's scope (obtained from the
            built-in function `locals()`).
        max_width : int
            Maximum width of the representation string. If this width is exceeded,
            the representation string will be split up with one argument or keyword
            argument per line.

        Examples
        --------
        >>> from libsubmit.utils import RepresentationMixin
        >>> class Foo(RepresentationMixin):
                def __init__(self, first, second, third='three', fourth='fourth'):
                    self.setup_representation(locals())
        >>> bar = Foo(1, 'two', fourth='baz')
        >>> bar
        Foo(1, 'two', fourth='baz')
        """
        argspec = inspect.getargspec(self.__init__)
        defaults = dict(zip(reversed(argspec.args), reversed(argspec.defaults)))

        args = [local_vars[a] for a in argspec.args[1:-len(defaults)]]
        kwargs = {}
        for key, value in defaults.items():
            if local_vars[key] != value:
                kwargs[key] = local_vars[key]

        def assemble_multiline(args, kwargs):
            def indent(text):
                lines = text.splitlines()
                if len(lines) <= 1:
                    return text
                return "\n".join("    " + l for l in lines).strip()
            args = ["\n    {},".format(indent(repr(a))) for a in args]
            kwargs = ["\n    {}={}".format(k, indent(repr(v)))
                      for k, v in sorted(kwargs.items())]

            info = ", ".join(args + kwargs)
            return self.__class__.__name__ + "({}\n)".format(info)

        def assemble_line(args, kwargs):
            kwargs = ['{}={}'.format(k, repr(v)) for k, v in sorted(kwargs.items())]

            info = ", ".join([repr(a) for a in args] + kwargs)
            return self.__class__.__name__ + "({})".format(info)

        if len(assemble_line(args, kwargs)) <= max_width:
            self.__representation = assemble_line(args, kwargs)
        else:
            self.__representation = assemble_multiline(args, kwargs)
