"""Metaprogramming utilities (decorators, metaclasses etc.)."""
from functools import update_wrapper


def composable(cls):
    """Decorator for turining a class into a composable class.

    A composable class has a special `__components__` attribute which
    is an (ordered) dict containing component objects and its attribute
    getter method is aware of components, so for instance
    if an attribute name is not found on the parent instance it will be
    looked after in component objects (search will follow insertion order).

    A composable class should keep its component objects in an ordered
    mapping (that keeps insertion order) in the special attribute
    `__components__`. It can be created for instance in an `__init__` method.
    It will be consisdered an empty dict if it does not exist on a given
    instance.

    Parameters
    ----------
    cls : type
        Any class object.

    Returns
    -------
    type
        A modified composable class object.
    """
    # pylint: disable=inconsistent-return-statements
    errmsg = f"'{cls.__name__}' "+"object has no attribute '{0}'"
    cls_getattr = getattr(cls, '__getattr__', None)
    def __getattr__(self, attr):
        if cls_getattr is not None:
            try:
                return cls_getattr(attr)
            except AttributeError:
                pass
        if '__components__' in dir(self):
            components = self.__components__
        else:
            components = {}
        for component in components.values():
            if hasattr(component, attr):
                return getattr(component, attr)
        raise AttributeError(errmsg.format(attr))
    cls.__getattr__ = __getattr__
    return cls


@composable
class Interface:
    """Interface class.

    `Interface` is a wrapper class that wraps around an existing class
    and has access to all its methods and attributes via composition,
    so it does not mess up with inheritance hierarchy whatsoever.
    This way it can be used to create instances of a given class
    with additional features (i.e. implementing particular interfaces)
    without subclassing the original class.
    Instead, additional methods are injected by subclassing
    `Interface`. Note that methods on the instance, even those with the
    same name as methods on `Interface` subclass can be still accessed
    through :py:attr:`_` attribute.

    It is implemented with :py:func:`dzeta.meta.composable` decorator.

    Attributes
    ----------
    _ : object
        Original instance object.
        The underscore is used not indicate that this is a private attribute
        but make it less likely that it will conflict with any name
        on the instance.
    """
    def __init__(self, instance):
        self.__components__ = dict(instance=instance)

    def __repr__(self):
        cname = self.__class__.__name__
        return f"{cname}({repr(self._)})"

    @property
    def _(self):
        return self.__components__['instance']

    @classmethod
    def inject(cls, instance, init=True, **kwds):
        """Inject reference to the interfaced instance in the original instance.

        Parameters
        ----------
        instance : object
            An object to be injected.
        init : bool
            If `instance` is a `type`, should `__init__` be decorated so
            instances are also injected with the interface?
        **kwds :
            Passed to :py:meth:`__init__` method.
        """
        interfaced = cls(instance=instance, **kwds)
        instance._ = interfaced
        if init and isinstance(instance, type):
            cls_init = instance.__init__
            def __init__(self, *args, **kwds):
                cls_init(self, *args, **kwds)
                interfaced = cls(self)
                self._ = interfaced
            instance.__init__ = update_wrapper(__init__, cls_init)
        return instance
