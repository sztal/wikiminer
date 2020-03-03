"""Dzeta utility functions."""
from datetime import datetime, date
from importlib import import_module
import dateparser


def parse_date(dt, preprocessor=None, date_formats=None, **kwds):
    """Parse date string flexibly using `dateutil` module.

    Parameters
    ----------
    dt : str
        Date string.
    preprocessor : func
        Optional preprocessing function.
        Useful for normalizing date strings to conform to one format string.
    date_formats : list of str or None
        Passed to :py:func:`dateparser.parse`.
    **kwds :
        Arguments passed to `preprocessor`.
    """
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, date):
        return datetime(*dt.timetuple()[:6])
    if preprocessor:
        dt = preprocessor(dt, **kwds)
    return dateparser.parse(dt, date_formats=date_formats)


def parse_bool(x, true=('true', 'yes', '1', 'on'), add_true=(),
               false=('false', 'no', '0', 'off'), add_false=()):
    """Parse boolean string.

    Parameters
    ----------
    x : bool or str
        Boolean value as `bool` or `str`.
    true : list of str
        List of accepted string representations of `True` value.
    add_true  : list of str
        Optional list to of `True` representations to append to the default list.
    false : list of str
        List of accepted string representations of `False` value.
    add_false : list of str
        Optional list of `False` representations to append to the default list.

    Notes
    -----
    `true` and `false` should always consist of only lowercase strings,
    as all comparisons are done after lowercasing `x`.

    Raises
    ------
    ValueError
        If `x` is not `bool` and not contained either in `true` or `false`.
    """
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)) and x == 0 or x == 1:
        return bool(x)
    x = str(x).lower()
    if add_true:
        true = (*true, *add_true)
    if add_false:
        false = (*false, *add_false)
    if x in true:
        return True
    if x in false:
        return False
    raise ValueError("Value '{}' can not be interpreted as boolean".format(x))


def import_python(path, package=None):
    """Get python module or object.
    Parameters
    ----------
    path : str
        Fully-qualified python path, i.e. `package.module:object`.
    package : str or None
        Package name to use as an anchor if `path` is relative.
    """
    parts = path.split(':')
    if len(parts) > 2:
        msg = f"Not a correct path ('{path}' has more than one object qualifier)"
        raise ValueError(msg)
    if len(parts) == 2:
        module_path, obj = parts
    else:
        module_path, obj = path, None
    module = import_module(module_path, package=package)
    if obj:
        return getattr(module, obj)
    return module
