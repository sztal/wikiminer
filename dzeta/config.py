"""Extended config class that may parse values from environmental variables."""
import os
from configparser import ConfigParser, _UNSET


class Config(ConfigParser):
    """Config parser supporting reading from envvars."""

    def getenvvar(self, section, option, *, convert_bool=False,
                  fallback=_UNSET, **kwds):
        """Get value from environmental variable."""
        value = self.get(section, option, fallback=fallback, **kwds)
        try:
            value = os.environ[value]
        except KeyError:
            if fallback is _UNSET:
                raise KeyError(f"Environment variable '{option}' is not defined")
            return fallback
        if convert_bool:
            value = self._convert_to_boolean(value)
        return value
