import re
import string
from collections import defaultdict
from pathlib import Path


class PartialFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val = super(PartialFormatter, self).get_field(field_name, args,
                                                          kwargs)
            # Python 3, 'super().get_field(field_name, args, kwargs)' works
        except (KeyError, AttributeError):
            val = '{' + field_name + '}', field_name
        return val

    def format_field(self, value, spec):
        # handle an invalid format
        if value is None:
            return spec
        try:
            return super(PartialFormatter, self).format_field(value, spec)
        except ValueError:
            return value[:-1] + ':' + spec + value[-1]


class CleanFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val = super(CleanFormatter, self).get_field(field_name,
                                                        args,
                                                        kwargs)
            # Python 3, 'super().get_field(field_name, args, kwargs)' works
        except (KeyError, AttributeError):
            val = '', field_name
        return val

    def format_field(self, value, spec):
        # handle an invalid format
        if value is None:
            return spec
        try:
            return super(CleanFormatter, self).format_field(value,
                                                            spec)
        except ValueError:
            return ''


def clean_path(path):
    cfmt = CleanFormatter()
    d = cfmt.format(path, (defaultdict(str)))
    print(d)
    y = re.sub(r"_\[(?s)(.*)=\]_", "_", d)
    print(y)
    x = re.sub(r"_\((?s)(.*)=\).", ".", y)
    print(x)
    z = re.sub(r"__+", "_", x)
    print(z)
    e = z.replace('[', '')
    e = e.replace(']', '')
    e = e.replace('(', '')
    e = e.replace(')', '')
    print(e)
    f = Path(e).as_posix()
    print(f)
    return f
