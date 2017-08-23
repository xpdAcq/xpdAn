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


cfmt = CleanFormatter()
pfmt = PartialFormatter()


def clean_template(template, removals=None, cfmt=cfmt):
    if removals is None:
        removals = ['temp', 'dx', 'dy']
    d = cfmt.format(template, defaultdict(str))

    for r in removals:
        d = d.replace('[{}=]'.format(r), '')
    z = re.sub(r"__+", "_", d)
    z = z.replace('_.', '.')
    e = z.replace('[', '')
    e = e.replace(']', '')
    e = e.replace('(', '')
    e = e.replace(')', '')
    f = Path(e).as_posix()
    f = f.replace('/_', '/')
    return f


def render_and_clean(string, formatter=pfmt, **kwargs):
    formatted_string = formatter.format(string, **kwargs)
    return clean_template(formatted_string)
