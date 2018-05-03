import re
import string
from collections import defaultdict
from pathlib import Path


class PartialFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        # Expects the return value to be a tuple of (obj,used_key)
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
    # this will essentially replace any nonexistent keys (field names) with ''
    d = cfmt.format(template, defaultdict(str))

    for r in removals:
        d = d.replace('[{}_]'.format(r), '')
    z = re.sub(r'_+', '_', d)
    z = z.replace('_.', '.')
    z = re.sub(r'[\[\]\(\)\']', '', z)
    f = Path(z).as_posix()
    f = f.replace('/_', '/')
    print('saving file at {}'.format(f))
    return f


# Replaces each parameter name within folder_tag_list with the
# value of that parameter.
def get_filename_prefix(folder_tag_list, md):
    result = ''
    for tag in folder_tag_list:
        if isinstance(tag, str):
            raw_addition = md.get(tag, '')
        else:
            sub_md = md.copy()
            for item in tag:
                if isinstance(sub_md, dict):
                    sub_md = sub_md.get(item, '')
            raw_addition = sub_md
        if type(raw_addition) != float:
            addition = str(raw_addition)
        else:
            if (raw_addition - int(raw_addition)) == 0:
                addition = str(int(raw_addition))
            else:
                addition = str(raw_addition).replace('.', '-')
        result += addition + '/'
    return result


def render(string, formatter=pfmt, **kwargs):
    md = kwargs.get('raw_start', '')
    filename_prefix = ''
    if md != '':
        folder_tag_list = md.get('folder_tag_list', ['sample_name'])
        filename_prefix = get_filename_prefix(folder_tag_list, md)
        if re.fullmatch(r'/+', filename_prefix):
            filename_prefix = md.get('sample_name')
    formatted_string = formatter.format(string, folder_prefix=filename_prefix,
                                        **kwargs)
    return formatted_string


def render_and_clean(string, formatter=pfmt, **kwargs):
    return clean_template(render(string, formatter=formatter, **kwargs))
