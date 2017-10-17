import re
import string
from collections import defaultdict
from pathlib import Path

base_template = (''
                 '{folder_tag_stuff}/'
                 '{analyzed_start[analysis_stage]}/'
                 '{raw_start[sample_name]}_'
                 '{human_timestamp}_'
                 '[temp={raw_event[data][temperature]:1.2f}'
                 '{raw_descriptor[data_keys][temperature][units]}]_'
                 '[dx={raw_event[data][diff_x]:1.3f}'
                 '{raw_descriptor[data_keys][diff_x][units]}]_'
                 '[dy={raw_event[data][diff_y]:1.3f}'
                 '{raw_descriptor[data_keys][diff_y][units]}]_'
                 '{raw_start[uid]:.6}_'
                 '{raw_event[seq_num]:03d}{ext}')


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
        d = d.replace('[{}=]'.format(r), '')
    z = re.sub(r"__+", "_", d)
    z = z.replace('_.', '.')
    e = z.replace('[', '')
    e = e.replace(']', '')
    e = e.replace('(', '')
    e = e.replace(')', '')
    f = Path(e).as_posix()
    f = f.replace('/_', '/')
    print('saving file at {}'.format(f))
    return f

# Replaces each parameter name within folder_tag_list with the value of that parameter.
# 
def get_filename_prefix(folder_tag_list,md):
    result =''
    for tag in folder_tag_list:
        if isinstance(tag,str):
            addition = str(md[tag])
        else:
            sub_md = md.copy()
            for item in tag:
                sub_md = sub_md[item]
            addition = str(sub_md) 
        result += addition + '/'
    return result


def render_and_clean(string, formatter=pfmt, **kwargs):
    try:
        folder_tag_list = kwargs['folder_tag_list']
    except(KeyError):
        folder_tag_list = []
    filename_prefix = get_filename_prefix(folder_tag_list, kwargs)
    # format replaces curly braces with the value in kwargs associated with the key in curly braces
    formatted_string = formatter.format(string, **kwargs)
    return clean_template(filename_prefix + formatted_string)
