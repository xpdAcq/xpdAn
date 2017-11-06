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
        d = d.replace('[{}=]'.format(r), '')
    z = re.sub(r"__+", "_", d)
    z = z.replace('_.', '.')
    e = z.replace('[', '')
    e = e.replace(']', '')
    e = e.replace('(', '')
    e = e.replace(')', '')
    e = e.replace('\'','')
    f = Path(e).as_posix()
    f = f.replace('/_', '/')
    print('saving file at {}'.format(f))
    return f

# Replaces each parameter name within folder_tag_list with the value of that parameter.
def get_filename_prefix(folder_tag_list,md):
    result =''
    for tag in folder_tag_list:
        if isinstance(tag,str):
            try:
                raw_addition = md[tag]
            except(KeyError):
                print("Error: " + str(tag) + " not valid folder tag. " + str(tag) + " was skipped in defining the file path.")
                raw_addition = ''
        else:
            sub_md = md.copy()
            for item in tag:
                try:
                    sub_md = sub_md[item]
                    raw_addition = sub_md 
                except(KeyError):
                    print("Error: " + str(tag) + " not valid folder tag. " + str(tag) + " was skipped in defining the file path.")
                    raw_addition = ''
        if (type(raw_addition)!=float):
            addition = str(raw_addition)
        else:
            if (raw_addition-int(raw_addition))==0:
                addition = str(int(raw_addition))
            else:
                addition = str(raw_addition).replace('.','-')
        result += addition + '/'
    return result


def render_and_clean(string, formatter=pfmt, **kwargs):
    try:
        folder_tag_list = kwargs['folder_tag_list']
    except(KeyError):
        #Current default behavior is no prefix. We could make a default, for example, sample name
        folder_tag_list = []
    filename_prefix = get_filename_prefix(folder_tag_list, kwargs)
    # format replaces curly braces with the value in kwargs associated with the key in curly braces
    formatted_string = formatter.format(string, **kwargs)
    return clean_template(filename_prefix + formatted_string)
