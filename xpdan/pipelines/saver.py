import shed.event_streams as es
import os
from ..glbl import an_glbl

dark_template = os.path.join(
    an_glbl['tiff_base'], 'dark/{human_timestamp}.{ext}')

light_template = os.path.join(
    an_glbl['tiff_base'],
    '{sample_name}/{folder_tag}/{analysis_stage}/'
    '{human_timestamp}{auxiliary}.{ext}')

data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm']  # known devices

start_data_fields = ['sample_name', 'folder_tag']

source = es.Stream(name='Template source')


def start_template(doc, template, data_fields):
    d = {}
    for df in data_fields:
        if df in doc.keys():
            d[df] = doc[df]
    return template.format()


start_as_event = es.Eventify(source, start_data_fields)

templated_start = es.map()

base_template_formatter = es.map(start_template, source,
                                 full_event=True, template=template,
                                 data_fields=data_fields)
