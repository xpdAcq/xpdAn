"""Example for XPD data"""

import os

import shed.event_streams as es
import tifffile

from skbeam.io.fit2d import fit2d_save
from skbeam.io.save_powder_output import save_output
from xpdan.db_utils import _timestampstr
from xpdan.glbl import an_glbl
from .light_processing import (source, dark_sub_fg, mask_stream, iq_stream,
                               pdf_stream)


# write to human readable files

# base string
light_template = os.path.join(
    an_glbl['tiff_base'],
    '{sample_name}/{folder_tag}/{{{analysis_stage}}}/'
    '{{human_timestamp}}{{auxiliary}}{{{ext}}}')

eventify_raw = es.eventify(source)


# format base string with data from experiment
# sample_name, folder_tag
def templater1_func(doc, template):
    d = {'sample_name': doc.get('sample_name', ''),
         'folder_tag': doc.get('folder_tag', '')}
    return template.format(**d)


template_stream_1 = es.map(templater1_func, eventify_raw, light_template,
                           full_event=True,
                           output_info=[('template', {'dtype': 'str'})])


# format with auxiliary and time
def templater2_func(doc, template, aux=None):
    if aux is None:
        aux = ['temperature', 'diff_x', 'diff_y', 'eurotherm']
    return template.format(
        # Change to include name as well
        auxiliary='_'.join([doc['data'].get(a, '') for a in aux]),
        human_timestamp=_timestampstr(doc['time'])
    )


template_stream_2 = es.map(templater2_func,
                           es.zip_latest(source,
                                         template_stream_1),
                           output_info=[('template', {'dtype': 'str'})])


# further format with data from analysis stage
def templater3_func(template, analysis_stage='raw', ext='.tiff'):
    return template.format(analysis_stage=analysis_stage, ext=ext)


eventifies = [eventify_raw] + [es.eventify(s) for s in
                               [dark_sub_fg,
                                mask_stream,
                                iq_stream,
                                pdf_stream]]

templater_streams_3 = [es.map(templater3_func,
                              es.zip_latest(template_stream_2, e),
                              full_event=True,
                              output_info=[('template',
                                            {'dtype': 'str'})],
                              ext=ext
                              ) for e, ext in zip(eventifies,
                                                  ['.tiff', '.tiff', '.msk',
                                                   '_Q.chi', '.gr'])]


# write and format with ext
def writer_templater_tiff(img, template):
    tifffile.imsave(template, img)
    return template


def writer_templater_mask(mask, template):
    fit2d_save(mask, template)
    return template


def writer_templater_chi(x, y, template):
    save_output(x, y, template, 'Q', ext='')
    return template


# TODO: need tth writer
# https://github.com/scikit-beam/scikit-beam/blob/master/skbeam/core/utils.py#L1054


def writer_templater_pdf(x, y, template):
    pdf_saver(x, y, template)
    return template


def writer_templater_fq(x, y, template):
    fq_saver(x, y, template)
    return template


iis = [
    {'img': ('pe1_image', 0), 'template': ('template', 1)},
    {'img': ('img', 0), 'template': ('template', 1)},
    {'mask': ('mask', 0), 'template': ('template', 1)},
    {'x': ('q', 0), 'y': ('iq', 1), 'template': ('template', 1)},
    {'x': ('r', 0), 'y': ('pdf', 1), 'template': ('template', 1)},
]

writer_streams = [
    es.map(writer_templater,
           es.zip_latest(s1, s2),
           input_info=ii,
           output_info=[('final_filename',
                         {'dtype': 'str'})],
           ) for s1, s2, ii, writer_templater in
    zip(
        [source, dark_sub_fg, mask_stream,
         iq_stream, pdf_stream],
        templater_streams_3,
        iis,
        [writer_templater_tiff, writer_templater_tiff, writer_templater_mask,
         writer_templater_chi, writer_templater_pdf]
    )]
