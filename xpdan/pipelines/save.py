from xpdan.pipelines.main import *

# '''
# SAVING
# May rethink how we are doing the saving. If the saving was attached to the
# translation nodes then it would be run before the rest of the graph was
# processed.

# This could be done by having each saver inside a callback which takes both
# analyzed and raw documents, and creates the path from those two.

start_yaml_string = (start_docs.map(lambda s: {'raw_start': s,
                                               'ext': '.yaml',
                                               'analysis_stage': 'meta'
                                               })
                     .map(lambda kwargs, string, **kwargs2: render(string,
                                                                   **kwargs,
                                                                   **kwargs2),
                          string=base_template,
                          base_folder=glbl_dict['tiff_base'])
                     )
start_yaml_string.map(clean_template).zip(start_docs, first=True).starsink(dump_yml)

# create filename string
filename_node = all_docs.map(
    lambda kwargs, string, **kwargs2: render(string, **kwargs, **kwargs2),
    string=base_template,
    stream_name='base path',
    base_folder=glbl_dict['tiff_base'])

# SAVING NAMES
filename_name_nodes = {}
for name, analysis_stage, ext in zip(
        ['dark_corrected_image_name', 'iq_name', 'tth_name', 'mask_fit2d_name',
         'mask_np_name', 'pdf_name', 'fq_name', 'sq_name', 'calib_name'],
        ['dark_sub', 'iq', 'itth', 'mask', 'mask', 'pdf', 'fq', 'sq', 'calib'],
        ['.tiff', '', '_tth', '', '_mask.npy', '.gr', '.fq', '.sq', '.poni']
):
    if ext:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage,
                                           ext=ext)
    else:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage)

    filename_name_nodes[name] = temp_name_node.map(clean_template,
                                                   stream_name=analysis_stage)

# dark corrected img
a = filename_name_nodes['dark_corrected_image_name'].zip(
    dark_corrected_foreground, first=True)
a.map(lambda l: l[0]).map(os.path.dirname).sink(os.makedirs, exist_ok=True)
(a.starsink(imsave, stream_name='dark corrected foreground'))


b = q.combine_latest(mean, emit_on=1, first=True).zip(
    filename_name_nodes['iq_name'])
b.map(lambda l: l[-1]).map(os.path.dirname).sink(os.makedirs, exist_ok=True)
# integrated intensities
(b
 .map(lambda l: (*l[0], l[1]))
 .starsink(save_output, 'Q',
           stream_name='save integration {}'.format('Q')))

c = tth.combine_latest(mean, emit_on=1, first=True).zip(filename_name_nodes['tth_name'])
c.map(lambda l: l[-1]).map(os.path.dirname).sink(os.makedirs, exist_ok=True, )
(c
 .map(lambda l: (*l[0], l[1]))
 .starsink(save_output, '2theta',
           stream_name='save integration {}'.format('tth')))
# Mask
d = mask.zip_latest(filename_name_nodes['mask_fit2d_name'], first=True)
d.map(lambda l: l[-1]).map(os.path.dirname).sink(os.makedirs, exist_ok=True, )
(d.sink(lambda x: fit2d_save(np.flipud(x[0]), x[1])))
(d.sink(lambda x: np.save(x[1], x[0])))

# PDF
for k, name, source in zip(['pdf_name', 'fq_name', 'sq_name'],
                           ['pdf saver', 'fq saver', 'sq saver'],
                           [pdf, fq, sq]):
    e = source.zip(filename_name_nodes[k], first=True)
    (e.map(lambda l: l[-1]).map(os.path.dirname).sink(os.makedirs,
                                                     exist_ok=True, ))
    (e.map(lambda l: (*l[0], l[1]))
     .starsink(pdf_saver, stream_name='name'))
# calibration
f = gen_geo.zip(filename_name_nodes['calib_name'], first=True)
f.map(lambda l: l[-1]).map(os.path.dirname).sink(os.makedirs, exist_ok=True, )
(f.starsink(lambda x, n: x.save(n), stream_name='cal saver'))
# '''

save_kwargs = start_yaml_string.kwargs
filename_node.kwargs = save_kwargs

raw_source.visualize(source_node=True)
