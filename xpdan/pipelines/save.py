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
                                               # TODO: talk to BLS ask if
                                               #  they like this
                                               # 'analysis_stage': 'meta'
                                               })
                     .map(lambda kwargs, string: render(string, **kwargs),
                          string=base_template)
                     )
start_yaml_string.map(clean_template).zip(start_docs).starsink(dump_yml)

# create filename string
filename_node = all_docs.map(lambda kwargs, string: render(string, **kwargs),
                             string=base_template,
                             stream_name='base path')

# SAVING NAMES
filename_name_nodes = {}
for name, analysis_stage, ext in zip(
        ['dark_corrected_image_name', 'iq_name', 'tth_name', 'mask_fit2d_name',
         'mask_np_name', 'pdf_name', 'fq_name', 'sq_name'],
        ['dark_sub', 'iq', 'itth', 'mask', 'mask', 'pdf', 'fq', 'sq'],
        ['.tiff', '', '_tth', '', '_mask.npy', '.gr', '.fq', '.sq']
):
    if ext:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage,
                                           ext=ext)
    else:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage)

    filename_name_nodes[name] = temp_name_node.map(clean_template)
    (filename_name_nodes[name].map(os.path.dirname)
     .sink(os.makedirs, exist_ok=True,))

# dark corrected img
(filename_name_nodes['dark_corrected_image_name']
 .zip(dark_corrected_foreground)
 .starsink(imsave, stream_name='dark corrected foreground'))
# integrated intensities
(q.combine_latest(mean, emit_on=1).zip(filename_name_nodes['iq_name'])
 .map(lambda l: (*l[0], l[1]))
 .starsink(save_output, 'Q',
           stream_name='save integration {}'.format('Q')))
(tth.combine_latest(mean, emit_on=1).zip(filename_name_nodes['tth_name'])
 .map(lambda l: (*l[0], l[1]))
 .starsink(save_output, '2theta',
           stream_name='save integration {}'.format('tth')))
# Mask
(mask.zip_latest(filename_name_nodes['mask_fit2d_name'])
 .sink(lambda x: fit2d_save(np.flipud(x[0]), x[1])))
(mask.zip_latest(filename_name_nodes['mask_np_name'])
 .sink(lambda x: np.save(x[1], x[0])))
# PDF
(pdf.zip(filename_name_nodes['pdf_name']).map(lambda l: (*l[0], l[1]))
 .starsink(pdf_saver, stream_name='pdf saver'))
# F(Q)
(fq.zip(filename_name_nodes['fq_name']).map(lambda l: (*l[0], l[1]))
 .starsink(pdf_saver, stream_name='fq saver'))
# S(Q)
(sq.zip(filename_name_nodes['sq_name']).map(lambda l: (*l[0], l[1]))
 .starsink(pdf_saver, stream_name='sq saver'))
# '''
