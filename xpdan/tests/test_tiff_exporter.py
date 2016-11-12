from xpdan.glbl import an_glbl
from xpdan.callbacks_core import XpdAcqLiveTiffExporter

# standard config

template = '/direct/XF28ID1/pe2_data/xpdUser/tiff_base/{start.sample_name}'
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm'] # known devices

def test_tiff_export_smoke_test(exp_db):
    tif_export = XpdAcqLiveTiffExporter('pe1_image', template, data_fields,
                                        overwrite=True, db=exp_db)
    an_glbl.exp_db.process(exp_db[-1], tif_export)
