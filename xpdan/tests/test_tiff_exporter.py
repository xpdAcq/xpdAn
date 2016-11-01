from xpdan.glbl import an_glbl
from xpdan.tiff_export import XpdAcqLiveTiffExporter


# standard config

template = '/direct/XF28ID1/pe2_data/xpdUser/tiff_base/{start.sample_name}'
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm'] # known devices


tif_export = XpdAcqLiveTiffExporter('pe1_image', template, data_fields,
                                    overwrite=True, db=an_glbl.exp_db)

# alias
db = an_glbl.exp_db

def tiff_export_smoke_test():
    db.process(db[-1], tif_export)
