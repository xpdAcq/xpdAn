from xpdan.formatters import render_and_clean
from xpdan.pipelines.pipeline_utils import base_template


def test_no_equal(test_md):
    a = render_and_clean(base_template,
                         raw_start=test_md,
                         raw_descriptor={'data_keys': {'temperature':
                                                           {'units': 'K'}}},
                         human_timestamp='19920116-000000',
                         raw_event={'seq_num': 100},
                         ext='.tif',
                         base_folder='hi'
                         )
    assert '=' not in a
    assert a == 'hi/undoped_ptp/undoped_ptp_19920116-' \
                '000000_temp_K_14c5fe_0100.tif'
