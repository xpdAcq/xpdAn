##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Elizabeth Culbertson
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################

import os
import pytest
from xpdan.formatters import (clean_template, get_filename_prefix, render_and_clean)
import yaml

@pytest.fixture(scope='module')
def test_md():
	return {'analysis_stage': 'raw', \
	'beamline_config': {}, \
	'beamline_id': '28-ID-2', \
	'bt_experimenters': ['Long', 'Yang', 'Elizabeth', 'Culbertson'], \
	'bt_piLast': 'Billinge', \
	'bt_safN': '301750', \
	'bt_uid': '34f53730', \
	'bt_wavelength': 0.1867, \
	'composition_string': 'C18.0H14.0', \
	'detector_calibration_client_uid': '57d48021-d6a3-4b98-99ac-dfc9ba4cdff2', \
	'detectors': ['pe1'], \
	'facility': 'NSLS-II', \
	'group': 'XPD', \
	'hints': {'dimensions': [['time', 'primary']]}, \
	'lead_experimenter': ['Elizabeth'], \
	'mask_client_uid': '57d48021-d6a3-4b98-99ac-dfc9ba4cdff2', \
	'num_intervals': 0, \
	'num_points': 1, \
	'plan_args': {'detectors': ["PerkinElmerContinuous(prefix='XF:28IDC-ES:1{Det:PE1}', name='pe1', read_attrs=['tiff', 'stats1.total'], configuration_attrs=['cam', 'images_per_set', 'number_of_sets'])"], 'num': 1}, 
	'plan_name': 'count', \
	'plan_type': 'generator', \
	'sa_uid': '67043ebb', \
	'sample_composition': {'C': 18.0, 'H': 14.0}, \
	'sample_name': 'undoped_ptp', \
	'sample_phase': {'C18H14': 1.0}, \
	'sc_dk_field_uid': 'ad7be0bf-b52e-44f6-ad99-9fb330414df2', \
	'scan_id': 980, \
	'sp_computed_exposure': 120.0, \
	'sp_num_frames': 60.0, \
	'sp_plan_name': 'ct', 
	'sp_requested_exposure': 120, \
	'sp_time_per_frame': 2.5, \
	'sp_type': 'ct', \
	'sp_uid': '6da5d267-257a-44e8-9b7c-068d08ab7f68', \
	'time': 1508919212.3547237, \
	'uid': '14c5fe8a-0462-4df4-8440-f738ccd83380', \
	'xpdacq_md_version': 0.1}

#case 1 - all tags are first-level
#case 2 - first-level tag with float
#case 3 - mixture of first-level and tuple tags
#case 4 - one tag doesn't actually exist in metadata
#case 5 - one tag, inside tuple, doesn't exist in metadata
#case 6 - no tags exist in metadata
@pytest.mark.parametrize("test_input, expected" ,
	[(["beamline_id"], "28-ID-2/"),
	(["beamline_id", "sp_time_per_frame"], "28-ID-2/2-5/"),
	(["lead_experimenter", "sample_name", ("sample_composition", "C")], "['Elizabeth']/undoped_ptp/18/"),
	(["lead_experimenter", "fake_tag", ("sample_composition", "C")], "['Elizabeth']//18/"),
	(["lead_experimenter", "sample_name", ("another_fake_tag", "C")], "['Elizabeth']/undoped_ptp//"),
	(["fake_tag", ("another_fake_tag", "fake_subtag")], "//")])
def test_get_filename_prefix(test_md, test_input, expected):
	md = test_md
	assert get_filename_prefix(test_input, md) == expected

#Adds the case of no folder list provided
@pytest.mark.parametrize("test_folder_list, expected" ,
	[([],"filename_details"),
	(["beamline_id"], "28-ID-2/filename_details"),
	(["beamline_id", "sp_time_per_frame"], "28-ID-2/2-5/filename_details"),
	(["lead_experimenter", "sample_name", ("sample_composition", "C")], "Elizabeth/undoped_ptp/18/filename_details"),
	(["lead_experimenter", "fake_tag", ("sample_composition", "C")], "Elizabeth/18/filename_details"),
	(["lead_experimenter", "sample_name", ("another_fake_tag", "C")], "Elizabeth/undoped_ptp/filename_details"),
	(["fake_tag", ("another_fake_tag", "fake_subtag")], "filename_details")])
def test_render_and_clean(test_md, test_folder_list, expected):
    md = test_md
    if test_folder_list!=[]:
    	md['folder_tag_list'] = test_folder_list
	assert render_and_clean("filename_details", md) == expected
