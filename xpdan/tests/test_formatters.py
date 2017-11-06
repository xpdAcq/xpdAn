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

def get_md_example(filename):
	with open(filename) as f:
    # use safe_load instead load
		md_example = yaml.safe_load(f)
		return md_example

#case 1 - all tags are first-level
#case 2 - first-level tag with float
#case 3 - mixture of first-level and tuple tags
#case 4 - one tag doesn't actually exist in metadata
#case 5 - one tag, inside tuple, doesn't exist in metadata
#case 6 - no tags exist in metadata
@pytest.mark.parameterize("test_input, expected" ,
	[(["beamline_id"],"28-ID-2/"),
	(["beamline_id","sp_time_per_frame"],"28-ID-2/2-5/"),
	(["lead_experimenter","sample_name",("sample_composition","C")],"['Elizabeth']/undoped_ptp/18/"),
	(["lead_experimenter","fake_tag",("sample_composition","C")],"['Elizabeth']//18/"),
	(["lead_experimenter","sample_name",("another_fake_tag","C")],"['Elizabeth']/undoped_ptp//"),
	(["fake_tag",("another_fake_tag","fake_subtag")],"//")])
def test_get_filename_prefix(test_input, expected, md = {}):
	assert get_filename_prefix(test_input,md) == expected 

#Adds the case of no folder list provided
@pytest.mark.parameterize("test_folder_list, expected" ,
	[([],"filename_details"),
	(["beamline_id"],"28-ID-2/filename_details"),
	(["beamline_id","sp_time_per_frame"],"28-ID-2/2-5/filename_details"),
	(["lead_experimenter","sample_name",("sample_composition","C")],"Elizabeth/undoped_ptp/18/filename_details"),
	(["lead_experimenter","fake_tag",("sample_composition","C")],"Elizabeth/18/filename_details"),
	(["lead_experimenter","sample_name",("another_fake_tag","C")],"Elizabeth/undoped_ptp/filename_details"),
	(["fake_tag",("another_fake_tag","fake_subtag")],"filename_details")])
def test_render_and_clean(md, test_folder_list,expected)
	if test_folder_list!=[]:
		md['folder_tag_list'] = test_folder_list
	assert render_and_clean("filename_details", md) == expected