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

import pytest

from xpdan.formatters import (PartialFormatter, get_filename_prefix,
                              render_and_clean)


@pytest.fixture(scope='module')
def pfmt():
    return PartialFormatter()


# case 1 - all tags are first-level
# case 2 - first-level tag with float
# case 3 - mixture of first-level and tuple tags
# case 4 - one tag doesn't actually exist in metadata
# case 5 - one tag, inside tuple, doesn't exist in metadata
# case 6 - no tags exist in metadata
@pytest.mark.parametrize("test_input, expected",
                         [(["beamline_id"], "28-ID-2/"),
                          (["beamline_id", "sp_time_per_frame"],
                           "28-ID-2/2-5/"),
                          (["lead_experimenter", "sample_name",
                            ("sample_composition", "C")],
                           "['Elizabeth']/undoped_ptp/18/"),
                          (["lead_experimenter", "fake_tag",
                            ("sample_composition", "C")],
                           "['Elizabeth']//18/"),
                          (["lead_experimenter", "sample_name",
                            ("another_fake_tag", "C")],
                           "['Elizabeth']/undoped_ptp//"),
                          (["fake_tag", ("another_fake_tag", "fake_subtag")],
                           "//")])
def test_get_filename_prefix(test_md, test_input, expected):
    md = test_md
    assert get_filename_prefix(test_input, md) == expected


@pytest.fixture()
def short_example_template():
    return (''
            '{folder_prefix}/'
            'filename_details')


# Adds the case of no folder list provided
@pytest.mark.parametrize("test_folder_list, expected",
                         [([], "undoped_ptp/filename_details"),
                          (["beamline_id"], "28-ID-2/filename_details"),
                          (["beamline_id", "sp_time_per_frame"],
                           "28-ID-2/2-5/filename_details"),
                          (["lead_experimenter", "sample_name",
                            ("sample_composition", "C")],
                           "Elizabeth/undoped_ptp/18/filename_details"),
                          (["lead_experimenter", "fake_tag",
                            ("sample_composition", "C")],
                           "Elizabeth/18/filename_details"),
                          (["lead_experimenter", "sample_name",
                            ("another_fake_tag", "C")],
                           "Elizabeth/undoped_ptp/filename_details"),
                          (["fake_tag", ("another_fake_tag", "fake_subtag")],
                           "undoped_ptp/filename_details")])
def test_render_and_clean(test_md, pfmt, short_example_template,
                          test_folder_list, expected):
    md = test_md
    if test_folder_list:
        md['folder_tag_list'] = test_folder_list
    formatter = pfmt
    assert render_and_clean(short_example_template, formatter=formatter,
                            raw_start=md) == expected
