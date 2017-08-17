##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os

import matplotlib

from xpdan.glbl_gen import make_glbl, load_configuration

# matplotlib.use('qt4agg')

try:
    env_code = os.environ['XPDAN_SETUP']
except KeyError:
    env_code = 1
print('ENV_CODE = {}'.format(env_code))
an_glbl = make_glbl(
    load_configuration('xpdan'),
    env_code)
