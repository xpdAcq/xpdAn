#!/usr/bin/env python
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
from xpdan.data_reduction_core import (integrate_and_save, save_tiff,
                                       integrate_and_save_last, save_last_tiff)
from xpdan.glbl import an_glbl
from functools import partial
import inspect

# We are going to do some inspection magic to make functions who's default
# kwargs come from the globals

int_save_kwargs = {}
for k in inspect.signature(integrate_and_save).parameters.keys():
    if k in an_glbl.keys():
        int_save_kwargs[k] = an_glbl[k]

int_save_kwargs.update({'db': an_glbl['exp_db'],
                        'save_dir': an_glbl['tiff_base']})

tiff_save_kwargs = {}
for k in inspect.signature(save_tiff).parameters.keys():
    if k in an_glbl.keys():
        tiff_save_kwargs[k] = an_glbl[k]

tiff_save_kwargs.update({'db': an_glbl['exp_db'],
                         'save_dir': an_glbl['tiff_base']})

integrate_and_save = partial(integrate_and_save, **int_save_kwargs)

integrate_and_save_last = partial(integrate_and_save_last, **int_save_kwargs)

save_tiff = partial(save_tiff, **tiff_save_kwargs)

save_last_tiff = partial(save_last_tiff, **tiff_save_kwargs)

