import os

import numpy as np
import yaml

from .formatters import CleanFormatter, PartialFormatter
from collections import defaultdict

template_hdr = '''[DEFAULT]
# input and output specifications
dataformat = QA
outputtype = gr

# PDF calculation setup
mode = xray
wavelength = {wavelength}
composition = {composition}
bgscale = {bgscale}
rpoly = {rpoly}
qmaxinst = {qmaxinst}
qmin = {qmin}
qmax = {qmax}
rmin = {rmin}
rmax = {rmax}
rstep = {rstep}

# End of config --------------------------------------------------------------

#### start data
#S 1
#L r(Å)  G(Å$^{-2}$)
'''

ordereditems = '''args configfile configsection dataformat
            inputfiles inputpatterns backgroundfile datapath
            output outputtypes force mode wavelength twothetazero
            composition bgscale rpoly qmaxinst qmin qmax
            rmin rmax rstep plot interact verbose'''.split()


def pdf_saver(r, pdf, filename, config):
    config_dict = {k: getattr(config, k, '') for k in ordereditems}
    rpdf = np.vstack((r, pdf))
    rpdf = rpdf.T
    pfmt = PartialFormatter()
    cfmt = CleanFormatter()
    header = pfmt.format(template_hdr, **config_dict)
    header = cfmt.format(header, defaultdict(str))
    np.savetxt(filename, rpdf, header=header)


def dump_yml(filename, data):
    os.makedirs(os.path.split(filename)[0], exist_ok=True)
    with open(filename, 'w') as f:
        yaml.dump(data, f)


def poni_saver(filename, calibration):
    calibration.geoRef.save(filename)
