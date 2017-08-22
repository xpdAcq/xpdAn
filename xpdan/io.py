import numpy as np


def pdf_saver(r, pdf, filename, header=''):
    rpdf = np.vstack((r, pdf))
    rpdf = rpdf.T
    np.savetxt(filename, rpdf, header=header)
