import numpy as np


def pdf_saver(r, pdf, template):
    rpdf = np.vstack((r, pdf))
    rpdf = rpdf.T
    np.savetxt(template, rpdf)