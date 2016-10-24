import os
try:
    from skbeam.io.fit2d_io import fit2d_save, read_fit2d_msk
except ImportError:
    from skbeam.io.save_powder_output import _create_file_path
    from fabio.fit2dmaskimage import Fit2dMaskImage
    import fabio


    def fit2d_save(mask, filename, dir_path=None):
        """ Compresses and wraps the mask for Fit2D use

        Parameters
        ----------
        mask: ndarray
            The mask
        filename: str
            The filename
        dir_path: str, optional
            Path to the destination file
        """
        saver = Fit2dMaskImage(data=~mask)
        saver.write(_create_file_path(dir_path, filename, '.msk'))


    def read_fit2d_msk(filename):
        """ Reads mask from fit2d `.msk` file

        Parameters
        ----------
        filename: str
            Filename, including path, relative path name supported

        Returns
        -------
        ndarray:
            The mask as boolean array
        """
        a = fabio.open(os.path.expanduser(filename))
        return ~a.data.astype(bool)

