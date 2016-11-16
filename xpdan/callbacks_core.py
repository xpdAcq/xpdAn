"""module includes Callback classes for xpdacq/xpdan"""

import os
import datetime
import numpy as np
from bluesky.callbacks.core import CallbackBase
import doct
import tifffile

# supplementary functions
def _timestampstr(timestamp):
    """convert timestamp to strftime formate"""
    timestring = datetime.datetime.fromtimestamp(float(timestamp)).strftime(
        '%Y%m%d-%H%M%S')
    return timestring


class XpdAcqLiveTiffExporter(CallbackBase):

    """Exporting tiff from given header(s).

    It is a variation of bluesky.callback.broker.LiveTiffExporter class
    It incorporate metadata and data from individual data points in
    the filenames.

    If also allow a room for dark subtraction if a valid dark_frame
    metdata scheme is taken

    Parameters
    ----------
    field : str
        a data key, e.g., 'image'
    data_dir_template : str
        A templated for directory where images will be saved to.
        It is expressed with curly brackets, which will be filled in with
        the attributes of 'start', 'event', and (for image stacks) 'i',
        a sequential number.
        e.g., "/xpdUser/tiff_base/{start.sample_name}/"
    data_fields : list, optional
        a list of strings for data fields want to be included. default
        is an empty list (not include any readback metadata in filename).
    save_dark : bool, optionl
        option to save dark frames, if True, subtracted images and dark
        images would be saved. default is False.
    dryrun : bool, optional
        default to False; if True, do not write any files
    overwrite : bool, optional
        default to False, raising an OSError if file exists
    db : Broker, optional
        The databroker instance to use, if not provided use databroker
        singleton
    """

    def __init__(self, field, data_dir_template,
                 data_fields=[], save_dark=False,
                 dryrun=False, overwrite=False, db=None):
        if db is None:
            # Read-only db
            from databroker.databroker import DataBroker as db

        self.db = db

        # required args
        self.field = field
        self.data_dir_template = data_dir_template
        # optioanal args 
        self.data_fields = data_fields  # list of keys for md to include
        self.save_dark = save_dark  # option of save dark 
        self.dryrun = dryrun
        self.overwrite = overwrite
        self.filenames = []
        self._start = None

    def _generate_filename(self, doc, stack_ind):
        """method to generate filename based on template

        It operates at event level, i.e., doc is event document
        """
        # convert time
        timestr = _timestampstr(doc['time'])
        # readback value for certain list of data keys
        data_val_list = []
        for key in self.data_fields:
            val = doc.get(key, None)
            if val is not None:
                data_val_list.append(val)
        data_val_trunk = '_'.join(data_val_list)

        # event sequence
        base_dir = self.data_dir_template.format(start=self._start,
                                                     event=doc)
        # standard, do need to expose it to user
        event_template = '{event.seq_num:03d}_{i}.tif'
        event_info = event_template.format(i=stack_ind, start=self._start,
                                           event=doc)

        # full path + complete filename
        filename = '_'.join([timestr, data_val_trunk, event_info])
        total_filename = os.path.join(base_dir, filename)

        return total_filename

    def _save_image(self, image, filename):
        """method to save image"""
        dir_path, fn = os.path.split(filename)
        os.makedirs(dir_path, exist_ok=True)

        if not self.overwrite and os.path.isfile(filename):
            raise OSError("There is already a file at {}. Delete "
                          "it and try again.".format(filename))
        if not self.dryrun:
            tifffile.imsave(filename, np.asarray(image))
            print("INFO: {} has been saved at {}"
                  .format(fn, dir_path))

        self.filenames.append(filename)

    def _pull_dark_uid(self, doc, dark_field_key='sc_dk_field_uid'):
        """method to relate dark to images

        Simply replace this method if scheme is changed in the future
        """
        if 'dark_frame' in doc:
            # found a dark header
            dark_uid = None  # dark header, pass
        else:
            dark_uid = doc.get(dark_field_key, None)
            if dark_uid is None:
                print("INFO: no dark frame is associated in this header, "
                      "subtraction will not be processed")
                dark_uid = None  # can't find a dark
        return dark_uid

    def start(self, doc):
        """method for start document"""
        self.filenames = []
        # Convert doc from dict into dottable dict, more convenient
        # in Python format strings: doc.key == doc['key']
        self._start = doct.Document('start', doc)

        # find dark scan uid
        dark_uid = self._pull_dark_uid(doc)
        if dark_uid is None:
            self.dark_img = None
            self._find_dark = False
        else:
            dark_header = self.db[dark_uid]
            self.dark_img = np.asarray(self.db.get_images(dark_header,
                                                          self.image_field)
                                      ).squeeze()
            self._find_dark = True
        super().start(doc)

    def event(self, doc):
        """tiff-saving operation applied at event level"""
        if self.field not in doc['data']:
            raise KeyError('required field = {} is not in header'
                           .format(self.field))

        self.db.fill_event(doc)  # modifies in place
        image = np.asarray(doc['data'][self.field])

        if self.dark_img is None:
            # make a dummy dark
            self.dark_img = np.zeros_like(image)

        if image.ndim == 2:
            image = np.expand_dims(image, 0)  # extend the first axis

        for i, plane in enumerate(image):
            image = np.subtract(plane, self.dark_img)
            filename = self._generate_filename(doc, i)
            path_dir, fn = os.path.split(filename)
            if self._find_dark:
                self._save_image(plane, os.path.join(path_dir,
                                                     'sub_'+fn))
            else:
                self._save_image(plane, filename)
            # if user wants raw dark
            if self.save_dark:
                self._save_image(self.dark_img, os.path.join(path_dir,
                                                             'dark_'+fn))
    def stop(self, doc):
        """method for stop document"""
        # TODO: include sum logic in the future
        self._start = None
        self.filenames = []
        super().stop(doc)

