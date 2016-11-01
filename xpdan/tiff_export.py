import os
import numpy as np
from bluesky.callbacks.broker import LiveTiffExporter


class XpdSubtractedTiffExporter(LiveTiffExporter):
    """ Exporting tiff from given header(s).

    Incorporate metadata and data from individual data points in
    the filenames. If scan was executed following xpdAcq workflow,
    dark frame subtraction would also be performed.

    Parameters
    ----------
    field : str
        a data key, e.g., 'image'
    template : str
        A templated file path, where curly brackets will be filled in with
        the attributes of 'start', 'event', and (for image stacks) 'i',
        a sequential number.
        e.g., "dir/scan{start.scan_id}_by_{start.experimenter}_{i}.tiff"
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

    Attributes
    ----------
    filenames : list of filenames written in ongoing or most recent run
    """

    def __init__(self, field, template, save_dark=False,
                 dryrun=False, overwrite=False, db=None):
        # additional attribute for save dark frame option
        self.save_dark = save_dark
        super().__init__(field, template, dryrun, overwrite, db)

    def start(self, doc):
        # The metadata refers to the scan uid of the dark scan.
        if 'dark_frame' in doc:
            # found a dark header
            self.dark_uid = None
            self._is_dark = True
        elif 'dark_frame' in doc:
            if 'sc_dk_field_uid' in doc:
                self.dark_uid = doc['sc_dk_field_uid']
            else:
                print("INFO: No dark_frame was associated in this scan."
                      "no subtraction will be performed")
                self.dark_uid = None
            self._is_dark = False
        else:
            # left extra slot here for edgy case
            pass
        super().start(doc)

    def _save_image(self, image, filename):
        """ method to save image """
        fn_head, fn_tail = os.path.splitext(filename)
        if not os.path.isdir(fn_head):
            os.makedirs(fn_head, exist_ok=True)

        if not self.overwrite:
            if os.path.isfile(filename):
                raise OSError("There is already a file at {}. Delete "
                              "it and try again.".format(filename))
        if not self.dryrun:
            self._tifffile.imsave(filename, np.asarray(image))
            print("INFO: {} has been saved at {}"
                  .format(fn_head, fn_tail))

        self.filenames.append(filename)

    def event(self, doc):
        """ tiff-saving operation applied at event level """
        if self.field not in doc['data']:
            raise KeyError('required field = {} is not in header'
                           .format(self.field))

        self.db.fill_event(doc)  # modifies in place
        image = np.asarray(doc['data'][self.field])

        # pull out dark image
        if self.dark_uid is not None:
            # find dark img
            dark_header = self.db[self.dark_uid]
            dark_img = self.db.get_images(dark_header,
                                          self.field).squeeze()
        else:
            # no dark_uid -> make a dummy dark
            dark_img = np.zeros_like(image)

        image = (image - dark_img)
        if image.ndim == 2:
            filename = self.template.format(start=self._start, event=doc)
            self._save_image(image, filename)
            # if user wants wants raw dark as well
            if self.save_dark:
                self._save_image(dark_img, 'dark_'+filename)
        if image.ndim == 3:
            for i, plane in enumerate(image):
                filename = self.template.format(i=i, start=self._start,
                                                event=doc)
                self._save_image(plane, filename)
                # if user wants wants raw dark as well
                if self.save_dark:
                    self._save_image(dark_img, 'dark_'+filename)
