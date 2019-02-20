import os
import time

import numpy as np
from bluesky.callbacks.core import CallbackBase
from skbeam.io import save_output
from skbeam.io.fit2d import fit2d_save
from tifffile import imsave
from xpdan.formatters import pfmt, clean_template, render2
from xpdan.io import pdf_saver, dump_yml
from xpdan.vend.callbacks.core import Retrieve
from xpdtools.dev_utils import _timestampstr


class StartStopCallback(CallbackBase):
    """Print the time for analysis"""

    def __init__(self):
        self.t0 = 0
        self.event_t0 = None
        self.total_analysis_time = 0
        self.n_events = 1

    def start(self, doc):
        self.t0 = time.time()
        self.total_analysis_time = 0
        self.n_events = 1
        print("START ANALYSIS ON {}".format(doc["uid"]))

    def event(self, doc):
        self.n_events = doc["seq_num"]
        if not self.event_t0:
            self.event_t0 = time.time()
        else:
            self.total_analysis_time += time.time() - self.event_t0
            print(
                "Single Event Analysis time {}".format(
                    time.time() - self.event_t0
                )
            )
            self.event_t0 = time.time()

    def stop(self, doc):
        print("FINISH ANALYSIS ON {}".format(doc.get("run_start", "NA")))
        print(
            "Average Event analysis time {}".format(
                self.total_analysis_time / self.n_events
            )
        )
        print("Analysis time {}".format(time.time() - self.t0))


class SaveBaseClass(Retrieve):
    """Base class for saving files with human friendly file names.

    For each document this class applys a format to the string based off the
    document which has been seen. When the time comes for the file to be
    written any templating which has not been formatted will be removed from
    the filename.

    Parameters
    ----------
    template : str
        The templated filename
    handler_reg : dict
        The registry of file handlers for loading files from disk
    root_map : dict
        Mapping between the old file root and a new root, used for loading
        files from disk
    kwargs : dict
        All extra kwargs are passed to the filename formatter when the start
        document is received

    Notes
    -----
    Every instance of ``__independent_vars__`` will be replaced with
    ``{name}_{data}_{units}_`` for each independent variable of the experiment
    in the template.
    """

    def __init__(
        self, template, handler_reg, root_map=None, base_folders=None, **kwargs
    ):
        if base_folders is None:
            base_folders = []
        elif isinstance(base_folders, str):
            base_folders = [base_folders]
        self.base_folders = base_folders
        self._template = template

        self.start_template = ""
        self.descriptor_templates = {}
        self.dim_names = []
        self.kwargs = kwargs
        self.in_dep_shapes = {}
        self.dep_shapes = {}
        # If you see this filename something bad happened (no metadata was
        #  captured/formatted)
        self.filenames = "something_horrible_happened.xxx"

        super().__init__(handler_reg, root_map)

    def start(self, doc):
        # Get the independent vars
        self.dim_names = [
            d[0][0]
            for d in doc.get("hints", {}).get("dimensions")
            if d[0][0] != "time"
        ]
        if "original_start_uid" not in doc:
            doc["original_start_uid"] = doc["uid"]
        if "original_start_time" not in doc:
            doc["original_start_time"] = doc["time"]

        # use the magic formatter to leave things behind
        self.start_template = render2(
            self._template,
            start=doc,
            human_timestamp=_timestampstr(doc["original_start_time"]),
            **doc,
            **self.kwargs,
        )
        return super().start(doc)

    def descriptor(self, doc):
        self.in_dep_shapes = {
            n: doc["data_keys"][n]["shape"] for n in self.dim_names
        }
        self.dep_shapes = {
            n: doc["data_keys"][n]["shape"]
            for n in set(self.dim_names) ^ set(doc["data_keys"])
        }

        # Use independent vars to create the filename
        independent_var_string = "_"
        for dim in sorted(self.dim_names):
            # Only use scalar data in filenames
            if len(self.in_dep_shapes[dim]) == 0:
                independent_var_string += "{name}_{data}_{units}_".format(
                    name=dim,
                    data=f"{{event[data][{dim}]:1.{doc['data_keys'][dim]['precision']}f}}",
                    # TODO: fill in the sig figs
                    units=f"{doc['data_keys'][dim].get('units', 'arb')}",
                )

        self.descriptor_templates[doc["uid"]] = pfmt.format(
            self.start_template,
            descriptor=doc,
            __independent_vars__=independent_var_string,
        )

        return super().descriptor(doc)

    def event(self, doc):
        self.filenames = [
            pfmt.format(
                self.descriptor_templates[doc["descriptor"]],
                event=doc,
                base_folder=bf,
            )
            .replace(".", ",")
            .replace("__", "_")
            for bf in self.base_folders
        ]
        # Note that formally there are more steps to the formatting, but we
        #  should have the folder by now
        for filename in self.filenames:
            print(f"Saving file to {filename}")
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        return super().event(doc)


class SaveTiff(SaveBaseClass):
    """Callback for saving Tiff files"""

    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for two_d_var in [
            k for k, v in self.dep_shapes.items() if len(v) == 2
        ]:
            for filename in self.filenames:
                imsave(
                    clean_template(
                        pfmt.format(filename, ext=f"_{two_d_var}.tiff")
                    ),
                    doc["data"][two_d_var],
                )


class SaveIntensity(SaveBaseClass):
    """Callback for saving Q and tth ``.chi`` files"""

    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for one_d_ind_var in [
            k for k, v in self.in_dep_shapes.items() if len(v) == 1
        ]:
            for one_d_dep_var in [
                k for k, v in self.dep_shapes.items() if len(v) == 1
            ]:
                for filename in self.filenames:
                    save_output(
                        doc["data"][one_d_ind_var],
                        doc["data"][one_d_dep_var],
                        clean_template(
                            pfmt.format(
                                filename,
                                ext=f"_{one_d_dep_var}_{one_d_ind_var}",
                            )
                        ),
                        {"tth": "2theta", "q": "Q"}.get(one_d_ind_var),
                    )


class SaveMask(SaveBaseClass):
    """Callback for saving masks as ``.msk`` and ``.npy`` files"""

    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for two_d_var in [
            k for k, v in self.dep_shapes.items() if len(v) == 2
        ]:
            for filename in self.filenames:
                fit2d_save(
                    np.flipud(doc["data"][two_d_var]),
                    clean_template(pfmt.format(filename, ext="")),
                )
                np.save(
                    clean_template(pfmt.format(filename, ext="_mask.npy")),
                    doc["data"][two_d_var],
                )


class SavePDFgetx3(SaveBaseClass):
    """Callback for saving PDF, F(Q), S(Q) files"""

    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for one_d_ind_var in [
            k for k, v in self.in_dep_shapes.items() if len(v) == 1
        ]:
            for one_d_dep_var in [
                k for k, v in self.dep_shapes.items() if len(v) == 1
            ]:
                for filename in self.filenames:
                    pdf_saver(
                        doc["data"][one_d_ind_var],
                        doc["data"][one_d_dep_var],
                        doc["data"]["config"],
                        clean_template(
                            pfmt.format(filename, ext=f".{one_d_dep_var}")
                        ),
                    )


class SaveMeta(SaveBaseClass):
    """Callback for saving metadata files"""

    def start(self, doc):
        doc = dict(doc)
        doc["analysis_stage"] = "meta"
        super().start(doc)
        self.filenames = [
            pfmt.format(self.start_template, base_folder=bf).replace(".", ",")
            for bf in self.base_folders
        ]

        for filename in self.filenames:
            fn = clean_template(pfmt.format(filename, ext=".yaml"))
            print(f"Saving file to {fn}")
            os.makedirs(os.path.dirname(fn), exist_ok=True)
            dump_yml(fn, doc)

    def event(self, doc):
        pass


class SaveCalib(SaveBaseClass):
    """Callback for saving pyFAI calibrations as ``.poni`` files"""

    def event(self, doc):
        doc = super().event(doc)

        for filename in self.filenames:
            doc["data"]["calibration"].save(
                clean_template(pfmt.format(filename, ext=".poni"))
            )


SAVER_MAP = {
    "dark_sub": SaveTiff,
    "integration": SaveIntensity,
    "mask": SaveMask,
    "pdf": SavePDFgetx3,
    "fq": SavePDFgetx3,
    "sq": SavePDFgetx3,
    "calib": SaveCalib,
    "raw": SaveMeta,
}


class Live3DView(CallbackBase):
    def __init__(self):
        self.cs_dict = {}
        self.x_dict = {}
        self.fields = []

    def descriptor(self, doc):
        import mayavi.mlab as mlab

        self.fields = [
            k for k, v in doc["data_keys"].items() if len(v["shape"]) == 3
        ]
        for field in self.fields:
            self.cs_dict[field] = mlab.figure(field)
            self.x_dict[field] = None

    def event(self, doc):
        import mayavi.mlab as mlab
        for field in self.fields:
            data = doc["data"][field]
            figure = self.cs_dict[field]
            x = self.x_dict[field]
            if x is None:
                x = mlab.pipeline.scalar_field(data, figure=figure)
                self.x_dict[field] = x
                mlab.pipeline.image_plane_widget(
                    x,
                    plane_orientation="z_axes",
                    slice_index=3,
                    figure=figure
                )
                mlab.pipeline.volume(x, figure=figure)
            else:
                x.mlab_source.scalars = data

