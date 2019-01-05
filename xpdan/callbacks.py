from bluesky.callbacks.core import CallbackBase
from skbeam.io import save_output
from skbeam.io.fit2d import fit2d_save
from tifffile import imsave
from xpdan.formatters import pfmt, clean_template, render2
from xpdan.io import pdf_saver, dump_yml
from xpdan.vend.callbacks.core import Retrieve
from xpdtools.dev_utils import _timestampstr
import numpy as np
import os


class StartStopCallback(CallbackBase):
    def __init__(self):
        self.t0 = 0

    def start(self, doc):
        self.t0 = time.time()
        print('START ANALYSIS ON {}'.format(doc['uid']))

    def stop(self, doc):
        print("FINISH ANALYSIS ON {}".format(doc.get("run_start", "NA")))
        print('Analysis time {}'.format(time.time() - self.t0))


class SaveBaseClass(Retrieve):
    def __init__(
        self, template, handler_reg, root_map=None, executor=None, **kwargs
    ):
        self._template = template

        self.start_template = ""
        self.descriptor_templates = {}
        self.dim_names = []
        self.kwargs = kwargs

        super().__init__(handler_reg, root_map, executor)

    def start(self, doc):
        # Get the independant vars
        self.dim_names = [
            d[0][0]
            for d in doc.get("hints", {}).get("dimensions")
            if d[0][0] != "time"
        ]

        # Use independant vars to create the filename
        independent_var_string = "_"
        for dim in self.dim_names:
            independent_var_string += "{name}_{data}_{units}_".format(
                name=dim,
                data=f"{{event[data][{dim}]:1.{{descriptor[data_keys]"
                f"[{dim}][precision]}}f}}",
                # TODO: fill in the sig figs
                units=f"{{descriptor[data_keys][{dim}][units]}}",
            )

        # use the magic formatter to leave things behind
        self.start_template = render2(
            self._template,
            start=doc,
            human_timestamp=_timestampstr(doc["time"]),
            __independent_vars__=independent_var_string,
            **doc,
            **self.kwargs,
        )
        return super().start(doc)

    def descriptor(self, doc):
        self.descriptor_templates[doc["uid"]] = pfmt.format(
            self.start_template, descriptor=doc
        )
        self.in_dep_shapes = {
            n: doc["data_keys"][n]["shape"] for n in self.dim_names
        }
        self.dep_shapes = {
            n: doc["data_keys"][n]["shape"]
            for n in set(self.dim_names) ^ set(doc["data_keys"])
        }
        return super().descriptor(doc)

    def event(self, doc):
        self.filename = pfmt.format(
            self.descriptor_templates[doc["descriptor"]], event=doc
        ).replace(".", ",")
        # Note that formally there are more steps to the formatting, but we
        #  should have the folder by now
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        return super().event(doc)


class SaveTiff(SaveBaseClass):
    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for two_d_var in [
            k for k, v in self.dep_shapes.items() if len(v) == 2
        ]:
            imsave(
                clean_template(
                    pfmt.format(self.filename, ext=f"_{two_d_var}.tiff")
                ),
                doc["data"][two_d_var],
            )


class SaveIntensity(SaveBaseClass):
    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for one_d_ind_var in [
            k for k, v in self.in_dep_shapes.items() if len(v) == 1
        ]:
            for one_d_dep_var in [
                k for k, v in self.dep_shapes.items() if len(v) == 1
            ]:
                save_output(
                    doc["data"][one_d_ind_var],
                    doc["data"][one_d_dep_var],
                    clean_template(
                        pfmt.format(
                            self.filename,
                            ext=f"_{one_d_dep_var}_{one_d_ind_var}",
                        )
                    ),
                    {"tth": "2theta", "q": "Q"}.get(one_d_ind_var),
                )


class SaveMask(SaveBaseClass):
    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for two_d_var in [
            k for k, v in self.dep_shapes.items() if len(v) == 2
        ]:
            fit2d_save(
                np.flipud(doc["data"][two_d_var]),
                clean_template(pfmt.format(self.filename, ext="")),
            )
            np.save(
                clean_template(pfmt.format(self.filename, ext="_mask.npy")),
                doc["data"][two_d_var],
            )


class SavePDFgetx3(SaveBaseClass):
    def event(self, doc):
        # fill the document
        doc = super().event(doc)

        for one_d_ind_var in [
            k for k, v in self.in_dep_shapes.items() if len(v) == 1
        ]:
            for one_d_dep_var in [
                k for k, v in self.dep_shapes.items() if len(v) == 1
            ]:
                pdf_saver(
                    doc["data"][one_d_ind_var],
                    doc["data"][one_d_dep_var],
                    doc["data"]["config"],
                    clean_template(
                        pfmt.format(self.filename, ext=f".{one_d_dep_var}")
                    ),
                )


class SaveMeta(SaveBaseClass):
    def start(self, doc):
        doc = dict(doc)
        doc["analysis_stage"] = "meta"
        super().start(doc)

        dump_yml(clean_template(pfmt.format(self.filename, ext=".yaml")), doc)


class SaveCalib(SaveBaseClass):
    def event(self, doc):
        doc = super().event(doc)

        doc["calib"].save(
            clean_template(pfmt.format(self.filename, ext=".poni"))
        )


SAVER_MAP = {
    "dark_sub": SaveTiff,
    "mean_intensity": SaveIntensity,
    "mask": SaveMask,
    "pdf": SavePDFgetx3,
    "fq": SavePDFgetx3,
    "sq": SavePDFgetx3,
    # Tricky because we need to kart the object around, maybe?
    "calib": SaveCalib,
    "raw": SaveMeta,
}
        print('FINISH ANALYSIS ON {}'.format(doc.get('run_start', 'NA')))
        print('Analysis time {}'.format(time.time() - self.t0))
