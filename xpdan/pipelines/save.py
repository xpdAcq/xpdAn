import os
import numpy as np
from skbeam.io import save_output
from skbeam.io.fit2d import fit2d_save
from tifffile import imsave
from xpdan.formatters import render, clean_template

# '''
# SAVING
# TODO: look at implementing hint/document based logic for saving
from xpdan.io import dump_yml, pdf_saver
from xpdan.pipelines.pipeline_utils import base_template

from xpdconf.conf import glbl_dict


def save_pipeline(
    start_docs,
    all_docs,
    dark_corrected_foreground,
    mean,
    q,
    tth,
    mask,
    pdf,
    fq,
    sq,
    gen_geo,
        **kwargs
):
    start_yaml_string = start_docs.map(
        lambda s: {"raw_start": s, "ext": ".yaml", "analysis_stage": "meta"}
    ).map(
        lambda kwargs, string, **kwargs2: render(string, **kwargs, **kwargs2),
        string=base_template,
        base_folder=glbl_dict["tiff_base"],
    )
    start_yaml_string.map(clean_template).zip(start_docs, first=True).starsink(
        dump_yml
    )

    # create filename string
    filename_node = all_docs.map(
        lambda kwargs, string, **kwargs2: render(string, **kwargs, **kwargs2),
        string=base_template,
        stream_name="base path",
        base_folder=glbl_dict["tiff_base"],
    )

    # SAVING NAMES
    filename_name_nodes = {}
    for name, analysis_stage, ext in zip(
        [
            "dark_corrected_image_name",
            "iq_name",
            "tth_name",
            "mask_fit2d_name",
            "mask_np_name",
            "pdf_name",
            "fq_name",
            "sq_name",
            "calib_name",
        ],
        ["dark_sub", "iq", "itth", "mask", "mask", "pdf", "fq", "sq", "calib"],
        [".tiff", "", "_tth", "", "_mask.npy", ".gr", ".fq", ".sq", ".poni"],
    ):
        if ext:
            temp_name_node = filename_node.map(
                render, analysis_stage=analysis_stage, ext=ext
            )
        else:
            temp_name_node = filename_node.map(
                render, analysis_stage=analysis_stage
            )

        filename_name_nodes[name] = temp_name_node.map(
            clean_template, stream_name=analysis_stage
        )
        filename_name_nodes[name].map(os.path.dirname).sink(
            os.makedirs, exist_ok=True
        )

    # dark corrected img
    (
        filename_name_nodes["dark_corrected_image_name"]
        .combine_latest(
            dark_corrected_foreground,
            emit_on=dark_corrected_foreground,
            first=dark_corrected_foreground,
        )
        .starsink(imsave, stream_name="dark corrected foreground")
    )

    # integrated intensities
    (
        q.combine_latest(mean, emit_on=1, first=True)
        .combine_latest(filename_name_nodes["iq_name"], emit_on=0)
        .map(lambda l: (*l[0], l[1]))
        .starsink(
            save_output, "Q", stream_name="save integration {}".format("Q")
        )
    )

    (
        tth.combine_latest(mean, emit_on=1, first=True)
        .combine_latest(filename_name_nodes["tth_name"], emit_on=0)
        .map(lambda l: (*l[0], l[1]))
        .starsink(
            save_output,
            "2theta",
            stream_name="save integration {}".format("tth"),
        )
    )
    # Mask
    d = mask.combine_latest(
        filename_name_nodes["mask_fit2d_name"], first=mask, emit_on=0
    )
    (d.sink(lambda x: fit2d_save(np.flipud(x[0]), x[1])))
    (d.sink(lambda x: np.save(x[1], x[0])))

    # PDF
    for k, name, upstream in zip(
        ["pdf_name", "fq_name", "sq_name"],
        ["pdf saver", "fq saver", "sq saver"],
        [pdf, fq, sq],
    ):
        (
            upstream.combine_latest(
                filename_name_nodes[k], first=upstream, emit_on=0
            )
            .map(lambda l: (*l[0], l[1]))
            .starsink(pdf_saver, stream_name="name")
        )
    # calibration
    (
        gen_geo.combine_latest(
            filename_name_nodes["calib_name"], first=gen_geo, emit_on=0
        ).starsink(lambda x, n: x.save(n), stream_name="cal saver")
    )
    # '''

    save_kwargs = start_yaml_string.kwargs
    filename_node.kwargs = save_kwargs
    return locals()
