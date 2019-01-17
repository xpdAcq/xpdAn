"""Event Model mirror of xpdtools.pipelines.raw_pipeline meant to accept nodes
from the raw pipeline and convert them to Event Model"""
from shed.simple import SimpleToEventStream, AlignEventStreams
from xpdtools.tools import overlay_mask


def to_event_stream_with_ind(raw_stripped, *nodes, publisher, **kwargs):
    for node in nodes:
        merge = AlignEventStreams(raw_stripped, node)
        merge.starsink(publisher)
    return locals()


def to_event_stream_no_ind(*nodes, publisher, **kwargs):
    for node in nodes:
        node.starsink(publisher)
    return locals()


def image_process(dark_corrected_foreground, bg_corrected_img, **kwargs):
    dark_corrected_tes = SimpleToEventStream(
        dark_corrected_foreground,
        ("dark_corrected_img",),
        analysis_stage="dark_sub",
    )
    bg_corrected_tes = SimpleToEventStream(
        bg_corrected_img, ("bg_corrected_img",), analysis_stage="bg_sub"
    )
    return locals()


def calibration(geometry, **kwargs):
    geometry_tes = SimpleToEventStream(
        geometry, ("calibration",), analysis_stage="calib"
    )
    return locals()


def gen_mask(mask, pol_corrected_img, **kwargs):
    mask_tes = SimpleToEventStream(mask, ("mask",), analysis_stage="mask")

    mask_overlay_tes = SimpleToEventStream(
        pol_corrected_img.combine_latest(mask).starmap(overlay_mask),
        ("mask_overlay",),
        analysis_stage="mask_overlay",
    )
    return locals()


def integration(mean, q, tth, std=None, **kwargs):
    if std:
        merge_names = ("mean", "std", "q", "tth")
        integration_merge = mean.combine_latest(*(std, q, tth), emit_on=1)

    else:
        integration_merge = mean.combine_latest(*(q, tth), emit_on=0)
        merge_names = ("mean", "q", "tth")

    # TODO: stuff q/tth hints into start doc
    integration_tes = SimpleToEventStream(
        integration_merge, merge_names, analysis_stage="integration"
    )
    return locals()


def pdf_gen(fq, pdf, **kwargs):
    # TODO: sq TES
    # TODO: stuff q/r hints into start doc
    fq_tes = SimpleToEventStream(
        fq, ("q", "fq", "config"), analysis_stage="fq"
    )

    pdf_tes = SimpleToEventStream(
        pdf, ("r", "gr", "config"), analysis_stage="pdf"
    )
    return locals()


pipeline_order = [image_process, calibration, gen_mask, integration, pdf_gen]
