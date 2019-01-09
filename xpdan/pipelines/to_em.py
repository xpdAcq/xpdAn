from shed.simple import SimpleToEventStream, AlignEventStreams
from xpdan.vend.callbacks.core import StripDepVar


# TODO: maybe merge these?
def to_event_stream_with_ind(raw, *nodes, publisher, **kwargs):
    # strip the dependant vars form the raw data
    raw_stripped = raw.starmap(StripDepVar())

    for node in nodes:
        merge = AlignEventStreams(raw_stripped, node)
        merge.starsink(publisher)
    return locals()


def to_event_stream_no_ind(*nodes, publisher, **kwargs):
    for node in nodes:
        node.starsink(publisher)
    return locals()


def to_em(
    dark_corrected_foreground, geometry, mask, mean, q, tth, pdf,
        mean_max, q_at_mean_max, gr_max, r_at_gr_max, std,
        **kwargs
):
    dark_corrected_tes = SimpleToEventStream(
        dark_corrected_foreground, ("img",), analysis_stage="dark_sub"
    )
    geometry_tes = SimpleToEventStream(
        geometry, ("calibration",), analysis_stage="calib"
    )

    mask_tes = SimpleToEventStream(mask, ("mask",), analysis_stage="mask")

    # TODO: masked image

    integration_merge = mean.combine_latest(std, q, tth, emit_on=0)
    # TODO: stuff q/tth hints into start doc
    integration_tes = SimpleToEventStream(
        integration_merge, ("mean", 'std', "q", "tth"),
        analysis_stage="integration"
    )
    integration_tes.sink(print)
    # TODO: sq and fq TES
    # TODO: stuff q/r hints into start doc
    pdf_tes = SimpleToEventStream(pdf, ("r", "gr", "config"), analysis_stage="pdf")

    max_tes = SimpleToEventStream(mean_max.combine_latest(q_at_mean_max,
                                                          emit_on=0),
                                  ("iq_max", "q_iq_max"), analysis_stage="max")
    max_pdf_tes = SimpleToEventStream(gr_max.combine_latest(r_at_gr_max,
                                                          emit_on=0),
                                  ("pdf_max", "r_pdf_max"),
                                  analysis_stage="max_pdf")
    return locals()
