"""Event Model mirror of xpdtools.pipelines.qoi meant to accept nodes
from the raw pipeline and convert them to Event Model"""
from shed.simple import SimpleToEventStream


def max_intensity_mean(mean_max, q_at_mean_max, **kwargs):
    max_tes = SimpleToEventStream(
        mean_max.combine_latest(q_at_mean_max, emit_on=0),
        ("iq_max", "q_iq_max"),
        analysis_stage="max",
    )
    return locals()


def max_gr_mean(gr_max, r_at_gr_max, **kwargs):
    max_pdf_tes = SimpleToEventStream(
        gr_max.combine_latest(r_at_gr_max, emit_on=0),
        ("pdf_max", "r_pdf_max"),
        analysis_stage="max_pdf",
    )
    return locals()


pipeline_order = [max_intensity_mean, max_gr_mean]
