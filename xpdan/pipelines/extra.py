from shed.simple import SimpleToEventStream


def z_score_tem(z_score, **kwargs):
    z_score_tes = SimpleToEventStream(
        z_score, ("z_score",), analysis_stage="z_score"
    )
    return locals()
