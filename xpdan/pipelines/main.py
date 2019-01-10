import os

import numpy as np
from shed.translation import FromEventStream
from rapidz import move_to_first
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import query_background, query_dark, temporal_prox
from xpdan.pipelines.pipeline_utils import (
    _timestampstr,
    clear_combine_latest,
    Filler,
)
from xpdconf.conf import glbl_dict
from xpdtools.calib import _save_calib_param
from xpdtools.pipelines.raw_pipeline import (
    image_process,
    calibration,
    scattering_correction,
    gen_mask,
    integration,
    pdf_gen,
)


# TODO: use oracle to get rid of this
def clear_geo_gen(source, geometry_img_shape, **kwargs):
    # If new calibration uid invalidate our current calibration cache
    a = FromEventStream("start", ("detector_calibration_client_uid",), source)
    move_to_first(a)
    (
        a.unique(history=1).sink(
            lambda x: geometry_img_shape.lossless_buffer.clear()
        )
    )


# TODO: use oracle to get rid of this
def clear_comp(source, iq_comp, **kwargs):
    # Clear composition every start document
    # FIXME: Needs to go after the iq_comp is defined
    a = FromEventStream("start", (), source)
    move_to_first(a)
    (a.sink(lambda x: clear_combine_latest(iq_comp, 1)))


def save_cal(start_timestamp, gen_geo_cal, **kwargs):
    # Save out calibration data to special place
    h_timestamp = start_timestamp.map(_timestampstr)
    (
        gen_geo_cal.pluck(0)
        .zip_latest(h_timestamp)
        .starsink(
            lambda x, y: _save_calib_param(
                x,
                y,
                os.path.join(
                    glbl_dict["config_base"], glbl_dict["calib_config_name"]
                ),
            )
        )
    )
    return locals()


# TODO: chunk this up a bit more so we can separate XRD from PDF
def start_gen(
    raw_source,
    image_name=glbl_dict["image_field"],
    db=glbl_dict["exp_db"],
    calibration_md_folder={"folder": "xpdAcq_calib_info.yml"},
    **kwargs
):
    raw_source.sink(lambda x: print(x[0]))
    filler = Filler(db=db)
    # Build the general pipeline from the raw_pipeline

    # TODO: change this when new dark logic comes
    # Check that the data isn't a dark
    dk_uid = FromEventStream("start", (), upstream=raw_source).map(
        lambda x: "sc_dk_field_uid" in x
    )
    # Fill the raw event stream
    source = (
        raw_source.combine_latest(dk_uid)
        .filter(lambda x: x[1])
        .pluck(0)
        # Filler returns None for resource/datum data
        .starmap(filler)
        .filter(lambda x: x is not None)
    )
    # Get all the documents
    start_docs = FromEventStream("start", (), source)
    descriptor_docs = FromEventStream(
        "descriptor", (), source, event_stream_name="primary"
    )
    event_docs = FromEventStream(
        "event", (), source, event_stream_name="primary"
    )
    all_docs = event_docs.combine_latest(
        start_docs, descriptor_docs, emit_on=0, first=True
    ).starmap(
        lambda e, s, d: {
            "raw_event": e,
            "raw_start": s,
            "raw_descriptor": d,
            "human_timestamp": _timestampstr(s["time"]),
        }
    )

    # PDF specific
    composition = FromEventStream("start", ("composition_string",), source)

    # Calibration information
    wavelength = FromEventStream("start", ("bt_wavelength",), source).unique(
        history=1
    )
    calibrant = FromEventStream("start", ("dSpacing",), source, principle=True
                                ).unique(
        history=1
    )
    detector = FromEventStream("start", ("detector",), source).unique(
        history=1
    )

    is_calibration_img = FromEventStream("start", (), source).map(
        lambda x: "detector_calibration_server_uid" in x
    )
    # Only pass through new calibrations (prevents us from recalculating cals)
    geo_input = FromEventStream("start", ("calibration_md",), source,
                                principle=True).unique(
        history=1
    )

    start_timestamp = FromEventStream("start", ("time",), source)

    # Clean out the cached darks and backgrounds on start
    # so that this will run regardless of background/dark status
    # note that we get the proper data (if it exists downstream)
    # FIXME: this is kinda an anti-pattern and needs to go lower in the
    # pipeline
    start_docs.sink(lambda x: raw_background_dark.emit(0.0))
    start_docs.sink(lambda x: raw_background.emit(0.0))
    start_docs.sink(lambda x: raw_foreground_dark.emit(0.0))

    bg_query = start_docs.map(query_background, db=db)
    bg_docs = (
        bg_query.zip(start_docs)
        .starmap(temporal_prox)
        .filter(lambda x: x != [])
        .map(lambda x: x[0].documents(fill=True))
        .flatten()
    )

    # Get foreground dark
    fg_dark_query = start_docs.map(query_dark, db=db)
    fg_dark_query.filter(lambda x: x == []).sink(
        lambda x: print("No dark found!")
    )
    raw_foreground_dark = FromEventStream(
        "event",
        ("data", image_name),
        fg_dark_query.filter(lambda x: x != [])
        .map(lambda x: x if not isinstance(x, list) else x[0])
        .map(lambda x: x.documents(fill=True))
        .flatten(),
    ).map(np.float32)

    # Get bg dark
    bg_dark_query = FromEventStream("start", (), bg_docs).map(
        query_dark, db=db
    )
    raw_background_dark = FromEventStream(
        "event",
        ("data", image_name),
        bg_dark_query.filter(lambda x: x != [])
        .map(lambda x: x if not isinstance(x, list) else x[0])
        .map(lambda x: x.documents(fill=True))
        .flatten(),
    ).map(np.float32)
    (
        FromEventStream(
            "event", ("data", image_name), source, event_stream_name="dark"
        )
        .map(np.float32)
        .connect(raw_foreground_dark)
    )
    # Get background
    raw_background = FromEventStream(
        "event", ("data", image_name), bg_docs
    ).map(np.float32)

    # Get foreground
    img_counter = FromEventStream(
        "event", ("seq_num",), source, stream_name="seq_num"
    )
    raw_foreground = FromEventStream(
        "event",
        ("data", image_name),
        source,
        principle=True,
        event_stream_name="primary",
        stream_name="raw_foreground",
    ).map(np.float32)
    raw_source.starsink(StartStopCallback())
    return locals()


pipeline_order = [
    start_gen,
    image_process,
    calibration,
    clear_geo_gen,
    save_cal,
    scattering_correction,
    gen_mask,
    integration,
    pdf_gen,
    clear_comp,
]

# If main print visualize pipeline
if __name__ == "__main__":  # no qa
    from rapidz import Stream
    from rapidz.link import link

    raw_source = Stream(stream_name="raw_source")
    ns = link(*pipeline_order, raw_source=raw_source)
    ns["raw_source"].visualize(source_node=True)
