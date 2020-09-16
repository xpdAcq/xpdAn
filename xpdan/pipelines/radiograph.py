import numpy as np
from rapidz import union
from shed.simple import (
    SimpleFromEventStream as FromEventStream,
    SimpleToEventStream,
)
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import (
    query_dark,
    query_flat_field,
)
from xpdan.pipelines.pipeline_utils import _timestampstr
from bluesky.callbacks.core import Retrieve
from xpdconf.conf import glbl_dict


def fes_radiograph(
    raw_source,
    radiograph_names=glbl_dict["radiograph_names"],
    db=glbl_dict["exp_db"],
    resets=None,
    **kwargs
):
    """Translate from event stream to data for radiograph processing

    Parameters
    ----------
    raw_source : Stream
        The raw data source
    radiograph_names : Stream
        The names of the data to perform radiograph transformations on
    db : Broker
        The databroker with the raw data
    resets : list of str
        Data keys which when updated with new data cause the averaging to reset
    """
    not_dark_scan = FromEventStream(
        "start", (), upstream=raw_source, stream_name="not dark scan"
    ).map(lambda x: not x.get("dark_frame", False))
    # Fill the raw event stream
    source = (
        # Emit on works here because we emit on the not_dark_scan first due
        # to the ordering of the nodes!
        raw_source.combine_latest(not_dark_scan, emit_on=0)
        .filter(lambda x: x[1])
        .pluck(0)
        .starmap(
            Retrieve(handler_reg=db.reg.handler_reg, root_map=db.reg.root_map)
        )
        .filter(lambda x: x[0] not in ["resource", "datum"])
    )

    # source.sink(lambda x: print('Source says ', x))
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

    start_timestamp = FromEventStream("start", ("time",), source)

    if resets:
        reset = union(*[FromEventStream("event", ('data', r), upstream=source).unique(history=1) for r in resets])

    flat_field_query = start_docs.map(query_flat_field, db=db)

    flat_field = union(
        *[
            FromEventStream(
                "event",
                ("data", image_name),
                flat_field_query.filter(lambda x: x != [])
                    .map(lambda x: x if not isinstance(x, list) else x[0])
                    .map(lambda x: x.documents(fill=True))
                    .flatten(),
                event_stream_name="primary",
            )
            for image_name in radiograph_names
        ]
    ).map(np.float32)

    # Get foreground dark
    fg_dark_query = start_docs.map(query_dark, db=db)
    fg_dark_query.filter(lambda x: x == []).sink(
        lambda x: print("No dark found!")
    )

    dark = union(
        *[
            FromEventStream(
                "event",
                ("data", image_name),
                fg_dark_query.filter(lambda x: x != [])
                .map(lambda x: x if not isinstance(x, list) else x[0])
                .map(lambda x: x.documents(fill=True))
                .flatten(),
                event_stream_name="primary",
            )
            for image_name in radiograph_names
        ]
    ).map(np.float32)

    # Pull darks from their stream if it exists
    for image_name in radiograph_names:
        (
            FromEventStream(
                "event", ("data", image_name), source, event_stream_name="dark"
            )
            .map(np.float32)
            .connect(dark)
        )

    img = union(
        *[
            FromEventStream(
                "event",
                ("data", image_name),
                source,
                principle=True,
                event_stream_name="primary",
                stream_name="raw_foreground",
            )
            for image_name in radiograph_names
        ]
    ).map(np.float32)
    raw_source.starsink(StartStopCallback())
    return locals()


def tes_radiograph(norm_img, ave_img, **kwargs):
    norm_img_tes = SimpleToEventStream(
        norm_img, ("normalized_img",), analysis_stage="norm_img"
    )
    ave_img_tes = SimpleToEventStream(
        ave_img, ("averaged_img",), analysis_stage="ave_img"
    )
    return locals()
