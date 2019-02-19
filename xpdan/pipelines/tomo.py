from rapidz import Stream
from shed import SimpleToEventStream, SimpleFromEventStream
from toolz import pluck


def pencil_tomo(source: Stream, qoi_name, translation, rotation, **kwargs):
    """Extract data from a raw stream for pencil beam tomography

    Parameters
    ----------
    source : Stream
        The stream of raw event model data
    qoi_name : str
        The name of the QOI for this reconstruction
    kwargs

    Returns
    -------
    dict :
        The namespace
    """
    x = SimpleFromEventStream('event', ('data', translation), upstream=source)
    th = SimpleFromEventStream('event', ('data', rotation), upstream=source)

    # Extract the index for the translation and rotation so we can
    # extract the dimensions and extents
    # TODO: turn into proper function
    translation_position = SimpleFromEventStream('start', ('motors', )).map(
        lambda x: x.index(translation))
    rotation_position = SimpleFromEventStream('start', ('motors',)).map(
        lambda x: x.index(rotation))

    dims = SimpleFromEventStream('start', ('shapes',),
                                 upstream=source)
    th_dim = rotation_position.zip(dims).starmap(pluck)
    x_dim = translation_position.zip(dims).starmap(pluck)

    extents = SimpleFromEventStream('start', ('extents', ),
                                    upstream=source)
    th_extents = rotation_position.zip(extents).starmap(pluck)
    x_extents = translation_position.zip(extents).starmap(pluck)

    qoi = SimpleFromEventStream('event', ('data', qoi_name),
                                upstream=source)
    center = SimpleFromEventStream('start', ('center',),
                                   upstream=source)
    return locals()


def tomo_event_stream(rec, *, qoi_name, **kwargs):
    rec_tes = SimpleToEventStream(
        rec,
        (qoi_name,),
        analysis_stage='{}_tomo'.format(qoi_name)
    )
    return locals()
