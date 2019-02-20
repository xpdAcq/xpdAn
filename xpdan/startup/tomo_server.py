import copy
import itertools
from pprint import pprint
from warnings import warn

import fire

from bluesky.utils import install_qt_kicker
from rapidz import Stream, move_to_first
from rapidz.link import link
from shed import SimpleToEventStream
from xpdan.pipelines.extra import z_score_tem
from xpdan.pipelines.main import pipeline_order
from xpdan.pipelines.qoi import pipeline_order as qoi_pipeline_order
from xpdan.pipelines.save import pipeline_order as save_pipeline_order
from xpdan.pipelines.to_event_model import (
    pipeline_order as tem_pipeline_order,
    to_event_stream_no_ind,
)
from xpdan.pipelines.to_event_model import to_event_stream_with_ind
from xpdan.pipelines.vis import vis_pipeline
from xpdan.vend.callbacks import CallbackBase
from xpdan.vend.callbacks.core import StripDepVar, RunRouter
from xpdan.vend.callbacks.zmq import Publisher, RemoteDispatcher
from xpdconf.conf import glbl_dict
from xpdtools.pipelines.extra import std_gen, median_gen, z_score_gen
from xpdtools.pipelines.qoi import max_intensity_mean, max_gr_mean
from xpdan.pipelines.tomo import pencil_tomo, tomo_event_stream, \
    full_field_tomo
from xpdtools.pipelines.tomo import tomo_prep, tomo_pipeline_piecewise, \
    tomo_pipeline_theta

pencil_order = [
    pencil_tomo,
    tomo_prep,
    tomo_pipeline_piecewise,
    tomo_event_stream,
]

full_field_order = [full_field_tomo, tomo_pipeline_theta, tomo_event_stream]


class PencilTomoCallback(CallbackBase):
    """This class caches and passes documents into the pencil tomography
    pipeline.

    The translation and rotation motors are inspected from the data as are the
    scalar quantities of interest.

    This class acts as a descriptor router for documents"""
    def __init__(self, pipeline_factory, publisher):
        self.pipeline_factory = pipeline_factory
        self.publisher = publisher

        self.start_doc = {}
        self.dim_names = []
        self.translation = None
        self.rotation = None
        self.sources = []

    def start(self, doc):
        self.start_doc = doc
        self.dim_names = [
            d[0][0]
            for d in doc.get("hints", {}).get("dimensions")
            if d[0][0] != "time"
        ]
        self.translation = doc["tomo"]["translation"]
        self.rotation = doc["tomo"]["rotation"]

    def descriptor(self, doc):
        # TODO: only listen to primary stream
        dep_shapes = {
            n: doc["data_keys"][n]["shape"]
            for n in doc["data_keys"] if n not in list(
            itertools.chain.from_iterable(
                [doc['object_keys'][n] for n in self.dim_names]))
        }

        # Only compute QOIs on scalars, currently
        qois = [k for k, v in dep_shapes.items() if len(v) == 0]
        rotation_pos = self.start_doc['motors'].index(self.rotation)
        translation_pos = self.start_doc['motors'].index(self.translation)

        self.sources = [Stream(stream_name=str(qoi)) for qoi in qois]
        pipelines = [
            self.pipeline_factory(
                source=s,
                qoi_name=qoi,
                translation=self.translation,
                rotation=self.rotation,
                x_dimension=self.start_doc['shape'][translation_pos],
                th_dimension=self.start_doc['shape'][rotation_pos]
            )
            for s, qoi in zip(self.sources, qois)
        ]
        for p in pipelines:
            to_event_stream_no_ind(p['rec_tes'], publisher=self.publisher)

        for s in self.sources:
            s.emit(('start', self.start_doc))
            s.emit(('descriptor', doc))

    def event(self, doc):
        for s in self.sources:
            s.emit(('event', doc))

    def stop(self, doc):
        for s in self.sources:
            s.emit(('stop', doc))
        # Need to destroy pipeline


class FullFieldTomoCallback(CallbackBase):
    """This class caches and passes documents into the pencil tomography
        pipeline.

        The translation and rotation motors are inspected from the data as are the
        scalar quantities of interest.

        This class acts as a descriptor router for documents"""

    def __init__(self, pipeline_factory, publisher):
        self.pipeline_factory = pipeline_factory
        self.publisher = publisher

        self.start_doc = {}
        self.dim_names = []
        self.rotation = None
        self.sources = []

    def start(self, doc):
        self.start_doc = doc
        self.dim_names = [
            d[0][0]
            for d in doc.get("hints", {}).get("dimensions")
            if d[0][0] != "time"
        ]
        self.rotation = doc["tomo"]["rotation"]

    def descriptor(self, doc):
        # TODO: only listen to primary stream
        dep_shapes = {
            n: doc["data_keys"][n]["shape"]
            for n in doc["data_keys"] if n not in list(
            itertools.chain.from_iterable(
                [doc['object_keys'][n] for n in self.dim_names]))
        }

        # Only compute QOIs on scalars, currently
        qois = [k for k, v in dep_shapes.items() if len(v) == 2]

        self.sources = [Stream(stream_name=str(qoi)) for qoi in qois]
        pipelines = [
            self.pipeline_factory(
                source=s,
                qoi_name=qoi,
                rotation=self.rotation,
            )
            for s, qoi in zip(self.sources, qois)
        ]
        for p in pipelines:
            to_event_stream_no_ind(p['rec_tes'], publisher=self.publisher)

        for s in self.sources:
            s.emit(('start', self.start_doc))
            s.emit(('descriptor', doc))

    def event(self, doc):
        for s in self.sources:
            s.emit(('event', doc))

    def stop(self, doc):
        for s in self.sources:
            s.emit(('stop', doc))
        # Need to destroy pipeline


def tomo_callback_factory(doc, publisher):
    # TODO: Eventually extract from plan hints?
    if doc.get("tomo", {}).get("type", None) == "pencil":
        return PencilTomoCallback(lambda **kwargs: link(*pencil_order,
                                                        **kwargs),
                                  publisher)
    elif doc.get("tomo", {}).get("type", None) == "full_field":
        return FullFieldTomoCallback(lambda **kwargs: link(*full_field_order,
                                                        **kwargs),
                                  publisher)


def run_server(
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    inbound_proxy_address=glbl_dict["inbound_proxy_address"],
    outbound_prefix=(b'raw', b"an", b"qoi"),
    inbound_prefix=b'tomo',
    **kwargs
):
    print(kwargs)
    publisher = Publisher(inbound_proxy_address, prefix=inbound_prefix)

    rr = RunRouter([lambda x: tomo_callback_factory(x, publisher=publisher)])

    d = RemoteDispatcher(outbound_proxy_address, prefix=outbound_prefix)
    install_qt_kicker(loop=d.loop)

    d.subscribe(rr)
    print("Starting Tomography Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
