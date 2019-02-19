import copy
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
from xpdan.pipelines.tomo import pencil_tomo, tomo_event_stream
from xpdtools.pipelines.tomo import tomo_prep, tomo_pipeline_piecewise

pencil_order = [pencil_tomo, tomo_prep, tomo_pipeline_piecewise,
                tomo_event_stream]


class StartCache(CallbackBase):
    def __init__(self, source_factory):
        self.source_factory = source_factory

    def start(self, doc):
        self.start_cache = doc
        self.dim_names = [
            d[0][0]
            for d in doc.get("hints", {}).get("dimensions")
            if d[0][0] != "time"
        ]

    def descriptor(self, doc):
        self.in_dep_shapes = {
            n: doc["data_keys"][n]["shape"] for n in self.dim_names
        }
        self.dep_shapes = {
            n: doc["data_keys"][n]["shape"]
            for n in set(self.dim_names) ^ set(doc["data_keys"])
        }

        # Only compute QOIs on scalars currently
        qois = [k for k, v in self.dep_shapes.items() if len(v) == 0]

        # XXX: note that this only works on single descriptor streams
        self.sources = [Stream(stream_name=str(qoi)) for qoi in qois]
        self.pipelines = [self.source_factory(
            source=s, qoi_name=qoi) for s, qoi in zip(self.sources, qois)]

        for s in self.sources:
            s.emit(self.start_cache)
            s.emit(doc)

    def event(self, doc):
        for s in self.sources:
            s.emit(doc)

    def stop(self, doc):
        for s in self.sources:
            s.emit(doc)


def start_cache_factory(doc):
    # Eventually extract from plan hints?
    if doc.get('tomo', None) == 'pencil':
        return StartCache(lambda **kwargs: link(pencil_order, **kwargs))
    elif doc.get('tomo', None) == 'flat_field':
        return StartCache()


def run_server(outbound_proxy_address=glbl_dict["outbound_proxy_address"],
               prefix=b"an", **kwargs):
    print(kwargs)

    rr = RunRouter([lambda x: start_cache_factory(x)])

    d = RemoteDispatcher(
        outbound_proxy_address,
        prefix=prefix,
    )
    install_qt_kicker(loop=d.loop)

    d.subscribe(rr)
    print("Starting Tomography Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
