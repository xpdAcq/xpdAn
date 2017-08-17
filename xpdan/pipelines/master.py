"""Decider between pipelines"""
from xpdan.pipelines.light_workflow import source as light_source
from xpdan.pipelines.dark_workflow import source as dark_source
from xpdan.pipelines.calibration_workflow import source
from streamz import Stream
import shed.event_streams as es


def is_dark_stream():
    pass


def is_light_stream():
    pass


def is_calibration_stream():
    pass


master_source = Stream()

dark_filter = es.filter(is_dark_stream, master_source, document_name='start')
dark_filter.connect(dark_source)

light_filter = es.filter(is_light_stream, master_source, document_name='start')
light_filter.connect(light_source)

calibration_filter = es.filter(is_calibration_stream, master_source,
                               document_name='start')

calibration_filter.connect(source)

master_source.visualize()