from collections import defaultdict

import copy
import os
import shutil
import subprocess
from collections import defaultdict

from bluesky.callbacks.core import CallbackBase
from databroker._core import _sanitize
from databroker.assets.path_only_handlers import \
    AreaDetectorTiffPathOnlyHandler
from databroker.utils import ensure_path_exists


class StartStopCallback(CallbackBase):

    def __init__(self):
        self.t0 = 0

    def start(self, doc):
        self.t0 = doc['time']
        print('START ANALYSIS ON {}'.format(doc['uid']))

    def stop(self, doc):
        print('FINISH ANALYSIS ON {}'.format(doc.get('run_start', 'NA')))
        print('Analysis time {}'.format(doc['time'] - self.t0))


class RunRouter(CallbackBase):
    """
    Routes documents, by run, to callbacks it creates from factory functions.

    A RunRouter is callable, and it is has the signature ``router(name, doc)``,
    suitable for subscribing to the RunEngine.

    The RunRouter maintains a list of factory functions with the signature
    ``callback_factory(start_doc)``. When the router receives a RunStart
    document, it passes it to each ``callback_factory`` function. Each factory
    should return ``None`` ("I am not interested in this run,") or a callback
    function with the signature ``cb(name, doc)``. All future documents related
    to that run will be forwarded to ``cb``. When the run is complete, the
    RunRouter will drop all its references to ``cb``. It is up to
    ``callback_factory`` whether to return a new callable each time (having a
    lifecycle of one run, garbage collected thereafter), or to return the same
    object every time, or some other pattern.

    To summarize, the RunRouter's promise is that it will call each
    ``callback_factory`` with each new RunStart document and that it will
    forward all other documents from that run to whatever ``callback_factory``
    returns (if not None).

    Parameters
    ----------
    callback_factories : list
        A list of callables with the signature:

            callback_factory(start_doc)

        which should return ``None`` or another callable with this signature:

            callback(name, doc)
    """
    def __init__(self, callback_factories):
        self.callback_factories = callback_factories
        self.callbacks = defaultdict(list)  # start uid -> callbacks
        self.descriptors = {}  # descriptor uid -> start uid
        self.resources = {}  # resource uid -> start uid

    def _event_or_bulk_event(self, doc):
        descriptor_uid = doc['descriptor']
        try:
            start_uid = self.descriptors[descriptor_uid]
        except KeyError:
            # The belongs to a run that we are not interested in.
            return []
        return self.callbacks[start_uid]

    def event(self, doc):
        for cb in self._event_or_bulk_event(doc):
            cb('event', doc)

    def bulk_event(self, doc):
        for cb in self._event_or_bulk_event(doc):
            cb('bulk_event', doc)

    def _datum_or_bulk_datum(self, doc):
        resource_uid = doc['resource']
        try:
            start_uid = self.resources[resource_uid]
        except KeyError:
            # The belongs to a run that we are not interested in.
            return []
        return self.callbacks[start_uid]

    def datum(self, doc):
        for cb in self._datum_or_bulk_datum(doc):
            cb('datum', doc)

    def bulk_datum(self, doc):
        for cb in self._datum_or_bulk_datum(doc):
            cb('bulk_datum', doc)

    def descriptor(self, doc):
        start_uid = doc['run_start']
        cbs = self.callbacks[start_uid]
        if not cbs:
            # This belongs to a run we are not interested in.
            return
        self.descriptors[doc['uid']] = start_uid
        for cb in cbs:
            cb('descriptor', doc)

    def resource(self, doc):
        start_uid = doc['run_start']
        cbs = self.callbacks[start_uid]
        if not cbs:
            # This belongs to a run we are not interested in.
            return
        self.resources[doc['uid']] = start_uid
        for cb in cbs:
            cb('resource', doc)

    def start(self, doc):
        for callback_factory in self.callback_factories:
            cb = callback_factory(doc)
            if cb is None:
                # The callback_factory is not interested in this run.
                continue
            self.callbacks[doc['uid']].append(cb)
        for cb in self.callbacks[doc['uid']]:
            cb('start', doc)

    def stop(self, doc):
        start_uid = doc['run_start']
        # Clean up references.
        cbs = self.callbacks.pop(start_uid)
        if not cbs:
            return
        to_remove = []
        for k, v in list(self.descriptors.items()):
            if v == start_uid:
                del self.descriptors[k]
        for k, v in list(self.resources.items()):
            if v == start_uid:
                del self.resources[k]
        for cb in cbs:
            cb('stop', doc)


class ReturnCallback(CallbackBase):
    def __call__(self, name, doc):
        ret = getattr(self, name)(doc)
        if ret:
            return name, ret
        return name, doc


class Retrieve(ReturnCallback):
    def __init__(self, handler_reg, root_map=None, executor=None):
        self.executor = executor
        if root_map is None:
            root_map = {}
        if handler_reg is None:
            handler_reg = {}
        self.resources = None
        self.handlers = None
        self.datums = None
        self.root_map = root_map
        self.handler_reg = handler_reg

    def start(self, doc):
        self.resources = {}
        self.handlers = {}
        self.datums = {}

    def resource(self, resource):
        self.resources[resource['uid']] = resource
        handler = self.handler_reg[resource['spec']]

        key = (str(resource['uid']), handler.__name__)

        kwargs = resource['resource_kwargs']
        rpath = resource['resource_path']
        root = resource.get('root', '')
        root = self.root_map.get(root, root)
        if root:
            rpath = os.path.join(root, rpath)
        ret = handler(rpath, **kwargs)
        self.handlers[key] = ret

    def datum(self, doc):
        self.datums[doc['datum_id']] = doc

    def retrieve_datum(self, datum_id):
        doc = self.datums[datum_id]
        resource = self.resources[doc['resource']]
        handler_class = self.handler_reg[resource['spec']]
        key = (str(resource['uid']), handler_class.__name__)
        # If we hand in an executor use it, allowing us to load in parallel
        if self.executor:
            return self.executor.submit(self.handlers[key],
                                        **doc['datum_kwargs'])
        return self.handlers[key](**doc['datum_kwargs'])

    def fill_event(self, event, fields=True, inplace=True):
        if fields is True:
            fields = set(event['data'])
        elif fields is False:
            # if no fields, we got nothing to do!
            # just return back as-is
            return event
        elif fields:
            fields = set(fields)

        if not inplace:
            ev = _sanitize(event)
            ev = copy.deepcopy(ev)
        else:
            ev = event
        data = ev['data']
        filled = ev['filled']
        for k in set(data) & fields:
            # Try to fill the data
            try:
                v = self.retrieve_datum(data[k])
                data[k] = v
                filled[k] = True
            # If retrieve fails keep going
            except (ValueError, KeyError) as e:
                raise e
        return event

    def event(self, doc):
        ev = self.fill_event(doc, inplace=False)
        return ev


class ExportCallback(Retrieve):
    """Callback to copy data to a new root"""

    def __init__(self, new_root, handler_reg, root_map=None):
        # TODO: fix this, since it is a dirty hack which only works for ADTIFF
        super().__init__(handler_reg, root_map)
        self.new_root = new_root
        self.old_root = None

    def resource(self, doc):
        doc = copy.deepcopy(doc)
        super().resource(doc)
        self.old_root = doc['root']
        doc.update(root=self.new_root)
        return doc

    def datum(self, doc):
        super().datum(doc)
        # retrieve the datum using path only handler?
        resource = self.resources[doc['resource']]
        handler_class = self.handler_reg[resource['spec']]
        key = (str(resource['uid']), handler_class.__name__)
        fin = self.handlers[key].get_file_list([doc['datum_kwargs']])[0]

        # replace the root with the new root
        fout = os.path.join(self.new_root, os.path.relpath(fin, self.old_root))

        ensure_path_exists(os.path.dirname(fout))
        # cp fin, new_path
        shutil.copy2(fin, fout)

    def event(self, doc):
        # don't fill
        return doc
