from collections import defaultdict

from bluesky.callbacks.core import CallbackBase


class StartStopCallback(CallbackBase):
    def start(self, doc):
        print('START ANALYSIS ON {}'.format(doc['uid']))

    def stop(self, doc):
        print('FINISH ANALYSIS ON {}'.format(doc.get('run_start', 'NA')))


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
        cbs = self.callbacks[doc['uid']]
        for cb in cbs:
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
