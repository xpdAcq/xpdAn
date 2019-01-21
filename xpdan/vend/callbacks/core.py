"""
Useful callbacks for the Run Engine
"""
from itertools import count
import warnings
from collections import deque, namedtuple, OrderedDict
import time as ttime

from datetime import datetime
import logging
from bluesky.utils import ensure_uid
from collections import defaultdict

import copy
import os
import shutil
import subprocess
from collections import defaultdict

from bluesky.callbacks.core import CallbackBase
from databroker._core import _sanitize
from databroker.assets.path_only_handlers import (
    AreaDetectorTiffPathOnlyHandler
)
from databroker.utils import ensure_path_exists


# deprecate callbacks moved to mpl_plotting ----------------------------------


def _deprecate_import_name(name):
    wmsg = (
        "In a future version of bluesky, {} will not be importable from "
        "bluesky.callbacks.core or bluesky.callbacks. Instead, import it from "
        "bluesky.callbacks.mpl_plotting. This change allows other callbacks, "
        "unrelated to matplotlib, to be imported and used without importing "
        "matplotlib.pyplot or configuring a DISPLAY."
    ).format(name)

    def f(*args, **kwargs):
        # per bluesky convention use UserWarning instead of DeprecationWarning
        warnings.warn(wmsg, UserWarning)
        from . import mpl_plotting

        cls = getattr(mpl_plotting, name)
        return cls(*args, **kwargs)

    f.__name__ = name
    return f


LiveScatter = _deprecate_import_name("LiveScatter")
LivePlot = _deprecate_import_name("LivePlot")
LiveGrid = _deprecate_import_name("LiveGrid")
LiveFitPlot = _deprecate_import_name("LiveFitPlot")
LiveRaster = _deprecate_import_name("LiveRaster")
LiveMesh = _deprecate_import_name("LiveMesh")

# ----------------------------------------------------------------------------


class CallbackBase:
    def __call__(self, name, doc):
        ret = getattr(self, name)(doc)
        if ret:
            return name, ret
        return name, doc

    def event(self, doc):
        pass

    def bulk_events(self, doc):
        pass

    def resource(self, doc):
        pass

    def datum(self, doc):
        pass

    def bulk_datum(self, doc):
        pass

    def descriptor(self, doc):
        pass

    def start(self, doc):
        pass

    def stop(self, doc):
        pass


class CallbackCounter:
    "As simple as it sounds: count how many times a callback is called."
    # Wrap itertools.count in something we can use as a callback.
    def __init__(self):
        self.counter = count()
        self(None, {})  # Pass a fake doc to prime the counter (start at 1).

    def __call__(self, name, doc):
        self.value = next(self.counter)


def print_metadata(name, doc):
    "Print all fields except uid and time."
    for field, value in sorted(doc.items()):
        # uid is returned by the RunEngine, and time is self-evident
        if field not in ["time", "uid"]:
            print("{0}: {1}".format(field, value))


def collector(field, output):
    """
    Build a function that appends data to a list.

    This is useful for testing but not advised for general use. (There is
    probably a better way to do whatever you want to do!)

    Parameters
    ----------
    field : str
        the name of a data field in an Event
    output : mutable iterable
        such as a list

    Returns
    -------
    func : function
        expects one argument, an Event dictionary
    """

    def f(name, event):
        output.append(event["data"][field])

    return f


def format_num(x, max_len=11, pre=5, post=5):
    if (abs(x) > 10 ** pre or abs(x) < 10 ** -post) and x != 0:
        x = "%.{}e".format(post) % x
    else:
        x = "%{}.{}f".format(pre, post) % x

    return x


def get_obj_fields(fields):
    """
    If fields includes any objects, get their field names using obj.describe()

    ['det1', det_obj] -> ['det1, 'det_obj_field1, 'det_obj_field2']"
    """
    string_fields = []
    for field in fields:
        if isinstance(field, str):
            string_fields.append(field)
        else:
            try:
                field_list = sorted(field.describe().keys())
            except AttributeError:
                raise ValueError(
                    "Fields must be strings or objects with a "
                    "'describe' method that return a dict."
                )
            string_fields.extend(field_list)
    return string_fields


class CollectThenCompute(CallbackBase):
    def __init__(self):
        self._start_doc = None
        self._stop_doc = None
        self._events = deque()
        self._descriptors = deque()

    def start(self, doc):
        self._start_doc = doc
        super().start(doc)

    def descriptor(self, doc):
        self._descriptors.append(doc)
        super().descriptor(doc)

    def event(self, doc):
        self._events.append(doc)
        super().event(doc)

    def stop(self, doc):
        self._stop_doc = doc
        self.compute()
        super().stop(doc)

    def reset(self):
        self._start_doc = None
        self._stop_doc = None
        self._events.clear()
        self._descriptors.clear()

    def compute(self):
        raise NotImplementedError("This method must be defined by a subclass.")


class LiveTable(CallbackBase):
    """Live updating table

    Parameters
    ----------
    fields : list
         List of fields to add to the table.

    stream_name : str, optional
         The event stream to watch for

    print_header_interval : int, optional
         Reprint the header every this many lines, defaults to 50

    min_width : int, optional
         The minimum width is spaces of the data columns.  Defaults to 12

    default_prec : int, optional
         Precision to use if it can not be found in descriptor, defaults to 3

    extra_pad : int, optional
         Number of extra spaces to put around the printed data, defaults to 1

    logbook : callable, optional
        Must take a sting as the first positional argument

           def logbook(input_str):
                pass

    out : callable, optional
        Function to call to 'print' a line.  Defaults to `print`
    """

    _FMTLOOKUP = {
        "s": "{pad}{{{k}: >{width}.{prec}{dtype}}}{pad}",
        "f": "{pad}{{{k}: >{width}.{prec}{dtype}}}{pad}",
        "g": "{pad}{{{k}: >{width}.{prec}{dtype}}}{pad}",
        "d": "{pad}{{{k}: >{width}{dtype}}}{pad}",
    }
    _FMT_MAP = {"number": "f", "integer": "d", "string": "s"}
    _fm_sty = namedtuple("fm_sty", ["width", "prec", "dtype"])
    water_mark = (
        "{st[plan_type]} {st[plan_name]} ['{st[uid]:.8s}'] "
        "(scan num: {st[scan_id]})"
    )
    ev_time_key = "SUPERLONG_EV_TIMEKEY_THAT_I_REALLY_HOPE_NEVER_CLASHES"

    def __init__(
        self,
        fields,
        *,
        stream_name="primary",
        print_header_interval=50,
        min_width=12,
        default_prec=3,
        extra_pad=1,
        logbook=None,
        out=print
    ):
        super().__init__()
        self._header_interval = print_header_interval
        # expand objects
        self._fields = get_obj_fields(fields)
        self._stream = stream_name
        self._start = None
        self._stop = None
        self._descriptors = set()
        self._pad_len = extra_pad
        self._extra_pad = " " * extra_pad
        self._min_width = min_width
        self._default_prec = default_prec
        self._format_info = OrderedDict(
            [
                ("seq_num", self._fm_sty(10 + self._pad_len, "", "d")),
                (self.ev_time_key, self._fm_sty(10 + 2 * extra_pad, 10, "s")),
            ]
        )
        self._rows = []
        self.logbook = logbook
        self._sep_format = None
        self._out = out

    def descriptor(self, doc):
        def patch_up_precision(p):
            try:
                return int(p)
            except (TypeError, ValueError):
                return self._default_prec

        if doc["name"] != self._stream:
            return

        self._descriptors.add(doc["uid"])

        dk = doc["data_keys"]
        for k in self._fields:
            width = max(
                self._min_width,
                len(k) + 2,
                self._default_prec + 1 + 2 * self._pad_len,
            )
            try:
                dk_entry = dk[k]
            except KeyError:
                # this descriptor does not know about this key
                continue

            if dk_entry["dtype"] not in self._FMT_MAP:
                warnings.warn(
                    "The key {} will be skipped because LiveTable "
                    "does not know how to display the dtype {}"
                    "".format(k, dk_entry["dtype"])
                )
                continue

            prec = patch_up_precision(
                dk_entry.get("precision", self._default_prec)
            )
            fmt = self._fm_sty(
                width=width, prec=prec, dtype=self._FMT_MAP[dk_entry["dtype"]]
            )

            self._format_info[k] = fmt

        self._sep_format = (
            "+"
            + "+".join("-" * f.width for f in self._format_info.values())
            + "+"
        )
        self._main_fmnt = "|".join(
            "{{: >{w}}}{pad}".format(
                w=f.width - self._pad_len, pad=" " * self._pad_len
            )
            for f in self._format_info.values()
        )
        headings = [
            k if k != self.ev_time_key else "time" for k in self._format_info
        ]
        self._header = "|" + self._main_fmnt.format(*headings) + "|"
        self._data_formats = OrderedDict(
            (
                k,
                self._FMTLOOKUP[f.dtype].format(
                    k=k,
                    width=f.width - 2 * self._pad_len,
                    prec=f.prec,
                    dtype=f.dtype,
                    pad=self._extra_pad,
                ),
            )
            for k, f in self._format_info.items()
        )

        self._count = 0

        self._print(self._sep_format)
        self._print(self._header)
        self._print(self._sep_format)
        super().descriptor(doc)

    def event(self, doc):
        # shallow copy so we can mutate
        if ensure_uid(doc["descriptor"]) not in self._descriptors:
            return
        data = dict(doc["data"])
        self._count += 1
        if not self._count % self._header_interval:
            self._print(self._sep_format)
            self._print(self._header)
            self._print(self._sep_format)
        fmt_time = str(datetime.fromtimestamp(doc["time"]).time())
        data[self.ev_time_key] = fmt_time
        data["seq_num"] = doc["seq_num"]
        cols = [
            f.format(**{k: data[k]})
            # Show data[k] if k exists in this Event and is 'filled'.
            # (The latter is only applicable if the data is
            # externally-stored -- hence the fallback to `True`.)
            if ((k in data) and doc.get("filled", {}).get(k, True))
            # Otherwise use a placeholder of whitespace.
            else " " * self._format_info[k].width
            for k, f in self._data_formats.items()
        ]
        self._print("|" + "|".join(cols) + "|")
        super().event(doc)

    def stop(self, doc):
        if ensure_uid(doc["run_start"]) != self._start["uid"]:
            return

        # This sleep is just cosmetic. It improves the odds that the bottom
        # border is not printed until all the rows from events are printed,
        # avoiding this ugly scenario:
        #
        # |         4 | 22:08:56.7 |      0.000 |
        # +-----------+------------+------------+
        # generator scan ['6d3f71'] (scan num: 1)
        # Out[2]: |         5 | 22:08:56.8 |      0.000 |
        ttime.sleep(0.1)

        if self._sep_format is not None:
            self._print(self._sep_format)
        self._stop = doc

        wm = self.water_mark.format(st=self._start)
        self._out(wm)
        if self.logbook:
            self.logbook("\n".join([wm] + self._rows))
        super().stop(doc)

    def start(self, doc):
        self._rows = []
        self._start = doc
        self._stop = None
        self._sep_format = None
        super().start(doc)

    def _print(self, out_str):
        self._rows.append(out_str)
        self._out(out_str)


class StartStopCallback(CallbackBase):
    def __init__(self):
        self.t0 = 0

    def start(self, doc):
        self.t0 = doc["time"]
        print("START ANALYSIS ON {}".format(doc["uid"]))

    def stop(self, doc):
        print("FINISH ANALYSIS ON {}".format(doc.get("run_start", "NA")))
        print("Analysis time {}".format(doc["time"] - self.t0))


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

    def __init__(self, callback_factories, **kwargs):
        self.callback_factories = callback_factories
        self.callbacks = defaultdict(list)  # start uid -> callbacks
        self.descriptors = {}  # descriptor uid -> start uid
        self.resources = {}  # resource uid -> start uid
        self.kwargs = kwargs

    def _event_or_bulk_event(self, doc):
        descriptor_uid = doc["descriptor"]
        try:
            start_uid = self.descriptors[descriptor_uid]
        except KeyError:
            # The belongs to a run that we are not interested in.
            return []
        return self.callbacks[start_uid]

    def event(self, doc):
        for cb in self._event_or_bulk_event(doc):
            cb("event", doc)

    def bulk_event(self, doc):
        for cb in self._event_or_bulk_event(doc):
            cb("bulk_event", doc)

    def _datum_or_bulk_datum(self, doc):
        resource_uid = doc["resource"]
        try:
            start_uid = self.resources[resource_uid]
        except KeyError:
            # The belongs to a run that we are not interested in.
            return []
        return self.callbacks[start_uid]

    def datum(self, doc):
        for cb in self._datum_or_bulk_datum(doc):
            cb("datum", doc)

    def bulk_datum(self, doc):
        for cb in self._datum_or_bulk_datum(doc):
            cb("bulk_datum", doc)

    def descriptor(self, doc):
        start_uid = doc["run_start"]
        cbs = self.callbacks[start_uid]
        if not cbs:
            # This belongs to a run we are not interested in.
            return
        self.descriptors[doc["uid"]] = start_uid
        for cb in cbs:
            cb("descriptor", doc)

    def resource(self, doc):
        start_uid = doc["run_start"]
        cbs = self.callbacks[start_uid]
        if not cbs:
            # This belongs to a run we are not interested in.
            return
        self.resources[doc["uid"]] = start_uid
        for cb in cbs:
            cb("resource", doc)

    def start(self, doc):
        for callback_factory in self.callback_factories:
            cb = callback_factory(doc, **self.kwargs)
            if cb is None:
                # The callback_factory is not interested in this run.
                continue
            self.callbacks[doc["uid"]].append(cb)
        for cb in self.callbacks[doc["uid"]]:
            cb("start", doc)

    def stop(self, doc):
        start_uid = doc["run_start"]
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
            cb("stop", doc)


class Retrieve(CallbackBase):
    """Callback for retrieving data from resource and datum documents. This
    can also be used to access file bound data within a scan.

    Parameters
    ----------
    handler_reg: dict
        The handler registry used for looking up the data
    root_map : dict, optional
        Map to replace the root with a new root
    executor : Executor, optional
        If provided run the data loading via the executor, opening the files
        as a future (potentially elsewhere).
    """

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
        self.resources[resource["uid"]] = resource
        handler = self.handler_reg[resource["spec"]]

        key = (str(resource["uid"]), handler.__name__)

        kwargs = resource["resource_kwargs"]
        rpath = resource["resource_path"]
        root = resource.get("root", "")
        root = self.root_map.get(root, root)
        if root:
            rpath = os.path.join(root, rpath)
        ret = handler(rpath, **kwargs)
        self.handlers[key] = ret

    def datum(self, doc):
        self.datums[doc["datum_id"]] = doc

    def retrieve_datum(self, datum_id):
        doc = self.datums[datum_id]
        resource = self.resources[doc["resource"]]
        handler_class = self.handler_reg[resource["spec"]]
        key = (str(resource["uid"]), handler_class.__name__)
        # If we hand in an executor use it, allowing us to load in parallel
        if self.executor:
            return self.executor.submit(
                self.handlers[key], **doc["datum_kwargs"]
            )
        return self.handlers[key](**doc["datum_kwargs"])

    def fill_event(self, event, fields=True, inplace=True):
        if fields is True:
            fields = set(event["data"])
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
        data = ev["data"]
        filled = ev["filled"]
        for k in set(data) & fields & set(k for k in data if not filled.get(k, True)):
            # Try to fill the data
            try:
                v = self.retrieve_datum(data[k])
                data[k] = v
                filled[k] = True
            # If retrieve fails keep going
            except (ValueError, KeyError):
                pass
        return ev

    def event(self, doc):
        ev = self.fill_event(doc, inplace=False)
        return ev


class ExportCallback(Retrieve):
    """Callback to copy file bound data to a new directory, modifying the
    resource and datum documents to reflect the changes. This is used for the
    copying of data to a portable databroker

    Parameters
    ----------
    new_root: str
        The filepath to the top folder where the data is to be stored
    handler_reg: dict
        The handler registry used for looking up the data
    root_map : dict, optional
        Map to replace the root with a new root
    """

    def __init__(self, new_root, handler_reg, root_map=None):
        super().__init__(handler_reg, root_map)
        self.new_root = new_root
        self.old_root = None

    def resource(self, doc):
        doc = copy.deepcopy(doc)
        super().resource(doc)
        self.old_root = doc["root"]
        doc.update(root=self.new_root)
        return doc

    def datum(self, doc):
        super().datum(doc)

        resource = self.resources[doc["resource"]]
        handler_class = self.handler_reg[resource["spec"]]
        key = (str(resource["uid"]), handler_class.__name__)

        # this comes back as a single element list for one datum
        fin = self.handlers[key].get_file_list([doc["datum_kwargs"]])[0]

        # replace the root with the new root
        fout = os.path.join(self.new_root, os.path.relpath(fin, self.old_root))

        ensure_path_exists(os.path.dirname(fout))
        # cp fin, new_path
        print(fin, fout)
        shutil.copy2(fin, fout)

    def event(self, doc):
        # don't fill
        return doc


class StripDepVar(CallbackBase):
    """Strip the dependent variables from a data stream. This creates a
    stream with only the independent variables, allowing the stream to be
    merged with other dependent variables (including analyzed data)"""

    def __init__(self):
        self.independent_vars = set()

    def start(self, doc):
        self.independent_vars = set(
            [n[0] for n, s in doc.get("hints", {}).get("dimensions", [])]
        )

    def descriptor(self, doc):
        new_doc = dict(doc)
        # TODO: maybe use all the keys in the set? (hints, object_keys, etc.)
        data_keys = set(new_doc["object_keys"].keys())
        for k in ["data_keys", "hints", "configuration", "object_keys"]:
            new_doc[k] = dict(doc[k])
            # all the things not in independent_vars
            for key in self.independent_vars ^ data_keys:
                new_doc[k].pop(key, None)
        return new_doc

    def event(self, doc):
        # make copies
        new_doc = dict(doc)
        new_doc["data"] = dict(doc["data"])
        new_doc["timestamps"] = dict(doc["timestamps"])
        data_keys = set(new_doc["data"].keys())
        # all the things not in
        for key in self.independent_vars ^ data_keys:
            new_doc["data"].pop(key)
            new_doc["timestamps"].pop(key)
        return new_doc
