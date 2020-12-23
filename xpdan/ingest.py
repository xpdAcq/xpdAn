from pathlib import Path
from dataclasses import dataclass, asdict
import yaml

import datetime
from event_model import compose_run
import numpy as np
from suitcase.msgpack import Serializer

from intake import Catalog as _Catalog
from databroker._drivers.msgpack import BlueskyMsgpackCatalog


@dataclass(frozen=True)
class SampleComp:
    composition_string: str
    contact_email: str
    notes: tuple
    sa_uid: str
    safety_concerns: str
    sample_composition: dict
    sample_name: str
    sample_phase: dict


@dataclass(frozen=True)
class BeamTime:
    bt_experimenters: tuple
    bt_piLast: str
    bt_safN: str
    bt_uid: str
    bt_wavelength: float


@dataclass(frozen=True)
class Sample:
    sample: SampleComp
    visit: BeamTime


def load_sample_yaml(base):
    """
    Load all of the sample metadata files.

    Parameters
    ----------
    base : Path
       The base of the beamtime directory.

    Returns
    -------
    dict[str, Sample]
        A mapping between the sample name and all of the metadata we know about it.

    """
    out = []
    for f in (base / "config_base" / "yml" / "samples").glob("*yml"):
        with open(f, "r") as fin:
            sc, bt = yaml.load(fin)
            sc.setdefault("notes", ())
            sc.setdefault("contact_email", "dolds@bnl.gov")
            sc.setdefault("safety_concerns", "None")
            out.append(Sample(SampleComp(**sc), BeamTime(**bt)))

    by_sample_name = {s.sample.sample_name: s for s in out}
    if len(out) != len(by_sample_name):
        raise ValueError("You have a duplicate sample!!")

    return by_sample_name


def ingest_chi(fpath, sample_name, md):
    """
    Ingest a single chi file and yields an event stream

    Parameters
    ----------
    fpath : Path
        The full path to the chi file to ingest.  The filename expected to
        have the pattern

          /some/path/SAMPLENAME_YYYYmmdd-HHMMSS_UID_NUM.chi

        which will be parsed to extract the meta data.

    sample_name : str
        The expected sample name.  We need this to be able to remove
        it from the front of the filename.

    md : dict
       Any additional metadata to be put in the start document

    Yields
    ------
    name : str
    doc : dict

    """
    *_, fname = fpath.parts
    assert fname.startswith(sample_name)
    # strip sample name
    fname = fname[len(sample_name) + 1 :]
    date_str, _, fname = fname.partition("_")
    dt = datetime.datetime.strptime(date_str, "%Y%m%d-%H%M%S")
    ts = dt.timestamp()
    uid, _, fname = fname.partition("_")
    num, _, fname = fname.partition("_")
    start_md = {
        "source_uid": uid,
        "iso_time": dt.isoformat(),
        "result_number": int(num),
        "original_path": str(fpath),
        "sample_name": sample_name,
        **md,
    }
    with open(fpath, "rb") as fin:
        # grab first line of the header which includes in interesting looking path
        md["chi_header"] = next(fin).decode("ascii")
        # skip 5 lines of comments for humans
        for j in range(5):
            next(fin)
        _, _, q_num = next(fin).partition(b":")
        q_num = int(q_num)
        md["num_bins"] = q_num
        Q = np.zeros(q_num)
        I = np.zeros(q_num)
        # skip line of comments
        next(fin)
        for k, ln in enumerate(fin):
            if ln.strip():
                Q[k], I[k] = map(float, ln.split(b" "))
        if k != q_num - 1:
            print(fpath, k, q_num)
        # first, compose the run start and get the rest of the factories
        run_bundle = compose_run(time=ts, metadata=start_md)
        # stash the start document for later, could also just serialize
        yield "start", run_bundle.start_doc

        # create the descriptor and factories
        desc_bundle = run_bundle.compose_descriptor(
            name="primary",
            data_keys={
                "Q": {
                    "dtype": "number",
                    "shape": [],
                    "source": "compute",
                    "units": "inverse angstrom",
                },
                "I": {
                    "dtype": "number",
                    "shape": [],
                    "source": "compute",
                    "units": "arbitrarily",
                },
            },
        )

        # stash the descriptor for later use
        desc = desc_bundle.descriptor_doc
        yield "descriptor", desc

        # construct and event page
        event_page = desc_bundle.compose_event_page(
            data={"Q": Q, "I": I},
            # we might make timestamps optional in the future
            timestamps={"Q": np.ones_like(Q) * ts, "I": np.ones_like(I) * ts},
            seq_num=list(range(len(Q))),
            time=np.ones_like(Q) * ts,
            # there is a PR to fix this in event-model the problem is
            # that it is that jsonschema is not identifying that numpy
            # arrays are "array" from a validation point of view 🤦
            validate=False,
        )
        yield "event_page", event_page
        yield "stop", run_bundle.compose_stop()


def ingest_beamtime(base, target_base):
    """
    Ingest a full beamtime and output to a msgpack backed databroker

    Parameters
    ----------
    base : Path
        Path to the root of beamtime directory

    target_base : Path
        Path to the root of where to write the beamtime

    Returns
    -------
    cat : Catalog
        Nested catalog of the beamtime results
    """
    s = load_sample_yaml(base)
    for name, sample in s.items():
        sample_dir = base / "tiff_base" / name
        if not (sample_dir).exists():
            continue
        for chi_file in (sample_dir / "integration").glob("*q.chi"):
            gen = ingest_chi(chi_file, name, asdict(sample))
            _name, doc = next(gen)
            run_uid = doc["source_uid"]
            write_target = target_base / name / str(run_uid) / "integration"
            write_target.mkdir(parents=True, exist_ok=True)
            ser = Serializer(write_target)
            ser(_name, doc)
            for _name, doc in gen:
                ser(_name, doc)

    return find_beamtime_catalog(target_base)


def find_beamtime_catalog(target_base):
    """
    Assuming the output from ingest_beamtime, build a (nested) catalog.

    Parameters
    ----------
    target_base : Path
        Path to the output

    Returns
    -------
    cat : Catalog
        Nested catalog of the beamtime results
    """

    class Catalog(_Catalog):
        """
        Allow catalogs to be built in memory
        """

        def __call__(self, name=None):
            return self

    cat = Catalog()

    for p in target_base.glob("*"):
        *_, sname = p.parts
        scat = Catalog()
        for rpath in p.glob("*"):

            *_, run_id = rpath.parts
            scat[run_id] = Catalog()
            scat[run_id]["integration"] = BlueskyMsgpackCatalog(
                str(rpath / "integration" / "*.msgpack")
            )
        cat[sname] = scat


if __name__ == "__main__":
    base = Path(
        "/mnt/data/bnl/xpdan_injest/user_data_Page_Sept2020_305010_4595ea07_2020-09-12-1146"
    )

    target_base = Path("/mnt/data/bnl/xpdan_injest/msgpack")

    ingest_beamtime(base, target_base)
