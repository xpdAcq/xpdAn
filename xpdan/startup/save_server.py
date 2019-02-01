"""Module for setting up and running a file saving server"""
import fire

from xpdan.callbacks import SAVER_MAP
from xpdan.vend.callbacks.core import RunRouter
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict


def setup_saver(doc, **kwargs):
    """Function to setup the correct savers, if the correct ``analysis_stage``
    is set in the start doc then a saver will be created appropriate for the
    data

    Parameters
    ----------
    doc : dict
        The start document

    Returns
    -------
    cb : CallbackBase or None
        The callback or nothing

    """
    cb = SAVER_MAP.get(doc.get("analysis_stage", ""), None)
    if cb:
        return cb(**kwargs)
    return


base_template = (
    "{base_folder}/{folder_prefix}/"
    "{start[analysis_stage]}/"
    "{start[sample_name]}_"
    # The writers handle the trailing ``_``
    "{human_timestamp}_"
    "{__independent_vars__}"
    "{start[original_start_uid]:.6}_"
    "{event[seq_num]:04d}{ext}"
)


def run_server(
    base_folders=None,
    template=base_template,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    db_names=("exp_db", "an_db"),
    prefix=None
):
    """Run file saving server

    Parameters
    ----------
    base_folders : list or str or str, optional
        Either a list of strings for base folders to save data into or a
        single str for a base folder to save data into.

        Defaults to the value of ``glbl_dict["tiff_base"]``.
    template : str, optional
        The string used as a template for the file names. Please see the
        :ref:`xpdan_callbacks` module docs for mor information on the
         templating.

        Defaults to::

          "{base_folder}/{folder_prefix}/"
          "{start[analysis_stage]}/{start[sample_name]}_{human_timestamp}"
          "_{__independent_vars__}{start[uid]:.6}_{event[seq_num]:04d}{ext}"
    outbound_proxy_address : str
        The address of the ZMQ proxy
    db_names : iterable of str
        The names of the databases in the ``glbl_dict`` which to use for data
        loading handlers
    prefix : binary strings
        Which topics to listen on for zmq
    """
    if prefix is None:
        prefix = [b'an', b'raw']
    if base_folders is None:
        base_folders = []

    if isinstance(base_folders, str):
        base_folders = [base_folders]
    if isinstance(base_folders, tuple):
        base_folders = list(base_folders)
    if isinstance(glbl_dict["tiff_base"], str):
        glbl_dict["tiff_base"] = [glbl_dict["tiff_base"]]

    base_folders += glbl_dict["tiff_base"]
    # TODO: support other protocols? (REST, maybe)
    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    dbs = [glbl_dict[k] for k in db_names if k in glbl_dict]
    handlers = {}
    for db in dbs:
        handlers.update(db.reg.handler_reg)
    print(base_folders)

    rr = RunRouter(
        [setup_saver],
        base_folders=base_folders,
        template=template,
        handler_reg=handlers,
    )

    d.subscribe(rr)
    print("Starting Save Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
