from xpdan.callbacks import SAVER_MAP
from xpdan.vend.callbacks.core import RunRouter
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict

template = (
    "{base_folder}/{folder_prefix}/"
    "{start[analysis_stage]}/"
    "{start[sample_name]}_"
    "{human_timestamp}_"
    "{__independent_vars__}"
    "{start[uid]:.6}_"
    "{event[seq_num]:04d}{ext}"
)

d = RemoteDispatcher(glbl_dict["outbound_proxy_address"])

# We might have things in other databases though
db = glbl_dict["exp_db"]


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


rr = RunRouter(
    [setup_saver],
    base_folders=glbl_dict["tiff_base"],
    template=template,
    handler_reg=db.reg,
)

d.subscribe(rr)

print("Starting Save Server")

if __name__ == "__main__":
    # TODO: enable users to enter multiple base folders
    d.start()
