.. _post_analysis:

Post Analysis
=============

Analysis can be done after the data has been acquired using the xpdAn server
system.
This can allow for running the analysis with different parameters or
modification of the documents to fix errors made at run time (eg. the dark is
incorrectly mapped to the data)

.. DANGER::
   This will cause problems if either the data was taken from a different
   beamtime or a scan is currently running. If this is the case a second proxy
   should be started and a new set of servers should point to that proxy (see
   below).

Here is an example in which we pass data through to the analysis directly,
to follow along you will need to activate the {{ collection_env }} and
start up ipython.

.. code-block:: python

   # Make the databroker
   import copy
   from databroker import Broker
   db = Broker.named('xpd')
   db.prepare_hook = lambda name, doc: copy.deepcopy(doc)


   # load up the global configuration so we can get the ZMQ proxy address
   from xpdconf.conf import glbl_dict

   # import the publisher which will send our data to the proxy
   from xpdan.vend.callbacks.zmq import Publisher
   # tell the publisher to send the data to the proxy with the prefix of raw
   # (which stands for raw data)
   pub = Publisher(glbl_dict['inbound_proxy_address'], prefix=b'raw')

    better_dark_uid = 'hello world'
    # get the header
    hdr = db[-1]

    for name, doc in hdr.documents():
        # change the dark
        if name == 'start':
            doc.update(sc_dk_field_uid=better_dark_uid)
        # send the data to the analysis system
        pub((name, doc))


Running on a new proxy
----------------------
If you want to run analysis while data is being acquired you will need to do
a few things (note that all these need to be running in terminals which have
been activated into the {{ collection_env }}:

1. Start a new proxy via ``bluesky-0MQ-proxy 5567 5568 -vvv`` (this will
   start a new proxy on the localhost).
1. Start each server 