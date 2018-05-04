.. _quick_start:

Quick Start
===========

This quick-start contains an overview of how the ``xpdAn`` software works.
To understand more details about it, please refer to the detailed documentation in :ref:`xpdu`

Please use this page as a reminder of the workflow and to copy & paste code snippets into the
ipython terminals that are controlling your experiment and analysis.  After
pasting, hit enter.

Remember, to post questions about anything XPD, including software, and to see archived answers, at the `XPD-Users Google group
<https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ . If you are not already a member please request to join
the community

XPD Data Analysis Workflow Summary
------------------------------------

This is the summary of the steps for analysis data at XPD. They are explained below.
Carry out the steps in this order to ensure a successful analysis.

  1. If you haven't already, join `XPD-Users Google group <https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ . Look here for answers if you get stuck and if the answer to your question is not already there, please ask it!

  2. If not started already start a proxy and an analysis session.
  To start the proxy run ``bluesky-0MQ-proxy {{ bs_proxy_config }}``.
  To start the analysis session run ``{{ analysis_session_cmd }}``.

  3. Start the analysis by running ``{{ start_analysis }}``

  5. Your data will be automatically saved and visualized via the analysis pipleine.

    * The data will be saved in ``.../xpdUser/tiff_base/<Sample_name_from_spreadsheet>``.

    * The pipeline will save:

        1. Dark corrected image (as ``.tiff``)
        2. Mask (as ``.msk``)
        3. I(Q) (as ``.chi``)
        4. I(tth) (as ``.chi``)
        5. G(r) (as ``.gr``)
        6. Calibration parameters (as ``.poni``, if applicable)
        7. Starting metadata (as ``.yaml``)


    * The pipeline will visualize the:

        1. Dark corrected image
        2. Mask
        3. I(Q)
        4. I(tth)
        5. F(Q)
        6. G(r)

  6. To change the pipeline configuration stop the ``{{ start_analysis }}`` by
  typing ``Ctrl+C`` in the terminal. Then edit one of the configuration
  dictionaries
     * ``mask_kwargs`` which controls the mask creation
     * ``pdf_kargs`` which controls the PDF creation
     * ``fq_kwargs`` which controls the FQ/SQ creation
     * ``mask_setting`` which controls how often masks are created
       (``auto``: all imgs masked,
       ``first``: first img masked,
       or ``none``: no imgs masked)
  Then restart the analysis with ``{{ start_analysis }}``

These and many more things are explained below and elsewhere in the
documentation. `XPD-Users Google group
<https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ .
