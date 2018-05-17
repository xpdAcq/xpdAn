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

This is the summary of the steps for analysis data at XPD.
The steps are explained below.
Carry out the steps in this order to ensure a successful analysis.

  1. If you haven't already, join `XPD-Users Google group <https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ . Look here for answers if you get stuck and if the answer to your question is not already there, please ask it!

  2. Start the analysis by running ``{{ start_analysis }}`` in a terminal

  3. Your data will be automatically saved and visualized via the analysis pipeline.

    * The data will be saved in the directory
      ``.../xpdUser/tiff_base/<Sample_name_from_spreadsheet>``.

    * The pipeline will save:

        1. Dark corrected image (as ``.tiff``)
        2. Mask (as ``.msk`` and ``.npy``,
           ``.msk`` files are read by ``fit2d`` while ``.npy`` files are read
           by numpy). The frequency of mask production is controlled by
           ``mask_settings``.
        3. Integrated scattering intensity, I(Q) (as ``.chi``)
        4. I(tth) (as ``.chi``), note that this is the same as I(Q) on the tth
           grid.
        5. The PDF, G(r) (as ``.gr``)
        6. Calibration parameters (as ``.poni``, if the scan is an analysis
           scan)
        7. Scan metadata (as ``.yaml``)


    * The pipeline will visualize the:

        1. Dark corrected image
        2. Mask
        3. I(Q)
        4. I(tth)
        5. F(Q)
        6. G(r)

  4. To change the pipeline configuration stop the ``{{ start_analysis }}`` by
     typing ``Ctrl+C`` in the terminal. Then edit one of the configuration
     dictionaries which are loaded into ipython.

     * ``mask_kwargs`` which controls the mask creation
     * ``pdf_kwargs`` which controls the PDF creation
     * ``fq_kwargs`` which controls the FQ/SQ creation
     * ``mask_setting`` which controls how often masks are created:

         * ``auto``: a mask is autocalculated for, and applied to, each image,
         * ``first``: a new mask is autocalculated for the first image in the scan,
           then this mask is applied to all images in the scan,
         * ``none``: no mask is applied)

     Note, the image that is saved is always the unmasked image, so the masking
     behavior affects only the real-time analysis. You can run masking in the
     analysis pipeline (recommended) and then apply different masking logic later
     in your final analysis.

     Then restart the analysis with ``{{ start_analysis }}``.
     Additionally ``{{ start_analysis }}`` accepts modification to these
     dictionaries when it is called. Please see the function signature via
     ``{{ start_analysis_func }}?``.

These and many more things are explained below and elsewhere in the
documentation. `XPD-Users Google group
<https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ .
