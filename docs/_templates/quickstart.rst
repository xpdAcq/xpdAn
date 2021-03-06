.. _quick_start:

Quick Start
===========

This quick-start contains an overview of how the ``xpdAn`` software works.

Please use this page as a reminder of the workflow and to copy & paste code snippets into the
ipython terminals that are controlling your experiment and analysis.  After
pasting, hit enter.

Remember, to post questions about anything XPD, including software, and to see archived answers, at the `XPD-Users Google group
<https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_. If you are not already a member please request to join
the community

XPD Data Analysis Workflow Summary
------------------------------------

This is a brief summary of the steps you have to take to enable real-time data analysis analysis at XPD.
More information about these steps can be found later in the more detailed documentation.
Treat this quickstart as a reference that you may return back to.
Carry out the steps in this order to ensure a successful analysis.

  1. If you haven't already, join `XPD-Users Google group <https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ . Look here for answers if you get stuck and if the answer to your question is not already there, please ask it!

  2. Insert your USB hard drive (or any other storage system) and mount it

  3. Start the servers, see :ref:`using_servers`_ for details on the server setups

    1. Run ``analysis_server``

    2. Run ``portable_db_server path/to/folder/on/USB/drive``

    3. Run ``save_server path/to/different/folder/on/USB/drive``

  4. Your data will be automatically processed, saved and visualized via the servers.

    * The data will be saved in the directory
      ``.../xpdUser/tiff_base/`` and whichever directory you provided on the
      save server command.

    * The pipeline will save:

        1. Dark corrected image (as ``.tiff``)
        2. Mask 

            1. by default a mask is auto-computed on the every image of the scan and used for real-time analysis (you are always given an unmasked tiff file so you can change the masking behavior later).
            2. alternative behavior avialable is to auto-compute the mask on the first image and this mask is used for all subsequent images in real-time analysisa new mask for each image or to turn off masking altogether during real-time analysis
            3. alternative ehavior available is to not mask any images.
            4. These settings can be changed by either running the analysis server with the ``--mask_settings`` kwarg. The three posibilities are denoted by "auto", "first", and "none" respectively. For example, to set the mask to only be created on the first image of the scan one would type ``--mask_setting={'setting': 'first'}``.
        3. Integrated scattered intensity, I(Q) (as ``.chi``)
        4. I(tth) (as ``.chi``), note that this is the same as I(Q) on the tth grid.
        5. The structure and reduced structure factor S(Q) and F(Q) (as ``.sq`` and ``.fq`` respecifvely)
        6. The PDF, G(r) (as ``.gr``)
        7. Calibration parameters (as ``.poni``, if the scan is an calibration scan)
        8. Scan metadata (as ``.yaml``)


    * The pipeline will plot:

        1. Dark corrected image
        2. Dark, Polarization, Background corrected Masked image
        3. I(Q)
        4. I(tth)
        5. F(Q)
        6. G(r)

  5. To change the pipeline configuration stop the analysis server by
     typing ``Ctrl+C`` in the terminal where it is running.
     Then re-run the command with new keyword arguments.

     * ``mask_kwargs`` which controls the mask creation (see the `xpdtools docs <https://xpdacq.github.io/xpdtools/xpdtools.html#xpdtools.tools.mask_img>`_)
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
     in your final analysis, if you want to.

     Then restart the analysis with ``{{ start_analysis }}``.
     Additionally ``{{ start_analysis }}`` accepts modification to these
     dictionaries when it is called see: :ref:`xpdan_startup`.
     
     For example:
     
     .. code-block:: python

        pdf_kwargs.update(qmax=30)
        mask_setting.update({'setting': 'first'})
        mask_kwargs['alpha'] = 2
    
     Would change the PDF calculation Q max to 30 inverse Angstrom, reuse the 
     mask from the first image of the scan on the other images, and change
     the auto masking tolarance to 2 standard deviations.

     .. DANGER::
        Never run ``pdf_kwargs = {'qmax': 30}`` or the like with any of the
        configuration dictionaries, this will detach them from the pipeline
        and you will need to reset the analysis completely (exit ipython 
        and run ``setup_analysis`` again).

Note that not all the pipeline functionality is loaded when ``{{ setup_analysis }}`` is run.
For instance the ``z-score`` pipeline chunk is not loaded automatically.
It can be loaded with ``from xpdan.pipelines.extra import *``.

These and many more things are explained below and elsewhere in the
documentation. `XPD-Users Google group
<https://groups.google.com/forum/#!forum/xpd-users;context-place=overview>`_ .
