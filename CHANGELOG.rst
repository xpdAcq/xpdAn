===========
 Change Log
===========

.. current developments

v0.5.2
====================

**Fixed:**

* Image streams specifically listen to the primary event stream
* ``AlignEventStream`` with the raw data listens to the primary event stream



v0.5.1
====================

**Fixed:**

* Fix mask overlay order so it isn't delayed



v0.5.0
====================

**Added:**

* Units for most reduced data streams (mostly so waterfall can plot them)
* Added ``plot_graph`` to analysis server so we can visualize the graph

**Changed:**

* Use ``dark_frame=True`` to determine if a scan is a dark scan.  This means
  that scans without darks can now be processed, although they will use the
  most recenlty cached dark.
* ``raw_stripped`` is run first, so all the aligned data can be emitted ASAP.
  This means that data can be saved as soon as it is created, rather than 
  waiting for the last data to be processed.
* Simplified the plotting via the new ``LiveWaterfall`` callback.
* Support ``original_start_uid`` system
* use raw start document time for file naming



v0.4.1
====================



v0.4.0
====================

**Added:**

* analysis server for running analysis in an external process
  without saving or visualization
* DB server for saving data into databrokers
* ``ExportCallback`` for copying data for databroker export
* ``Retrieve`` callback for accessing data from resources and datums
* ``ReturnCallback`` a callback which always returns name document pairs
* ``RunRouter`` which allows us to make pipelines on the fly
* Server for handling saving of data
* Tests for the servers
* All the callbacks from ``bluesky``

**Changed:**

* Chunk the analysis pipelines into pieces so they can be composed
* ``StartStopCallback`` also reports the analysis time for diagnostics
* Multiple detectors are supported for the main processing pipeline
  note that this does not support multiple detectors at the same
  time.
* ``StartStopCallback`` now prints event analysis times
* Moved file saving print statements closer to saving action
* Moved ``hinted_fields`` to the ``xpdan.vend.callbacks.core``
* ``BestEffortCallback`` has a teardown kwarg for managing figures
  at the end of a run, preventing us from having too many windows
  open.

**Fixed:**

* Removed ``sanitize_np`` from zmq system.

**Authors:**

* Christopher J. Wright



v0.3.6
====================

**Fixed:**

* Both the foreground dark and foreground light are properly pulled based off
  their stream names




v0.3.5
====================

**Fixed:**

* Restore calib saving
* Make certain that files are saved before additional analysis is performed.
  This should make the pipeline more robust to analysis failures near the
  end of the pipeline.
* ``xpdan.callbacks.StopStartCallback`` to be no-stop doc tolerant
* ``conftest.py`` now properly reports a ``bt_uid``
* ``xpdan.db_utils.query_dark`` now always returns a list




v0.3.4
====================

**Added:**

* ``save_tiff`` pipeline for simple save tiff




v0.3.3
====================

**Fixed:**

* Splay out args for calibration saving properly

* Pull calibration information from ``dSpacings``




v0.3.2
====================

**Fixed:**

* Cast to numpy float32 on way into pipeline




v0.3.1
====================

**Added:**

* Add print statement for server startup


**Changed:**

* metadata now stored into its own folder


**Fixed:**

* Files saved in sub dir of ``tiff_base``

* Filter out ``None`` produced by ``Filler`` on ``datum/resource`` docs




v0.3.0
====================

**Added:**

* ``Filler`` and ``clear_combine_latest`` to ``pipeline_utils``

* Tests for main pipeline
* Support for QOI plotting
* ``start_analysis`` function for starting the ``RemoteDispatcher`` and pushing
  data through the pipeline live


**Changed:**

* ``db_utils`` query functions signatures are now reversed (docs first, db
  second)

* Simplified ``render_and_clean``

* ``MainCallback`` ``analysis_stage`` syntax in line with pipeline

* ``conftest`` databroker now reports raw dicts
* Separated saving, visualization, and QOI pipeline chunks
* Darks and Backgrounds are zeroed out at start of scan in case scan has no
  dark or background.


**Removed:**

* ``xpdan.tools`` is now in ``xpdtools.tools``

* ``shim`` module is now in ``xpdtools``


**Fixed:**

* Analysis pipeline now runs on xpdtools pipeline




v0.2.3rc
====================

**Changed:**

* xpdAcq now outsources ``glbl` configuration management to xpdConf
* Use conda-forge ``xpdconf`` for CI builds


**Removed:**

* ``load_configuration`` (which is now in xpdConf)




v0.2.2
====================



v0.2.1
====================

**Added:**

* Requirements folder

* ``push_tag`` to rever 

* Flexible folder tag
* Add rever changelog activity
* Speed up masking via median based sigma clipping
* Z score visualization to callback pipeline




