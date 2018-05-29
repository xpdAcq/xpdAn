===========
 Change Log
===========

.. current developments

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




