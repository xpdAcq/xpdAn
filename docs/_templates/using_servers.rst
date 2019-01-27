.. _using_servers:

Using Servers
=============

One of the major features of ``xpdAn`` are servers which handle data
processing, visualization, saving and database integration.

Currently there are four servers implemented,
``analysis``,
``portable_db``,
``db``,
``save`` and
``viz``.
Each server is started by the command ``<server_name>_server`` on the command
line while inside the analysis conda environment.
For example, to start the analysis one would run

.. code-block:: bash

 conda activate {{ analysis_env }}
 analysis_server

Some of the servers take optional arguments and keyword arguments.
All the servers are using ``Fire`` to create command line interfaces.
The documentation for ``Fire`` can be found
`here <https://github.com/google/python-fire#python-fire->`_.

{% set servers_dict=dict(
analysis='The analysis server handles the majority of the number crunching,
reducing data from images to 1D scattering patterns and PDFs.',
portable_db='The portable db server saves raw and analyzed data in a portable
databroker for users to walk away with. Users will need to specify
which folder they would like the data to be placed in before running
their experiments.
Please see the
`databroker documentation <http://nsls-ii.github.io/databroker/>`_
for more on how to use a databroker.',
db='The db server stores analyzed data in a central database at 28-ID.
This server should already be running, only beamline staff should need to
start it.',
save='The save server saves files with human readable filenames to various
folders.
Users will need to specify which folder(s) they would like the data to be
placed in before running their experiments.',
viz='The viz server visualizes raw and processed data as it becomes available.
This server should already be running, only beamline staff should need to
start it.'
) %}

{% for name, blurb in servers_dict.items() %}

``{{ name }}``
------------
{{ blurb }}

See the ``run_server`` function in the
:ref:`{{ name }}_server` for the keyword arguments.
{% endfor %}