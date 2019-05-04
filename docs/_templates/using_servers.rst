.. _using_servers:
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
qoi='The QOI server performs additional data processing and analysis,
extracting quantities of interest from raw and reduced data.',
viz='The viz server visualizes raw and processed data as it becomes available.
This server should already be running, only beamline staff should need to
start it.',
tomo='The tomo server runs tomographic reconstructions on all scalar data (raw and analyzed). The reconstruction algorithm can be changed via the ``algorithm`` keyword.',
intensity='The intensity server calculates the intensity at specified points from 1D patterns.',
) %}

Using Servers
=============

One of the major features of ``xpdAn`` are servers which handle data
processing, visualization, saving and database integration.

Currently there are {{ len(servers_dict) }} servers implemented:
{% set servers_list=list(servers_dict.keys()) %}
{%- for name in servers_list[:-1] -%}

``{{ name }}``,
{% endfor -%}
and ``{{- servers_list[-1] -}}``.
Each server is started by the command ``<server_name>_server`` on the command
line while inside the analysis conda environment.
For example, to start the analysis (with a background scale of .75) one would
run

.. code-block:: bash

 conda activate {{ analysis_env }}
 analysis_server --bg_scale=.75

Some of the servers take optional arguments and keyword arguments.
All the servers are using ``Fire`` to create command line interfaces.
The documentation for ``Fire`` can be found
`here <https://github.com/google/python-fire#python-fire->`_.

Servers
+++++++
{% for name, blurb in servers_dict.items() %}

``{{ name }}``
{{ '-' * (len(name)+4) }}
{{ blurb }}

See the ``run_server`` function in the
:ref:`{{ name }}_server` for the keyword arguments.
{% endfor %}


Running from ipython
++++++++++++++++++++
All of these server commands can be run from ipython (and python) directly.
This provides more control over the servers operation, especially for the
analysis server.
Each server can be imported via
``from xpdan.startup.<server_name> import run_server`` replacing
``<server_name>`` with the appropriate name, eg.
``from xpdan.startup.qoi_server import run_server`` to import the quantity
of interest server.
