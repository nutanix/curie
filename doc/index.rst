.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Home

   self

Curie Documentation
====================

Curie is a scenario-execution engine for virtualized infrastructure tests. It
is the backbone for scenarios implemented in the Nutanix X-Ray product.

Learn more about X-Ray here `www.nutanix.com/xray
<http://www.nutanix.com/xray>`_.

Scenarios are the combination of a virtualized infrastructure (VMs), workloads
that run on those VMs, and a sequence of events. These events or steps are
operations like: VM power ops, clones, workload start/stop, node power ops,
etc. Scenarios are described at the highest level in YAML and are
cross platform and abstracted from underlying APIs, meaning that any supported
virtual infrastructure should be able to perform the events and scenario authors
need not know about specific APIs to perform the events. Example scenarios
can be found at this location `https://gitlab.com/nutanix/xray-scenarios
<https://gitlab.com/nutanix/xray-scenarios>`_

To learn more about writing scenarios in YAML, requirements, and the API,
check out the following sections:

.. toctree::
   :maxdepth: 2

   yaml
   goldimages
   apidoc/modules

Currently Supported Virtual Infrastructures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- VMware vSphere
- Nutanix AHV


Release Notes
-------------

v3.0.0
~~~~~~

Initial open source release.

Contributing and Improvement Requests
-------------------------------------
Curie is developed internally by Nutanix, but the code is made available
through GitLab on a regular basis. Public contributions are encouraged
and will be accepted if they meet the standards of the project. Submit
enhancement requests `https://gitlab.com/nutanix/curie/issues/new
<https://gitlab.com/nutanix/curie/issues/new>`_.

Original Authors
----------------
- Brent Chun
- George Dowding
- Bostjan Ferlic
- Ryan Hardin
- Jason Klein
- Iztok Prelog
- Christopher Wilson

License
-------
Curie is licensed under the MIT license. Copyright (c) 2018 Nutanix Inc.

Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
