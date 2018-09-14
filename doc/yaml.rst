Creating Scenarios with YAML
============================

Scenarios in Curie are implemented as YAML but will include other peripheral
files such as those for workload descriptions or additional scripts.


Curie Scenario Structure
-------------------------
Scenarios must have the following structure to be valid:

::


   /<test_name>
   |── test.yml
   |-- <some_file.fio>
   ├── <additional resources>
   ├── ...

Scenario resource files may be referenced within each of the steps. For
example, if a workload generator is used to create storage traffic in the
scenario, the workload configuration files are placed in the subdirectory
(alongside ``test.yml``), and are referenced in steps from the
:mod:`curie.steps.workload` module.

A typical scenario directory structure might look like this:
::

   /custom_mixed_database_scenario
   ├── test.yml
   ├── dss.fio
   └── oltp.fio

Scenarios included with X-Ray and examples on how to create scenarios can be
found in the `X-Ray Scenarios Repository
<http://www.gitlab.com/nutanix/xray-scenarios>`_.

Required Fields
---------------

name
~~~~
The internally used name of the scenario. This must be unique.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 1
   :linenos:
   :lineno-match:

display_name
~~~~~~~~~~~~
The externally visible name of the scenario. This must be unique.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 3
   :linenos:
   :lineno-match:


summary
~~~~~~~
A one-line description of the scenario; A tagline.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 5
   :linenos:
   :lineno-match:

description
~~~~~~~~~~~
A detailed description of the scenario. This should cover the goals of the
scenario, the steps performed, and how to interpret the results. Today, this
section is formatted in HTML.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 7-45
   :linenos:
   :lineno-match:

tags
~~~~
A list of strings that can be used when filtering search results.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 47-50
   :linenos:
   :lineno-match:

estimated_runtime
~~~~~~~~~~~~~~~~~
The estimated duration of the scenario in seconds.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 52
   :linenos:
   :lineno-match:

vms
~~~
A list of VM group names, with configuration information for each. A VM group
represents a group of VMs that are all identical, and perform operations at the
same time.

* Required
   * ``template`` The name of the VM image template to use.

* Optional
   * ``vcpus`` The number of vCPUs that should be allocated for each VM in the
     VM group.
   * ``ram_mb`` The amount of RAM in megabytes that will be allocated for each
     VM in the VM group when deployed.
   * ``data_disks`` A dictionary containing ``count`` and ``size`` of the data
     disks that should be created for the
   * ``nodes`` A string that describes the nodes in the cluster upon which the
     VMs should be deployed. This string is a comma-separated series of slices
     described using Python's built-in slicing behavior.
   * ``count_per_node`` The number of VMs to deploy on each of the nodes
     described by ``nodes``.
   * ``count_per_cluster`` The total number of VMs to deploy evenly across the
     nodes described by ``nodes``.

.. note:: Either ``count_per_node`` or ``count_per_cluster`` must be defined.
          They can not be used together.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 54-63
   :linenos:
   :lineno-match:

workloads
~~~~~~~~~
A list of workload names, with configuration information for each. A workload
is used to generate storage load on the cluster using a VM group.

* Required
   * ``vm_group``: The name of the VM group to run the workload. This is
     defined in the ``vms`` section.
   * ``config_file``: The name of the workload configuration file to run. This
     is usually a .fio file in the same directory as the scenario's YAML file.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 74-77
   :linenos:
   :lineno-match:

results
~~~~~~~
A list of result names, with configuration information for each. Each result
will show up as a plot in the X-Ray UI.

* Required
   * ``workload_name``: The name of the workload generating the data for this
     set of results. This is defined in the ``workloads`` section.
   * ``result_type``: The type of plot to render for this result. Valid choices
     are:
      * ``iops``: Create a line plot showing the number of I/O operations per
        second over time.
      * ``errors``: Create a line plot showing the number of cumulative I/O
        errors over time.
      * ``active``: Create a line plot showing I/O activity (boolean) over
        time.
      * ``latency``: Create a line plot showing latency of I/O operations over
        time.
* Optional
   * ``result_hint``: Provides text to the interface on how to interpret the
     result.
   * ``result_expected_value``: Provides an indication of the expected value
     of the result. Can be used to create a line indicating this value.
   * ``result_value_bands``: A list of dictionaries with name, upper, and lower
     fields used to help create visual bands, for example a 95% prediction
     interval.
   * ``aggregate``: Combine the results from all VMs associated with
     the workload into a single plot. Options include:
      * ``sum``
      * ``min``
      * ``mean``
      * ``median``
      * ``max``

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 85-97
   :linenos:
   :lineno-match:

setup
~~~~~
A list of step names, with parameters for each. These steps are performed
during the scenario's setup phase. These are the steps that must occur before
the run phase can be reached.

Because these setup steps occur before the measurement period of the scenario
begins, no results are rendered in the X-Ray UI during these steps.

For a complete list of the steps available, see the
:doc:`/apidoc/curie.steps`.

.. note:: The ``test`` parameter of each step is passed in implicitly, and
          should not be included in the YAML.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 111-118
   :linenos:
   :lineno-match:

run
~~~
A list of step names, with parameters for each. These steps are performed
during the scenario's run phase.

Results are rendered in the X-Ray UI during these steps.

For a complete list of the steps available, see the
:doc:`/apidoc/curie.steps`.

.. note:: The ``test`` parameter of each step is passed in implicitly, and
          should not be included in the YAML.

.. literalinclude:: example_test.yml
   :language: yaml
   :lines: 132-137
   :linenos:
   :lineno-match:

Steps
-----
For a complete list of the steps available, see the
:doc:`apidoc/curie.steps`.

.. note:: The ``test`` parameter of each step is passed in implicitly, and
          should not be included in the YAML.

Working Example
---------------

.. literalinclude:: example_test.yml
   :caption: Database Colocation Scenario
   :language: yaml
   :linenos:
   :lineno-match:

