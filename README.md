Petrel
======

Tools for writing, submitting, debugging, and monitoring Storm topologies in pure Python.

NOTE: The base Storm package provides storm.py, which supports Python 2.6. Petrel, however, requires Python 2.7 or 3.5.

Overview
========
Petrel offers some important improvements over the storm.py module provided with Storm:

* Topologies are implemented in 100% Python
* Petrel's packaging support automatically sets up a Python virtual environment for your topology and makes it easy to install additional Python packages.
* "petrel.mock" allows testing of single components or single chains of related components.
* Petrel automatically sets up logging for every spout or bolt and logs a stack trace on unhandled errors.

Installation
============

* Python 2.7
* System packages
  * libyaml
  * thrift
* Python packages (you install)
    * virtualenv
* Python packages (installed automatically by setup.py)
    * simplejson 2.6.1
    * thrift 0.8.0
    * PyYAML 3.10

Installing Petrel as an egg
---------------------------

Before installing Petrel, make sure Storm is installed and in your path. Run the following command:

    storm version

This will print the version of Storm active on your system, a number such as "1.0.2". You must use a version of Petrel whose first 3 digits match this version.

Install the egg:

easy_install petrel*.egg

This will download a few dependencies and then print a message like:

    Finished processing dependencies for petrel==1.0.2.0.3

Topology Configuration
======================

Petrel's "--config" parameter accepts a YAML file with standard Storm configuration options. The file can also include some Petrel-specific settings. See below.

```
topology.message.timeout.secs: 150
topology.ackers: 1
topology.workers: 5
topology.max.spout.pending: 1
worker.childopts: "-Xmx4096m"
topology.worker.childopts: "-Xmx4096m"

# Controls how Petrel installs its own dependencies, e.g. simplejson, thrift, PyYAML.
petrel.pip_options: "--no-index -f http://10.255.3.20/pip/"

# If you prefer, you can configure parallelism here instead of in setSpout() or
# setBolt().
petrel.parallelism.splitsentence: 1
```

Building and submitting topologies
==================================

Use the following command to package and submit a topology to Storm:

<pre>
petrel submit --sourcejar ../../jvmpetrel/target/storm-petrel-*-SNAPSHOT.jar --config localhost.yaml
</pre>

The above command builds and submits a topology in local mode. It will run until you stop it with Control-C. This mode is useful for simple development and testing.

If you want to run the topology on a Storm cluster, run the following command instead:

<pre>
petrel submit --sourcejar ../../jvmpetrel/target/storm-petrel-*-SNAPSHOT.jar --config localhost.yaml wordcount
</pre>

You can find instructions on setting up a Storm cluster here:

https://github.com/nathanmarz/storm/wiki/Setting-up-a-Storm-cluster

Build
-----

* Get the topology definition by loading the create.py script and calling create().
* Package a JAR containing the topology definition, code, and configuration.
* Files listed in manifest.txt, e.g. additional configuration files

Deploy and Run
--------------

To deploy and run a Petrel topology on a Storm cluster, each Storm worker must have the following installed:

* Python 2.7
* setuptools
* virtualenv

Note that the worker machines don't require Petrel itself to be installed. Only the *submitting* machine needs to have Petrel. Each time you submit a topology using Petrel, it creates a custom jar file with the Petrel egg and and your Python spout and bolt code. These files in the wordcount example show how this works:

* buildandrun
* manifest.txt

Because Petrel topologies are self contained, it is easy to run multiple versions of a topology on the same cluster, as long as the code differences are contained within virtualenv. Before a spout or bolt starts up, Petrel creates a new Python virtualenv and runs the optional topology-specific setup.sh script to install Python packages. This virtual environment is shared by all the spouts or bolts from that instance of the topology on that machine.