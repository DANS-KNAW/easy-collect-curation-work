easy-collect-curation-work
==========================
[![Build Status](https://travis-ci.org/DANS-KNAW/easy-collect-curation-work.png?branch=master)](https://travis-ci.org/DANS-KNAW/easy-collect-curation-work)


SYNOPSIS
--------

    easy-collect-curation-work

DESCRIPTION
-----------

Job that collects the curated deposits from the data managers' personal curation areas


ARGUMENTS
---------
```
  Usage: 
  
     easy-split-multi-deposit

  Options:
  
    -h, --help      Show help message
    -v, --version   Show version of this program
```

INSTALLATION AND CONFIGURATION
------------------------------
The preferred way of install this module is using the RPM package. This will install the binaries to
`/opt/dans.knaw.nl/easy-collect-curation-work`, the configuration files to `/etc/opt/dans.knaw.nl/easy-collect-curation-work`,

If you are on a system that does not support RPM, you can use the tarball. You will need to copy the
service scripts to the appropiate locations yourself.


BUILDING FROM SOURCE
--------------------

Prerequisites:

* Java 8 or higher
* Maven 3.3.3 or higher

Steps:

        git clone https://github.com/DANS-KNAW/easy-collect-curation-work.git
        cd easy-collect-curation-work
        mvn install
