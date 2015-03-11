Qsome Core
==========
[![Build Status](https://travis-ci.org/seomoz/qsome-core.svg?branch=master)](https://travis-ci.org/seomoz/qsome-core)

It's like [`qless`](https://github.com/seomoz/qless-core), but gruesome.

This is in part an experiment to build a library on top of qless by including
the core qless functionality within the body of this library's lua scripts. In
this particular case, we need to implement atomic queue logic but would like to
take advantage of the facilities available in `qless` already.

Building
========
This, like `qless` is made into modular scripts which are then concatenated to
a single unified library. To build it:

```bash
# From the root qsome-core directory
git submodule update --init --recursive
virtualenv ENV
source ENV/bin/activate
pip install -r requirements.txt
make
make test
```

Purpose
=======
For a particular project, we need to implement queues that are composed of
subqueues. Each subqueue may have at most one job being processed at any given
time. This is functionality that should be fairly easy to describe in terms of
existing `qless` operations.

![Status: Incubating](https://img.shields.io/badge/status-incubating-blue.svg?style=flat)
![Team: Big Data](https://img.shields.io/badge/team-big_data-green.svg?style=flat)
![Scope: External](https://img.shields.io/badge/scope-external-green.svg?style=flat)
![Open Source: Yes](https://img.shields.io/badge/open_source-MIT-green.svg?style=flat)
![Critical: No](https://img.shields.io/badge/critical-no-lightgrey.svg?style=flat)
