Qsome Core
==========
It's like `qless`, but gruesome.

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
git submodule init
git submodule update
make qsome.lua
```

Purpose
=======
For a particular project, we need to implement queues that are composed of
subqueues. Each subqueue may have at most one job being processed at any given
time. This is functionality that should be fairly easy to describe in terms of
existing `qless` operations.

