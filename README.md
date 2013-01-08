OpenNode Knot
-------------

OpenNode Knot is a plugin for [OpenNode Management
Service](https://github.com/opennode/opennode-management/), which allows to monitor and manage virtualized
data center. Primary target are servers running OpenNode6 OS.

Functionality includes:

 * auto-discovery of existing infrastructure setup;
 * life-cycle management of virtual machines;
 * management of networks, templates and storage.
 * console (ssh, openvz) and VNC proxying.

Requirements
============

Knot is a plugin of OMS and inherits its requirements. In addition, Salt is required to be installed as Knot
relies on Salt as an RPC system.

Setup
=====

Knot depends on [OMS]( and is best installed following standard
[OMS plugin installation](http://opennodecloud.com/docs/opennode.oms.core/intro.html#plugins) procedure.

License
-------

OpenNode Knot is released under an open-source GPLv3 license. Commercial licensing is possible, please
contact <info@opennodecloud.com> for more information.
