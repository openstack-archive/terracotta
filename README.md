# Terracotta: A Framework for Dynamic Consolidation of Virtual Machines in Openstack Clouds


Terracotta is an extension to OpenStack implementing dynamic consolidation
of Virtual Machines (VMs) using live migration. The major objective of dynamic
VM consolidation is to improve the utilization of physical resources and reduce
energy consumption by re-allocating VMs using live migration according to their
real-time resource demand and switching idle hosts to the sleep mode.

For example, assume that two VMs are placed on two different hosts, but the
combined resource capacity required by the VMs to serve the current load can be
provided by just one of the hosts. Then, one of the VMs can be migrated to the
host serving the other VM, and the idle host can be switched to a low power mode
to save energy. When the resource demand of either of the VMs increases, they
get deconsolidated to avoid performance degradation. This process is dynamically
managed by Terracotta.

In general, the problem of dynamic VM consolidation can be split into 4
sub-problems:

- Deciding when a host is considered to be underloaded, so that all the VMs
  should be migrated from it, and the host should be switched to a low power
  mode, such as the sleep mode.
- Deciding when a host is considered to be overloaded, so that some VMs should
  be migrated from the host to other hosts to avoid performance degradation.
- Selecting VMs to migrate from an overloaded host out of the full set of the
  VMs currently served by the host.
- Placing VMs selected for migration to other active or re-activated hosts.

The aim of the Terracotta project is to provide an extensible framework for
dynamic consolidation of VMs based on the OpenStack platform. The framework
provides an infrastructure enabling the interaction of components implementing
the 4 decision-making algorithms listed above. The framework allows
configuration-driven switching of different implementations of the
decision-making algorithms.


## More details

The Terracotta project idea originally comes from the
[paper](http://beloglazov.info/papers/2014-ccpe-openstack-neat.pdf) describing
the architecture and implementation of OpenStack Neat and Chapter 6 of Anton
Beloglazov's PhD thesis: http://beloglazov.info/thesis.pdf. After discussion with
the author, we are authorized to use Neat as the Terracotta code base at the very
early stage. However, a lot of work need to be done according to OpenStack project's
requirement.
