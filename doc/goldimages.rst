Goldimages for Curie
=====================

For nearly all scenarios, VMs are deployed to the target virtual
infrastructure. The image(s) for these VMs must be made available to curie
and must contain certain agents/binaries.

Goldimage Requirements
----------------------

The VM images must meet the following requirements for successful operation:

 - VMDK or QCOW2 format
 - Curie agent and dependencies installed
 - FIO installed - https://github.com/axboe/fio
 - Prometheus Node Exporter installed - https://github.com/prometheus/node_exporter/