# Copyright 2012 Anton Beloglazov
# Copyright 2015 Huawei Technologies Co. Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The main local manager module.

The local manager component is deployed on every compute host and is
invoked periodically to determine when it necessary to reallocate VM
instances from the host. First of all, it reads from the local storage
the historical data on the resource usage by VMs stored by the data
collector. Then, the local manager invokes the specified in the
configuration underload detection algorithm to determine whether the
host is underloaded. If the host is underloaded, the local manager
sends a request to the global manager's REST API to migrate all the
VMs from the host and switch the host to the sleep mode.

If the host is not underloaded, the local manager proceeds to invoking
the specified in the configuration overload detection algorithm. If
the host is overloaded, the local manager invokes the configured VM
selection algorithm to select the VMs to migrate from the host. Once
the VMs to migrate from the host are selected, the local manager sends
a request to the global manager's REST API to migrate the selected VMs
from the host.

Similarly to the global manager, the local manager can be configured
to use specific underload detection, overload detection, and VM
selection algorithm using the configuration file discussed further in
the paper.

Underload detection is done by a specified in the configuration
underload detection algorithm (algorithm_underload_detection). The
algorithm has a pre-defined interface, which allows substituting
different implementations of the algorithm. The configured algorithm
is invoked by the local manager and accepts historical data on the
resource usage by VMs running on the host as an input. An underload
detection algorithm returns a decision of whether the host is
underloaded.

Overload detection is done by a specified in the configuration
overload detection algorithm (algorithm_overload_detection). Similarly
to underload detection, all overload detection algorithms implement a
pre-defined interface to enable configuration-driven substitution of
difference implementations. The configured algorithm is invoked by the
local manager and accepts historical data on the resource usage by VMs
running on the host as an input. An overload detection algorithm
returns a decision of whether the host is overloaded.

If a host is overloaded, it is necessary to select VMs to migrate from
the host to avoid performance degradation. This is done by a specified
in the configuration VM selection algorithm (algorithm_vm_selection).
Similarly to underload and overload detection algorithms, different VM
selection algorithm can by plugged in according to the configuration.
A VM selection algorithm accepts historical data on the resource usage
by VMs running on the host and returns a set of VMs to migrate from
the host.

The local manager will be implemented as a Linux daemon running in the
background and every local_manager_interval seconds checking whether
some VMs should be migrated from the host. Every time interval, the
local manager performs the following steps:

1. Read the data on resource usage by the VMs running on the host from
   the <local_data_directory>/vm directory.

2. Call the function specified in the algorithm_underload_detection
   configuration option and pass the data on the resource usage by the
   VMs, as well as the frequency of the CPU as arguments.

3. If the host is underloaded, send a request to the REST API of the
   global manager and pass a list of the UUIDs of all the VMs
   currently running on the host in the vm_uuids parameter, as well as
   the reason for migration as being 0.

4. If the host is not underloaded, call the function specified in the
   algorithm_overload_detection configuration option and pass the data
   on the resource usage by the VMs, as well as the frequency of the
   host's CPU as arguments.

5. If the host is overloaded, call the function specified in the
   algorithm_vm_selection configuration option and pass the data on
   the resource usage by the VMs, as well as the frequency of the
   host's CPU as arguments

6. If the host is overloaded, send a request to the REST API of the
   global manager and pass a list of the UUIDs of the VMs selected by
   the VM selection algorithm in the vm_uuids parameter, as well as
   the reason for migration as being 1.

7. Schedule the next execution after local_manager_interval seconds.
"""

from hashlib import sha1
import libvirt
import os

from oslo_config import cfg
from oslo_log import log as logging

from terracotta import common
from terracotta.openstack.common import periodic_task
from terracotta.utils import db_utils


local_manager_opts = [
    cfg.FloatOpt('host_cpu_usable_by_vms',
                 default=1.0,
                 help='The threshold on the overall (all cores) '
                      'utilization of the physical CPU of a host '
                      'that can be allocated to VMs.'),
    cfg.StrOpt('os_admin_user',
               default='admin',
               help='The admin user name for authentication '
                    'with Nova using Keystone.'),
    cfg.StrOpt('os_admin_password',
               default='admin',
               help='The admin user password for authentication '
                    'with Nova using Keystone.'),
    cfg.StrOpt('local_data_directory',
               default='/var/lib/terracotta',
               help='he directory used by the data collector to store '
                    'the data on the resource usage by the VMs running '
                    'on the host.'),
]


CONF = cfg.CONF
CONF.register_opts(local_manager_opts)
LOG = logging.getLogger(__name__)


class LocalManager(periodic_task.PeriodicTasks):
    def __init__(self):
        super(LocalManager, self).__init__()
        self.state = self.init_state()

    def init_state(self):
        """Initialize a dict for storing the state of the local manager.

        :param config: A config dictionary.
         :type config: dict(str: *)

        :return: A dictionary, initial state of the local manager.
         :rtype: dict
        """
        vir_connection = libvirt.openReadOnly(None)
        if vir_connection is None:
            message = 'Failed to open a connection to the hypervisor'
            LOG.critical(message)
            raise OSError(message)

        physical_cpu_mhz_total = int(
            common.physical_cpu_mhz_total(vir_connection) *
            CONF.host_cpu_usable_by_vms)
        return {'previous_time': 0.,
                'vir_connection': vir_connection,
                'db': db_utils.init_db(),
                'physical_cpu_mhz_total': physical_cpu_mhz_total,
                'hostname': vir_connection.getHostname(),
                'hashed_username': sha1(CONF.os_admin_user).hexdigest(),
                'hashed_password': sha1(CONF.os_admin_password).hexdigest()}

    @periodic_task.periodic_task(spacing=10, run_immediately=True)
    def execute(self, ctx=None):
        """Execute an iteration of the local manager.

        1. Read the data on resource usage by the VMs running on the host from
           the <local_data_directory>/vm directory.

        2. Call the function specified in the algorithm_underload_detection
           configuration option and pass the data on the resource usage by the
           VMs, as well as the frequency of the CPU as arguments.

        3. If the host is underloaded, send a request to the REST API of the
           global manager and pass a list of the UUIDs of all the VMs
           currently running on the host in the vm_uuids parameter, as well as
           the reason for migration as being 0.

        4. If the host is not underloaded, call the function specified in the
           algorithm_overload_detection configuration option and pass the data
           on the resource usage by the VMs, as well as the frequency of the
           host's CPU as arguments.

        5. If the host is overloaded, call the function specified in the
           algorithm_vm_selection configuration option and pass the data on
           the resource usage by the VMs, as well as the frequency of the
           host's CPU as arguments

        6. If the host is overloaded, send a request to the REST API of the
           global manager and pass a list of the UUIDs of the VMs selected by
           the VM selection algorithm in the vm_uuids parameter, as well as
           the reason for migration as being 1.
        """
        LOG.info('Started an iteration')
        state = self.state

        vm_path = common.build_local_vm_path(CONF.local_data_directory)
        vm_cpu_mhz = self.get_local_vm_data(vm_path)
        vm_ram = self.get_ram(state['vir_connection'], vm_cpu_mhz.keys())
        vm_cpu_mhz = self.cleanup_vm_data(vm_cpu_mhz, vm_ram.keys())

        if not vm_cpu_mhz:
            LOG.info('Skipped an iteration')
            return

        host_path = common.build_local_host_path(CONF.local_data_directory)
        host_cpu_mhz = self.get_local_host_data(host_path)

        host_cpu_utilization = self.vm_mhz_to_percentage(
            vm_cpu_mhz.values(),
            host_cpu_mhz,
            state['physical_cpu_mhz_total'])
        LOG.debug('The total physical CPU Mhz: %s',
                  str(state['physical_cpu_mhz_total']))
        LOG.debug('VM CPU MHz: %s', str(vm_cpu_mhz))
        LOG.debug('Host CPU MHz: %s', str(host_cpu_mhz))
        LOG.debug('CPU utilization: %s', str(host_cpu_utilization))

        if not host_cpu_utilization:
            LOG.info('Not enough data yet - skipping to the next iteration')
            LOG.info('Skipped an iteration')
            return

        time_step = CONF.data_collector_interval
        migration_time = common.calculate_migration_time(
            vm_ram, CONF.network_migration_bandwidth)

        if 'underload_detection' not in state:
            underload_detection_params = common.parse_parameters(
                CONF.local_manager.algorithm_underload_detection_parameters)
            underload_detection = common.call_function_by_name(
                CONF.local_manager.algorithm_underload_detection_factory,
                [time_step,
                 migration_time,
                 underload_detection_params])
            state['underload_detection'] = underload_detection
            state['underload_detection_state'] = {}

            overload_detection_params = common.parse_parameters(
                CONF.local_manager.algorithm_overload_detection_parameters)
            overload_detection = common.call_function_by_name(
                CONF.local_manager.algorithm_overload_detection_factory,
                [time_step,
                 migration_time,
                 overload_detection_params])
            state['overload_detection'] = overload_detection
            state['overload_detection_state'] = {}

            vm_selection_params = common.parse_parameters(
                CONF.local_manager.algorithm_vm_selection_parameters)
            vm_selection = common.call_function_by_name(
                CONF.local_manager.algorithm_vm_selection_factory,
                [time_step,
                 migration_time,
                 vm_selection_params])
            state['vm_selection'] = vm_selection
            state['vm_selection_state'] = {}
        else:
            underload_detection = state['underload_detection']
            overload_detection = state['overload_detection']
            vm_selection = state['vm_selection']

        LOG.info('Started underload detection')
        underload, state['underload_detection_state'] = underload_detection(
            host_cpu_utilization, state['underload_detection_state'])
        LOG.info('Completed underload detection')

        LOG.info('Started overload detection')
        overload, state['overload_detection_state'] = overload_detection(
            host_cpu_utilization, state['overload_detection_state'])
        LOG.info('Completed overload detection')

        if underload:
            LOG.info('Underload detected')
            # TODO(xylan): send rpc message to global manager
        else:
            if overload:
                LOG.info('Overload detected')

                LOG.info('Started VM selection')
                vm_uuids, state['vm_selection_state'] = vm_selection(
                    vm_cpu_mhz, vm_ram, state['vm_selection_state'])
                LOG.info('Completed VM selection')

                LOG.info('Selected VMs to migrate: %s', str(vm_uuids))
                # TODO(xylan): send rpc message to global manager
            else:
                LOG.info('No underload or overload detected')

        LOG.info('Completed an iteration')
        self.state = state

    def get_local_vm_data(self, path):
        """Read the data about VMs from the local storage.

        :param path: A path to read VM UUIDs from.
        :return: A map of VM UUIDs onto the corresponing CPU MHz values.
        """
        result = {}
        for uuid in os.listdir(path):
            with open(os.path.join(path, uuid), 'r') as f:
                result[uuid] = [int(x) for x in f.read().strip().splitlines()]
        return result

    def get_local_host_data(self, path):
        """Read the data about the host from the local storage.

        :param path: A path to read the host data from.
        :return: A history of the host CPU usage in MHz.
        """
        if not os.access(path, os.F_OK):
            return []
        with open(path, 'r') as f:
            result = [int(x) for x in f.read().strip().splitlines()]
        return result

    def cleanup_vm_data(self, vm_data, uuids):
        """Remove records for the VMs that are not in the list of UUIDs.

        :param vm_data: A map of VM UUIDs to some data.
        :param uuids: A list of VM UUIDs.
        :return: The cleaned up map of VM UUIDs to data.
        """
        for uuid, _ in vm_data.items():
            if uuid not in uuids:
                del vm_data[uuid]
        return vm_data

    def get_ram(self, vir_connection, vm_ids):
        """Get the maximum RAM for a set of VM UUIDs.

        :param vir_connection: A libvirt connection object.
        :param vm_ids: A list of VM UUIDs.
        :return: The maximum RAM for the VM UUIDs.
        """
        vms_ram = {}
        for uuid in vm_ids:
            ram = self.get_max_ram(vir_connection, uuid)
            if ram:
                vms_ram[uuid] = ram

        return vms_ram

    def get_max_ram(self, vir_connection, uuid):
        """Get the max RAM allocated to a VM UUID using libvirt.

        :param vir_connection: A libvirt connection object.
        :param uuid: The UUID of a VM.
        :return: The maximum RAM of the VM in MB.
        """
        try:
            domain = vir_connection.lookupByUUIDString(uuid)
            return domain.maxMemory() / 1024
        except libvirt.libvirtError:
            return None

    def vm_mhz_to_percentage(self, vm_mhz_history, host_mhz_history,
                             physical_cpu_mhz):
        """Convert VM CPU utilization to the host's CPU utilization.

        :param vm_mhz_history: List of CPU utilization histories of VMs in MHz.
        :param host_mhz_history: A history if the CPU usage by the host in MHz.
        :param physical_cpu_mhz: Total frequency of the physical CPU in MHz.
        :return: The history of the host's CPU utilization in percentages.
        """
        max_len = max(len(x) for x in vm_mhz_history)
        if len(host_mhz_history) > max_len:
            host_mhz_history = host_mhz_history[-max_len:]
        mhz_history = [[0] * (max_len - len(x)) + x
                       for x in vm_mhz_history + [host_mhz_history]]
        return [float(sum(x)) / physical_cpu_mhz for x in zip(*mhz_history)]
