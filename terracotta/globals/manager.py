# Copyright 2012 Anton Beloglazov
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

"""The main global manager module.

The global manager is deployed on the management host and is
responsible for making VM placement decisions and initiating VM
migrations. It exposes a REST web service, which accepts requests from
local managers. The global manager processes only one type of requests
-- reallocation of a set of VM instances. Once a request is received,
the global manager invokes a VM placement algorithm to determine
destination hosts to migrate the VMs to. Once a VM placement is
determined, the global manager submits a request to the Nova API to
migrate the VMs. The global manager is also responsible for switching
idle hosts to the sleep mode, as well as re-activating hosts when
necessary.

The global manager is agnostic of a particular implementation of the
VM placement algorithm in use. The VM placement algorithm to use can
be specified in the configuration file using the
`algorithm_vm_placement_factory` option. A VM placement algorithm can
call the Nova API to obtain the information about host characteristics
and current VM placement. If necessary, it can also query the central
database to obtain the historical information about the resource usage
by the VMs.

The global manager component provides a REST web service implemented
using the Bottle framework. The authentication is done using the admin
credentials specified in the configuration file. Upon receiving a
request from a local manager, the following steps will be performed:

1. Parse the `vm_uuids` parameter and transform it into a list of
   UUIDs of the VMs to migrate.

2. Call the Nova API to obtain the current placement of VMs on the
   hosts.

3. Call the function specified in the `algorithm_vm_placement_factory`
   configuration option and pass the UUIDs of the VMs to migrate and
   the current VM placement as arguments.

4. Call the Nova API to migrate the VMs according to the placement
   determined by the `algorithm_vm_placement_factory` algorithm.

When a host needs to be switched to the sleep mode, the global manager
will use the account credentials from the `compute_user` and
`compute_password` configuration options to open an SSH connection
with the target host and then invoke the command specified in the
`sleep_command`, which defaults to `pm-suspend`.

When a host needs to be re-activated from the sleep mode, the global
manager will leverage the Wake-on-LAN technology and send a magic
packet to the target host using the `ether-wake` program and passing
the corresponding MAC address as an argument. The mapping between the
IP addresses of the hosts and their MAC addresses is initialized in
the beginning of the global manager's execution.
"""

from hashlib import sha1
import platform
import subprocess
import time

import novaclient
from novaclient.v2 import client
from oslo_config import cfg
from oslo_log import log as logging

from terracotta import common
from terracotta.utils import db_utils

dist = platform.linux_distribution(full_distribution_name=0)[0]
if dist in ['redhat', 'centos']:
    etherwake = 'ether-wake'
else:
    etherwake = 'etherwake'


global_mgr_ops = [
    cfg.StrOpt('os_admin_user',
               default='admin',
               help='The admin user name for authentication '
                    'with Nova using Keystone.'),
    cfg.StrOpt('os_admin_password',
               default='admin',
               help='The admin user password for authentication '
                    'with Nova using Keystone.'),
]


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONF.register_opts(global_mgr_ops)


def host_mac(host):
    """Get mac address of a host.

    :param host: A host name.
    :return: The mac address of the host.
    """
    mac = subprocess.Popen(
        ("ping -c 1 {0} > /dev/null;" +
         "arp -a {0} | awk '{{print $4}}'").format(host),
        stdout=subprocess.PIPE,
        shell=True).communicate()[0].strip()
    if len(mac) != 17:
        LOG.warning('Received a wrong mac address for %s: %s',
                    host, mac)
        return ''
    return mac


def flavors_ram(nova):
    """Get a dict of flavor IDs to the RAM limits.

    :param nova: A Nova client.
    :return: A dict of flavor IDs to the RAM limits.
    """
    return dict((str(fl.id), fl.ram) for fl in nova.flavors.list())


def vms_ram_limit(nova, vms):
    """Get the RAM limit from the flavors of the VMs.

    :param nova: A Nova client.
    :param vms: A list of VM UUIDs.
    :return: A dict of VM UUIDs to the RAM limits.
    """
    flavors_to_ram = flavors_ram(nova)
    vms_ram = {}
    for uuid in vms:
        try:
            vm = nova.servers.get(uuid)
            vms_ram[uuid] = flavors_to_ram[vm.flavor['id']]
        except novaclient.exceptions.NotFound:
            pass
    return vms_ram


def host_used_ram(nova, host):
    """Get the used RAM of the host using the Nova API.

    :param nova: A Nova client.
    :param host: A host name.
    :return: The used RAM of the host.
    """
    data = nova.hosts.get(host)
    if len(data) > 2 and data[2].memory_mb != 0:
        return data[2].memory_mb
    return data[1].memory_mb


def vms_by_hosts(nova, hosts):
    """Get a map of host names to VMs using the Nova API.

    :param nova: A Nova client.
    :param hosts: A list of host names.
    :return: A dict of host names to lists of VM UUIDs.
    """
    result = dict((host, []) for host in hosts)
    for vm in nova.servers.list():
        result[vm_hostname(vm)].append(str(vm.id))
    return result


def vms_by_host(nova, host):
    """Get VMs from the specified host using the Nova API.

    :param nova: A Nova client.
    :param host: A host name.
    :return: A list of VM UUIDs from the specified host.
    """
    return [str(vm.id) for vm in nova.servers.list()
            if (vm_hostname(vm) == host and str(getattr(
                vm, 'OS-EXT-STS:vm_state')) == 'active')]


def vm_hostname(vm):
    """Get the name of the host where VM is running.

    :param vm: A Nova VM object.
    :return: The hostname.
    """
    return str(vm.get('OS-EXT-SRV-ATTR:host'))


def migrate_vms(db, nova, vm_instance_directory, placement, block_migration):
    """Synchronously live migrate a set of VMs.

    :param db: The database object.
    :param nova: A Nova client.
    :param vm_instance_directory: The VM instance directory.
    :param placement: A dict of VM UUIDs to host names.
    :param block_migration: Whether to use block migration.
    """
    retry_placement = {}
    vms = placement.keys()
    # Migrate only 2 VMs at a time, as otherwise migrations may fail
    # vm_pairs = [vms[x:x + 2] for x in xrange(0, len(vms), 2)]
    # Temporary migrates VMs one by one
    vm_pairs = [vms[x:x + 1] for x in xrange(0, len(vms), 1)]
    for vm_pair in vm_pairs:
        start_time = time.time()
        for vm_uuid in vm_pair:
            migrate_vm(nova, vm_instance_directory, vm_uuid,
                       placement[vm_uuid], block_migration)

        time.sleep(10)

        while True:
            for vm_uuid in list(vm_pair):
                vm = nova.servers.get(vm_uuid)
                LOG.debug('VM %s: %s, %s',
                          vm_uuid,
                          vm_hostname(vm),
                          vm.status)
                if vm_hostname(vm) == placement[vm_uuid] and \
                                vm.status == u'ACTIVE':
                    vm_pair.remove(vm_uuid)
                    db.insert_vm_migration(vm_uuid, placement[vm_uuid])
                    LOG.info('Completed migration of VM %s to %s',
                             vm_uuid, placement[vm_uuid])
                elif time.time() - start_time > 300 and vm_hostname(
                        vm) != placement[vm_uuid] and vm.status == u'ACTIVE':
                    vm_pair.remove(vm_uuid)
                    retry_placement[vm_uuid] = placement[vm_uuid]
                    LOG.warning('Time-out for migration of VM %s to %s, ' +
                                'will retry', vm_uuid, placement[vm_uuid])
                else:
                    break
            else:
                break
            time.sleep(3)

    if retry_placement:
        LOG.info('Retrying the following migrations: %s',
                 str(retry_placement))
        migrate_vms(db, nova, vm_instance_directory,
                    retry_placement, block_migration)


def migrate_vm(nova, vm_instance_directory, vm, host, block_migration):
    """Live migrate a VM.

    :param nova: A Nova client.
    :param vm_instance_directory: The VM instance directory.
    :param vm: The UUID of a VM to migrate.
    :param host: The name of the destination host.
    :param block_migration: Whether to use block migration.
    """
    # To avoid problems with migration, need the following:
    subprocess.call('chown -R nova:nova ' + vm_instance_directory,
                    shell=True)
    nova.servers.live_migrate(vm, host, block_migration, False)
    LOG.info('Started migration of VM %s to %s', vm, host)


def switch_hosts_off(db, sleep_command, hosts):
    """Switch hosts to a low-power mode.

    :param db: The database object.
    :param sleep_command: A Shell command to switch off a host.
    :param hosts: A list of hosts to switch off.
    """
    if sleep_command:
        for host in hosts:
            command = 'ssh {0} "{1}"'.format(host, sleep_command)
            LOG.debug('Calling: %s', command)
            subprocess.call(command, shell=True)
    LOG.info('Switched off hosts: %s', str(hosts))
    db.insert_host_states(dict((x, 0) for x in hosts))


class GlobalManager(object):
    def __init__(self, *args, **kwargs):
        self.state = self.init_state()
        self.switch_hosts_on(self.state['compute_hosts'])

    def init_state(self):
        """Initialize a dict for storing the state of the global manager.
        """
        return {'previous_time': 0,
                'db': db_utils.init_db(),
                'nova': client.Client(2,
                                      CONF.os_admin_user,
                                      CONF.os_admin_password,
                                      CONF.global_manager.os_admin_tenant_name,
                                      CONF.global_manager.os_auth_url,
                                      service_type="compute"),
                'hashed_username': sha1(CONF.os_admin_user).hexdigest(),
                'hashed_password': sha1(CONF.os_admin_password).hexdigest(),
                'compute_hosts': CONF.global_manager.compute_hosts,
                'host_macs': {}}

    def switch_hosts_on(self, hosts):
        """Switch hosts to the active mode.
        """
        for host in hosts:
            if host not in self.state['host_macs']:
                self.state['host_macs'][host] = host_mac(host)

            command = '{0} -i {1} {2}'.format(
                etherwake,
                CONF.global_manager.ether_wake_interface,
                self.state['host_macs'][host])

            LOG.debug('Calling: %s', command)
            subprocess.call(command, shell=True)

        LOG.info('Switched on hosts: %s', str(hosts))
        self.state['db'].insert_host_states(
            dict((x, 1) for x in hosts))

    def execute_underload(self, host):
        """Process an underloaded host: migrate all VMs from the host.

        1. Prepare the data about the current states of the hosts and VMs.

        2. Call the function specified in the `algorithm_vm_placement_factory`
           configuration option and pass the data on the states of the hosts
           and VMs.

        3. Call the Nova API to migrate the VMs according to the placement
           determined by the `algorithm_vm_placement_factory` algorithm.

        4. Switch off the host at the end of the VM migration.

        :param host: A host name.
        :return: The updated state dictionary.
        """
        LOG.info('Started processing an underload request')
        underloaded_host = host
        hosts_cpu_total, _, hosts_ram_total = self.state[
            'db'].select_host_characteristics()

        hosts_to_vms = vms_by_hosts(self.state['nova'],
                                    self.state['compute_hosts'])
        vms_last_cpu = self.state['db'].select_last_cpu_mhz_for_vms()
        hosts_last_cpu = self.state['db'].select_last_cpu_mhz_for_hosts()

        # Remove VMs from hosts_to_vms that are not in vms_last_cpu
        # These VMs are new and no data have been collected from them
        for host, vms in hosts_to_vms.items():
            for i, vm in enumerate(vms):
                if vm not in vms_last_cpu:
                    del hosts_to_vms[host][i]

        LOG.debug('hosts_to_vms: %s', str(hosts_to_vms))

        hosts_cpu_usage = {}
        hosts_ram_usage = {}
        hosts_to_keep_active = set()
        for host, vms in hosts_to_vms.items():
            if vms:
                host_cpu_mhz = hosts_last_cpu[host]
                for vm in vms:
                    if vm not in vms_last_cpu:
                        LOG.info('No data yet for VM: %s - skipping host %s',
                                 vm,
                                 host)
                        hosts_to_keep_active.add(host)
                        hosts_cpu_total.pop(host, None)
                        hosts_ram_total.pop(host, None)
                        hosts_cpu_usage.pop(host, None)
                        hosts_ram_usage.pop(host, None)
                        break
                    host_cpu_mhz += vms_last_cpu[vm]
                else:
                    hosts_cpu_usage[host] = host_cpu_mhz
                    hosts_ram_usage[host] = host_used_ram(
                        self.state['nova'], host)
            else:
                # Exclude inactive hosts
                hosts_cpu_total.pop(host, None)
                hosts_ram_total.pop(host, None)

        LOG.debug('Host CPU usage: %s', str(hosts_last_cpu))
        LOG.debug('Host total CPU usage: %s', str(hosts_cpu_usage))

        # Exclude the underloaded host
        hosts_cpu_usage.pop(underloaded_host, None)
        hosts_cpu_total.pop(underloaded_host, None)
        hosts_ram_usage.pop(underloaded_host, None)
        hosts_ram_total.pop(underloaded_host, None)

        LOG.debug('Excluded the underloaded host %s', underloaded_host)
        LOG.debug('Host CPU usage: %s', str(hosts_last_cpu))
        LOG.debug('Host total CPU usage: %s', str(hosts_cpu_usage))

        vms_to_migrate = vms_by_host(self.state['nova'], underloaded_host)
        vms_cpu = {}
        for vm in vms_to_migrate:
            if vm not in vms_last_cpu:
                LOG.info('No data yet for VM: %s - dropping the request',
                         vm)
                LOG.info('Skipped an underload request')
                return self.state
            vms_cpu[vm] = self.state['db'].select_cpu_mhz_for_vm(
                vm,
                CONF.data_collector_data_length)
        vms_ram = vms_ram_limit(self.state['nova'], vms_to_migrate)

        # Remove VMs that are not in vms_ram
        # These instances might have been deleted
        for i, vm in enumerate(vms_to_migrate):
            if vm not in vms_ram:
                del vms_to_migrate[i]

        if not vms_to_migrate:
            LOG.info('No VMs to migrate - completed the underload request')
            return self.state

        for vm in vms_cpu.keys():
            if vm not in vms_ram:
                del vms_cpu[vm]

        time_step = CONF.data_collector_interval
        migration_time = common.calculate_migration_time(
            vms_ram,
            CONF.network_migration_bandwidth)

        if 'vm_placement' not in self.state:
            vm_placement_params = common.parse_parameters(
                CONF.global_manager.algorithm_vm_placement_parameters)
            vm_placement_state = None
            vm_placement = common.call_function_by_name(
                CONF.global_manager.algorithm_vm_placement_factory,
                [time_step,
                 migration_time,
                 vm_placement_params])
            self.state['vm_placement'] = vm_placement
            self.state['vm_placement_state'] = {}
        else:
            vm_placement = self.state['vm_placement']
            vm_placement_state = self.state['vm_placement_state']

        LOG.info('Started underload VM placement')
        placement, vm_placement_state = vm_placement(
            hosts_cpu_usage, hosts_cpu_total,
            hosts_ram_usage, hosts_ram_total,
            {}, {},
            vms_cpu, vms_ram,
            vm_placement_state)
        LOG.info('Completed underload VM placement')
        self.state['vm_placement_state'] = vm_placement_state

        LOG.info('Underload: obtained a new placement %s', str(placement))

        active_hosts = hosts_cpu_total.keys()
        inactive_hosts = set(self.state['compute_hosts']) - set(active_hosts)
        prev_inactive_hosts = set(self.state['db'].select_inactive_hosts())
        hosts_to_deactivate = list(
            inactive_hosts - prev_inactive_hosts - hosts_to_keep_active)

        if not placement:
            LOG.info('Nothing to migrate')
            if underloaded_host in hosts_to_deactivate:
                hosts_to_deactivate.remove(underloaded_host)
        else:
            LOG.info('Started underload VM migrations')
            migrate_vms(self.state['db'],
                        self.state['nova'],
                        CONF.global_manager.vm_instance_directory,
                        placement,
                        CONF.global_manager.block_migration)
            LOG.info('Completed underload VM migrations')

        if hosts_to_deactivate:
            switch_hosts_off(self.state['db'],
                             CONF.global_manager.sleep_command,
                             hosts_to_deactivate)

        LOG.info('Completed processing an underload request')
        return self.state

    def execute_overload(self, host, vm_uuids):
        """Process an overloaded host: migrate the selected VMs from it.

        1. Prepare the data about the current states of the hosts and VMs.

        2. Call the function specified in the `algorithm_vm_placement_factory`
           configuration option and pass the data on the states of the hosts
           and VMs.

        3. Call the Nova API to migrate the VMs according to the placement
           determined by the `algorithm_vm_placement_factory` algorithm.

        4. Switch on the inactive hosts required to accommodate the VMs.

        """
        LOG.info('Started processing an overload request')
        overloaded_host = host
        hosts_cpu_total, _, hosts_ram_total = self.state[
            'db'].select_host_characteristics()
        hosts_to_vms = vms_by_hosts(self.state['nova'],
                                    self.state['compute_hosts'])
        vms_last_cpu = self.state['db'].select_last_cpu_mhz_for_vms()
        hosts_last_cpu = self.state['db'].select_last_cpu_mhz_for_hosts()

        # Remove VMs from hosts_to_vms that are not in vms_last_cpu
        # These VMs are new and no data have been collected from them
        for host, vms in hosts_to_vms.items():
            for i, vm in enumerate(vms):
                if vm not in vms_last_cpu:
                    del hosts_to_vms[host][i]

        hosts_cpu_usage = {}
        hosts_ram_usage = {}
        inactive_hosts_cpu = {}
        inactive_hosts_ram = {}
        for host, vms in hosts_to_vms.items():
            if vms:
                host_cpu_mhz = hosts_last_cpu[host]
                for vm in vms:
                    if vm not in vms_last_cpu:
                        LOG.info(
                            'No data yet for VM: %s - skipping host %s',
                            vm, host)
                        hosts_cpu_total.pop(host, None)
                        hosts_ram_total.pop(host, None)
                        hosts_cpu_usage.pop(host, None)
                        hosts_ram_usage.pop(host, None)
                        break
                    host_cpu_mhz += vms_last_cpu[vm]
                else:
                    hosts_cpu_usage[host] = host_cpu_mhz
                    hosts_ram_usage[host] = host_used_ram(self.state['nova'],
                                                          host)
            else:
                inactive_hosts_cpu[host] = hosts_cpu_total[host]
                inactive_hosts_ram[host] = hosts_ram_total[host]
                hosts_cpu_total.pop(host, None)
                hosts_ram_total.pop(host, None)

        # Exclude the overloaded host
        hosts_cpu_usage.pop(overloaded_host, None)
        hosts_cpu_total.pop(overloaded_host, None)
        hosts_ram_usage.pop(overloaded_host, None)
        hosts_ram_total.pop(overloaded_host, None)

        LOG.debug('Host CPU usage: %s', str(hosts_last_cpu))
        LOG.debug('Host total CPU usage: %s', str(hosts_cpu_usage))

        vms_to_migrate = vm_uuids
        vms_cpu = {}
        for vm in vms_to_migrate:
            if vm not in vms_last_cpu:
                LOG.info(
                    'No data yet for VM: %s - dropping the request',
                    vm)
                LOG.info('Skipped an underload request')
                return self.state
            vms_cpu[vm] = self.state['db'].select_cpu_mhz_for_vm(
                vm,
                CONF.data_collector_data_length)
        vms_ram = vms_ram_limit(self.state['nova'], vms_to_migrate)

        # Remove VMs that are not in vms_ram
        # These instances might have been deleted
        for i, vm in enumerate(vms_to_migrate):
            if vm not in vms_ram:
                del vms_to_migrate[i]

        if not vms_to_migrate:
            LOG.info(
                'No VMs to migrate - completed the overload request')
            return self.state

        for vm in vms_cpu.keys():
            if vm not in vms_ram:
                del vms_cpu[vm]

        time_step = CONF.data_collector_interval
        migration_time = common.calculate_migration_time(
            vms_ram,
            CONF.network_migration_bandwidth)

        if 'vm_placement' not in self.state:
            vm_placement_params = common.parse_parameters(
                CONF.global_manager.algorithm_vm_placement_parameters)
            vm_placement_state = None
            vm_placement = common.call_function_by_name(
                CONF.global_manager.algorithm_vm_placement_factory,
                [time_step,
                 migration_time,
                 vm_placement_params])
            self.state['vm_placement'] = vm_placement
            self.state['vm_placement_state'] = {}
        else:
            vm_placement = self.state['vm_placement']
            vm_placement_state = self.state['vm_placement_state']

        LOG.info('Started overload VM placement')
        placement, vm_placement_state = vm_placement(
            hosts_cpu_usage, hosts_cpu_total,
            hosts_ram_usage, hosts_ram_total,
            inactive_hosts_cpu, inactive_hosts_ram,
            vms_cpu, vms_ram,
            vm_placement_state)
        LOG.info('Completed overload VM placement')
        self.state['vm_placement_state'] = vm_placement_state

        LOG.info('Overload: obtained a new placement %s', str(placement))

        if not placement:
            LOG.info('Nothing to migrate')
        else:
            hosts_to_activate = list(
                set(inactive_hosts_cpu.keys()).intersection(
                    set(placement.values())))
            if hosts_to_activate:
                self.switch_hosts_on(hosts_to_activate)
            LOG.info('Started overload VM migrations')
            migrate_vms(self.state['db'],
                        self.state['nova'],
                        CONF.global_manager.vm_instance_directory,
                        placement,
                        CONF.global_manager.block_migration)
            LOG.info('Completed overload VM migrations')
        LOG.info('Completed processing an overload request')
        return self.state

    def service(self, reason, host, vm_uuids):
        try:
            if reason == 0:
                LOG.info('Processing an underload of a host %s', host)
                self.execute_underload(host)
            else:
                LOG.info('Processing an overload, VMs: %s', str(vm_uuids))
                self.execute_overload(host, vm_uuids)
        except Exception:
            LOG.exception('Exception during request processing:')
            raise
