# Copyright 2012 Anton Beloglazov
# Copyright 2015 Huawei Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Bin Packing based VM placement algorithms.
"""

from oslo_log import log as logging


LOG = logging.getLogger(__name__)


def best_fit_decreasing_factory(time_step, migration_time, params):
    """ Creates the Best Fit Decreasing (BFD) heuristic for VM placement.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the BFD algorithm.
    """
    return lambda hosts_cpu_usage, hosts_cpu_total, \
                  hosts_ram_usage, hosts_ram_total, \
                  inactive_hosts_cpu, inactive_hosts_ram, \
                  vms_cpu, vms_ram, state=None: \
        (best_fit_decreasing(
            params['last_n_vm_cpu'],
            get_available_resources(
                    params['cpu_threshold'],
                    hosts_cpu_usage,
                    hosts_cpu_total),
            get_available_resources(
                    params['ram_threshold'],
                    hosts_ram_usage,
                    hosts_ram_total),
            inactive_hosts_cpu,
            inactive_hosts_ram,
            vms_cpu,
            vms_ram),
         {})


def get_available_resources(threshold, usage, total):
    """ Get a map of the available resource capacity.

    :param threshold: A threshold on the maximum allowed resource usage.
    :param usage: A map of hosts to the resource usage.
    :param total: A map of hosts to the total resource capacity.
    :return: A map of hosts to the available resource capacity.
    """
    return dict((host, int(threshold * total[host] - resource))
                for host, resource in usage.items())


def best_fit_decreasing(last_n_vm_cpu, hosts_cpu, hosts_ram,
                        inactive_hosts_cpu, inactive_hosts_ram,
                        vms_cpu, vms_ram):
    """ The Best Fit Decreasing (BFD) heuristic for placing VMs on hosts.

    :param last_n_vm_cpu: The last n VM CPU usage values to average.
    :param hosts_cpu: A map of host names and their available CPU in MHz.
    :param hosts_ram: A map of host names and their available RAM in MB.
    :param inactive_hosts_cpu: A map of inactive hosts and available CPU MHz.
    :param inactive_hosts_ram: A map of inactive hosts and available RAM MB.
    :param vms_cpu: A map of VM UUID and their CPU utilization in MHz.
    :param vms_ram: A map of VM UUID and their RAM usage in MB.
    :return: A map of VM UUIDs to host names, or {} if cannot be solved.
    """
    LOG.debug('last_n_vm_cpu: %s', str(last_n_vm_cpu))
    LOG.debug('hosts_cpu: %s', str(hosts_cpu))
    LOG.debug('hosts_ram: %s', str(hosts_ram))
    LOG.debug('inactive_hosts_cpu: %s', str(inactive_hosts_cpu))
    LOG.debug('inactive_hosts_ram: %s', str(inactive_hosts_ram))
    LOG.debug('vms_cpu: %s', str(vms_cpu))
    LOG.debug('vms_ram: %s', str(vms_ram))
    vms_tmp = []
    for vm, cpu in vms_cpu.items():
        if cpu:
            last_n_cpu = cpu[-last_n_vm_cpu:]
            vms_tmp.append((sum(last_n_cpu) / len(last_n_cpu),
                            vms_ram[vm],
                            vm))
        else:
            LOG.warning('No CPU data for VM: %s - skipping', vm)

    vms = sorted(vms_tmp, reverse=True)
    hosts = sorted(((v, hosts_ram[k], k)
                    for k, v in hosts_cpu.items()))
    inactive_hosts = sorted(((v, inactive_hosts_ram[k], k)
                             for k, v in inactive_hosts_cpu.items()))
    mapping = {}
    for vm_cpu, vm_ram, vm_uuid in vms:
        mapped = False
        while not mapped:
            for _, _, host in hosts:
                if hosts_cpu[host] >= vm_cpu and \
                    hosts_ram[host] >= vm_ram:
                        mapping[vm_uuid] = host
                        hosts_cpu[host] -= vm_cpu
                        hosts_ram[host] -= vm_ram
                        mapped = True
                        break
            else:
                if inactive_hosts:
                    activated_host = inactive_hosts.pop(0)
                    hosts.append(activated_host)
                    hosts = sorted(hosts)
                    hosts_cpu[activated_host[2]] = activated_host[0]
                    hosts_ram[activated_host[2]] = activated_host[1]
                else:
                    break

    if len(vms) == len(mapping):
        return mapping
    return {}
