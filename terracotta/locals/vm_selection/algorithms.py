# Copyright 2012 Anton Beloglazov
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

""" VM selection algorithms.
"""

from random import choice
import operator


def random_factory(time_step, migration_time, params):
    """ Creates the random VM selection algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the random VM selection algorithm.
    """
    return lambda vms_cpu, vms_ram, state=None: ([random(vms_cpu)], {})


def minimum_utilization_factory(time_step, migration_time, params):
    """ Creates the minimum utilization VM selection algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the minimum utilization VM selection.
    """
    return lambda vms_cpu, vms_ram, state=None: \
        ([minimum_utilization(vms_cpu)], {})


def minimum_migration_time_factory(time_step, migration_time, params):
    """ Creates the minimum migration time VM selection algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the minimum migration time VM selection.
    """
    return lambda vms_cpu, vms_ram, state=None: \
        ([minimum_migration_time(vms_ram)], {})


def minimum_migration_time_max_cpu_factory(time_step, migration_time, params):
    """ Creates the minimum migration time / max CPU usage VM selection algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the minimum migration time / max CPU VM selection.
    """
    return lambda vms_cpu, vms_ram, state=None: \
        ([minimum_migration_time_max_cpu(params['last_n'],
                                         vms_cpu,
                                         vms_ram)], {})


def minimum_migration_time(vms_ram):
    """ Selects the VM with the minimum RAM usage.

    :param vms_ram: A map of VM UUID and their RAM usage data.
    :return: A VM to migrate from the host.
    """
    min_index, min_value = min(enumerate(vms_ram.values()),
                               key=operator.itemgetter(1))
    return vms_ram.keys()[min_index]


def minimum_utilization(vms_cpu):
    """ Selects the VM with the minimum CPU utilization.

    :param vms_cpu: A map of VM UUID and their CPU utilization histories.
    :return: A VM to migrate from the host.
    """
    last_utilization = [x[-1] for x in vms_cpu.values()]
    min_index, min_value = min(enumerate(last_utilization),
                               key=operator.itemgetter(1))
    return vms_cpu.keys()[min_index]


def random(vms_cpu):
    """ Selects a random VM.

    :param vms_cpu: A map of VM UUID and their CPU utilization histories.
    :return: A VM to migrate from the host.
    """
    return choice(vms_cpu.keys())


def minimum_migration_time_max_cpu(last_n, vms_cpu, vms_ram):
    """ Selects the VM with the minimum RAM and maximum CPU usage.

    :param last_n: The number of last CPU utilization values to average.
    :param vms_cpu: A map of VM UUID and their CPU utilization histories.
    :param vms_ram: A map of VM UUID and their RAM usage data.
    :return: A VM to migrate from the host.
    """
    min_ram = min(vms_ram.values())
    max_cpu = 0
    selected_vm = None
    for vm, cpu in vms_cpu.items():
        if vms_ram[vm] > min_ram:
            continue
        vals = cpu[-last_n:]
        avg = float(sum(vals)) / len(vals)
        if max_cpu < avg:
            max_cpu = avg
            selected_vm = vm
    return selected_vm
