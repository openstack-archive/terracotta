# Copyright 2015 Huawei Technologies Co. Ltd
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

"""
The functions from this module are shared by other components.
"""

import json
import numpy
import os
import re
import subprocess


def build_local_vm_path(local_data_directory):
    """Build the path to the local VM data directory.

    :param local_data_directory: The base local data path.
    :return: The path to the local VM data directory.
    """
    return os.path.join(local_data_directory, 'vms')


def build_local_host_path(local_data_directory):
    """Build the path to the local host data file.

    :param local_data_directory: The base local data path.
    :return: The path to the local host data file.
    """
    return os.path.join(local_data_directory, 'host')


def physical_cpu_count(vir_connection):
    """Get the number of physical CPUs using libvirt.

    :param vir_connection: A libvirt connection object.
    :return: The number of physical CPUs.
    """
    return vir_connection.getInfo()[2]


def physical_cpu_mhz(vir_connection):
    """Get the CPU frequency in MHz using libvirt.

    :param vir_connection: A libvirt connection object.
    :return: The CPU frequency in MHz.
    """
    return vir_connection.getInfo()[3]


def physical_cpu_mhz_total(vir_connection):
    """Get the sum of the core CPU frequencies in MHz using libvirt.

    :param vir_connection: A libvirt connection object.
    :return: The total CPU frequency in MHz.
    """
    return physical_cpu_count(vir_connection) * \
           physical_cpu_mhz(vir_connection)


def frange(start, end, step):
    """A range generator for floats.

    :param start: The starting value.
    :param end: The end value.
    :param step: The step.
    """
    while start <= end:
        yield start
        start += step


def call_function_by_name(name, args):
    """Call a function specified by a fully qualified name.

    :param name: A fully qualified name of a function.
    :param args: A list of positional arguments of the function.
    :return: The return value of the function call.
    """
    fragments = name.split('.')
    module = '.'.join(fragments[:-1])
    fromlist = fragments[-2]
    function = fragments[-1]
    m = __import__(module, fromlist=fromlist)
    return getattr(m, function)(*args)


def parse_parameters(params):
    """Parse algorithm parameters from the config file.

    :param params: JSON encoded parameters.
    :return: A dict of parameters.
    """
    return dict((str(k), v)
                for k, v in json.loads(params).items())


def parse_compute_hosts(compute_hosts):
    """Transform a coma-separated list of host names into a list.

    :param compute_hosts: A coma-separated list of host names.
    :return: A list of host names.
    """
    return filter(None, re.split('[^a-zA-Z0-9\-_]+', compute_hosts))


def calculate_migration_time(vms, bandwidth):
    """Calculate the mean migration time from VM RAM usage data.

    :param vms: A map of VM UUIDs to the corresponding maximum RAM in MB.
    :param bandwidth: The network bandwidth in MB/s.
    :return: The mean VM migration time in seconds.
    """
    return float(numpy.mean(vms.values()) / bandwidth)


def execute_on_hosts(hosts, commands):
    """Execute Shell command on hosts over SSH.

    :param hosts: A list of host names.
    :param commands: A list of Shell commands.
    """
    commands_merged = ''
    for command in commands:
        commands_merged += 'echo $ ' + command + ';'
        commands_merged += command + ';'

    for host in hosts:
        print 'Host: ' + host
        print subprocess.Popen(
            'ssh ' + host + ' "' + commands_merged + '"',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True).communicate()[0]
