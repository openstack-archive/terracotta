# Copyright (c) 2015 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
Defines interface for DB access.
"""

from oslo_config import cfg
from oslo_db import concurrency as db_concurrency


CONF = cfg.CONF

_BACKEND_MAPPING = {'sqlalchemy': 'terracotta.db.sqlalchemy.api'}

IMPL = db_concurrency.TpoolDbapiWrapper(CONF, backend_mapping=_BACKEND_MAPPING)


def select_cpu_mhz_for_vm(uuid, limit):
    """Select n last values of CPU MHz for a VM UUID.

    :param uuid: The UUID of a VM.
    :param limit: The number of last values to select.
    :return: The list of n last CPU Mhz values.
    """
    return IMPL.select_cpu_mhz_for_vm(uuid, limit)


def select_last_cpu_mhz_for_vms():
    """Select the last value of CPU MHz for all the VMs.

    :return: A dict of VM UUIDs to the last CPU MHz values.
    """
    return IMPL.select_last_cpu_mhz_for_vms()


def select_vm_id(uuid):
    """Select the ID of a VM by the VM UUID, or insert a new record.

    :param uuid: The UUID of a VM.
    :return: The ID of the VM.
    """
    return IMPL.select_vm_id(uuid)


def insert_vm_cpu_mhz(data):
    """Insert a set of CPU MHz values for a set of VMs.

    :param data: A dictionary of VM UUIDs and CPU MHz values.
    """
    return IMPL.insert_vm_cpu_mhz()


def update_host(hostname, cpu_mhz, cpu_cores, ram):
    """Insert new or update the corresponding host record.

    :param hostname: A host name.
    :param cpu_mhz: The total CPU frequency of the host in MHz.
    :param cpu_cores: The number of physical CPU cores.
    :param ram: The total amount of RAM of the host in MB.
    :return: The ID of the host.
    """
    return update_host(hostname, cpu_mhz, cpu_cores, ram)


def insert_host_cpu_mhz(hostname, cpu_mhz):
    """Insert a CPU MHz value for a host.

    :param hostname: A host name.
    :param cpu_mhz: The CPU usage of the host in MHz.
    """
    return IMPL.insert_host_cpu_mhz(hostname, cpu_mhz)


def select_cpu_mhz_for_host(hostname, limit):
    """Select n last values of CPU MHz for a host.

    :param hostname: A host name.
    :param limit: The number of last values to select.
    :return: The list of n last CPU Mhz values.
    """
    return IMPL.select_cpu_mhz_for_host(hostname, limit)


def select_last_cpu_mhz_for_hosts():
    """Select the last value of CPU MHz for all the hosts.

    :return: A dict of host names to the last CPU MHz values.
    """
    return IMPL.select_last_cpu_mhz_for_hosts()


def select_host_characteristics(self):
    """Select the characteristics of all the hosts.

    :return: Three dicts of hostnames to CPU MHz, cores, and RAM.
    """
    return IMPL.select_host_characteristics()


def select_host_id(hostname):
    """Select the ID of a host.

    :param hostname: A host name.
    :return: The ID of the host.
    """
    return IMPL.select_host_id(hostname)


def select_host_ids():
    """Select the IDs of all the hosts.

    :return: A dict of host names to IDs.
    """
    return IMPL.select_host_ids()


def cleanup_vm_resource_usage(datetime_threshold):
    """Delete VM resource usage data older than the threshold.

    :param datetime_threshold: A datetime threshold.
    """
    IMPL.cleanup_vm_resource_usage(datetime_threshold)


def cleanup_host_resource_usage(sdatetime_threshold):
    """Delete host resource usage data older than the threshold.

    :param datetime_threshold: A datetime threshold.
    """
    IMPL.cleanup_host_resource_usage()


def insert_host_states(hosts):
    """Insert host states for a set of hosts.

    :param hosts: A dict of hostnames to states (0, 1).
    """
    IMPL.insert_host_states(hosts)


def select_host_states():
    """Select the current states of all the hosts.

    :return: A dict of host names to states.
    """
    return IMPL.select_host_states()


def select_active_hosts():
    """Select the currently active hosts.

    :return: A list of host names.
    """
    return IMPL.select_active_hosts()


def select_inactive_hosts():
    """Select the currently inactive hosts.

    :return: A list of host names.
    """
    return IMPL.select_inactive_hosts()


def insert_host_overload(hostname, overload):
    """Insert whether a host is overloaded.

    :param hostname: A host name.
    :param overload: Whether the host is overloaded.
    """
    IMPL.insert_host_overload(hostname, overload)


def insert_vm_migration(vm, hostname):
    """Insert a VM migration.

    :param hostname: A VM UUID.
    :param hostname: A host name.
    """
    IMPL.insert_vm_migration(vm, hostname)
