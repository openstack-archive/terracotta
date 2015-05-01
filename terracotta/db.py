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

import datetime
from sqlalchemy import *
from sqlalchemy.engine.base import Connection

from oslo_log import log as logging


LOG = logging.getLogger(__name__)


class Database(object):
    """ A class representing the database, where fields are tables.
    """

    def __init__(self, connection, hosts, host_resource_usage, vms,
                 vm_resource_usage, vm_migrations, host_states, host_overload):
        """ Initialize the database.

        :param connection: A database connection table.
        :param hosts: The hosts table.
        :param host_resource_usage: The host_resource_usage table.
        :param vms: The vms table.
        :param vm_resource_usage: The vm_resource_usage table.
        :param vm_migrations: The vm_migrations table.
        :param host_states: The host_states table.
        :param host_overload: The host_overload table.
        """
        self.connection = connection
        self.hosts = hosts
        self.host_resource_usage = host_resource_usage
        self.vms = vms
        self.vm_resource_usage = vm_resource_usage
        self.vm_migrations = vm_migrations
        self.host_states = host_states
        self.host_overload = host_overload
        LOG.debug('Instantiated a Database object')

    def select_cpu_mhz_for_vm(self, uuid, n):
        """ Select n last values of CPU MHz for a VM UUID.

        :param uuid: The UUID of a VM.
        :param n: The number of last values to select.
        :return: The list of n last CPU Mhz values.
        """
        sel = select([self.vm_resource_usage.c.cpu_mhz]). \
            where(and_(
            self.vms.c.id == self.vm_resource_usage.c.vm_id,
            self.vms.c.uuid == uuid)). \
            order_by(self.vm_resource_usage.c.id.desc()). \
            limit(n)
        res = self.connection.execute(sel).fetchall()
        return list(reversed([int(x[0]) for x in res]))

    def select_last_cpu_mhz_for_vms(self):
        """ Select the last value of CPU MHz for all the VMs.

        :return: A dict of VM UUIDs to the last CPU MHz values.
        """
        vru1 = self.vm_resource_usage
        vru2 = self.vm_resource_usage.alias()
        sel = select([vru1.c.vm_id, vru1.c.cpu_mhz], from_obj=[
            vru1.outerjoin(vru2, and_(
                vru1.c.vm_id == vru2.c.vm_id,
                vru1.c.id < vru2.c.id))]). \
            where(vru2.c.id == None)
        vms_cpu_mhz = dict(self.connection.execute(sel).fetchall())
        vms_uuids = dict(self.vms.select().execute().fetchall())

        vms_last_mhz = {}
        for id, uuid in vms_uuids.items():
            if id in vms_cpu_mhz:
                vms_last_mhz[str(uuid)] = int(vms_cpu_mhz[id])
            else:
                vms_last_mhz[str(uuid)] = 0
        return vms_last_mhz

    def select_vm_id(self, uuid):
        """ Select the ID of a VM by the VM UUID, or insert a new record.

        :param uuid: The UUID of a VM.
        :return: The ID of the VM.
        """
        sel = select([self.vms.c.id]).where(self.vms.c.uuid == uuid)
        row = self.connection.execute(sel).fetchone()
        if row is None:
            id = self.vms.insert().execute(uuid=uuid).inserted_primary_key[0]
            LOG.info('Created a new DB record for a VM %s, id=%d', uuid, id)
            return int(id)
        else:
            return int(row['id'])

    def insert_vm_cpu_mhz(self, data):
        """ Insert a set of CPU MHz values for a set of VMs.

        :param data: A dictionary of VM UUIDs and CPU MHz values.
        """
        if data:
            query = []
            for uuid, cpu_mhz in data.items():
                vm_id = self.select_vm_id(uuid)
                query.append({'vm_id': vm_id,
                              'cpu_mhz': cpu_mhz})
            self.vm_resource_usage.insert().execute(query)

    def update_host(self, hostname, cpu_mhz, cpu_cores, ram):
        """ Insert new or update the corresponding host record.

        :param hostname: A host name.
        :param cpu_mhz: The total CPU frequency of the host in MHz.
        :param cpu_cores: The number of physical CPU cores.
        :param ram: The total amount of RAM of the host in MB.
        :return: The ID of the host.
        """
        sel = select([self.hosts.c.id]). \
            where(self.hosts.c.hostname == hostname)
        row = self.connection.execute(sel).fetchone()
        if row is None:
            id = self.hosts.insert().execute(
                hostname=hostname,
                cpu_mhz=cpu_mhz,
                cpu_cores=cpu_cores,
                ram=ram).inserted_primary_key[0]
            LOG.info('Created a new DB record for a host %s, id=%d',
                     hostname, id)
            return int(id)
        else:
            self.connection.execute(self.hosts.update().
                                    where(self.hosts.c.id == row['id']).
                                    values(cpu_mhz=cpu_mhz,
                                           cpu_cores=cpu_cores,
                                           ram=ram))
            return int(row['id'])

    def insert_host_cpu_mhz(self, hostname, cpu_mhz):
        """ Insert a CPU MHz value for a host.

        :param hostname: A host name.
        :param cpu_mhz: The CPU usage of the host in MHz.
        """
        self.host_resource_usage.insert().execute(
            host_id=self.select_host_id(hostname),
            cpu_mhz=cpu_mhz)

    def select_cpu_mhz_for_host(self, hostname, n):
        """ Select n last values of CPU MHz for a host.

        :param hostname: A host name.
        :param n: The number of last values to select.
        :return: The list of n last CPU Mhz values.
        """
        sel = select([self.host_resource_usage.c.cpu_mhz]). \
            where(and_(
            self.hosts.c.id == self.host_resource_usage.c.host_id,
            self.hosts.c.hostname == hostname)). \
            order_by(self.host_resource_usage.c.id.desc()). \
            limit(n)
        res = self.connection.execute(sel).fetchall()
        return list(reversed([int(x[0]) for x in res]))

    def select_last_cpu_mhz_for_hosts(self):
        """ Select the last value of CPU MHz for all the hosts.

        :return: A dict of host names to the last CPU MHz values.
        """
        hru1 = self.host_resource_usage
        hru2 = self.host_resource_usage.alias()
        sel = select([hru1.c.host_id, hru1.c.cpu_mhz], from_obj=[
            hru1.outerjoin(hru2, and_(
                hru1.c.host_id == hru2.c.host_id,
                hru1.c.id < hru2.c.id))]). \
            where(hru2.c.id == None)
        hosts_cpu_mhz = dict(self.connection.execute(sel).fetchall())

        sel = select([self.hosts.c.id, self.hosts.c.hostname])
        hosts_names = dict(self.connection.execute(sel).fetchall())

        hosts_last_mhz = {}
        for id, hostname in hosts_names.items():
            if id in hosts_cpu_mhz:
                hosts_last_mhz[str(hostname)] = int(hosts_cpu_mhz[id])
            else:
                hosts_last_mhz[str(hostname)] = 0
        return hosts_last_mhz

    def select_host_characteristics(self):
        """ Select the characteristics of all the hosts.

        :return: Three dicts of hostnames to CPU MHz, cores, and RAM.
        """
        hosts_cpu_mhz = {}
        hosts_cpu_cores = {}
        hosts_ram = {}
        for x in self.hosts.select().execute().fetchall():
            hostname = str(x[1])
            hosts_cpu_mhz[hostname] = int(x[2])
            hosts_cpu_cores[hostname] = int(x[3])
            hosts_ram[hostname] = int(x[4])
        return hosts_cpu_mhz, hosts_cpu_cores, hosts_ram

    def select_host_id(self, hostname):
        """ Select the ID of a host.

        :param hostname: A host name.
        :return: The ID of the host.
        """
        sel = select([self.hosts.c.id]). \
            where(self.hosts.c.hostname == hostname)
        row = self.connection.execute(sel).fetchone()
        if not row:
            raise LookupError('No host found for hostname: %s', hostname)
        return int(row['id'])

    def select_host_ids(self):
        """ Select the IDs of all the hosts.

        :return: A dict of host names to IDs.
        """
        return dict((str(x[1]), int(x[0]))
                    for x in self.hosts.select().execute().fetchall())

    def cleanup_vm_resource_usage(self, datetime_threshold):
        """ Delete VM resource usage data older than the threshold.

        :param datetime_threshold: A datetime threshold.
        """
        self.connection.execute(
            self.vm_resource_usage.delete().where(
                self.vm_resource_usage.c.timestamp < datetime_threshold))

    def cleanup_host_resource_usage(self, datetime_threshold):
        """ Delete host resource usage data older than the threshold.

        :param datetime_threshold: A datetime threshold.
        """
        self.connection.execute(
            self.host_resource_usage.delete().where(
                self.host_resource_usage.c.timestamp < datetime_threshold))

    def insert_host_states(self, hosts):
        """ Insert host states for a set of hosts.

        :param hosts: A dict of hostnames to states (0, 1).
        """
        host_ids = self.select_host_ids()
        to_insert = [{'host_id': host_ids[k],
                      'state': v}
                     for k, v in hosts.items()]
        self.connection.execute(
            self.host_states.insert(), to_insert)

    def select_host_states(self):
        """ Select the current states of all the hosts.

        :return: A dict of host names to states.
        """
        hs1 = self.host_states
        hs2 = self.host_states.alias()
        sel = select([hs1.c.host_id, hs1.c.state], from_obj=[
            hs1.outerjoin(hs2, and_(
                hs1.c.host_id == hs2.c.host_id,
                hs1.c.id < hs2.c.id))]). \
            where(hs2.c.id == None)
        data = dict(self.connection.execute(sel).fetchall())
        host_ids = self.select_host_ids()
        host_states = {}
        for host, id in host_ids.items():
            if id in data:
                host_states[str(host)] = int(data[id])
            else:
                host_states[str(host)] = 1
        return host_states

    def select_active_hosts(self):
        """ Select the currently active hosts.

        :return: A list of host names.
        """
        return [host
                for host, state in self.select_host_states().items()
                if state == 1]

    def select_inactive_hosts(self):
        """ Select the currently inactive hosts.

        :return: A list of host names.
        """
        return [host
                for host, state in self.select_host_states().items()
                if state == 0]

    def insert_host_overload(self, hostname, overload):
        """ Insert whether a host is overloaded.

        :param hostname: A host name.
        :param overload: Whether the host is overloaded.
        """
        self.host_overload.insert().execute(
            host_id=self.select_host_id(hostname),
            overload=int(overload))

    def insert_vm_migration(self, vm, hostname):
        """ Insert a VM migration.

        :param hostname: A VM UUID.
        :param hostname: A host name.
        """
        self.vm_migrations.insert().execute(
            vm_id=self.select_vm_id(vm),
            host_id=self.select_host_id(hostname))
