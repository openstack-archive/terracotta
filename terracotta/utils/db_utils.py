# Copyright 2012 Anton Beloglazov
# Copyright 2015 - Huawei Technologies Co. Ltd
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

from sqlalchemy import *
from sqlalchemy.sql import func

from oslo_config import cfg
from oslo_log import log as logging

from terracotta import db_temp as database


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


def init_db():
    """ Initialize the database.

    :param sql_connection: A database connection URL.
    :return: The initialized database.
    """
    engine = create_engine(CONF.database.sql_connection)
    metadata = MetaData()
    metadata.bind = engine

    hosts = Table('hosts', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('hostname', String(255), nullable=False),
                  Column('cpu_mhz', Integer, nullable=False),
                  Column('cpu_cores', Integer, nullable=False),
                  Column('ram', Integer, nullable=False))

    host_resource_usage = \
        Table('host_resource_usage', metadata,
              Column('id', Integer, primary_key=True),
              Column('host_id', Integer, ForeignKey('hosts.id'),
                     nullable=False),
              Column('timestamp', DateTime, default=func.now()),
              Column('cpu_mhz', Integer, nullable=False))

    vms = Table('vms', metadata,
                Column('id', Integer, primary_key=True),
                Column('uuid', String(36), nullable=False))

    vm_resource_usage = \
        Table('vm_resource_usage', metadata,
              Column('id', Integer, primary_key=True),
              Column('vm_id', Integer, ForeignKey('vms.id'), nullable=False),
              Column('timestamp', DateTime, default=func.now()),
              Column('cpu_mhz', Integer, nullable=False))

    vm_migrations = \
        Table('vm_migrations', metadata,
              Column('id', Integer, primary_key=True),
              Column('vm_id', Integer, ForeignKey('vms.id'), nullable=False),
              Column('host_id', Integer, ForeignKey('hosts.id'),
                     nullable=False),
              Column('timestamp', DateTime, default=func.now()))

    host_states = \
        Table('host_states', metadata,
              Column('id', Integer, primary_key=True),
              Column('host_id', Integer, ForeignKey('hosts.id'),
                     nullable=False),
              Column('timestamp', DateTime, default=func.now()),
              Column('state', Integer, nullable=False))

    host_overload = \
        Table('host_overload', metadata,
              Column('id', Integer, primary_key=True),
              Column('host_id', Integer, ForeignKey('hosts.id'),
                     nullable=False),
              Column('timestamp', DateTime, default=func.now()),
              Column('overload', Integer, nullable=False))

    metadata.create_all()
    connection = engine.connect()
    db = database.Database(connection, hosts, host_resource_usage, vms,
                           vm_resource_usage, vm_migrations, host_states,
                           host_overload)

    LOG.debug('Initialized a DB connection to %s', CONF.database.sql_connection)
    return db
