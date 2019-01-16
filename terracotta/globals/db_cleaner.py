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

"""The database cleaner module.

The database cleaner periodically cleans up the data on resource usage
by VMs stored in the database. This is required to avoid excess growth
of the database size.
"""

import datetime

from oslo_log import log as logging

import terracotta.common as common
from terracotta.config import cfg
from terracotta.utils.db_utils import init_db

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def start():
    """Start the database cleaner loop.

    :return: The final state.
    """
    config = CONF

    common.init_logging(
        config['log_directory'],
        'db-cleaner.log',
        int(config['log_level']))

    interval = config['db_cleaner_interval']
    LOG.info('Starting the database cleaner, ' +
             'iterations every %s seconds', interval)
    return common.start(
        init_state,
        execute,
        config,
        int(interval))


def init_state(config):
    """Initialize a dict for storing the state of the database cleaner.

    :param config: A config dictionary.
    :return: A dictionary containing the initial state of the database cleaner.
    """
    return {
        'db': init_db(config['sql_connection']),
        'time_delta': datetime.timedelta(
            seconds=int(config['db_cleaner_interval']))}


def execute(config, state):
    """Execute an iteration of the database cleaner.

    :param config: A config dictionary.
    :param state: A state dictionary.
    :return: The updated state dictionary.
    """
    datetime_threshold = today() - state['time_delta']
    state['db'].cleanup_vm_resource_usage(datetime_threshold)
    state['db'].cleanup_host_resource_usage(datetime_threshold)
    LOG.info('Cleaned up data older than %s',
             datetime_threshold.strftime('%Y-%m-%d %H:%M:%S'))
    return state


def today():
    """Return the today's datetime.

    :return: A datetime object representing current date and time.
    """
    return datetime.datetime.today()
