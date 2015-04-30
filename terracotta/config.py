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

"""
Configuration options registration and useful routines.
"""

from oslo_config import cfg
from oslo_log import log as logging

from terracotta import version

from contracts import contract
import os
import ConfigParser


log = logging.getLogger(__name__)


# This is the default config, which should not be modified
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__),
                                   '..',
                                   'neat.conf')

# This is the custom config, which may override the defaults
CONFIG_PATH = "/etc/neat/neat.conf"
# The following value is used for testing purposes
#CONFIG_PATH = os.path.join(os.path.dirname(__file__),
#                           '..',
#                           'neat.conf')

# These fields must present in the configuration file
REQUIRED_FIELDS = [
    'log_directory',
    'log_level',
    'vm_instance_directory',
    'sql_connection',
    'os_admin_tenant_name',
    'os_admin_user',
    'os_admin_password',
    'os_auth_url',
    'compute_hosts',
    'global_manager_host',
    'global_manager_port',
    'db_cleaner_interval',
    'local_data_directory',
    'local_manager_interval',
    'data_collector_interval',
    'data_collector_data_length',
    'host_cpu_overload_threshold',
    'host_cpu_usable_by_vms',
    'compute_user',
    'compute_password',
    'sleep_command',
    'ether_wake_interface',
    'block_migration',
    'network_migration_bandwidth',
    'algorithm_underload_detection_factory',
    'algorithm_underload_detection_parameters',
    'algorithm_overload_detection_factory',
    'algorithm_overload_detection_parameters',
    'algorithm_vm_selection_factory',
    'algorithm_vm_selection_parameters',
    'algorithm_vm_placement_factory',
    'algorithm_vm_placement_parameters',
]


@contract
def read_config(paths):
    """ Read the configuration files and return the options.

    :param paths: A list of required configuration file paths.
     :type paths: list(str)

    :return: A dictionary of the configuration options.
     :rtype: dict(str: str)
    """
    configParser = ConfigParser.ConfigParser()
    for path in paths:
        configParser.read(path)
    return dict(configParser.items("DEFAULT"))


@contract
def validate_config(config, required_fields):
    """ Check that the config contains all the required fields.

    :param config: A config dictionary to check.
     :type config: dict(str: str)

    :param required_fields: A list of required fields.
     :type required_fields: list(str)

    :return: Whether the config is valid.
     :rtype: bool
    """
    for field in required_fields:
        if not field in config:
            return False
    return True


@contract
def read_and_validate_config(paths, required_fields):
    """ Read the configuration files, validate and return the options.

    :param paths: A list of required configuration file paths.
     :type paths: list(str)

    :param required_fields: A list of required fields.
     :type required_fields: list(str)

    :return: A dictionary of the configuration options.
     :rtype: dict(str: str)
    """
    config = read_config(paths)
    if not validate_config(config, required_fields):
        message = 'The config dictionary does not contain ' + \
                  'all the required fields'
        log.critical(message)
        raise KeyError(message)
    return config


launch_opt = cfg.ListOpt(
    'server',
    default=['all'],
    help='Specifies which mistral server to start by the launch script. '
         'Valid options are all or any combination of '
         'api, engine, and executor.'
)

api_opts = [
    cfg.StrOpt('host', default='0.0.0.0', help='Mistral API server host'),
    cfg.IntOpt('port', default=8989, help='Mistral API server port')
]

pecan_opts = [
    cfg.StrOpt('root', default='mistral.api.controllers.root.RootController',
               help='Pecan root controller'),
    cfg.ListOpt('modules', default=["mistral.api"],
                help='A list of modules where pecan will search for '
                     'applications.'),
    cfg.BoolOpt('debug', default=False,
                help='Enables the ability to display tracebacks in the '
                     'browser and interactively debug during '
                     'development.'),
    cfg.BoolOpt('auth_enable', default=True,
                help='Enables user authentication in pecan.')
]

use_debugger = cfg.BoolOpt(
    "use-debugger",
    default=False,
    help='Enables debugger. Note that using this option changes how the '
    'eventlet library is used to support async IO. This could result '
    'in failures that do not occur under normal operation. '
    'Use at your own risk.'
)

engine_opts = [
    cfg.StrOpt('engine', default='default',
               help='Mistral engine plugin'),
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the engine node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='engine',
               help='The message topic that the engine listens on.'),
    cfg.StrOpt('version', default='1.0',
               help='The version of the engine.')
]

executor_opts = [
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the executor node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='executor',
               help='The message topic that the executor listens on.'),
    cfg.StrOpt('version', default='1.0',
               help='The version of the executor.')
]

wf_trace_log_name_opt = cfg.StrOpt(
    'workflow_trace_log_name',
    default='workflow_trace',
    help='Logger name for pretty '
    'workflow trace output.'
)

CONF = cfg.CONF

CONF.register_opts(api_opts, group='api')
CONF.register_opts(engine_opts, group='engine')
CONF.register_opts(pecan_opts, group='pecan')
CONF.register_opts(executor_opts, group='executor')
CONF.register_opt(wf_trace_log_name_opt)

CONF.register_cli_opt(use_debugger)
CONF.register_cli_opt(launch_opt)

CONF.import_opt('verbose', 'mistral.openstack.common.log')
CONF.set_default('verbose', True)
CONF.import_opt('debug', 'mistral.openstack.common.log')
CONF.import_opt('log_dir', 'mistral.openstack.common.log')
CONF.import_opt('log_file', 'mistral.openstack.common.log')
CONF.import_opt('log_config_append', 'mistral.openstack.common.log')
CONF.import_opt('log_format', 'mistral.openstack.common.log')
CONF.import_opt('log_date_format', 'mistral.openstack.common.log')
CONF.import_opt('use_syslog', 'mistral.openstack.common.log')
CONF.import_opt('syslog_log_facility', 'mistral.openstack.common.log')

# Extend oslo default_log_levels to include some that are useful for mistral
# some are in oslo logging already, this is just making sure it stays this
# way.
default_log_levels = cfg.CONF.default_log_levels

logs_to_quieten = [
    'sqlalchemy=WARN',
    'oslo.messaging=INFO',
    'iso8601=WARN',
    'eventlet.wsgi.server=WARN',
    'stevedore=INFO',
    'mistral.openstack.common.loopingcall=INFO',
    'mistral.openstack.common.periodic_task=INFO',
    'mistral.services.periodic=INFO'
]

for chatty in logs_to_quieten:
    if chatty not in default_log_levels:
        default_log_levels.append(chatty)

cfg.set_defaults(
    log.log_opts,
    default_log_levels=default_log_levels
)


def parse_args(args=None, usage=None, default_config_files=None):
    CONF(
        args=args,
        project='terracotta',
        version=version,
        usage=usage,
        default_config_files=default_config_files
    )
