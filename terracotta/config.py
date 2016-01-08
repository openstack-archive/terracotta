# Copyright 2015 - Huawei Technologies Co. Ltd
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


launch_opt = cfg.ListOpt(
    'server',
    default=['global-manager', 'local-manager', 'local-collector'],
    help='Specifies which terracotta server to start by the launch script. '
         'Valid options are all or any combination of '
         'global-manager, local-manager, and local-collector.'
)

default_opts = [
    cfg.StrOpt('global_manager_host', default='controller',
               help='The name of the host running the global manager'),
    cfg.IntOpt('global_manager_port', default=60080,
               help='The port of the REST web service exposed by the global '
                    'manager'),
    cfg.IntOpt('db_cleaner_interval', default=7200,
               help='The time interval between subsequent invocations of the '
                    'database'),
    cfg.StrOpt('os_admin_user', default='user',
               help='The admin user name for authentication '
                    'with Nova using Keystone'),
    cfg.StrOpt('os_admin_password', default='userpassword',
               help='The admin password for authentication '
                    'with Nova using Keystone'),
    cfg.StrOpt('local_data_directory', default='/var/lib/terracotta',
               help='The directory used by the data collector to store the '
                    'data on the resource usage by the VMs running on the '
                    'host'),
    cfg.FloatOpt('host_cpu_usable_by_vms', default=1.0,
                 help='The threshold on the overall (all cores) utilization '
                      'of the physical CPU of a host that can be allocated to '
                      'VMs.'),
    cfg.IntOpt('data_collector_data_length', default=100,
               help='The number of the latest data values stored locally '
                    'by the data collector and passed to the underload / '
                    'overload detection and VM placement algorithms'),
    cfg.FloatOpt('network_migration_bandwidth', default=10,
                 help='The network bandwidth in MB/s available for '
                      'VM migration'),
    cfg.IntOpt('data_collector_interval', default=300,
               help='The time interval between subsequent invocations '
                    'of the data collector in seconds')
]

api_opts = [
    cfg.StrOpt('host', default='0.0.0.0', help='Terracotta API server host'),
    cfg.IntOpt('port', default=9090, help='Terracotta API server port')
]

pecan_opts = [
    cfg.StrOpt('root', default='terracotta.api.'
                               'controllers.root.RootController',
               help='Pecan root controller'),
    cfg.ListOpt('modules', default=["terracotta.api"],
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

global_manager_opts = [
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the global_manager node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='global_manager',
               help='The message topic that the global_manager listens on.'),
    cfg.StrOpt('vm_instance_directory', default='/var/lib/nova/instances',
               help='The directory, where the VM instance data are stored'),
    cfg.StrOpt('os_admin_tenant_name', default='tenantname',
               help='The admin tenant name for authentication '
                    'with Nova using Keystone'),
    cfg.StrOpt('os_auth_url', default='http://controller:5000/v2.0/',
               help='The OpenStack authentication URL'),
    cfg.ListOpt('compute_hosts', default=['compute1', 'compute2', 'compute3'],
                help='A coma-separated list of compute host names'),
    cfg.BoolOpt('block_migration', default=True,
                help='Whether to use block migration'
                     '(includes disk migration)'),
    cfg.StrOpt('compute_user', default='terracotta',
               help='The user name for connecting to the compute hosts '
                    'to switch them into the sleep mode'),
    cfg.StrOpt('compute_password', default='terracottapassword',
               help='The password of the user account used for connecting '
                    'to the compute hosts to switch them into the sleep mode'),
    cfg.StrOpt('sleep_command', default='pm-suspend',
               help='A shell command used to switch a host into the sleep '
                    'mode, the compute_user must have permissions to execute '
                    'this command'),
    cfg.StrOpt('ether_wake_interface', default='eth0',
               help='The network interface to send a magic packet from '
                    'using ether-wake'),
    cfg.StrOpt('algorithm_vm_placement_factory',
               default='terracotta.globals.vm_placement.bin_packing.'
                       'best_fit_decreasing_factory',
               help='The fully qualified name of a Python factory function '
                    'that returns a function implementing a VM placement '
                    'algorithm'),
    cfg.DictOpt('algorithm_vm_placement_parameters',
                default={'cpu_threshold': 0.8,
                         'ram_threshold': 0.95,
                         'last_n_vm_cpu': 2},
                help='A JSON encoded parameters, which will be parsed and '
                     'passed to the specified VM placement algorithm factory')
]

local_manager_opts = [
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the local_manager node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='local_manager',
               help='The message topic that the local_manager listens on.'),
    cfg.IntOpt('local_manager_interval', default=300,
               help='The time interval between subsequent invocations '
                    'of the local manager in seconds'),
    cfg.StrOpt('algorithm_underload_detection_factory',
               default='terracotta.locals.underload.trivial.last_n_average '
                       'threshold_factory',
               help='The fully qualified name of a Python factory function '
                    'that returns a function implementing an underload '
                    'detection algorithm'),
    cfg.StrOpt('algorithm_underload_detection_parameters',
                default="{'threshold': 0.5, 'n': 2}",
                help='A JSON encoded parameters, which will be parsed and '
                     'passed to the specified underload detection algorithm '
                     'factory'),
    cfg.StrOpt('algorithm_overload_detection_factory',
               default='terracotta.locals.overload.mhod.core.mhod_factory',
               help='The fully qualified name of a Python factory function '
                    'that returns a function implementing an overload '
                    'detection algorithm'),
    cfg.StrOpt('algorithm_overload_detection_parameters',
                default="'state_config': [0.8],"
                         "'otf': 0.1,"
                         "'history_size': 500,"
                         "'window_sizes': [30, 40, 50, 60, 70, 80, 90, 100],"
                         "'bruteforce_step': 0.5,"
                         "'learning_steps': 10}",
                help='A JSON encoded parameters, which will be parsed and '
                     'passed to the specified overload detection algorithm '
                     'factory'),
    cfg.StrOpt('algorithm_vm_selection_factory',
               default='terracotta.locals.vm_selection.algorithms.'
                       'minimum_migration_time_max_cpu_factory',
               help='The fully qualified name of a Python factory function '
                    'that returns a function implementing a VM selection '
                    'algorithm'),
    cfg.StrOpt('algorithm_vm_selection_parameters',
                default={'last_n': 2},
                help='A JSON encoded parameters, which will be parsed and '
                     'passed to the specified VM selection algorithm factory')
]

collector_opts = [
    cfg.FloatOpt('host_cpu_overload_threshold', default=0.8,
                 help='The threshold on the overall (all cores) utilization '
                      'of the physical CPU of a host, above which the host '
                      'is considered to be overloaded.'
                      'This is used for logging host overloads into the'
                      ' database.'),
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the collector node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='collector',
               help='The message topic that the collector listens on.'),
]

database_opts = [
    cfg.StrOpt('sql_connection', default='mysql://terracotta:terracottapasswd@'
                                         'controller/terracotta',
               help='The host name and credentials for connecting '
                    'to the MySQL database specified in the format '
                    'supported by SQLAlchemy')
]

CONF = cfg.CONF

CONF.register_opts(pecan_opts, group='pecan')
CONF.register_opts(default_opts, group='DEFAULT')
CONF.register_opts(api_opts, group='api')
CONF.register_opts(global_manager_opts, group='global_manager')
CONF.register_opts(local_manager_opts, group='local_manager')
CONF.register_opts(collector_opts, group='collector')
CONF.register_opts(database_opts, group='database')

CONF.register_cli_opt(use_debugger)
CONF.register_cli_opt(launch_opt)


def parse_args(args=None, usage=None, default_config_files=None):
    logging.register_options(CONF)
    CONF(
        args=args,
        project='terracotta',
        version=version,
        usage=usage,
        default_config_files=default_config_files
    )
