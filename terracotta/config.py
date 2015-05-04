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

api_opts = [
    cfg.StrOpt('host', default='0.0.0.0', help='Terracotta API server host'),
    cfg.IntOpt('port', default=9090, help='Terracotta API server port')
]

pecan_opts = [
    cfg.StrOpt('root', default='terracotta.api.controllers.root.RootController',
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

local_manager_opts = [
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the executor node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='executor',
               help='The message topic that the executor listens on.'),
    cfg.StrOpt('version', default='1.0',
               help='The version of the executor.')
]

collector_opts = [
    cfg.StrOpt('host', default='0.0.0.0',
               help='Name of the executor node. This can be an opaque '
                    'identifier. It is not necessarily a hostname, '
                    'FQDN, or IP address.'),
    cfg.StrOpt('topic', default='executor',
               help='The message topic that the executor listens on.'),
    cfg.StrOpt('version', default='1.0',
               help='The version of the executor.')
]

CONF = cfg.CONF

CONF.register_opts(pecan_opts, group='pecan')
CONF.register_opts(api_opts, group='api')
CONF.register_opts(global_manager_opts, group='global_manager')
CONF.register_opts(local_manager_opts, group='local_manager')
CONF.register_opts(collector_opts, group='collector')

CONF.register_cli_opt(use_debugger)
CONF.register_cli_opt(launch_opt)


def parse_args(args=None, usage=None, default_config_files=None):
    CONF(
        args=args,
        project='terracotta',
        version=version,
        usage=usage,
        default_config_files=default_config_files
    )
