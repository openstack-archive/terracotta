# Copyright 2015 - Huawei Technologies Co. Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import sys

import eventlet

eventlet.monkey_patch(
    os=True,
    select=True,
    socket=True,
    thread=False if '--use-debugger' in sys.argv else True,
    time=True)

import os

POSSIBLE_TOPDIR = os.path.normpath(os.path.join(os.path.abspath(sys.argv[0]),
                                                os.pardir,
                                                os.pardir))
if os.path.exists(os.path.join(POSSIBLE_TOPDIR, 'terracotta', '__init__.py')):
    sys.path.insert(0, POSSIBLE_TOPDIR)

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging

from terracotta import config
from terracotta import rpc
from terracotta.locals import collector
from terracotta.locals import manager as local_mgr
from terracotta.globals import manager as global_mgr
from terracotta import version


LOG = logging.getLogger(__name__)


def launch_lm(transport):
    target = messaging.Target(
        topic=cfg.CONF.local_manager.topic,
        server=cfg.CONF.local_manager.host
    )

    local_manager = local_mgr.LocalManager()
    endpoints = [rpc.LocalManagerServer(local_manager)]

    server = messaging.get_rpc_server(
        transport,
        target,
        endpoints,
        executor='eventlet',
        serializer=ctx.RpcContextSerializer(ctx.JsonPayloadSerializer())
    )

    server.start()
    server.wait()


def launch_gm(transport):
    target = messaging.Target(
        topic=cfg.CONF.global_manager.topic,
        server=cfg.CONF.global_manager.host
    )

    global_manager = global_mgr.GlobalManager()
    endpoints = [rpc.GlobalManagerServer(global_manager)]

    server = messaging.get_rpc_server(
        transport,
        target,
        endpoints,
        executor='eventlet',
        serializer=ctx.RpcContextSerializer(ctx.JsonPayloadSerializer())
    )

    server.start()
    server.wait()


def launch_collector(transport):
    target = messaging.Target(
        topic=cfg.CONF.local_collector.topic,
        server=cfg.CONF.local_collector.host
    )

    global_manager = collector.Collector()
    endpoints = [rpc.GlobalManagerServer(global_manager)]

    server = messaging.get_rpc_server(
        transport,
        target,
        endpoints,
        executor='eventlet',
        serializer=ctx.RpcContextSerializer(ctx.JsonPayloadSerializer())
    )

    server.start()
    server.wait()


def launch_any(transport, options):
    threads = [eventlet.spawn(LAUNCH_OPTIONS[option], transport)
               for option in options]

    print('Server started.')

    [thread.wait() for thread in threads]


LAUNCH_OPTIONS = {
    'global-manager': launch_gm,
    'local-collector': launch_collector,
    'local-manager': launch_lm
}


TERRACOTTA_TITLE = """
##### ##### #####  #####  ##### ##### ##### ##### ##### #####
  #   #     #   #  #   #  #   # #     #   #   #     #   #   #
  #   ##### #####  #####  ##### #     #   #   #     #   #####
  #   #     #  #   #  #   #   # #     #   #   #     #   #   #
  #   #     #   #  #   #  #   # #     #   #   #     #   #   #
  #   ##### #    # #    # #   # ##### #####   #     #   #   #

Terracotta Dynamic Scheduling Service, version %s
""" % version.version_string()


def print_service_info():
    print(TERRACOTTA_TITLE)

    comp_str = ("[%s]" % ','.join(LAUNCH_OPTIONS)
                if cfg.CONF.server == ['all'] else cfg.CONF.server)

    print('Launching server components %s...' % comp_str)


def main():
    try:
        config.parse_args()
        print_service_info()
        logging.setup(cfg.CONF, 'Terracotta')
        transport = rpc.get_transport()

        launch_any(transport, set(cfg.CONF.server))

    except RuntimeError as excp:
        sys.stderr.write("ERROR: %s\n" % excp)
        sys.exit(1)


if __name__ == '__main__':
    main()
