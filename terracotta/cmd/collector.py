# Copyright 2016 - Huawei Technologies Co. Ltd
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


import eventlet
import os
from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
import sys
from terracotta import config
from terracotta.locals import collector
from terracotta.openstack.common import threadgroup
from terracotta import rpc
from terracotta import version

eventlet.monkey_patch(
    os=True,
    select=True,
    socket=True,
    thread=False if '--use-debugger' in sys.argv else True,
    time=True)


POSSIBLE_TOPDIR = os.path.normpath(os.path.join(os.path.abspath(sys.argv[0]),
                                                os.pardir,
                                                os.pardir))
if os.path.exists(os.path.join(POSSIBLE_TOPDIR, 'terracotta', '__init__.py')):
    sys.path.insert(0, POSSIBLE_TOPDIR)


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def launch_collector(transport):
    target = messaging.Target(
        topic=cfg.CONF.collector.topic,
        server=cfg.CONF.collector.host
    )

    launch_collector = collector.Collector()
    endpoints = [rpc.GlobalManagerServer(launch_collector)]

    tg = threadgroup.ThreadGroup()
    tg.add_dynamic_timer(
        launch_collector.run_periodic_tasks,
        initial_delay=None,
        periodic_interval_max=None,
        context=None
    )

    server = messaging.get_rpc_server(
        transport,
        target,
        endpoints,
        executor='eventlet'
    )

    server.start()
    server.wait()


def launch_any(transport, options):
    thread = eventlet.spawn(launch_collector, transport)

    print('Server started.')

    thread.wait()


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

    comp_str = ("collector"
                if cfg.CONF.server == ['all'] else cfg.CONF.server)

    print('Launching server components %s...' % comp_str)


def main():
    try:
        config.parse_args()
        print_service_info()
        logging.setup(CONF, 'Terracotta')
        transport = rpc.get_transport()

        launch_any(transport, set(cfg.CONF.server))

    except RuntimeError as excp:
        sys.stderr.write("ERROR: %s\n" % excp)
        sys.exit(1)


if __name__ == '__main__':
    main()
