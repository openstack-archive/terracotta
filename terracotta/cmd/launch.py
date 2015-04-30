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

# If ../mistral/__init__.py exists, add ../ to Python search path, so that
# it will override what happens to be installed in /usr/(local/)lib/python...
POSSIBLE_TOPDIR = os.path.normpath(os.path.join(os.path.abspath(sys.argv[0]),
                                   os.pardir,
                                   os.pardir))
if os.path.exists(os.path.join(POSSIBLE_TOPDIR, 'mistral', '__init__.py')):
    sys.path.insert(0, POSSIBLE_TOPDIR)

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from wsgiref import simple_server

from mistral.api import app
from terracotta import config
from terracotta import rpc
from terracotta.globals import manager as global_mgr

from mistral import context as ctx
from mistral.db.v2 import api as db_api
from mistral.engine import default_engine as def_eng
from mistral.engine import default_executor as def_executor
from mistral.engine import rpc
from mistral.services import scheduler
from mistral import version


LOG = logging.getLogger(__name__)


def launch_executor(transport):
    target = messaging.Target(
        topic=cfg.CONF.executor.topic,
        server=cfg.CONF.executor.host
    )

    executor_v2 = def_executor.DefaultExecutor(rpc.get_engine_client())

    endpoints = [rpc.ExecutorServer(executor_v2)]

    server = messaging.get_rpc_server(
        transport,
        target,
        endpoints,
        executor='eventlet',
        serializer=ctx.RpcContextSerializer(ctx.JsonPayloadSerializer())
    )

    server.start()
    server.wait()


def launch_engine(transport):
    target = messaging.Target(
        topic=cfg.CONF.engine.topic,
        server=cfg.CONF.engine.host
    )

    engine_v2 = def_eng.DefaultEngine(rpc.get_engine_client())
    endpoints = [rpc.EngineServer(engine_v2)]


    # Setup scheduler in engine.
    db_api.setup_db()
    scheduler.setup()

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

    engine_v2 = def_eng.DefaultEngine(rpc.get_engine_client())

    endpoints = [rpc.EngineServer(engine_v2)]

    # Setup scheduler in engine.
    db_api.setup_db()
    scheduler.setup()

    server = messaging.get_rpc_server(
        transport,
        target,
        endpoints,
        executor='eventlet',
        serializer=ctx.RpcContextSerializer(ctx.JsonPayloadSerializer())
    )

    server.start()
    server.wait()


def launch_api(transport):
    host = cfg.CONF.api.host
    port = cfg.CONF.api.port

    server = simple_server.make_server(
        host,
        port,
        app.setup_app()
    )

    LOG.info("Mistral API is serving on http://%s:%s (PID=%s)" %
             (host, port, os.getpid()))

    server.serve_forever()


def launch_any(transport, options):
    # Launch the servers on different threads.
    threads = [eventlet.spawn(LAUNCH_OPTIONS[option], transport)
               for option in options]

    print('Server started.')

    [thread.wait() for thread in threads]


LAUNCH_OPTIONS = {
    # 'api': launch_api,
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


def print_server_info():
    print(TERRACOTTA_TITLE)

    comp_str = ("[%s]" % ','.join(LAUNCH_OPTIONS)
                if cfg.CONF.server == ['all'] else cfg.CONF.server)

    print('Launching server components %s...' % comp_str)


def main():
    try:
        config.parse_args()
        print_server_info()
        logging.setup(cfg.CONF, 'Terracotta')
        transport = rpc.get_transport()

        # Validate launch option.
        if set(cfg.CONF.server) - set(LAUNCH_OPTIONS.keys()):
            raise Exception('Valid options are all or any combination of '
                            'api, engine, and executor.')

        # Launch distinct set of server(s).
        launch_any(transport, set(cfg.CONF.server))

    except RuntimeError as excp:
        sys.stderr.write("ERROR: %s\n" % excp)
        sys.exit(1)


if __name__ == '__main__':
    main()
