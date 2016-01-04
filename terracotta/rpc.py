# Copyright 2015 Huawei Technologies Co. Ltd
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

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_messaging.rpc import client

from terracotta import exceptions as exc


LOG = logging.getLogger(__name__)


_TRANSPORT = None
_ENGINE_CLIENT = None
_EXECUTOR_CLIENT = None


def cleanup():
    """Intended to be used by tests to recreate all RPC related objects."""

    global _TRANSPORT
    global _ENGINE_CLIENT
    global _EXECUTOR_CLIENT

    _TRANSPORT = None
    _ENGINE_CLIENT = None
    _EXECUTOR_CLIENT = None


def get_transport():
    global _TRANSPORT

    if not _TRANSPORT:
        _TRANSPORT = messaging.get_transport(cfg.CONF)

    return _TRANSPORT


def get_engine_client():
    global _ENGINE_CLIENT

    if not _ENGINE_CLIENT:
        _ENGINE_CLIENT = EngineClient(get_transport())

    return _ENGINE_CLIENT


def get_executor_client():
    global _EXECUTOR_CLIENT

    if not _EXECUTOR_CLIENT:
        _EXECUTOR_CLIENT = ExecutorClient(get_transport())

    return _EXECUTOR_CLIENT


class GlobalManagerServer(object):
    """RPC Engine server."""

    def __init__(self, manager):
        self._manager = manager


def wrap_messaging_exception(method):
    """This decorator unwrap remote error in one of MistralException.

    oslo.messaging has different behavior on raising exceptions
    when fake or rabbit transports are used. In case of rabbit
    transport it raises wrapped RemoteError which forwards directly
    to API. Wrapped RemoteError contains one of MistralException raised
    remotely on Engine and for correct exception interpretation we
    need to unwrap and raise given exception and manually send it to
    API layer.
    """
    def decorator(*args, **kwargs):
        try:
            return method(*args, **kwargs)

        except client.RemoteError as e:
            exc_cls = getattr(exc, e.exc_type)
            raise exc_cls(e.value)

    return decorator


class EngineClient():
    """RPC Engine client."""

    def __init__(self, transport):
        """Constructs an RPC client for engine.

        :param transport: Messaging transport.
        """
        serializer = auth_ctx.RpcContextSerializer(
            auth_ctx.JsonPayloadSerializer())

        self._client = messaging.RPCClient(
            transport,
            messaging.Target(topic=cfg.CONF.engine.topic),
            serializer=serializer
        )


class LocalManagerServer(object):
    """RPC Executor server."""

    def __init__(self, manager):
        self._executor = manager


class ExecutorClient():
    """RPC Executor client."""

    def __init__(self, transport):
        """Constructs an RPC client for the Executor.

        :param transport: Messaging transport.
        :type transport: Transport.
        """
        serializer = auth_ctx.RpcContextSerializer(
            auth_ctx.JsonPayloadSerializer()
        )

        self.topic = cfg.CONF.executor.topic
        self._client = messaging.RPCClient(
            transport,
            messaging.Target(),
            serializer=serializer
        )
