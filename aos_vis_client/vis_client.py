# Copyright (c) 2018 EPAM Systems
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

import ssl
import time
import logging
import weakref
import json
from threading import Thread, Lock

from websocket import create_connection, WebSocketTimeoutException

from aos_vis_client.vis_data_accessors import VISDataAccessor, VISDataSubscription, VISBase


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class VISClient(Thread):
    """
    Vehicle Information Service client.
    """
    _SOCKET_TIMEOUT = 0.1
    _SLEEP_INTERVAL = 0.1

    def __init__(self, vis_server_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__vis_server_url = vis_server_url

        self._ws = None
        self._data_accessors = []
        self._lock = Lock()
        self._process_flag = True

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            logger.error("Unhandled exception: {exc}".format(exc=exc_val))
        self.stop()
        self.join()
        return True

    def _connect_ws(self):
        """
        Create web socket connection.
        """
        self._ws = create_connection(
            self.__vis_server_url,
            timeout=self._SOCKET_TIMEOUT,
            sslopt={"cert_reqs": ssl.CERT_NONE}
        )

    def _disconnect_ws(self):
        """
        Disconnect web socket.
        """
        try:
            if self._ws:
                self._ws.close()
        except Exception as exc:
            logger.error("Exception occur on closing connection: %s", str(exc))
        finally:
            self._ws = None

    def _clear_deleted_accessor_refs(self):
        """
        Delete all empty references from data accessors list.
        """
        self._data_accessors = [accessor_ref for accessor_ref in self._data_accessors if accessor_ref()]

    def _handle_vis_messages(self):
        """
        Receive message from web-socket and try to handle it.
        """
        try:
            vis_data = self._ws.recv()
            if not vis_data:
                logger.info("Received empty message.")
                return
            logger.info("Received message: {message}".format(message=vis_data))
            vis_data = json.loads(vis_data)

            request_id = vis_data.get(VISDataAccessor.VIS_REQUEST_ID)
            subscription_id = vis_data.get(VISDataAccessor.VIS_SUBSCRIPTION_ID)
            message_id = request_id or subscription_id

            data_accessor = None
            flag_clean_data_accessors = False
            for accessor_ref in self._data_accessors:
                accessor = accessor_ref()

                if not accessor:
                    flag_clean_data_accessors = True
                elif message_id in accessor.request_ids:
                    data_accessor = accessor
                    break

            if flag_clean_data_accessors:
                logger.debug("Cleaning data accessors references")
                self._clear_deleted_accessor_refs()

            if not data_accessor:
                logger.warning("Received unexpected message: '%s'.", str(vis_data))
                return

            data_accessor.process(data=vis_data)

        except WebSocketTimeoutException:
            logger.debug("Web socket timeout error. No message received in {interval} interval.".format(
                interval=self._SOCKET_TIMEOUT)
            )
            return

        except json.JSONDecodeError:
            logger.error("Unable to decode JSON data")

    def run(self):
        """
        Listen web-socket.
        """
        while True:
            with self._lock:
                if not self._process_flag:
                    break

                try:
                    if not self._ws:
                        logger.warning("Web-socket disconnected. Trying to connect to '{url}'".format(
                            url=self.__vis_server_url)
                        )
                        self._connect_ws()
                    self._handle_vis_messages()
                except Exception as exc:
                    logger.error("Received unexpected exception: %s", str(exc))
                    self._disconnect_ws()
            time.sleep(self._SLEEP_INTERVAL)

    def _unsubscribe_all(self):
        """
        Send 'unsubscribe' message for all existing subscriptions.
        """
        for data_accessor in self._data_accessors:
            if issubclass(type(data_accessor), VISDataSubscription):
                data_accessor.unsubscribe()

    def start(self):
        """
        Initialize web-socket and run thread to listen it.
        """
        logger.info("Starting up VIS client.")
        if not self._ws:
            self._connect_ws()
        super().start()

    def stop(self):
        """
        Stop VIS client. Send 'unsubscribe' messages for all subscriptions and clear resources.
        """
        logger.info("Shutting down VIS client.")
        with self._lock:
            self._process_flag = False

            if self._ws:
                self._unsubscribe_all()
                self._disconnect_ws()

    def register_vis_data(self, vis_data):
        """
        Register data accessor and set up a web socket for it.
        """
        if not isinstance(vis_data, VISBase):
            raise TypeError("'vis_data' should be an instance of VISBase or VISBase inheritor.")

        if vis_data in [accessor_ref() for accessor_ref in self._data_accessors]:
            raise ValueError("vis data '{vis_data}' already registered.".format(vis_data=vis_data))

        self._data_accessors.append(weakref.ref(vis_data))
        vis_data.ws = self._ws


__all__ = [VISClient, ]
