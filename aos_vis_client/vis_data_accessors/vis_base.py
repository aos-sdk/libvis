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

import logging
import json
from collections import namedtuple
from abc import ABCMeta, abstractmethod
from uuid import uuid4

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

RequestInfo = namedtuple("RequestInfo", ["id", "timeout"])


class VISClientBaseException(Exception):
    pass


class VISClientNoValueException(VISClientBaseException):
    pass


class VISDataNotRegistered(VISClientBaseException):
    pass


class VISBase(metaclass=ABCMeta):
    """
    Base class for all VIS data classes
    """
    VIS_VALUE = "value"
    VIS_REQUEST_ID = "requestId"
    VIS_SUBSCRIPTION_ID = "subscriptionId"

    WAITING_INTERVAL = 0.1
    TIMEOUT_LIMIT = 5

    def __init__(self, path):
        self.ws = None
        self._path = path

    def _send_set_action(self, value):
        logger.debug("Setting '{path}' path with '{value}' value.".format(path=self.path, value=value))
        if not self.ws:
            raise VISDataNotRegistered("VISData object not registered in client.")

        request_id = str(uuid4())
        self.ws.send(json.dumps({
            "action": "set",
            "path": self.path,
            "value": value,
            "requestId": request_id
        }))
        return request_id

    def _send_get_action(self):
        logger.debug("Request data get for '{path}' path.".format(path=self.path))
        if not self.ws:
            raise VISDataNotRegistered("VISData object not registered in client.")

        request_id = str(uuid4())
        self.ws.send(json.dumps({
            "action": "get",
            "requestId": request_id,
            "path": self.path
        }))
        return request_id

    def _send_subscribe_action(self):
        logger.debug("Subscribing to '{path}' path.".format(path=self.path))
        if not self.ws:
            raise VISDataNotRegistered("VISData object not registered in client.")

        request_id = str(uuid4())
        self.ws.send(json.dumps({
            "action": "subscribe",
            "requestId": request_id,
            "path": self.path
        }))
        return request_id

    def _send_unsubscribe_action(self, subscription_id, request_id):
        logging.debug("Sending unsubscribe action for  subscription id: '{sub_id}' and request id: '{req_id}'".format(
            sub_id=subscription_id,
            req_id=request_id,
        ))
        if not self.ws:
            raise VISDataNotRegistered("VISData object not registered in client.")

        self.ws.send(json.dumps({
            "action": "unsubscribe",
            "subscriptionId": subscription_id,
            "requestId": request_id,
        }))
        return request_id

    @property
    def path(self):
        return self._path

    @abstractmethod
    def process(self, data):
        """
        Should contains logic for processing message from VIS.
        """
        pass

    @property
    @abstractmethod
    def request_ids(self):
        """
        Should returns request ids related to this data accessor object.
        """
        pass

    @abstractmethod
    def get_value(self):
        pass
