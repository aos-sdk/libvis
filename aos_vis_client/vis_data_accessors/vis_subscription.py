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

from time import time, sleep
from copy import copy

from aos_vis_client.vis_data_accessors import VISDataAccessor
from aos_vis_client.vis_data_accessors import VISBase, RequestInfo, VISClientNoValueException


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class VISDataSubscription(VISBase):
    """
    Provides access to VIS subscriptions
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Web socket. Should be initialized by VISClient
        self.ws = None

        self._value = None
        self._waiting_flag = False

        # Info related to CIS request.
        self._subscription_id = None
        self._request_id = None
        self._subscription_request = None

        # Initial subscription data access point
        self._initial_data = None

    @property
    def request_ids(self):
        result = []
        if self._subscription_id:
            result.append(self._subscription_id)
        for item in (self._subscription_request,):
            if item:
                result.append(item.id)
        if self._initial_data:
            result.extend(self._initial_data.request_ids)
        return result

    def get_value(self, wait_timeout=None):
        """
        Returns VIS value.
        Will wait for initial data or data update is `wait_timeout` is setted.
        Will raise `VISClientNoValueException` if there is no value and `wait_timeout` is None.
        """
        self._waiting_flag = True
        if wait_timeout:
            start_time = time()
            while not self._initial_data.is_data_received or self._waiting_flag:
                sleep(self.WAITING_INTERVAL)
                if wait_timeout and start_time + wait_timeout < time():
                    raise TimeoutError()
        else:
            if not self._initial_data.is_data_received:
                raise VISClientNoValueException()

        return copy(self._value)

    def send_subscribe_action(self):
        """
        Initialize and send 'get' action for initial data and 'subscribe' action.
        """
        if self._value is None:
            self._initial_data = VISDataAccessor(path=self.path)
            self._initial_data.ws = self.ws
            self._initial_data.send_get_action()

        if self._subscription_id is None:
            if self._subscription_request and self._subscription_request.timeout < time():
                logger.error(
                    "We did not get subscription respond from VIS for {path} path in {seconds} seconds, "
                    "requesting it again.".format(path=self.path, seconds=self.TIMEOUT_LIMIT)
                )
                self._subscription_request = None
            else:
                logger.debug("Waiting for VIS response for {} path.".format(self.path))

            if self._subscription_request is None:
                request_id = self._send_subscribe_action()
                self._subscription_request = RequestInfo(id=request_id, timeout=time() + self.TIMEOUT_LIMIT)

    def _handle_initial_data(self, data):
        """
        Handle message with initial data.
        """
        logger.info("Received initial data for {} path.".format(self.path))
        self._initial_data.process(data=data)
        try:
            self._value = self._initial_data.get_value()
        except VISClientNoValueException:
            logger.info("No initial value yet.")

    def _handle_subscription_id(self, data):
        """
        Handle message with new subscription id.
        """
        self._subscription_id = data[self.VIS_SUBSCRIPTION_ID]
        self._request_id = data[self.VIS_REQUEST_ID]
        logger.info("Received subscription id {sid} for path: {path}.".format(
            sid=self._subscription_id,
            path=self.path
        ))

    def _handle_subscription_message(self, data):
        """
        Handle  message with updated data.
        """
        subscription_id = data.get(self.VIS_SUBSCRIPTION_ID)

        if subscription_id != self._subscription_id:
            logger.error("Subscription id does not belong to this Subscription, expected {ex}, got {got}.".format(
                ex=self._subscription_id,
                got=subscription_id
            ))
            return

        logger.info("Received subscription data update for path {}.".format(self.path))
        if self._initial_data.is_data_received:
            try:
                if isinstance(data[self.VIS_VALUE], dict) and isinstance(self._value, dict):
                    self._value.update(data[self.VIS_VALUE])
                else:
                    self._value = data[self.VIS_VALUE]
            except KeyError:
                logger.error("No value payload in subscription update.")
        else:
            logger.debug("Initial value was not received, getting it.")
            try:
                self._value = self._initial_data.get_value()
            except VISClientNoValueException:
                logger.info("No initial value for {path} yet.".format(path=self.path))
        self._waiting_flag = False

    def process(self, data):
        request_id = data.get(self.VIS_REQUEST_ID)
        subscription_id = data.get(self.VIS_SUBSCRIPTION_ID)

        if request_id is not None:
            # Handle initial data or subscription id
            if self._subscription_request and self._subscription_request.id == request_id:
                self._handle_subscription_id(data)
            elif request_id in self._initial_data.request_ids:
                self._handle_initial_data(data)
            else:
                logger.error("Message does not belong to this subscription: {}".format(str(data)))

        elif subscription_id is not None:
            # Handle message with new data for subscription
            self._handle_subscription_message(data)

        else:
            logger.error("Data structure does not have '{r}' nor '{s}' fields.".format(
                r=self.VIS_REQUEST_ID,
                s=self.VIS_SUBSCRIPTION_ID
            ))

    def unsubscribe(self):
        """
        Send 'unsubscribe' message.
        """
        self._send_unsubscribe_action(self._subscription_id, self._request_id)
