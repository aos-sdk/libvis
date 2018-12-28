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

from time import sleep, time
from copy import copy

from aos_vis_client.vis_data_accessors import VISBase, RequestInfo, VISClientNoValueException

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class VISDataAccessor(VISBase):
    """
    Provide functionality for reading/writing data from/to VIS.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ws = None
        self.is_data_received = False

        self._value = None
        self._get_request_info = None
        self._set_request_info = None

    @property
    def request_ids(self):
        requests_info = (self._get_request_info, self._set_request_info, )
        return [item.id for item in requests_info if item]

    def get_value(self, wait_timeout=None):
        """
        Returns VIS value.
        Will raise `VISClientNoValueException` if there is no value and `wait_timeout` is None.
        """
        if wait_timeout:
            start_time = time()
            while not self.is_data_received:
                sleep(self.WAITING_INTERVAL)
                if wait_timeout and start_time + wait_timeout < time():
                    raise TimeoutError()
        else:
            if not self.is_data_received:
                raise VISClientNoValueException()

        return copy(self._value)

    def send_get_action(self):
        """
        Initialize and send 'get' action to VIS.
        """
        if self._get_request_info and self._get_request_info.timeout < time():
            logger.error(
                "Response was not received from VIS for {path} path in {seconds} seconds, requesting data again."
                    .format(path=self.path, seconds=self.TIMEOUT_LIMIT)
            )
        else:
            logger.debug("Waiting for VIS response for {} path.".format(self.path))

        if self._get_request_info is None:
            request_id = self._send_get_action()
            self._get_request_info = RequestInfo(id=request_id, timeout=time() + self.TIMEOUT_LIMIT)

    def send_set_action(self, value):
        """
        Initialize and send 'set' message to VIS.
        """
        if self._set_request_info and self._set_request_info.timeout < time():
            logger.error(
                "Set respond was not received from VIS for setting {path} path in {seconds} seconds.".format(
                    path=self.path, seconds=self.TIMEOUT_LIMIT
                )
            )
        else:
            logger.debug("VIS did not respond for the last set operation for {} path yet.".format(self.path))

        if self._set_request_info is None:
            request_id = self._send_set_action(value)
            self._set_request_info = RequestInfo(id=request_id, timeout=time() + self.TIMEOUT_LIMIT)

    def process(self, data):
        """
        Handle response to request message.
        """
        request_id = data.get(self.VIS_REQUEST_ID)

        if not request_id:
            logger.error("Data structure does not have {} field.".format(self.VIS_REQUEST_ID))
            return

        if request_id == self._get_request_info.id:
            logger.info("Received value for path {}".format(self.path))
            self._value = data.get(self.VIS_VALUE)
            self.is_data_received = True
        elif request_id == self._set_request_info.id:
            logger.info("Received VIS confirmation to data set for path {}".format(self.path))
        else:
            logger.error("Received message with unexpected request id {mid}, expected: {sid}".format(
                mid=request_id,
                sid=str(self.request_ids)
            ))
