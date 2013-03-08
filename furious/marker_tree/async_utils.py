#
# Copyright 2013 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from furious.marker_tree.marker import Marker


def handle_async_done(async, start_time=None):
    """
    Args:
        an Async instance

    This will mark and async as done and will
    begin the process to see if all the other asyncs
    in it's context, if it has one, are done
    """
    if async.id:
        marker = Marker.get(async.id)
        if not marker:
            marker = Marker.from_async(async)
        if start_time:
            marker.start_time = start_time
        marker.done = True
        marker.result = async.result
        marker.update_done(persist_first=True)