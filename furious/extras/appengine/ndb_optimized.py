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

import logging
from google.appengine.api import memcache
from google.appengine.ext.ndb import Future
from google.appengine.ext import ndb
logger = logging.getLogger('marker_tree')


def filter_known_done(ids):
    known_done = memcache.get_multi(
        ["done{0}".format(idx) for idx in ids],
        namespace="__furious")
    not_known_done = filter(lambda idx: "done{0}".format(idx)
                            not in known_done, ids)
    if len(not_known_done) < len(ids):
        logger.debug("saved {0} db api calls with memcache".format(
            len(ids) - len(not_known_done)
        ))
    return not_known_done


def all_done(ids):
    """Asynchronously request all the entities by this key.
    Wait until the first one that is returned that is not
    done or until all are returned and done.
    """
    # Filter out known done ids
    ids = filter_known_done(ids)
    # Make list of keys
    keys = [ndb.Key('MarkerPersist', idx) for idx in ids]
    futures = ndb.get_multi_async(keys)
    while futures:
        Future.wait_any(futures)
        futures_not_done = []
        for future in futures:
            if future.done():
                # The api task is done, now what is the result?
                result = future.get_result()
                if not result:
                    # No entity was stored, so at least one is not done.
                    return False
                elif not result.done:
                    # An entity exists, but it's not done.
                    return False
            else:
                futures_not_done.append(future)

        futures = futures_not_done

    return True
