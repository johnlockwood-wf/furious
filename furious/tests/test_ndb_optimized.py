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

import unittest
from google.appengine.ext import testbed


class TestNDBOptimizedDone(unittest.TestCase):
    def setUp(self):
        import os
        import uuid

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

        # Ensure each test looks like it is in a new request.
        os.environ['REQUEST_ID_HASH'] = uuid.uuid4().hex

    def test_none_done(self):
        from furious.extras.appengine.ndb_persistence import MarkerPersist
        from furious.extras.appengine.ndb_optimized import all_done
        for x in xrange(3):
            marker_persisted = MarkerPersist(id=str(x))
            marker_persisted.put()

        self.assertFalse(all_done(['0', '1', '2']))

    def test_all_done(self):
        from furious.extras.appengine.ndb_persistence import MarkerPersist
        from furious.extras.appengine.ndb_optimized import all_done
        for x in xrange(3):
            marker_persisted = MarkerPersist(id=str(x), done=True)
            marker_persisted.put()

        self.assertTrue(all_done(['0', '1', '2']))

    def tearDown(self):
        self.testbed.deactivate()
