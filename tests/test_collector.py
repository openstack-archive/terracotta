# Copyright 2012 Anton Beloglazov
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

from mocktest import *
from pyqcy import *

import libvirt
import neat.collector as collector


class Collector(TestCase):

    # def setUp(self):
    #     MockTransaction.__enter__()

    # def tearDown(self):
    #     MockTransaction.__exit__()

    @qc(10)
    def start(iterations=int_(0, 10)):
        with MockTransaction:
            expect(collector).collect(any_dict).exactly(iterations).times()
            assert collector.start(iterations) == iterations

    @qc(1)
    def get_previous_vms():
        local_data_directory = os.path.join(
            os.path.dirname(__file__), 'resources', 'vms')
        previous_vms = collector.get_previous_vms(local_data_directory)
        assert len(previous_vms) == 3
        assert 'ec452be0-e5d0-11e1-aff1-0800200c9a66' in previous_vms
        assert 'e615c450-e5d0-11e1-aff1-0800200c9a66' in previous_vms
        assert 'f3e142d0-e5d0-11e1-aff1-0800200c9a66' in previous_vms

    @qc
    def get_current_vms(
        ids=dict_(
            keys=int_(min=0),
            values=str_(of='abc123-', min_length=36, max_length=36),
            min_length=0, max_length=10
        )
    ):
        with MockTransaction:

            def init_vm(id):
                vm = mock('vm')
                expect(vm).UUIDString().and_return(ids[id]).once()
                return vm

            connection = libvirt.virConnect()
            expect(connection).listDomainsID().and_return(ids.keys()).once()
            if ids:
                expect(connection).lookupByID(any_int) \
                    .and_call(lambda id: init_vm(id))
            assert collector.get_current_vms(connection) == ids.values()

    @qc
    def build_local_vm_path(
        x=str_(of='abc123_-/')
    ):
        assert collector.build_local_vm_path(x) == os.path.join(x, 'vms')

    @qc
    def get_added_vms(
        x=list_(
            of=str_(of='abc123-', min_length=36, max_length=36),
            min_length=0, max_length=5
        ),
        y=list_(
            of=str_(of='abc123-', min_length=36, max_length=36),
            min_length=0, max_length=5
        )
    ):
        previous_vms = list(x)
        if x:
            x.pop(random.randrange(len(x)))
        x.extend(y)
        assert set(collector.get_added_vms(previous_vms, x)) == set(y)

    @qc
    def get_removed_vms(
        x=list_(
            of=str_(of='abc123-', min_length=36, max_length=36),
            min_length=0, max_length=5
        ),
        y=list_(
            of=str_(of='abc123-', min_length=36, max_length=36),
            min_length=0, max_length=5
        )
    ):
        previous_vms = list(x)
        removed = []
        if x:
            to_remove = random.randrange(len(x))
            for _ in xrange(to_remove):
                removed.append(x.pop(random.randrange(len(x))))
        x.extend(y)
        assert set(collector.get_removed_vms(previous_vms, x)) == set(removed)

    @qc
    def substract_lists(
        x=list_(of=int_(min=0, max=20), max_length=10),
        y=list_(of=int_(min=0, max=20), max_length=10)
    ):
        assert set(collector.substract_lists(x, y)) == \
            set([item for item in x if item not in y])
