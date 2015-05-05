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

""" OTF threshold based algorithms.
"""

from oslo_log import log as logging


LOG = logging.getLogger(__name__)


def otf_factory(time_step, migration_time, params):
    """ Creates the OTF algorithm with limiting and migration time.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the OTF algorithm.
    """
    migration_time_normalized = float(migration_time) / time_step
    def otf_wrapper(utilization, state=None):
        if state is None or state == {}:
            state = {'overload': 0,
                     'total': 0}
        return otf(params['otf'],
                   params['threshold'],
                   params['limit'],
                   migration_time_normalized,
                   utilization,
                   state)

    return otf_wrapper


def otf(otf, threshold, limit, migration_time, utilization, state):
    """ The OTF threshold algorithm with limiting and migration time.

    :param otf: The threshold on the OTF value.
    :param threshold: The utilization overload threshold.
    :param limit: The minimum number of values in the utilization history.
    :param migration_time: The VM migration time in time steps.
    :param utilization: The history of the host's CPU utilization.
    :param state: The state dictionary.
    :return: The decision of the algorithm and updated state.
    """
    state['total'] += 1
    overload = (utilization[-1] >= threshold)
    if overload:
        state['overload'] += 1

    LOG.debug('OTF overload:' + str(overload))
    LOG.debug('OTF overload steps:' + str(state['overload']))
    LOG.debug('OTF total steps:' + str(state['total']))
    LOG.debug('OTF:' + str(float(state['overload']) / state['total']))
    LOG.debug('OTF migration time:' + str(migration_time))
    LOG.debug('OTF + migration time:' +
              str((migration_time + state['overload']) / \
                      (migration_time + state['total'])))
    LOG.debug('OTF decision:' +
              str(overload and (migration_time + state['overload']) / \
                      (migration_time + state['total']) >= otf))

    if not overload or len(utilization) < limit:
        decision = False
    else:
        decision = (migration_time + state['overload']) / \
            (migration_time + state['total']) >= otf

    return (decision, state)
