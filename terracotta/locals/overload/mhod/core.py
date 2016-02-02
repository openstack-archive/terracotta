# Copyright 2012 Anton Beloglazov
# Copyright 2015 Huawei Technologies Co., Ltd.
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

"""This is the main module of the MHOD algorithm.
"""

from oslo_log import log as logging

import terracotta.locals.overload.mhod.bruteforce as bruteforce
from terracotta.locals.overload.mhod.l_2_states import ls
import terracotta.locals.overload.mhod.multisize_estimation as estimation


LOG = logging.getLogger(__name__)


def mhod_factory(time_step, migration_time, params):
    """Creates the MHOD algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the MHOD algorithm.
    """

    def mhod_wrapper(utilization, state=None):
        if not state:
            state = init_state(params['history_size'],
                               params['window_sizes'],
                               len(params['state_config']) + 1)
        return mhod(params['state_config'],
                    params['otf'],
                    params['window_sizes'],
                    params['bruteforce_step'],
                    params['learning_steps'],
                    time_step,
                    migration_time,
                    utilization,
                    state)

    return mhod_wrapper


def init_state(history_size, window_sizes, number_of_states):
    """Initialize the state dictionary of the MHOD algorithm.

    :param history_size: The number of last system states to store.
    :param window_sizes: The required window sizes.
    :param number_of_states: The number of states.
    :return: The initialization state dictionary.
    """
    return {
        'previous_state': 0,
        'previous_utilization': [],
        'time_in_states': 0,
        'time_in_state_n': 0,
        'request_windows': estimation.init_request_windows(
            number_of_states, max(window_sizes)),
        'estimate_windows': estimation.init_deque_structure(
            window_sizes, number_of_states),
        'variances': estimation.init_variances(
            window_sizes, number_of_states),
        'acceptable_variances': estimation.init_variances(
            window_sizes, number_of_states)}


def mhod(state_config, otf, window_sizes, bruteforce_step, learning_steps,
         time_step, migration_time, utilization, state):
    """The MHOD algorithm returning whether the host is overloaded.

    :param state_config: The state configuration.
    :param otf: The OTF parameter.
    :param window_sizes: A list of window sizes.
    :param bruteforce_step: The step of the bruteforce algorithm.
    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param utilization: The history of the host's CPU utilization.
    :param state: The state of the algorithm.
    :return: The updated state and decision of the algorithm.
    """
    utilization_length = len(utilization)
    #    if utilization_length == state['time_in_states'] and \
    #      utilization == state['previous_utilization']:
    #        # No new utilization values
    #        return False, state

    number_of_states = len(state_config) + 1
    previous_state = 0
    #    state['previous_utilization'] = utilization
    state['request_windows'] = estimation.init_request_windows(
        number_of_states, max(window_sizes))
    state['estimate_windows'] = estimation.init_deque_structure(
        window_sizes, number_of_states)
    state['variances'] = estimation.init_variances(
        window_sizes, number_of_states)
    state['acceptable_variances'] = estimation.init_variances(
        window_sizes, number_of_states)

    for i, current_state in enumerate(
            utilization_to_states(state_config, utilization)):
        state['request_windows'] = estimation.update_request_windows(
            state['request_windows'],
            previous_state,
            current_state)
        state['estimate_windows'] = estimation.update_estimate_windows(
            state['estimate_windows'],
            state['request_windows'],
            previous_state)
        state['variances'] = estimation.update_variances(
            state['variances'],
            state['estimate_windows'],
            previous_state)
        state['acceptable_variances'] = estimation.update_acceptable_variances(
            state['acceptable_variances'],
            state['estimate_windows'],
            previous_state)
        previous_state = current_state

    selected_windows = estimation.select_window(
        state['variances'],
        state['acceptable_variances'],
        window_sizes)
    p = estimation.select_best_estimates(
        state['estimate_windows'],
        selected_windows)
    # These two are saved for testing purposes
    state['selected_windows'] = selected_windows
    state['p'] = p

    state_vector = build_state_vector(state_config, utilization)
    current_state = get_current_state(state_vector)
    state['previous_state'] = current_state

    state_n = len(state_config)
    #    if utilization_length > state['time_in_states'] + 1:
    #        for s in utilization_to_states(
    #                state_config,
    #                utilization[-(utilization_length -
    # state['time_in_states']):]):
    #            state['time_in_states'] += 1
    #            if s == state_n:
    #                state['time_in_state_n'] += 1
    #    else:
    state['time_in_states'] += 1
    if current_state == state_n:
        state['time_in_state_n'] += 1

    LOG.debug('MHOD utilization:' + str(utilization))
    LOG.debug('MHOD time_in_states:' + str(state['time_in_states']))
    LOG.debug('MHOD time_in_state_n:' + str(state['time_in_state_n']))
    LOG.debug('MHOD p:' + str(p))
    LOG.debug('MHOD current_state:' + str(current_state))
    LOG.debug('MHOD p[current_state]:' + str(p[current_state]))

    if utilization_length >= learning_steps:
        if current_state == state_n and p[state_n][state_n] > 0:
            # if p[current_state][state_n] > 0:
            policy = bruteforce.optimize(
                bruteforce_step, 1.0, otf, (migration_time / time_step), ls, p,
                state_vector, state['time_in_states'],
                state['time_in_state_n'])
            # This is saved for testing purposes
            state['policy'] = policy
            LOG.debug('MHOD policy:' + str(policy))
            command = issue_command_deterministic(policy)
            LOG.debug('MHOD command:' + str(command))
            return command, state
    return False, state


def build_state_vector(state_config, utilization):
    """Build the current state PMF corresponding to the utilization
        history and state config.

    :param state_config: The state configuration.
    :param utilization: The history of the host's CPU utilization.
    :return: The current state vector.
    """
    state = utilization_to_state(state_config, utilization[-1])
    return [int(state == x) for x in range(len(state_config) + 1)]


def utilization_to_state(state_config, utilization):
    """Transform a utilization value into the corresponding state.

    :param state_config: The state configuration.
    :param utilization: A utilization value.
    :return: The state corresponding to the utilization value.
    """
    prev = -1
    for state, threshold in enumerate(state_config):
        if utilization >= prev and utilization < threshold:
            return state
        prev = state
    return prev + 1


def get_current_state(state_vector):
    """Get the current state corresponding to the state probability
    vector.

    :param state_vector: The state PMF vector.
    :return: The current state.
    """
    return state_vector.index(1)


def utilization_to_states(state_config, utilization):
    """Get the state history corresponding to the utilization history.
    Adds the 0 state to the beginning to simulate the first transition.
    (map (partial utilization-to-state state-config) utilization))

    :param state_config: The state configuration.
    :param utilization: The history of the host's CPU utilization.
    :return: The state history.
    """
    return [utilization_to_state(state_config, x) for x in utilization]


def issue_command_deterministic(policy):
    """Issue a migration command according to the policy PMF p.

    :param policy: A policy PMF.
    :return: A migration command.
    """
    return len(policy) == 0
