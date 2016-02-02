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

"""Multisize sliding window workload estimation functions.
"""

from collections import deque
from itertools import islice


def mean(data, window_size):
    """Get the data mean according to the window size.

    :param data: A list of values.
    :param window_size: A window size.
    :return: The mean value.
    """
    return float(sum(data)) / window_size


def variance(data, window_size):
    """Get the data variance according to the window size.

    :param data: A list of values.
    :param window_size: A window size.
    :return: The variance value.
    """
    m = mean(data, window_size)
    return float(sum((x - m) ** 2 for x in data)) / (window_size - 1)


def acceptable_variance(probability, window_size):
    """Get the acceptable variance.

    :param probability: The probability to use.
    :param window_size: A window size.
    :return: The acceptable variance.
    """
    return float(probability * (1 - probability)) / window_size


def estimate_probability(data, window_size, state):
    """Get the estimated probability.

    :param data: A list of data values.
    :param window_size: The window size.
    :param state: The current state.
    :return: The estimated probability.
    """
    return float(data.count(state)) / window_size


def update_request_windows(request_windows, previous_state, current_state):
    """Update and return the updated request windows.

    :param request_windows: The previous request windows.
    :param previous_state: The previous state.
    :param current_state: The current state.
    :return: The updated request windows.
    """
    request_windows[previous_state].append(current_state)
    return request_windows


def update_estimate_windows(estimate_windows, request_windows,
                            previous_state):
    """Update and return the updated estimate windows.

    :param estimate_windows: The previous estimate windows.
    :param request_windows: The current request windows.
    :param previous_state: The previous state.
    :return: The updated estimate windows.
    """
    request_window = request_windows[previous_state]
    state_estimate_windows = estimate_windows[previous_state]
    for state, estimate_window in enumerate(state_estimate_windows):
        for window_size, estimates in estimate_window.items():
            slice_from = len(request_window) - window_size
            if slice_from < 0:
                slice_from = 0
            estimates.append(
                estimate_probability(
                    list(islice(request_window, slice_from, None)),
                    window_size, state))
    return estimate_windows


def update_variances(variances, estimate_windows, previous_state):
    """Updated and return the updated variances.

    :param variances: The previous variances.
    :param estimate_windows: The current estimate windows.
    :param previous_state: The previous state.
    :return: The updated variances.
    """
    estimate_window = estimate_windows[previous_state]
    for state, variance_map in enumerate(variances[previous_state]):
        for window_size in variance_map:
            estimates = estimate_window[state][window_size]
            if len(estimates) < window_size:
                variance_map[window_size] = 1.0
            else:
                variance_map[window_size] = variance(
                    list(estimates), window_size)
    return variances


def update_acceptable_variances(acceptable_variances, estimate_windows,
                                previous_state):
    """Update and return the updated acceptable variances.

    :param acceptable_variances: The previous acceptable variances.
    :param estimate_windows: The current estimate windows.
    :param previous_state: The previous state.
    :return: The updated acceptable variances.
    """
    estimate_window = estimate_windows[previous_state]
    state_acc_variances = acceptable_variances[previous_state]
    for state, acceptable_variance_map in enumerate(state_acc_variances):
        for window_size in acceptable_variance_map:
            estimates = estimate_window[state][window_size]
            acceptable_variance_map[window_size] = acceptable_variance(
                estimates[-1], window_size)
    return acceptable_variances


def select_window(variances, acceptable_variances, window_sizes):
    """Select window sizes according to the acceptable variances.

    :param variances: The variances.
    :param acceptable_variances: The acceptable variances.
    :param window_sizes: The available window sizes.
    :return: The selected window sizes.
    """
    n = len(variances)
    selected_windows = []
    for i in range(n):
        selected_windows.append([])
        for j in range(n):
            selected_size = window_sizes[0]
            for window_size in window_sizes:
                if variances[i][j][window_size] > \
                        acceptable_variances[i][j][window_size]:
                    break
                selected_size = window_size
            selected_windows[i].append(selected_size)
    return selected_windows


def select_best_estimates(estimate_windows, selected_windows):
    """Select the best estimates according to the selected windows.

    :param estimate_windows: The estimate windows.
    :param selected_windows: The selected window sizes.
    :return: The selected best estimates.
    """
    n = len(estimate_windows)
    selected_estimates = []
    for i in range(n):
        selected_estimates.append([])
        for j in range(n):
            estimates = estimate_windows[i][j][selected_windows[i][j]]
            if estimates:
                selected_estimates[i].append(estimates[-1])
            else:
                selected_estimates[i].append(0.0)
    return selected_estimates


def init_request_windows(number_of_states, max_window_size):
    """Initialize a request window data structure.

    :param number_of_states: The number of states.
    :param max_window_size: The max size of the request windows.
    :return: The initialized request windows data structure.
    """
    return [deque([], max_window_size)
            for _ in range(number_of_states)]


def init_variances(window_sizes, number_of_states):
    """Initialize a variances data structure.

    :param window_sizes: The required window sizes.
    :param number_of_states: The number of states.
    :return: The initialized variances data structure.
    """
    variances = []
    for i in range(number_of_states):
        variances.append([])
        for j in range(number_of_states):
            variances[i].append(dict(zip(window_sizes,
                                         len(window_sizes) * [1.0])))
    return variances


def init_deque_structure(window_sizes, number_of_states):
    """Initialize a 3 level deque data structure.

    :param window_sizes: The required window sizes.
    :param number_of_states: The number of states.
    :return: The initialized 3 level deque data structure.
    """
    structure = []
    for i in range(number_of_states):
        structure.append([])
        for j in range(number_of_states):
            structure[i].append(dict((size, deque([], size))
                                     for size in window_sizes))
    return structure


def init_selected_window_sizes(window_sizes, number_of_states):
    """Initialize a selected window sizes data structure.

    :param window_sizes: The required window sizes.
    :param number_of_states: The number of states.
    :return: The initialized selected window sizes data structure.
    """
    structure = []
    for i in range(number_of_states):
        structure.append([])
        for j in range(number_of_states):
            structure[i].append(window_sizes[0])
    return structure
