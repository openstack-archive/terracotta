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

"""Functions for defing the NLP problem of the MHOD algorithm.
"""

import operator


def build_objective(ls, state_vector, p):
    """Creates an objective function, which is a sum of the L functions.

    :param ls: A list of L functions.
    :param state-vector: A state vector.
    :param p: A matrix of transition probabilities.
    :return: An objective function.
    """
    def objective(*m):
        return sum(l(state_vector, p, list(m)) for l in ls)
    return objective


def build_constraint(otf, migration_time, ls, state_vector,
                     p, time_in_states, time_in_state_n):
    """Creates an optimization constraint from the L functions.

    :param otf: The OTF parameter.
    :param migration_time: The VM migration time in time steps.
    :param ls: A list of L functions.
    :param state-vector: A state vector.
    :param p: A matrix of transition probabilities.
    :param time_in_states: The total time in all the states in time steps.
    :param time_in_state_n: The total time in the state N in time steps.
    :return: The created constraint.
    """
    def constraint(*m):
        m_list = list(m)
        return float(migration_time +
                     time_in_state_n +
                     ls[-1](state_vector, p, m_list)) / \
               (migration_time +
                time_in_states +
                sum(l(state_vector, p, m_list) for l in ls))
    return constraint, operator.le, otf
