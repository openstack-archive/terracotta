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

"""Functions for solving NLP problems using the bruteforce method.
"""

import nlp
from terracotta.common import frange


def solve2(objective, constraint, step, limit):
    """Solve a maximization problem for 2 states.

    :param objective: The objective function.
    :param constraint: A tuple representing the constraint.
    :param step: The step size.
    :param limit: The maximum value of the variables.
    :return: The problem solution.
    """
    res_best = 0
    solution = []
    for x in frange(0, limit, step):
        for y in frange(0, limit, step):
            try:
                res = objective(x, y)
                if res > res_best and \
                        constraint[1](constraint[0](x, y), constraint[2]):
                    res_best = res
                    solution = [x, y]
            except ZeroDivisionError:
                pass
    return solution


def optimize(step, limit, otf, migration_time, ls, p, state_vector,
             time_in_states, time_in_state_n):
    """Solve a MHOD optimization problem.

    :param step: The step size for the bruteforce algorithm.
    :param limit: The maximum value of the variables.
    :param otf: The OTF parameter.
    :param migration_time: The VM migration time in time steps.
    :param ls: L functions.
    :param p: A matrix of transition probabilities.
    :param state_vector: A state vector.
    :param time_in_states: The total time in all the states in time steps.
    :param time_in_state_n: The total time in the state N in time steps.
    :return: The solution of the problem.
    """
    objective = nlp.build_objective(ls, state_vector, p)
    constraint = nlp.build_constraint(otf, migration_time, ls, state_vector,
                                      p, time_in_states, time_in_state_n)
    return solve2(objective, constraint, step, limit)
