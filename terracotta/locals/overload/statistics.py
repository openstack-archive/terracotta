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

"""Statistics based overload detection algorithms.
"""

import numpy as np
from numpy import median
from scipy.optimize import leastsq


def loess_factory(time_step, migration_time, params):
    """Creates the Loess based overload detection algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the OTF algorithm.
    """
    migration_time_normalized = float(migration_time) / time_step
    return lambda utilization, state=None: \
        (loess(params['threshold'],
               params['param'],
               params['length'],
               migration_time_normalized,
               utilization),
         {})


def loess_robust_factory(time_step, migration_time, params):
    """Creates the robust Loess based overload detection algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the OTF algorithm.
    """
    migration_time_normalized = float(migration_time) / time_step
    return lambda utilization, state=None: \
        (loess_robust(params['threshold'],
                      params['param'],
                      params['length'],
                      migration_time_normalized,
                      utilization),
         {})


def mad_threshold_factory(time_step, migration_time, params):
    """Creates the MAD based utilization threshold algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the static threshold algorithm.
    """
    return lambda utilization, state=None: \
        (mad_threshold(params['threshold'],
                       params['limit'],
                       utilization),
         {})


def iqr_threshold_factory(time_step, migration_time, params):
    """Creates the IQR based utilization threshold algorithm.

    :param time_step: The length of the simulation time step in seconds.
    :param migration_time: The VM migration time in time seconds.
    :param params: A dictionary containing the algorithm's parameters.
    :return: A function implementing the static threshold algorithm.
    """
    return lambda utilization, state=None: \
        (iqr_threshold(params['threshold'],
                       params['limit'],
                       utilization),
         {})


def loess(threshold, param, length, migration_time, utilization):
    """The Loess based overload detection algorithm.

    :param threshold: The CPU utilization threshold.
    :param param: The safety parameter.
    :param length: The required length of the utilization history.
    :param migration_time: The VM migration time in time steps.
    :param utilization: The utilization history to analize.
    :return: A decision of whether the host is overloaded.
    """
    return loess_abstract(loess_parameter_estimates,
                          threshold,
                          param,
                          length,
                          migration_time,
                          utilization)


def loess_robust(threshold, param, length, migration_time, utilization):
    """The robust Loess based overload detection algorithm.

    :param threshold: The CPU utilization threshold.
    :param param: The safety parameter.
    :param length: The required length of the utilization history.
    :param migration_time: The VM migration time in time steps.
    :param utilization: The utilization history to analize.
    :return: A decision of whether the host is overloaded.
    """
    return loess_abstract(loess_robust_parameter_estimates,
                          threshold,
                          param,
                          length,
                          migration_time,
                          utilization)


def loess_abstract(estimator, threshold, param, length, migration_time,
                   utilization):
    """The abstract Loess algorithm.

    :param estimator: A parameter estimation function.
    :param threshold: The CPU utilization threshold.
    :param param: The safety parameter.
    :param length: The required length of the utilization history.
    :param migration_time: The VM migration time in time steps.
    :param utilization: The utilization history to analize.
    :return: A decision of whether the host is overloaded.
    """
    if len(utilization) < length:
        return False
    estimates = estimator(utilization[-length:])
    prediction = (estimates[0] + estimates[1] * (length + migration_time))
    return param * prediction >= threshold


def mad_threshold(param, limit, utilization):
    """The MAD based threshold algorithm.

    :param param: The safety parameter.
    :param limit: The minimum allowed length of the utilization history.
    :param utilization: The utilization history to analize.
    :return: A decision of whether the host is overloaded.
    """
    return utilization_threshold_abstract(lambda x: 1 - param * mad(x),
                                          limit,
                                          utilization)


def iqr_threshold(param, limit, utilization):
    """The IQR based threshold algorithm.

    :param param: The safety parameter.
    :param limit: The minimum allowed length of the utilization history.
    :param utilization: The utilization history to analize.
    :return: A decision of whether the host is overloaded.
    """
    return utilization_threshold_abstract(lambda x: 1 - param * iqr(x),
                                          limit,
                                          utilization)


def utilization_threshold_abstract(f, limit, utilization):
    """The abstract utilization threshold algorithm.

    :param f: A function to calculate the utilization threshold.
    :param limit: The minimum allowed length of the utilization history.
    :param utilization: The utilization history to analize.
    :return: A decision of whether the host is overloaded.
    """
    if (len(utilization) < limit):
        return False
    return f(utilization) <= utilization[-1]


def mad(data):
    """Calculate the Median Absolute Deviation from the data.

    :param data: The data to analyze.
    :return: The calculated MAD.
    """
    data_median = median(data)
    return float(median([abs(data_median - x) for x in data]))


def iqr(data):
    """Calculate the Interquartile Range from the data.

    :param data: The data to analyze.
    :return: The calculated IQR.
    """
    sorted_data = sorted(data)
    n = len(data) + 1
    q1 = int(round(0.25 * n)) - 1
    q3 = int(round(0.75 * n)) - 1
    return float(sorted_data[q3] - sorted_data[q1])


def loess_parameter_estimates(data):
    """Calculate Loess parameter estimates.

    :param data: A data set.
    :return: The parameter estimates.
    """
    def f(p, x, y, weights):
        return weights * (y - (p[0] + p[1] * x))

    n = len(data)
    estimates, _ = leastsq(f, [1., 1.], args=(
        np.array(range(1, n + 1)),
        np.array(data),
        np.array(tricube_weights(n))))

    return estimates.tolist()


def loess_robust_parameter_estimates(data):
    """Calculate Loess robust parameter estimates.

    :param data: A data set.
    :return: The parameter estimates.
    """
    def f(p, x, y, weights):
        return weights * (y - (p[0] + p[1] * x))

    n = len(data)
    x = np.array(range(1, n + 1))
    y = np.array(data)
    weights = np.array(tricube_weights(n))
    estimates, _ = leastsq(f, [1., 1.], args=(x, y, weights))

    p = estimates.tolist()
    residuals = (y - (p[0] + p[1] * x))

    weights2 = np.array(tricube_bisquare_weights(residuals.tolist()))
    estimates2, _ = leastsq(f, [1., 1.], args=(x, y, weights2))

    return estimates2.tolist()


def tricube_weights(n):
    """Generates a list of weights according to the tricube function.

    :param n: The number of weights to generate.
    :return: A list of generated weights.
    """
    spread = top = float(n - 1)
    weights = []
    for i in range(2, n):
        weights.append((1 - ((top - i) / spread) ** 3) ** 3)
    return [weights[0], weights[0]] + weights


def tricube_bisquare_weights(data):
    """Generates a weights according to the tricube bisquare function.

    :param data: The input data.
    :return: A list of generated weights.
    """
    n = len(data)
    s6 = 6 * median(map(abs, data))
    weights = tricube_weights(n)
    weights2 = []
    for i in range(2, n):
            weights2.append(weights[i] * (1 - (data[i] / s6) ** 2) ** 2)
    return [weights2[0], weights2[0]] + weights2
