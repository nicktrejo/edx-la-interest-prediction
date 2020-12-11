# -*- coding: utf-8 -*-
"""
Created on Wed Sep 09 20:00:00 2020
@author: nicktrejo

Main idea:
<https://scielo.conicyt.cl/scielo.php?pid=S0718-07642019000100247&script=sci_arttext>

Ideas in links:
<https://www.datacamp.com/community/tutorials/moving-averages-in-pandas>
<https://stackoverflow.com/questions/20618804/how-to-smooth-a-curve-in-the-right-way>
<https://en.wikipedia.org/wiki/Moving_average>
<https://www.ibiblio.org/e-notes/Splines/Intro.htm>
Google 'sma cma ema average python'
Google 'splines , b-splines, bezier'

Fuzzy:
<https://es.wikipedia.org/wiki/L%C3%B3gica_difusa>
<https://pypi.org/project/fuzzylogic/>
<https://github.com/amogorkon/fuzzylogic/blob/master/fuzzylogic-0.1.1.post1/setup.py>
<https://pythonhosted.org/scikit-fuzzy/auto_examples/plot_tipping_problem_newapi.html>

"""
# IMPORTS

import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# IMPORTANTS NOTES

# CAN EXECUTE THIS FILE FORM A PYTHON CONSOLE USING
# exec(open("/home/nicolas/Documents/MasterII/TFM/Proyecto/\
# edx-la-interest-prediction/Scripts/Python/smoother.py").read())

FOLDER_PATH = '/home/nicolas/ACCOMP/OUTPUT-2020-08-25/'
ALL_INDICATORS_FILTERED = FOLDER_PATH + 'allIndicators_Filtered.csv'
PAST_WEIGHT = 0.6
DEBUG = False
NUM_WEEKS = 1
ALL_INDICATORS_FILTERED_SMOOTHED = ALL_INDICATORS_FILTERED.replace('.csv',
                                                                   '_smoothed.csv')

print('\n###  Starting smoother.py  ###\n\n')
if DEBUG:
    random.seed(123456)  # Using seed for debuging purposes

# My functions

def smoother(dataf, column='num_events', type='weeks', num_weeks=NUM_WEEKS, past_weight=PAST_WEIGHT):
    if type == 'weeks':
        return smoother_weeks(dataf, column, num_weeks)
    if type == 'nico':
        return smoother_nico(dataf, column, past_weight)

def smoother_weeks(dataf, column='num_events', num_weeks=NUM_WEEKS):
    new_column = dataf.groupby('user_id')[column].rolling(window=num_weeks*7).mean()
    return new_column.reset_index(level=0, drop=True)

def smoother_nico(dataf, column='num_events', past_weight=PAST_WEIGHT):
    _unique_user_id = dataf['user_id'].unique()
    column_nico = f'{column}_nico'
    for user in _unique_user_id:
        user_index = dataf[dataf['user_id']==user].index
        dataf.loc[user_index[0],column_nico] = dataf.loc[user_index[0], column]
        prev_i = user_index[0]
        for enum, i in enumerate(user_index[1:]):
            dataf.loc[i,f'{column}_nico'] = (past_weight * dataf.loc[prev_i,column_nico]
                                           + (1-past_weight) * dataf.loc[i,'num_events']) /2
            prev_i = i
    return


# Read and prepare data

df = pd.read_csv(ALL_INDICATORS_FILTERED)
df.sort_values(by=['user_id', 'dt_date'], inplace=True, ignore_index=True)
# df.reset_index(drop=True)
unique_user_id = df['user_id'].unique()
# DEBUG using just columns: ['user_id', 'dt_date', 'num_events', 'enrollment_mode']
if DEBUG:
    for col in df.columns[2:-1]:
        del df[col]

# Calculate `simple moving average (SMA)` using rolling window of 7 days
## Not working
# df['num_events_SMA'] = df.groupby('user_id')['num_events'].rolling(window=7).mean()

try:
    for column in df.columns[2:-1]:
        df[f'{column}_{NUM_WEEKS}w'] = smoother(df, column, type='weeks',
                                                num_weeks=NUM_WEEKS)
        if DEBUG:
            df[f'{column}_nico'] = smoother(df, column, type='nico',
                                            past_weight=PAST_WEIGHT)
except:
    new_column = df.groupby('user_id')['num_events'].rolling(window=NUM_WEEKS*7).mean()
    df['num_events_SMA'] = new_column.reset_index(level=0, drop=True)

if DEBUG:
    TESTING = [14, 21, 30]
    for i in TESTING:
        new_column = df.groupby('user_id')['num_events'].rolling(window=i).mean()
        df[f'num_events_SMA_{i}'] = new_column.reset_index(level=0, drop=True)

# Nico style:
#          XX_{0} = X_{0}
#          XX_{n} = ( XX_{n-1} + X_{n} ) /2
if DEBUG:
    for user in unique_user_id:
        user_index = df[df['user_id']==user].index
        df.loc[user_index[0],'num_events_NICO'] = df.loc[user_index[0],'num_events']
        prev_i = user_index[0]
        for enum, i in enumerate(user_index[1:]):
            df.loc[i,'num_events_NICO'] = (PAST_WEIGHT * df.loc[prev_i,'num_events_NICO']
                                           + (1-PAST_WEIGHT) * df.loc[i,'num_events']) /2
            prev_i = i

#print(df.head(10))


def random_user(new_seed=None):
    if new_seed:
        random.seed(new_seed)
    return unique_user_id[random.randint(0,len(unique_user_id))]

# Plot
if DEBUG:
    def do_plot(user=5210510):
        # plt.figure(figsize=[15,10])
        # plt.grid(True)
        df_plt = df[df['user_id']==user].reset_index(drop=True, inplace=False)
        plt.plot(df_plt['num_events'],label='data')
        plt.plot(df_plt['num_events_SMA'],label='SMA')
        if DEBUG:
            plt.plot(df_plt['num_events_NICO'],label=f'NICO ({PAST_WEIGHT})')
            for i in TESTING:
                plt.plot(df_plt[f'num_events_SMA_{i}'],label=f'SMA_{i}')
        plt.title(f'User_id: {user}')
        plt.legend(loc=2)
        plt.show()

# do_plot(random_user())

df.to_csv(path_or_buf=ALL_INDICATORS_FILTERED_SMOOTHED, encoding='utf-8', index=False)








# CLASS

# class SmootherTransformer:
#     """
#     Apply a power transform featurewise to make data more Gaussian-like.
#     Power transforms are a family of parametric, monotonic transformations
#     that are applied to make data more Gaussian-like. This is useful for
#     modeling issues related to heteroscedasticity (non-constant variance),
#     or other situations where normality is desired.
#     Currently, PowerTransformer supports the Box-Cox transform and the
#     Yeo-Johnson transform. The optimal parameter for stabilizing variance and
#     minimizing skewness is estimated through maximum likelihood.
#     Box-Cox requires input data to be strictly positive, while Yeo-Johnson
#     supports both positive or negative data.
#     By default, zero-mean, unit-variance normalization is applied to the
#     transformed data.
#     Parameters
#     ----------
#     method : str, (default='yeo-johnson')
#         The power transform method. Available methods are:
#         - 'yeo-johnson' , works with positive and negative values
#         - 'box-cox' , only works with strictly positive values
#     standardize : boolean, default=True
#         Set to True to apply zero-mean, unit-variance normalization to the
#         transformed output.
#     copy : boolean, optional, default=True
#         Set to False to perform inplace computation during transformation.
#     Attributes
#     ----------
#     lambdas_ : array of float, shape (n_features,)
#         The parameters of the power transformation for the selected features.
#     Examples
#     The EdXEvent object aims to extract info from a singular log edx event
#
#     More info:
#     https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html
#     :param raw_event: the raw singular event (line) as it is in the log file
#     :type raw_event: str
#     """
#
#     def __init__(self, raw_event):
#         """
#         Constructor method
#         :param raw_event: the raw singular event (line) as it is in the log file
#         :type raw_event: str
#         """
#
#
# # CODE
#
#
#
#
#
# # TESTING
#
# if __name__ == '__main__':
#     pass
#
#
# ##########################################################################
# ##########################################################################
#
#
# from itertools import chain, combinations
# import numbers
# import warnings
# from itertools import combinations_with_replacement as combinations_w_r
#
# import numpy as np
# from scipy import sparse
# from scipy import stats
# from scipy import optimize
# from scipy.special import boxcox
#
# from ..base import BaseEstimator, TransformerMixin
# from ..utils import check_array
# from ..utils.extmath import row_norms
# from ..utils.extmath import _incremental_mean_and_var
# from ..utils.sparsefuncs_fast import (inplace_csr_row_normalize_l1,
#                                       inplace_csr_row_normalize_l2)
# from ..utils.sparsefuncs import (inplace_column_scale,
#                                  mean_variance_axis, incr_mean_variance_axis,
#                                  min_max_axis)
# from ..utils.validation import (check_is_fitted, check_random_state,
#                                 FLOAT_DTYPES, _deprecate_positional_args)
#
# from ._csr_polynomial_expansion import _csr_polynomial_expansion
#
# from ._encoders import OneHotEncoder
#
# BOUNDS_THRESHOLD = 1e-7
#
# __all__ = [
#     'Binarizer',
#     'KernelCenterer',
#     'MinMaxScaler',
#     'MaxAbsScaler',
#     'Normalizer',
#     'OneHotEncoder',
#     'RobustScaler',
#     'StandardScaler',
#     'QuantileTransformer',
#     'PowerTransformer',
#     'add_dummy_feature',
#     'binarize',
#     'normalize',
#     'scale',
#     'robust_scale',
#     'maxabs_scale',
#     'minmax_scale',
#     'quantile_transform',
#     'power_transform',
# ]
#
#
# def _handle_zeros_in_scale(scale, copy=True):
#     ''' Makes sure that whenever scale is zero, we handle it correctly.
#     This happens in most scalers when we have constant features.'''
#
#     # if we are fitting on 1D arrays, scale might be a scalar
#     if np.isscalar(scale):
#         if scale == .0:
#             scale = 1.
#         return scale
#     elif isinstance(scale, np.ndarray):
#         if copy:
#             # New array to avoid side-effects
#             scale = scale.copy()
#         scale[scale == 0.0] = 1.0
#         return scale
#
#
# @_deprecate_positional_args
# def scale(X, *, axis=0, with_mean=True, with_std=True, copy=True):
#     """Standardize a dataset along any axis
#     Center to the mean and component wise scale to unit variance.
#     Parameters
#     ----------
#     X : {array-like, sparse matrix}
#         The data to center and scale.
#     axis : int (0 by default)
#         axis used to compute the means and standard deviations along. If 0,
#         independently standardize each feature, otherwise (if 1) standardize
#         each sample.
#     with_mean : boolean, True by default
#         If True, center the data before scaling.
#     with_std : boolean, True by default
#         If True, scale the data to unit variance (or equivalently,
#         unit standard deviation).
#     copy : boolean, optional, default True
#         set to False to perform inplace row normalization and avoid a
#         copy (if the input is already a numpy array or a scipy.sparse
#         CSC matrix and if axis is 1).
#     Notes
#     -----
#     This implementation will refuse to center scipy.sparse matrices
#     since it would make them non-sparse and would potentially crash the
#     program with memory exhaustion problems.
#     Instead the caller is expected to either set explicitly
#     `with_mean=False` (in that case, only variance scaling will be
#     performed on the features of the CSC matrix) or to call `X.toarray()`
#     if he/she expects the materialized dense array to fit in memory.
#     To avoid memory copy the caller should pass a CSC matrix.
#     NaNs are treated as missing values: disregarded to compute the statistics,
#     and maintained during the data transformation.
#     We use a biased estimator for the standard deviation, equivalent to
#     `numpy.std(x, ddof=0)`. Note that the choice of `ddof` is unlikely to
#     affect model performance.
#     For a comparison of the different scalers, transformers, and normalizers,
#     see :ref:`examples/preprocessing/plot_all_scaling.py
#     <sphx_glr_auto_examples_preprocessing_plot_all_scaling.py>`.
#     See also
#     --------
#     StandardScaler: Performs scaling to unit variance using the``Transformer`` API
#         (e.g. as part of a preprocessing :class:`sklearn.pipeline.Pipeline`).
#     """
#     X = check_array(X, accept_sparse='csc', copy=copy, ensure_2d=False,
#                     estimator='the scale function', dtype=FLOAT_DTYPES,
#                     force_all_finite='allow-nan')
#     if sparse.issparse(X):
#         if with_mean:
#             raise ValueError(
#                 "Cannot center sparse matrices: pass `with_mean=False` instead"
#                 " See docstring for motivation and alternatives.")
#         if axis != 0:
#             raise ValueError("Can only scale sparse matrix on axis=0, "
#                              " got axis=%d" % axis)
#         if with_std:
#             _, var = mean_variance_axis(X, axis=0)
#             var = _handle_zeros_in_scale(var, copy=False)
#             inplace_column_scale(X, 1 / np.sqrt(var))
#     else:
#         X = np.asarray(X)
#         if with_mean:
#             mean_ = np.nanmean(X, axis)
#         if with_std:
#             scale_ = np.nanstd(X, axis)
#         # Xr is a view on the original array that enables easy use of
#         # broadcasting on the axis in which we are interested in
#         Xr = np.rollaxis(X, axis)
#         if with_mean:
#             Xr -= mean_
#             mean_1 = np.nanmean(Xr, axis=0)
#             # Verify that mean_1 is 'close to zero'. If X contains very
#             # large values, mean_1 can also be very large, due to a lack of
#             # precision of mean_. In this case, a pre-scaling of the
#             # concerned feature is efficient, for instance by its mean or
#             # maximum.
#             if not np.allclose(mean_1, 0):
#                 warnings.warn("Numerical issues were encountered "
#                               "when centering the data "
#                               "and might not be solved. Dataset may "
#                               "contain too large values. You may need "
#                               "to prescale your features.")
#                 Xr -= mean_1
#         if with_std:
#             scale_ = _handle_zeros_in_scale(scale_, copy=False)
#             Xr /= scale_
#             if with_mean:
#                 mean_2 = np.nanmean(Xr, axis=0)
#                 # If mean_2 is not 'close to zero', it comes from the fact that
#                 # scale_ is very small so that mean_2 = mean_1/scale_ > 0, even
#                 # if mean_1 was close to zero. The problem is thus essentially
#                 # due to the lack of precision of mean_. A solution is then to
#                 # subtract the mean again:
#                 if not np.allclose(mean_2, 0):
#                     warnings.warn("Numerical issues were encountered "
#                                   "when scaling the data "
#                                   "and might not be solved. The standard "
#                                   "deviation of the data is probably "
#                                   "very close to 0. ")
#                     Xr -= mean_2
#     return X
#
#
#
# ################################################
# ################################################
# ################################################
#
#
#
# class PowerTransformer(TransformerMixin, BaseEstimator):
#     """Apply a power transform featurewise to make data more Gaussian-like.
#     Power transforms are a family of parametric, monotonic transformations
#     that are applied to make data more Gaussian-like. This is useful for
#     modeling issues related to heteroscedasticity (non-constant variance),
#     or other situations where normality is desired.
#     Currently, PowerTransformer supports the Box-Cox transform and the
#     Yeo-Johnson transform. The optimal parameter for stabilizing variance and
#     minimizing skewness is estimated through maximum likelihood.
#     Box-Cox requires input data to be strictly positive, while Yeo-Johnson
#     supports both positive or negative data.
#     By default, zero-mean, unit-variance normalization is applied to the
#     transformed data.
#     Parameters
#     ----------
#     method : str, (default='yeo-johnson')
#         The power transform method. Available methods are:
#         - 'yeo-johnson' , works with positive and negative values
#         - 'box-cox' , only works with strictly positive values
#     standardize : boolean, default=True
#         Set to True to apply zero-mean, unit-variance normalization to the
#         transformed output.
#     copy : boolean, optional, default=True
#         Set to False to perform inplace computation during transformation.
#     Attributes
#     ----------
#     lambdas_ : array of float, shape (n_features,)
#         The parameters of the power transformation for the selected features.
#     Examples
#     --------
#     >>> import numpy as np
#     >>> from sklearn.preprocessing import PowerTransformer
#     >>> pt = PowerTransformer()
#     >>> data = [[1, 2], [3, 2], [4, 5]]
#     >>> print(pt.fit(data))
#     PowerTransformer()
#     >>> print(pt.lambdas_)
#     [ 1.386... -3.100...]
#     >>> print(pt.transform(data))
#     [[-1.316... -0.707...]
#      [ 0.209... -0.707...]
#      [ 1.106...  1.414...]]
#     See also
#     --------
#     power_transform : Equivalent function without the estimator API.
#     QuantileTransformer : Maps data to a standard normal distribution with
#         the parameter `output_distribution='normal'`.
#     Notes
#     -----
#     NaNs are treated as missing values: disregarded in ``fit``, and maintained
#     in ``transform``.
#     For a comparison of the different scalers, transformers, and normalizers,
#     see :ref:`examples/preprocessing/plot_all_scaling.py
#     <sphx_glr_auto_examples_preprocessing_plot_all_scaling.py>`.
#     """
#     @_deprecate_positional_args
#     def __init__(self, method='yeo-johnson', *, standardize=True, copy=True):
#         self.method = method
#         self.standardize = standardize
#         self.copy = copy
#
#     def fit(self, X, y=None):
#         """Estimate the optimal parameter lambda for each feature.
#         The optimal lambda parameter for minimizing skewness is estimated on
#         each feature independently using maximum likelihood.
#         Parameters
#         ----------
#         X : array-like, shape (n_samples, n_features)
#             The data used to estimate the optimal transformation parameters.
#         y : Ignored
#         Returns
#         -------
#         self : object
#         """
#         self._fit(X, y=y, force_transform=False)
#         return self
#
#     def fit_transform(self, X, y=None):
#         return self._fit(X, y, force_transform=True)
#
#     def _fit(self, X, y=None, force_transform=False):
#         X = self._check_input(X, in_fit=True, check_positive=True,
#                               check_method=True)
#
#         if not self.copy and not force_transform:  # if call from fit()
#             X = X.copy()  # force copy so that fit does not change X inplace
#
#         optim_function = {'box-cox': self._box_cox_optimize,
#                           'yeo-johnson': self._yeo_johnson_optimize
#                           }[self.method]
#         with np.errstate(invalid='ignore'):  # hide NaN warnings
#             self.lambdas_ = np.array([optim_function(col) for col in X.T])
#
#         if self.standardize or force_transform:
#             transform_function = {'box-cox': boxcox,
#                                   'yeo-johnson': self._yeo_johnson_transform
#                                   }[self.method]
#             for i, lmbda in enumerate(self.lambdas_):
#                 with np.errstate(invalid='ignore'):  # hide NaN warnings
#                     X[:, i] = transform_function(X[:, i], lmbda)
#
#         if self.standardize:
#             self._scaler = StandardScaler(copy=False)
#             if force_transform:
#                 X = self._scaler.fit_transform(X)
#             else:
#                 self._scaler.fit(X)
#
#         return X
#
#     def transform(self, X):
#         """Apply the power transform to each feature using the fitted lambdas.
#         Parameters
#         ----------
#         X : array-like, shape (n_samples, n_features)
#             The data to be transformed using a power transformation.
#         Returns
#         -------
#         X_trans : array-like, shape (n_samples, n_features)
#             The transformed data.
#         """
#         check_is_fitted(self)
#         X = self._check_input(X, in_fit=False, check_positive=True,
#                               check_shape=True)
#
#         transform_function = {'box-cox': boxcox,
#                               'yeo-johnson': self._yeo_johnson_transform
#                               }[self.method]
#         for i, lmbda in enumerate(self.lambdas_):
#             with np.errstate(invalid='ignore'):  # hide NaN warnings
#                 X[:, i] = transform_function(X[:, i], lmbda)
#
#         if self.standardize:
#             X = self._scaler.transform(X)
#
#         return X
#
#     def inverse_transform(self, X):
#         """Apply the inverse power transformation using the fitted lambdas.
#         The inverse of the Box-Cox transformation is given by::
#             if lambda_ == 0:
#                 X = exp(X_trans)
#             else:
#                 X = (X_trans * lambda_ + 1) ** (1 / lambda_)
#         The inverse of the Yeo-Johnson transformation is given by::
#             if X >= 0 and lambda_ == 0:
#                 X = exp(X_trans) - 1
#             elif X >= 0 and lambda_ != 0:
#                 X = (X_trans * lambda_ + 1) ** (1 / lambda_) - 1
#             elif X < 0 and lambda_ != 2:
#                 X = 1 - (-(2 - lambda_) * X_trans + 1) ** (1 / (2 - lambda_))
#             elif X < 0 and lambda_ == 2:
#                 X = 1 - exp(-X_trans)
#         Parameters
#         ----------
#         X : array-like, shape (n_samples, n_features)
#             The transformed data.
#         Returns
#         -------
#         X : array-like, shape (n_samples, n_features)
#             The original data
#         """
#         check_is_fitted(self)
#         X = self._check_input(X, in_fit=False, check_shape=True)
#
#         if self.standardize:
#             X = self._scaler.inverse_transform(X)
#
#         inv_fun = {'box-cox': self._box_cox_inverse_tranform,
#                    'yeo-johnson': self._yeo_johnson_inverse_transform
#                    }[self.method]
#         for i, lmbda in enumerate(self.lambdas_):
#             with np.errstate(invalid='ignore'):  # hide NaN warnings
#                 X[:, i] = inv_fun(X[:, i], lmbda)
#
#         return X
#
#     def _box_cox_inverse_tranform(self, x, lmbda):
#         """Return inverse-transformed input x following Box-Cox inverse
#         transform with parameter lambda.
#         """
#         if lmbda == 0:
#             x_inv = np.exp(x)
#         else:
#             x_inv = (x * lmbda + 1) ** (1 / lmbda)
#
#         return x_inv
#
#     def _yeo_johnson_inverse_transform(self, x, lmbda):
#         """Return inverse-transformed input x following Yeo-Johnson inverse
#         transform with parameter lambda.
#         """
#         x_inv = np.zeros_like(x)
#         pos = x >= 0
#
#         # when x >= 0
#         if abs(lmbda) < np.spacing(1.):
#             x_inv[pos] = np.exp(x[pos]) - 1
#         else:  # lmbda != 0
#             x_inv[pos] = np.power(x[pos] * lmbda + 1, 1 / lmbda) - 1
#
#         # when x < 0
#         if abs(lmbda - 2) > np.spacing(1.):
#             x_inv[~pos] = 1 - np.power(-(2 - lmbda) * x[~pos] + 1,
#                                        1 / (2 - lmbda))
#         else:  # lmbda == 2
#             x_inv[~pos] = 1 - np.exp(-x[~pos])
#
#         return x_inv
#
#     def _yeo_johnson_transform(self, x, lmbda):
#         """Return transformed input x following Yeo-Johnson transform with
#         parameter lambda.
#         """
#
#         out = np.zeros_like(x)
#         pos = x >= 0  # binary mask
#
#         # when x >= 0
#         if abs(lmbda) < np.spacing(1.):
#             out[pos] = np.log1p(x[pos])
#         else:  # lmbda != 0
#             out[pos] = (np.power(x[pos] + 1, lmbda) - 1) / lmbda
#
#         # when x < 0
#         if abs(lmbda - 2) > np.spacing(1.):
#             out[~pos] = -(np.power(-x[~pos] + 1, 2 - lmbda) - 1) / (2 - lmbda)
#         else:  # lmbda == 2
#             out[~pos] = -np.log1p(-x[~pos])
#
#         return out
#
#     def _box_cox_optimize(self, x):
#         """Find and return optimal lambda parameter of the Box-Cox transform by
#         MLE, for observed data x.
#         We here use scipy builtins which uses the brent optimizer.
#         """
#         # the computation of lambda is influenced by NaNs so we need to
#         # get rid of them
#         _, lmbda = stats.boxcox(x[~np.isnan(x)], lmbda=None)
#
#         return lmbda
#
#     def _yeo_johnson_optimize(self, x):
#         """Find and return optimal lambda parameter of the Yeo-Johnson
#         transform by MLE, for observed data x.
#         Like for Box-Cox, MLE is done via the brent optimizer.
#         """
#
#         def _neg_log_likelihood(lmbda):
#             """Return the negative log likelihood of the observed data x as a
#             function of lambda."""
#             x_trans = self._yeo_johnson_transform(x, lmbda)
#             n_samples = x.shape[0]
#
#             loglike = -n_samples / 2 * np.log(x_trans.var())
#             loglike += (lmbda - 1) * (np.sign(x) * np.log1p(np.abs(x))).sum()
#
#             return -loglike
#
#         # the computation of lambda is influenced by NaNs so we need to
#         # get rid of them
#         x = x[~np.isnan(x)]
#         # choosing bracket -2, 2 like for boxcox
#         return optimize.brent(_neg_log_likelihood, brack=(-2, 2))
#
#     def _check_input(self, X, in_fit, check_positive=False, check_shape=False,
#                      check_method=False):
#         """Validate the input before fit and transform.
#         Parameters
#         ----------
#         X : array-like, shape (n_samples, n_features)
#         check_positive : bool
#             If True, check that all data is positive and non-zero (only if
#             ``self.method=='box-cox'``).
#         check_shape : bool
#             If True, check that n_features matches the length of self.lambdas_
#         check_method : bool
#             If True, check that the transformation method is valid.
#         """
#         X = self._validate_data(X, ensure_2d=True, dtype=FLOAT_DTYPES,
#                                 copy=self.copy, force_all_finite='allow-nan')
#
#         with np.warnings.catch_warnings():
#             np.warnings.filterwarnings(
#                 'ignore', r'All-NaN (slice|axis) encountered')
#             if (check_positive and self.method == 'box-cox' and
#                     np.nanmin(X) <= 0):
#                 raise ValueError("The Box-Cox transformation can only be "
#                                  "applied to strictly positive data")
#
#         if check_shape and not X.shape[1] == len(self.lambdas_):
#             raise ValueError("Input data has a different number of features "
#                              "than fitting data. Should have {n}, data has {m}"
#                              .format(n=len(self.lambdas_), m=X.shape[1]))
#
#         valid_methods = ('box-cox', 'yeo-johnson')
#         if check_method and self.method not in valid_methods:
#             raise ValueError("'method' must be one of {}, "
#                              "got {} instead."
#                              .format(valid_methods, self.method))
#
#         return X
#
#     def _more_tags(self):
#         return {'allow_nan': True}
#
#
# @_deprecate_positional_args
# def power_transform(X, method='yeo-johnson', *, standardize=True, copy=True):
#     """
#     Power transforms are a family of parametric, monotonic transformations
#     that are applied to make data more Gaussian-like. This is useful for
#     modeling issues related to heteroscedasticity (non-constant variance),
#     or other situations where normality is desired.
#     Currently, power_transform supports the Box-Cox transform and the
#     Yeo-Johnson transform. The optimal parameter for stabilizing variance and
#     minimizing skewness is estimated through maximum likelihood.
#     Box-Cox requires input data to be strictly positive, while Yeo-Johnson
#     supports both positive or negative data.
#     By default, zero-mean, unit-variance normalization is applied to the
#     transformed data.
#     Read more in the :ref:`User Guide <preprocessing_transformer>`.
#     Parameters
#     ----------
#     X : array-like, shape (n_samples, n_features)
#         The data to be transformed using a power transformation.
#     method : {'yeo-johnson', 'box-cox'}, default='yeo-johnson'
#         The power transform method. Available methods are:
#         - 'yeo-johnson' [1]_, works with positive and negative values
#         - 'box-cox' [2]_, only works with strictly positive values
#         .. versionchanged:: 0.23
#             The default value of the `method` parameter changed from
#             'box-cox' to 'yeo-johnson' in 0.23.
#     standardize : boolean, default=True
#         Set to True to apply zero-mean, unit-variance normalization to the
#         transformed output.
#     copy : boolean, optional, default=True
#         Set to False to perform inplace computation during transformation.
#     Returns
#     -------
#     X_trans : array-like, shape (n_samples, n_features)
#         The transformed data.
#     Examples
#     --------
#     >>> import numpy as np
#     >>> from sklearn.preprocessing import power_transform
#     >>> data = [[1, 2], [3, 2], [4, 5]]
#     >>> print(power_transform(data, method='box-cox'))
#     [[-1.332... -0.707...]
#      [ 0.256... -0.707...]
#      [ 1.076...  1.414...]]
#     """
#     pt = PowerTransformer(method=method, standardize=standardize, copy=copy)
#     return pt.fit_transform(X)
