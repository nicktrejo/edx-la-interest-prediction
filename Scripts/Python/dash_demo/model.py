# -*- coding: utf-8 -*-
# author: nicktrejo

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

FOLDER_PATH = '/home/nicolas/ACCOMP/OUTPUT-2020-08-25/'
ALL_INDICATORS_FILTERED = FOLDER_PATH + 'allIndicators_Filtered.csv'
ALL_INDICATORS_FILTERED_SMOOTHED = ALL_INDICATORS_FILTERED.replace('.csv',
                                                                   '_smoothed.csv')

df = pd.read_csv(ALL_INDICATORS_FILTERED_SMOOTHED)
df.sort_values(by=['user_id', 'dt_date'], inplace=True, ignore_index=True)
df['enrollment_mode'] = df['enrollment_mode'].apply(str)

df_users = list(df['user_id'].unique())
df_dates = df['dt_date'].unique()
df_columns = list(df.columns)
df_columns_1w = [col for col in df.columns if '_1w' in col]
# df_enrollment = list(df['enrollment_mode'].unique())


#################################
# CLUSTER with KMEANS
#################################

# ADD PCA
# https://plotly.com/python/v3/ipython-notebooks/baltimore-vital-signs/
# from sklearn.decomposition import PCA
# from sklearn.preprocessing import StandardScaler


def distance(a, b) -> float:
    """
    Calculate Euclidean distance of 2 numpy arrays

    :param a: first array
    :param b: second array
    :type a: numpy.ndarray
    :type b: numpy.ndarray
    :rtype: float
    """
    return np.linalg.norm(a-b)


def _reorder_aux(array1, array2) -> None:
    """
    Calculate matching order

    :param array1: first array
    :param array2: second array
    :type array1: numpy.ndarray
    :type array2: numpy.ndarray
    :rtype: list
    """
    # Build distance matrix
    matrix_dist = np.array([[distance(array1_, array2_) for array1_ in
                             array1] for array2_ in array2])
    # Find smallest number and save its location
    res = []
    for i in range(len(array1)):
        res.append( np.where(matrix_dist == np.amin(matrix_dist)) )
        # Replace used values with max
        matrix_dist[res[-1][0]] = np.amax(matrix_dist)
        matrix_dist[:,res[-1][1]] = np.amax(matrix_dist)
    return res


def reorder(array1, use_first_as_media=True) -> np.ndarray:
    """
    Reorder numpy array

    :param array1: first array
    :param array2: second array
    :type array1: numpy.ndarray
    :type array2: numpy.ndarray
    :rtype: numpy.ndarray
    """
    # Crear array de respuesta
    ordered_array = np.empty_like(array1)
    ordered_array[0] = array1[0]
    # Media de los clusters
    if use_first_as_media:
        cluster_centers_mean = array1[0]
    else:
        cluster_centers_mean = np.mean(array1, (0))
    # Iterar 1 a 1 reordenando y actualizar media de los clusters
    for i, array1_ in enumerate(array1[1:], start=1):
        order = _reorder_aux(cluster_centers_mean, array1_)
        for order_ in order:
            ordered_array[i][order_[1]] = array1_[order_[0]]
        cluster_centers_mean = (cluster_centers_mean * i + ordered_array[i]) / (i+1)
    return ordered_array


def calculate_k_means(df, date, n_clusters=3, col1='num_events_1w', col2='connected_days_1w'):
    # df_ = df[(df['dt_date']==date)
    #          # & (df['user_id']==df_users[:20])
    #         ]['num_events_1w', 'connected_days_1w'].copy()
    df_ = df[(df['dt_date'] == date) &
             (df[[col1, col2]].notnull().all(axis='columns'))
             ][[col1, col2]].copy()

    # X = np.array(df_)
    kmeans = KMeans(init='k-means++',
                    n_clusters=n_clusters,  # change this
                    verbose=0,  # 1,
                    n_jobs=4, # or 4
                    n_init=10)
    kmeans.fit(df_)
    return kmeans

# kmeans.fit(df[])
#
# # kmeans.labels_
# # kmeans.cluster_centers_
#
#
#
# import numpy as np
# import matplotlib.pyplot as plt
#
#
# from sklearn.datasets import make_blobs
#
# n_samples = 1500
# random_state = 170
# X, y = make_blobs(n_samples=n_samples, random_state=random_state)
#
# # Incorrect number of clusters
# y_pred = KMeans(n_clusters=2, random_state=random_state).fit_predict(X)
#
# plt.subplot(221)
# plt.scatter(X[:, 0], X[:, 1], c=y_pred)
# plt.title("Incorrect Number of Blobs")
#
# # Anisotropicly distributed data
# transformation = [[0.60834549, -0.63667341], [-0.40887718, 0.85253229]]
# X_aniso = np.dot(X, transformation)
# y_pred = KMeans(n_clusters=3, random_state=random_state).fit_predict(X_aniso)
#
# plt.subplot(222)
# plt.scatter(X_aniso[:, 0], X_aniso[:, 1], c=y_pred)
# plt.title("Anisotropicly Distributed Blobs")
#
# # Different variance
# X_varied, y_varied = make_blobs(n_samples=n_samples,
#                                 cluster_std=[1.0, 2.5, 0.5],
#                                 random_state=random_state)
# y_pred = KMeans(n_clusters=3, random_state=random_state).fit_predict(X_varied)
#
# plt.subplot(223)
# plt.scatter(X_varied[:, 0], X_varied[:, 1], c=y_pred)
# plt.title("Unequal Variance")
#
# # Unevenly sized blobs
# X_filtered = np.vstack((X[y == 0][:500], X[y == 1][:100], X[y == 2][:10]))
# y_pred = KMeans(n_clusters=3,
#                 random_state=random_state).fit_predict(X_filtered)
#
# plt.subplot(224)
# plt.scatter(X_filtered[:, 0], X_filtered[:, 1], c=y_pred)
# plt.title("Unevenly Sized Blobs")
#
# plt.show()
