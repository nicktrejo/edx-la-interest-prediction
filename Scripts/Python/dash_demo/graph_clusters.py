# -*- coding: utf-8 -*-
# author: nicktrejo

import argparse  # https://docs.python.org/3.6/howto/argparse.html
import os

import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import StandardScaler

from model import calculate_k_means, df, df_columns_1w, df_dates, reorder
# from model import calculate_k_means, df, df_columns, df_columns_1w, df_dates, df_users, reorder, distance
FOLDER = '/home/nicolas/Documents/MasterII/TFM/Proyecto/edx-la-interest' \
         '-prediction/Scripts/Python/dash_demo/assets/clusters/'


def parse_arguments(columns):
    # Handle arguments:
    # https://docs.python.org/3.6/howto/argparse.html
    # http://zetcode.com/python/argparse/
    parser = argparse.ArgumentParser()
    # https://blog.cambridgespark.com/how-to-determine-the-optimal-number-of-clusters-for-k-means-clustering-14f27070048f
    parser.add_argument('-n', '--n_clusters', type=int, help='Number of clusters (default=3)',
                        default=3, choices=[2, 3, 4, 5, 6, 7, 8, 9, 10])
    parser.add_argument('--col1', type=str, help='Name of first column',
                        default=columns[0], choices=columns)  # default: num_events_1w
    parser.add_argument('--col2', type=str, help='Name of second column',
                        default=columns[12], choices=columns)  # default: connected_days_1w
    parser.add_argument('--scale', '-s', help="Don't scale values",
                        action='store_true')  # default: None
    # args = parser.parse_args()
    return parser.parse_args()


def main(col1, col2, n_clusters, scale):
    # Check correct input values
    if col1 == col2:
        raise Exception('Columns must be different')

    # Scale df using StandardScaler (other option: MinMaxScaler)
    # https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html
    if not scale:
        scaler = StandardScaler()
        df[df.columns[2:]] = scaler.fit_transform(df[df.columns[2:]])

    try:
        k_means = [calculate_k_means(df, df_date, n_clusters=n_clusters,
                                     col1=col1, col2=col2) for
                   df_date in df_dates]
        # clusteres = np.array([calculate_k_means(df, df_date,
        #                                         n_clusters=n_clusters,
        #                                         col1=col1,
        #                                         col2=col2
        #                                         ).cluster_centers_ for
        #                      df_date in df_dates])
    except ValueError:
        print('Not able to calculate ALL, just working on first 490 dates')
        # CALCULATE CLUSTERS FOR FIRST 490 DATES (then it gives error)
        # https://blog.cambridgespark.com/how-to-determine-the-optimal-number-of-clusters-for-k-means-clustering-14f27070048f

        k_means = [calculate_k_means(df, df_date, n_clusters=n_clusters,
                                     col1=col1, col2=col2) for
                   df_date in df_dates[:490]]
        # clusteres = np.array([calculate_k_means(df, df_date,
        #                                         n_clusters=n_clusters).cluster_centers_ for
        #                       df_date in df_dates[:490]])

    inertia = np.array([k_mean.inertia_ for k_mean in k_means])
    clusteres = np.array([k_mean.cluster_centers_ for k_mean in k_means])

    # Reorder the clusters twice to avoid local problems
    clusteres = reorder(clusteres)
    clusteres = reorder(clusteres, False)

    # SCATTER GRAPH
    # https://matplotlib.org/3.3.2/api/_as_gen/matplotlib.pyplot.scatter.html
    colors_list = [[i for i in range(clusteres.shape[1])] for _ in range(clusteres.shape[0])]
    colors = np.array(colors_list)

    clusteres_mean = np.mean(clusteres, (0))
    plt.scatter(clusteres[:, :, 0], clusteres[:, :, 1], c=colors, s=6)   # All clusteres
    plt.scatter(clusteres_mean[:, 0], clusteres_mean[:, 1], marker='+', s=100, c='red')   # mean clusteres
    plt.xlabel(col1)
    plt.ylabel(col2)

    mean_inertia = np.mean(inertia)

    file_name = f'{col1}_{col2}_{n_clusters}centers_{int(mean_inertia)}.png'

    # Comment the following line to show graph instead of saving
    save_file = os.path.join(FOLDER, file_name)

    if save_file:
        plt.savefig(save_file)
        plt.close()
    else:
        plt.show()
    return mean_inertia


if __name__ == '__main__':
    args = parse_arguments(df_columns_1w)
    inertia = main(args.col1, args.col2, args.n_clusters, args.scale)
