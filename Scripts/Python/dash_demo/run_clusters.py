# -*- coding: utf-8 -*-
# author: nicktrejo

# Execute with:
# $ python run_clusters.py

from graph_clusters import main as gph_cluster
from model import df_columns_1w
from time import sleep


CENTERS = [2, 3, 4, 5]

if __name__ == '__main__':
    inertia = []

    for ctr in [CENTERS[3]]:
        for col1 in df_columns_1w:
            for col2 in df_columns_1w:

                # Do not calculate for equal columns
                if col1 == col2:
                    continue
                mean_inertia = gph_cluster(col1, col2, ctr, False)
                inertia.append(mean_inertia)  # MEAN
                sleep(3)  # sleep for 3 seconds

    print(inertia)
