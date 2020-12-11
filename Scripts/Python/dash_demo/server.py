# -*- coding: utf-8 -*-
# author: nicktrejo


import dash
import dash_bootstrap_components as dbc

external_stylesheets = [
                        # 'https://codepen.io/chriddyp/pen/bWLwgP.css',
                        dbc.themes.BOOTSTRAP,
                       ]
app = dash.Dash('TFM Nico', external_stylesheets=external_stylesheets,
                prevent_initial_callbacks=False, title='Dash Nico')

# DEBUG MODE:
DEBUG = True
