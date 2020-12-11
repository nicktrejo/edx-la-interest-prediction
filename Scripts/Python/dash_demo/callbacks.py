# -*- coding: utf-8 -*-
# author: nicktrejo

from dash.dependencies import Input, Output
from flask import request
import numpy as np  # REFERENCE: https://numpy.org/doc/stable/reference/
import plotly.express as px  # REFERENCE: https://plotly.com/python-api-reference/plotly.express.html
import plotly.graph_objects as go

try:
    from .model import calculate_k_means, df, df_columns, df_columns_1w, df_dates, df_users
    from .server import app
except (ModuleNotFoundError, ImportError) as err:
    from model import calculate_k_means, df, df_columns, df_columns_1w, df_dates, df_users
    from server import app


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def get_figure(df, x_col, y_col, selectedpoints, selectedpoints_local):
    if selectedpoints_local and selectedpoints_local['range']:
        ranges = selectedpoints_local['range']
        selection_bounds = {'x0': ranges['x'][0], 'x1': ranges['x'][1],
                            'y0': ranges['y'][0], 'y1': ranges['y'][1]}
    else:
        selection_bounds = {'x0': np.min(df[x_col]), 'x1': np.max(df[x_col]),
                            'y0': np.min(df[y_col]), 'y1': np.max(df[y_col])}

    # set which points are selected with the `selectedpoints` property
    # and style those points with the `selected` and `unselected`
    # attribute. see
    # https://medium.com/@plotlygraphs/notes-from-the-latest-plotly-js-release-b035a5b43e21
    # for an explanation
    fig = px.scatter(df, x=df[x_col], y=df[y_col], text=df.index)

    fig.update_traces(selectedpoints=selectedpoints,
                      customdata=df.index,
                      mode='markers+text', marker={'color': 'rgba(0, 116, 217, 0.7)', 'size': 20}, unselected={'marker': {'opacity': 0.}, 'textfont': {'color': 'rgba(0, 0, 0, 0)'}})

    fig.update_layout(margin={'l': 20, 'r': 0, 'b': 15, 't': 5}, dragmode='select', hovermode=False)
    fig.add_shape(dict({'type': 'rect',
                        'line': {'width': 1, 'dash': 'dot', 'color': 'darkgrey'}},
                       **selection_bounds))
    return fig


#####################################################
# CALLBACKS (logic / controls)
#####################################################


@app.callback([Output('main-alert', 'children'),
               Output('main-alert', 'is_open')],
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/shutdown':
        print('Server shutting down...')
        shutdown_server()
        return 'Server shutting down...', True
    if pathname is None or pathname == '/':
        return None, False
    return pathname[1:], True


@app.callback(
    # Output(component_id='p-test', component_property='children'),
    Output(component_id='graph-one', component_property='figure'),
    [Input(component_id='user-selection', component_property='value'),
     Input(component_id='variable-selection', component_property='value')]
)
def update_graph_one(input_user, input_variable):
    print(f'USER: {input_user}\n'
          f'VARIABLE: {input_variable}', end='\n---\n')
    if not isinstance(input_user, list):
        _input_user = [input_user]
    else:
        _input_user = input_user
    _df = df[df['user_id'].isin(_input_user)]
    # fig = px.scatter(_df, x='dt_date', y=input_variable, color='enrollment_mode',
    #                  hover_name='user_id', title='Graph',
    #                  )
    fig = px.line(_df,
                  x='dt_date',
                  y=[input_variable.replace('_1w', ''), input_variable],
                  # color='enrollment_mode',
                  line_group='user_id')
    return fig


@app.callback(
    # Output(component_id='p-test', component_property='children'),
    Output(component_id='graph-two', component_property='figure'),
    [Input(component_id='user-selection-2', component_property='value'),
     Input(component_id='variable-selection-2', component_property='value'),
     Input(component_id='variable-selection-3', component_property='value'),
     Input(component_id='date-slider', component_property='value')]
)
def update_graph_two(input_user, input_variable1, input_variable2, input_date):
    print(f'USER: {input_user}\n'
          f'VARIABLE1: {input_variable1}\n',
          f'VARIABLE2: {input_variable2}\n',
          f'DATE: {input_date}', end='\n---\n')
    if not isinstance(input_user, list):
        _input_user = [input_user]
    else:
        _input_user = input_user
    _df = df[(df['user_id'].isin(_input_user)) &
             (df['dt_date'] == df_dates[input_date])]
    # fig = px.scatter(_df, x='dt_date', y=input_variable, color='enrollment_mode',
    #                  hover_name='user_id', title='Graph',
    #                  )
    fig = px.scatter(_df,
                     x=input_variable1,
                     y=input_variable2,
                     color='enrollment_mode',
                     # hover_data=['user_id'], title='test2', text='user_id',
                     # line_group='user_id',
                     )
    return fig


@app.callback(
    Output(component_id='testing', component_property='children'),
    [Input(component_id='graph-two', component_property='selectedData'),
     ]
)
def crossfiltering_test(selected_data):
    print(f'Selected Data:\n\t{selected_data}')
    return selected_data


# this callback defines 3 figures
# as a function of the intersection of their 3 selections
@app.callback(
    [Output('g1', 'figure'),
     Output('g2', 'figure'),
     Output('g3', 'figure')],
    [Input('g1', 'selectedData'),
     Input('g2', 'selectedData'),
     Input('g3', 'selectedData')]
)
def callback_test(selection1, selection2, selection3):
    df_test = df[df['user_id'] == df_users[0]].copy()
    selectedpoints = df_test.index  # df.index
    for selected_data in [selection1, selection2, selection3]:
        if selected_data and selected_data['points']:
            selectedpoints = np.intersect1d(selectedpoints,
                                            [p['customdata'] for p in selected_data['points']])

    return [get_figure(df_test,  # df,
                       df_columns[1], df_columns[2], selectedpoints, selection1),
            get_figure(df_test,  # df,
                       df_columns[1], df_columns[3], selectedpoints, selection2),
            get_figure(df_test,  # df,
                       df_columns[1], df_columns[4], selectedpoints, selection3)]


@app.callback(
    Output(component_id='graph-four', component_property='figure'),
    [Input(component_id='date-slider-test', component_property='value')]
)
def update_graph_four(input_date):
    date_ = df_dates[input_date]
    kmeans_ = calculate_k_means(df, date=date_)  # ,n_clusters=2
    # Optimal numer of clusters 'n_clusters'
    # https://www.datanovia.com/en/lessons/determining-the-optimal-number-of-clusters-3-must-know-methods/

    # kmeans_ = calculate_k_means(df[df['user_id']==df_users[:20]], date=date_)  # , n_clusters=3

    print(f'DATE: {date_} ({input_date})',
          end='\n---\n')

    # _df = df.copy()
    _df = df[df['dt_date'] == date_]
    # [['num_events_1w','connected_days_1w']]
    # _df = df[df['dt_date']==date_].copy()  # df[df['user_id']==df_users[
    # :20]].copy()
    # fig = px.scatter(_df, x='dt_date', y=input_variable, color='enrollment_mode',
    #                  hover_name='user_id', title='Graph',
    #                  )

    # Represent all users
    # trace0 = go.Scatter(_df,
    #                  x='num_events_1w',
    #                  y='connected_days_1w',
    #                  color='enrollment_mode',
    #                  # line_group='user_id'
    #
    #     x=df_X_reduced[0],
    #                  y=df_X_reduced[1],
    #                  text=df.index,
    #                  name='',
    #                  mode='markers',
    #                  marker=pgo.Marker(size=df['tpop10'],
    #                                    sizemode='diameter',
    #                                    sizeref=df['tpop10'].max()/50,
    #                                    opacity=0.5,
    #                                    color=Z),
    #                  showlegend=False
    # )

    fig = px.scatter(_df,
                     x='num_events_1w',
                     y='connected_days_1w',
                     color='enrollment_mode',
                     # line_group='user_id'
                     )

    fig.add_trace(go.Scatter(x=kmeans_.cluster_centers_[:, 0],
                             y=kmeans_.cluster_centers_[:, 1],
                             name='',
                             mode='markers',
                             marker=go.scatter.Marker(symbol='x', size=12, color=1/3),
                             # marker=go.Marker(symbol='x',
                             #                  size=12,
                             #                  color=range(n_clusters)),
                             showlegend=False
                             )
                  )

    return fig
