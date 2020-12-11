# -*- coding: utf-8 -*-
# author: nicktrejo

# Run this app with `python3 dash_nico.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
# import plotly.express as px

try:
    from .model import df, df_columns_1w, df_dates, df_users
    from .server import app, DEBUG
    from . import callbacks
except ModuleNotFoundError:
    from model import df, df_columns_1w, df_dates, df_users
    from server import app, DEBUG
    import callbacks

selectedpoints_local_test = dict()
if DEBUG:
    selectedpoints_local_test = {
        'points': [{'curveNumber': 0, 'pointNumber': 7, 'pointIndex': 7, 'x': 20, 'y': 0.8571428571428571, 'text': 3785517, 'customdata': [3785517]},
                   {'curveNumber': 0, 'pointNumber': 12, 'pointIndex': 12,  'x': 20.142857142857142, 'y': 2, 'text': 6066180, 'customdata': [6066180]},
                   {'curveNumber': 0, 'pointNumber': 22, 'pointIndex': 22, 'x': 7.571428571428571, 'y': 1.4285714285714286, 'text': 15958339, 'customdata': [15958339]},
                   {'curveNumber': 0, 'pointNumber': 43, 'pointIndex': 43, 'x': 11.428571428571427, 'y': 1.8571428571428568, 'text': 23785094, 'customdata': [23785094]},
                   {'curveNumber': 0, 'pointNumber': 47, 'pointIndex': 47, 'x': 7.571428571428571, 'y': 0.5714285714285714, 'text': 24043615, 'customdata': [24043615]}],
        'range': {'x': [5.396095328167589, 25.70762180791837], 'y': [-0.062319393695973606, 4.234920521378336]}}


# see https://plotly.com/python/px-arguments/ for more options

# https://plotly.com/python-api-reference/generated/plotly.express.scatter.html
# PARAMETERS FOR plotly.express.scatter:
# ['data_frame', 'x', 'y', 'color', 'symbol', 'size', 'hover_name',
# 'hover_data', 'custom_data', 'text', 'facet_row', 'facet_col',
# 'facet_col_wrap', 'facet_row_spacing', 'facet_col_spacing', 'error_x',
# 'error_x_minus', 'error_y', 'error_y_minus', 'animation_frame',
# 'animation_group', 'category_orders', 'labels', 'orientation',
# 'color_discrete_sequence', 'color_discrete_map', 'color_continuous_scale',
# 'range_color', 'color_continuous_midpoint', 'symbol_sequence',
# 'symbol_map', 'opacity', 'size_max', 'marginal_x', 'marginal_y',
# 'trendline', 'trendline_color_override', 'log_x', 'log_y', 'range_x',
# 'range_y', 'render_mode', 'title', 'template', 'width', 'height']


#####################################################
# FIGURES
#####################################################

# fig1 = px.scatter(df, x='dt_date', y='num_events', color='enrollment_mode',
#                   hover_data=['user_id'], title='test', text='user_id',
#                  )

# fig2 = px.scatter(df[df['dt_date']==df_dates[0]],
#                   x='num_events_1w', y='num_sessions_1w',
#                   color='enrollment_mode', hover_data=['user_id'],
#                   title='test2', text='user_id',
#                  )


#####################################################
# LAYOUT / VIEWS
#####################################################


PLOTLY_LOGO = 'plotly-logomark.png'  # 'https://images.plot.ly/logo/new-branding/plotly-logomark.png'

search_bar = dbc.Row(
    [
        dbc.Col(dbc.Input(type='search', placeholder='Search (not available)')),
        dbc.Col(
            dbc.Button('Search', color='primary', disabled=True,
                       className='ml-2'),
            width='auto',
        ),
    ],
    no_gutters=True,
    className='ml-auto flex-nowrap mt-3 mt-md-0',
    align='center',
)

navbar = dbc.Navbar(
    [
        html.A(
            # Use row and col to control vertical alignment of logo / brand
            dbc.Row(
                [
                    dbc.Col(html.Img(src=app.get_asset_url(PLOTLY_LOGO), height='40px')),
                    dbc.Col(dbc.NavbarBrand('Dash Nico', className='ml-2')),
                ],
                align='center',
                no_gutters=True,
            ),
            href='#',
        ),
        dbc.NavbarToggler(id='navbar-toggler'),
        dbc.Collapse(search_bar, id='navbar-collapse', navbar=True),
    ],
    color='dark',
    dark=True,
)

app.layout = html.Div(children=[
    dcc.Location(id='url', refresh=False),
    navbar,
    dbc.Container([
        dbc.Alert('', id='main-alert', color='info',
                  dismissable=True, is_open=False),

        html.Div('', id='testing'),
        html.H1('Dash - Nico'),
        html.H3('''
            Dash: A web application framework for Python.
        '''),
        html.Hr(),

        # Section 1
        html.H4('''Section 1'''),
        html.Label(['Select user',
                    dcc.Dropdown(id='user-selection',
                                 options=[{'label': i, 'value': i} for i in df_users],
                                 value=58798,  # value=df_users[0],
                                 multi=True, ), ]),

        html.Label(['Select variable',
                    dcc.Dropdown(id='variable-selection',
                                 options=[{'label': i, 'value': i} for i in df_columns_1w],
                                 value=df_columns_1w[0],),
                    ]),

        dcc.Graph(
            id='graph-one',
            figure=None,  # fig1
        ),
        html.Hr(),

        # Section 2
        html.H4('''Section 2'''),
        html.Label(['Select user',
                    dcc.Dropdown(id='user-selection-2',
                                 options=[{'label': i, 'value': i} for i in df_users],
                                 value=list(df_users[:40]),  # value=df_users[0],
                                 multi=True, ), ]),
        html.Label(['Select variable',
                    dcc.Dropdown(id='variable-selection-2',
                                 options=[{'label': i, 'value': i} for i in df_columns_1w],
                                 value=df_columns_1w[0],),
                    ]),
        html.Label(['Select variable',
                    dcc.Dropdown(id='variable-selection-3',
                                 options=[{'label': i, 'value': i} for i in df_columns_1w],
                                 value=df_columns_1w[1],),
                    ]),
        dcc.Graph(
            id='graph-two',
            figure=None,  # fig2
        ),
        dcc.Slider(
            id='date-slider',
            min=1,  # df_dates.min(),
            max=len(df_dates),  # df_dates.max(),
            value=1,  # df_dates.min(),
            marks={30*num: str(df_date)for num, df_date in enumerate(df_dates[::30])},
            # marks={str(df_date): str(df_date) for df_date in df_dates[:100]},
            # step=None
        ),
        html.Hr(),

        # Section 3
        html.H4('''Section 3'''),
        html.Div([
            html.Div(
                dcc.Graph(figure=callbacks.get_figure(df, 'user_id',
                                                      df_columns_1w[1], [1], selectedpoints_local_test),
                          id='g1', config={'displayModeBar': False}),
                className='six columns'
            ),
            html.Div(
                dcc.Graph(id='g2', config={'displayModeBar': False}),
                className='four columns'
                ),
            html.Div(
                dcc.Graph(id='g3', config={'displayModeBar': False}),
                className='three columns'
            )
        ], className='row'),
        html.Hr(),

        # Section 4
        html.H4('''Section 4'''),
        html.Label(['Select user',
                    dcc.Dropdown(id='user-selection-test',
                                 options=[{'label': i, 'value': i} for i in df_users],
                                 value=list(df_users[:40]),  # value=df_users[0],
                                 multi=True, ), ]),
        html.Label(['Select variable',
                    dcc.Dropdown(id='variable-selection-test2',
                                 options=[{'label': i, 'value': i} for i in df_columns_1w],
                                 value=df_columns_1w[0],),
                    ]),
        html.Label(['Select variable',
                    dcc.Dropdown(id='variable-selection-test3',
                                 options=[{'label': i, 'value': i} for i in df_columns_1w],
                                 value=df_columns_1w[1],),
                    ]),
        dcc.Graph(
            id='graph-four',
            figure=None,  # fig2
        ),
        dcc.Slider(
            id='date-slider-test',
            min=1,  # df_dates.min(),
            max=len(df_dates),  # df_dates.max(),
            value=100,  # df_dates.min(),  # 1
            marks={30*num: str(df_date)for num, df_date in enumerate(df_dates[::30])},
            # marks={str(df_date): str(df_date) for df_date in df_dates[:100]},
            # step=None
        ),
        html.Hr(),


    ], className='first-container')
])


#####################################################
# Debbuging mode
#####################################################

if __name__ == '__main__':
    app.run_server(debug=DEBUG, dev_tools_hot_reload=True,
                   dev_tools_hot_reload_interval=10,
                   dev_tools_hot_reload_watch_interval=3,
                   )

# else:
#     app.run_server(debug=False)
