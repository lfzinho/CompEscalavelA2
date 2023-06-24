from dash import Dash, dash_table, dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import dashboard_data_collector as ddc
import time

# ===== Constants =====
DEBUG = True
GRAPH_UPDATE_INTERVAL = 1000 # ms
NUMROADS_UPDATE_INTERVAL = 1000 # ms
NUMCARS_UPDATE_INTERVAL = 500 # ms
NUMOVERSPEED_UPDATE_INTERVAL = 100 # ms
NUMCOLLISIONSRISK_UPDATE_INTERVAL = 50 # ms

NUMLISTOVERSPEED_UPDATE_INTERVAL = 1000 # ms
NUMLISTCOLLISIONSRISK_UPDATE_INTERVAL = 1000 # ms
NUMLISTBANNEDCARS_UPDATE_INTERVAL = 1000 # ms
NUMLISTDANGEROUSCARS_UPDATE_INTERVAL = 1000 # ms
NUMLISTTOP100_UPDATE_INTERVAL = 1000 # ms
NUMLISTROADS_UPDATE_INTERVAL = 1000 # ms

# ===== App Layout =====

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], update_title=None)


app.layout = html.Div([
    # Header
    dbc.Row(className="header", justify="center", children=[
        dbc.Col([
            html.H3("🚗 Dashboard - Grupo Top", className="headerElement text-center bold"),
        ], width=8),
    ]),
    # Body 
    html.Div(className="body", children=[
        # Body - Info & Graph
        dbc.Row([
            # Body - Info
            dbc.Col([
                # Number of roads
                html.Div([
                    html.H3("🛣️ Número de rodovias: ", className="infoText"),
                    html.H3("0", className="infoText", id="numRoads"),
                    dcc.Interval(
                        id='numRoads-update-interval',
                        interval=NUMROADS_UPDATE_INTERVAL
                    ),
                    html.Br(),
                    html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                    html.P("312", className="infoTimeUpdate", id="numRoadsTime"),
                    html.P("ms atrás", className="infoTimeUpdate"),
                    html.Div(className="space"),
                ], className="info"),

                # Number of cars
                html.Div([
                    html.H3("🚗 Número de carros: ", className="infoText"),
                    html.H3("0", className="infoText", id="numCars"),
                    dcc.Interval(
                        id='numCars-update-interval',
                        interval=NUMCARS_UPDATE_INTERVAL
                    ),
                    html.Br(),
                    html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                    html.P("312", className="infoTimeUpdate", id="numCarsTime"),
                    html.P("ms atrás", className="infoTimeUpdate"),
                    html.Div(className="space"),
                ], className="info"),

                # Number of over speed
                html.Div([
                    html.H3("🚨 Acima da velocidade: ", className="infoText"),
                    html.H3("0", className="infoText", id="numOverSpeed"),
                    dcc.Interval(
                        id='numOverSpeed-update-interval',
                        interval=NUMOVERSPEED_UPDATE_INTERVAL
                    ),
                    html.Br(),
                    html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                    html.P("312", className="infoTimeUpdate", id="numOverSpeedTime"),
                    html.P("ms atrás", className="infoTimeUpdate"),
                    html.Div(className="space"),
                ], className="info"),

                # Number of collisions risk
                html.Div([
                    html.H3("🚧 Risco de colisão: ", className="infoText"),
                    html.H3("0", className="infoText", id="numCollisionsRisk"),
                    dcc.Interval(
                        id='numCollisionsRisk-update-interval',
                        interval=NUMCOLLISIONSRISK_UPDATE_INTERVAL
                    ),
                    html.Br(),
                    html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                    html.P("312", className="infoTimeUpdate", id="numCollisionsRiskTime"),
                    html.P("ms atrás", className="infoTimeUpdate"),
                    html.Div(className="space"),
                ], className="info"),
            ]),
            # Body - Graph
            dbc.Col([
                dcc.Graph(
                    id='graph',
                    figure= ddc.get_general_graph()
                ),
                dcc.Interval(
                    id='graph-update-interval',
                    interval=GRAPH_UPDATE_INTERVAL
                )
            ])
        ]),
        # Body - Tables
        html.Div([
            html.H3("📄 Tabelas", className="text-center bold title pageTitle"),
            dbc.Row([
                # Body - Tables, Col 1
                dbc.Col([
                    # List of over speed vehicles
                    html.Div([
                        html.H3("🚨 Lista de veículos acima da velocidade", className="bold titleTable"),
                        html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                        html.P("312", className="infoTimeUpdate", id="numListSpeedTime"),
                        html.P("ms atrás", className="infoTimeUpdate"),
                        html.Div(className="space"),
                    ], className="info"),

                    dash_table.DataTable(
                        id='table_over_speed',
                        columns=[{"name": i, "id": i} for i in ['Placa', 'Velocidade', 'Risco de colisão']],
                        data=[],
                        style_cell={'textAlign': 'center'},
                        style_header={
                            'backgroundColor': 'white',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ]
                    ),
                    dcc.Interval(
                    id='interval-ListSpeed',
                    interval=NUMLISTOVERSPEED_UPDATE_INTERVAL,
                    ),
                ]),

                # Body - Tables, Col 2
                dbc.Col([
                    # List of vehicles at risk of collision
                    html.Div([
                        html.H3("🚧 Lista de veículos em risco de colisão", className="bold titleTable"),
                        html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                        html.P("312", className="infoTimeUpdate", id="numListCollisionsRisk"),
                        html.P("ms atrás", className="infoTimeUpdate"),
                        html.Div(className="space"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_list_collisions_risk',
                        columns=[{"name": i, "id": i} for i in ['Placa', 'Velocidade']],
                        data=[],
                        style_cell={'textAlign': 'center'},
                        style_header={
                            'backgroundColor': 'white',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ]
                    ),

                    dcc.Interval(
                    id='interval-ListCollisionsRisk',
                    interval=NUMLISTCOLLISIONSRISK_UPDATE_INTERVAL,
                    ),
                ])
            ]),

            dbc.Row([
                # Lista de carros proibidos de circular.
                dbc.Col([
                    html.Div([
                        html.H3("🚫 Lista de carros proibidos de circular", className="bold titleTable"),
                        html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                        html.P("312", className="infoTimeUpdate", id="numListBannedCars"),
                        html.P("ms atrás", className="infoTimeUpdate"),
                        html.Div(className="space"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_list_banned_cars',
                        columns=[{"name": i, "id": i} for i in ['Placa']],
                        data=[],
                        style_cell={'textAlign': 'center'},
                        style_header={
                            'backgroundColor': 'white',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ]
                    ),

                    dcc.Interval(
                    id='interval-ListBannedCars',
                    interval=NUMLISTBANNEDCARS_UPDATE_INTERVAL,
                    ),
                ]),

                # Lista de carros com direção perigosa
                dbc.Col([
                    html.Div([
                        html.H3("🚗 Lista de carros com direção perigosa", className="bold titleTable"),
                        html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                        html.P("312", className="infoTimeUpdate", id="numListDangerousCars"),
                        html.P("ms atrás", className="infoTimeUpdate"),
                        html.Div(className="space"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_list_dangerous_cars',
                        columns=[{"name": i, "id": i} for i in ['Placa']],
                        data=[],
                        style_cell={'textAlign': 'center'},
                        style_header={
                            'backgroundColor': 'white',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ]
                    ),

                    dcc.Interval(
                    id='interval-ListDangerousCars',
                    interval=NUMLISTDANGEROUSCARS_UPDATE_INTERVAL,
                    ),
                ])
            ]),

            dbc.Row([
                # Ranking dos TOP 100 veículos que passaram por mais rodovias.
                dbc.Col([
                    html.Div([
                        html.H3("🛣️ Ranking dos top 100 veículos", className="bold titleTable"),
                        html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                        html.P("312", className="infoTimeUpdate", id="numListTop100"),
                        html.P("ms atrás", className="infoTimeUpdate"),
                        html.Div(className="space"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_list_top_100',
                        columns=[{"name": i, "id": i} for i in ['Placa', 'Número de rodovias']], # não precisa ter esse número de rodivia, a posição no ranking jé é suficiente
                        data=[],
                        style_cell={'textAlign': 'center'},
                        style_header={
                            'backgroundColor': 'white',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ]
                    ),
                    
                    dcc.Interval(
                    id='interval-Top100',
                    interval=NUMLISTTOP100_UPDATE_INTERVAL,
                    ),
                ]),
            ]),

            dbc.Row([
                # Tabela com estatísticas de cada rodovia
                dbc.Col([
                    html.Div([
                        html.H3("📊 Tabela com estatísticas de cada rodovia", className="bold titleTable"),
                        html.P("⏱️ Atualizado a ", className="infoTimeUpdate"),
                        html.P("312", className="infoTimeUpdate", id="numListRoads"),
                        html.P("ms atrás", className="infoTimeUpdate"),
                        html.Div(className="space"),
                    ], className="info"),
                    # Table
                    dash_table.DataTable(
                        id='table_list_roads',
                        columns=[{"name": i, "id": i} for i in ['Rodovia', 'Velocidade média dos carros', 'Tempo médio de travessia', 'Número de acidentes']],
                        data=[],
                        style_cell={'textAlign': 'center'},
                        style_header={
                            'backgroundColor': 'white',
                            'fontWeight': 'bold'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }
                        ]
                    ),

                    dcc.Interval(
                    id='interval-Roads',
                    interval=NUMLISTROADS_UPDATE_INTERVAL,
                    ),
                ])
            ])

        ])
    ])
])

# ===== Callbacks =====
# Callbacks - Graph
@app.callback(
    Output('graph', 'figure'),
    [Input('graph-update-interval', 'n_intervals')])
def update_graph(n):
    return ddc.get_general_graph()

# Callbacks - Num Roads
@app.callback(
    [Output('numRoads', 'children'),
     Output('numRoadsTime', 'children')],
    [Input('numRoads-update-interval', 'n_intervals')])
def update_num_roads(n):
    event_time, value = ddc.get_n_roads()
    return value, round(time.time() - event_time)

# Callbacks - Num Cars
@app.callback(
    [Output('numCars', 'children'),
     Output('numCarsTime', 'children')],
    [Input('numCars-update-interval', 'n_intervals')])
def update_num_cars(n):
    event_time, value = ddc.get_n_cars()
    return value, round(time.time() - event_time)

# Callbacks - Num Over Speed
@app.callback(
    [Output('numOverSpeed', 'children'),
     Output('numOverSpeedTime', 'children')],
    [Input('numOverSpeed-update-interval', 'n_intervals')])
def update_num_over_speed(n):
    event_time, value = ddc.get_n_over_speed()
    return value, round(time.time() - event_time)

# Callbacks - Num 
@app.callback(
    [Output('numCollisionsRisk', 'children'),
     Output('numCollisionsRiskTime', 'children')],
    [Input('numCollisionsRisk-update-interval', 'n_intervals')])
def update_num_over_speed(n):
    event_time, value = ddc.get_n_collisions_risk()
    return value, round(time.time() - event_time)

@app.callback(
    [Output('table_over_speed', 'data'),
    Output('numListSpeedTime', 'children')],
    [Input('interval-ListSpeed', 'n_intervals')])
def update_table(n_intervals):
    event_time, value = ddc.get_list_over_speed()
    value = value.to_dict('records')
    return value, round(time.time() - event_time)

@app.callback(
    [Output('table_list_collisions_risk', 'data'),
    Output('numListCollisionsRisk', 'children')],
    [Input('interval-ListCollisionsRisk', 'n_intervals')])
def update_table(n_intervals):
    event_time, value = ddc.get_list_collisions_risk()
    value = value.to_dict('records')
    return value, round(time.time() - event_time)


@app.callback(
    [Output('table_list_banned_cars', 'data'),
    Output('numListBannedCars', 'children')],
    [Input('interval-ListBannedCars', 'n_intervals')])
def update_table(n_intervals):
    event_time, value = ddc.get_list_banned_cars()
    value = value.to_dict('records')
    return value, round(time.time() - event_time)

@app.callback(
    [Output('table_list_dangerous_cars', 'data'),
    Output('numListDangerousCars', 'children')],
    [Input('interval-ListDangerousCars', 'n_intervals')])
def update_table(n_intervals):
    event_time, value = ddc.get_list_dangerous_cars()
    value = value.to_dict('records')
    return value, round(time.time() - event_time)

@app.callback(
    [Output('table_list_top_100', 'data'),
    Output('numListTop100', 'children')],
    [Input('interval-Top100', 'n_intervals')])
def update_table(n_intervals):
    event_time, value = ddc.get_top_100()
    value = value.to_dict('records')
    return value, round(time.time() - event_time)

@app.callback(
    [Output('table_list_roads', 'data'),
    Output('numListRoads', 'children')],
    [Input('interval-Roads', 'n_intervals')])
def update_table(n_intervals):
    event_time, value = ddc.get_list_roads()
    value = value.to_dict('records')
    return value, round(time.time() - event_time)


# ===== Main =====
if __name__ == '__main__':
    app.run_server(debug=DEBUG, port=8051)