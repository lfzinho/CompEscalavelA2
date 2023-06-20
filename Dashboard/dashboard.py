from dash import Dash, dash_table, dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import dashboard_data_collector as ddc

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
                    html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                ], className="info"),

                # Number of cars
                html.Div([
                    html.H3("🚗 Número de carros: ", className="infoText"),
                    html.H3("0", className="infoText", id="numCars"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                ], className="info"),

                # Number of over speed
                html.Div([
                    html.H3("🚨 Acima da velocidade: ", className="infoText"),
                    html.H3("0", className="infoText", id="numOverSpeed"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                ], className="info"),

                # Number of collisions risk
                html.Div([
                    html.H3("🚧 Risco de colisão: ", className="infoText"),
                    html.H3("0", className="infoText", id="numCollisionsRisk"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                ], className="info"),
            ]),
            # Body - Graph
            dbc.Col([
                dcc.Graph(
                    id='graph',
                    figure= ddc.get_general_graph()
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
                        html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                    ], className="info"),

                    # Table
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
                    )

                ]),

                # Body - Tables, Col 2
                dbc.Col([
                    # List of vehicles at risk of collision
                    html.Div([
                        html.H3("🚧 Lista de veículos em risco de colisão", className="bold titleTable"),
                        html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_collisions_risk',
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
                    )
                ])
            ]),

            dbc.Row([
                # Lista de carros proibidos de circular.
                dbc.Col([
                    html.Div([
                        html.H3("🚫 Lista de carros proibidos de circular", className="bold titleTable"),
                        html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_prohibited_cars',
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
                    )
                ]),

                # Lista de carros com direção perigosa
                dbc.Col([
                    html.Div([
                        html.H3("🚗 Lista de carros com direção perigosa", className="bold titleTable"),
                        html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_dangerous_driving',
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
                    )
                ])
            ]),

            dbc.Row([
                # Ranking dos TOP 100 veículos que passaram por mais rodovias.
                dbc.Col([
                    html.Div([
                        html.H3("🛣️ Ranking dos top 100 veículos", className="bold titleTable"),
                        html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_ranking',
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
                    )
                ]),
            ]),

            dbc.Row([
                # Tabela com estatísticas de cada rodovia
                dbc.Col([
                    html.Div([
                        html.H3("📊 Tabela com estatísticas de cada rodovia", className="bold titleTable"),
                        html.P("⏱️ Atualizado a 312ms atrás", className="infoTimeUpdate"),
                    ], className="info"),

                    # Table
                    dash_table.DataTable(
                        id='table_statistics',
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
                    )
                ])
            ])

        ])
    ])
])

# ===== Main =====
if __name__ == '__main__':
    app.run_server(debug=True)