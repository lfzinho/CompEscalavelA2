from dash import Dash, dash_table, dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

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
                    html.H3("🛣️ Número de rodovias: 0", className="bold title"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="update-time"),
                ], className="info"),

                # Number of cars
                html.Div([
                    html.H3("🚗 Número de carros: 0", className="bold title"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="update-time"),
                ], className="info"),

                # Number of over speed
                html.Div([
                    html.H3("🚨 Acima da velocidade: 0", className="bold title"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="update-time"),
                ], className="info"),

                # Number of collisions risk
                html.Div([
                    html.H3("🚧 Risco de colisão: 0", className="bold title"),
                    html.P("⏱️ Atualizado a 312ms atrás", className="update-time"),
                ], className="info"),
            ]),
            # Body - Graph
            dbc.Col([
            ])
        ]),
        # Body - Tables
        html.Div([
            html.H3("📄 Tabelas", className="text-center bold title page-title"),
            dbc.Row([
                # Body - Tables, Col 1
                dbc.Col([]),
                # Body - Tables, Col 2
                dbc.Col([])
            ])
        ])
    ])
])

# ===== Main =====
if __name__ == '__main__':
    app.run_server(debug=True)