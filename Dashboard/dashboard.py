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
        dbc.Row([]),
        # Body - Tables
        html.Div([
            html.H3("📄 Tabelas", className="text-center bold title page-title")
        ])
    ])
])

# ===== Main =====
if __name__ == '__main__':
    app.run_server(debug=True)