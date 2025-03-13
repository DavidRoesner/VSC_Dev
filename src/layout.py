"""Defines the dash layout.

UI-Components are created, configured and customized.
"""

import dash
import dash_ag_grid as dag
from dash import dcc, html

app = dash.Dash(__name__)
app.layout = html.Div(
    [
        html.Div(
            children=[
                html.Img(
                    src="https://www.bix-consulting.com/wp-content/uploads/2020/07/biX_Logo_transparent_neu.png",
                    style={"height": "50px", "width": "auto", "position": "absolute", "top": "20px", "left": "20px"},
                )
            ],
            style={"position": "relative"},
        ),
        html.Div(
            children=[
                html.Img(
                    src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png",
                    style={"height": "50px", "width": "auto", "position": "absolute", "top": "20px", "right": "20px"},
                )
            ],
            style={"position": "relative"},
        ),
        html.H1(
            "Databricks Planning",
            style={
                "textAlign": "center",
                "font-family": "'Roboto', sans-serif",
                "font-size": "40px",
                "color": "#f18900",
            },
        ),
        dcc.Input(
            id="table-name-input",
            type="text",
            placeholder="Enter table name",
            style={"width": "300px", "margin": "10px", "marginBottom": "20px"},
        ),
        dcc.Loading(id="loading-1", type="default", children=html.Div(id="loading-output-1")),
        html.Button(
            "Load Data",
            id="load-data-button",
            n_clicks=0,
            style={
                "marginTop": "30px",
                "background-color": "#f18900",
                "color": "white",
                "border": "none",
                "padding": "8px 15px",
                "font-size": "16px",
                "cursor": "pointer",
                "width": "150px",
            },
        ),
        dcc.Store(id="changed-rows-store", data=[]),
        dag.AgGrid(
            id="table-grid",
            columnDefs=[],
            rowData=[],
            dashGridOptions={"undoRedoCellEditing": True, "undoRedoCellEditingLimit": 20, "filter": True},
            persistence=True,
            persisted_props=["filterModel"],
            cellValueChanged=[],
        ),
        dcc.Loading(id="loading-2", type="default", children=html.Div(id="loading-output-2")),
        html.Button(
            "Save",
            id="Saving-button",
            n_clicks=0,
            style={
                "marginTop": "30px",
                "background-color": "#f18900",
                "color": "white",
                "border": "none",
                "padding": "8px 15px",
                "font-size": "16px",
                "cursor": "pointer",
                "width": "150px",
            },
        ),
        html.Div(id="Save-confirmed"),
    ]
)
