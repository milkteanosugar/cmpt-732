import dash
import dash_core_components as dcc
import dash_html_components as html
import json
import sys
sys.path.insert(0,'./code/')
import q3_data as data

from dash.dependencies import Input, Output
from plotly import graph_objs as go
from plotly.graph_objs import *
from datetime import datetime as dt

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

# Plotly mapbox public token
mapbox_access_token = "pk.eyJ1IjoicGxvdGx5bWFwYm94IiwiYSI6ImNqdnBvNDMyaTAxYzkzeW5ubWdpZ2VjbmMifQ.TXcBE-xg9BFdV2ocecc_7g"

# Layout of Dash App

app.layout = html.Div(
    children=[
        html.Div(
            className="row",
            children=[
                # Column for user controls
                html.Div(
                    className="four columns div-user-controls",
                    children=[
                        html.H2("NYC sugguested Bike Stations"),
                        html.P(
                            "We sugguest to add the following stations:"
                        ),
                        # Change to side-by-side for mobile layout
                        html.Div(
                            className="row",
                            children=[
                                # Column for user controls
                                html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        dcc.Dropdown(
                                            id="station-dropdown1",
                                            options=[
                                                {'label': 'Add 10', 'value': 'ten'},
                                                {'label': 'Add 50', 'value': 'fif'},
                                                {'label': 'Add 100', 'value': 'hun'}
                                            ],
                                            value ='10',
                                            placeholder='10',
                                            clearable=False,
                                        ),
                                    ]
                                ),
                            ],
                        ),

                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="eight columns div-for-charts bg-grey",
                    children=[
                        dcc.Graph(id="map-graph")
                    ],
                ),
            ],
        )
    ]
)



#########################################  map part  ###########################################
@app.callback(
    Output("map-graph", "figure"),
    [Input("station-dropdown1", "value")]
)
def update_graph(station_size):
    zoom = 12.0
    latInitial = 40.7272
    lonInitial = -73.991251
    bearing = 0

    name,lon,lat = data.all_stations()
    if station_size:
        newlon,newlat = data.get_new_stations(station_size)

    return go.Figure(
        data=[
            # Plot of important locations on the map
            Scattermapbox(
                lat=lat,
                lon=lon,
                mode="markers",
                hoverinfo="text",
                text=name,
                marker=dict(size=5, color="#3d9154"),
            ),

            Scattermapbox(
                lat=newlat,
                lon=newlon,
                mode="markers",
                hoverinfo="text",
                text="New station",
                marker=dict(size=15, color="#DC143C"),
            ),
        ],
        layout=Layout(
            autosize=True,
            margin=go.layout.Margin(l=0, r=35, t=0, b=0),
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                center=dict(lat=latInitial, lon=lonInitial),  # 40.7272  # -73.991251
                style="dark",
                bearing=bearing,
                zoom=zoom,
            ),
            updatemenus=[
                dict(
                    buttons=(
                        [
                            dict(
                                args=[
                                    {
                                        "mapbox.zoom": 12,
                                        "mapbox.center.lon": "-73.991251",
                                        "mapbox.center.lat": "40.7272",
                                        "mapbox.bearing": 0,
                                        "mapbox.style": "dark",
                                    }
                                ],
                                label="Reset Zoom",
                                method="relayout",
                            )
                        ]
                    ),
                    direction="left",
                    pad={"r": 0, "t": 0, "b": 0, "l": 0},
                    showactive=False,
                    type="buttons",
                    x=0.45,
                    y=0.02,
                    xanchor="left",
                    yanchor="bottom",
                    bgcolor="#323130",
                    borderwidth=1,
                    bordercolor="#6d6d6d",
                    font=dict(color="#FFFFFF"),
                )
            ],
        ),
    )

#recieve the selected shortage station
@app.callback(
    Output('station-dropdown1','value'),
    [Input('station-dropdown1','options')]
)
def set_station(avaliable_options):
    return avaliable_options[0]['value']


if __name__ == "__main__":
    app.run_server(debug=True,port=8052)
