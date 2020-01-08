import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import sys
sys.path.insert(0,'./code/')
import UIdata as realtime
from ui_nearby import get_nearby_stations
import time


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
                        html.H2("NYC Bike Shortage and Overload"),
                        html.P(
                            "Check out each day shortage stations during year 2016"
                        ),
                        # Change to side-by-side for mobile layout
                        html.Div(
                            className="row",
                            children=[
                                # Column for user controls
                                html.Div(
                                    className="div-user-controls",
                                    children=[
                                        dcc.DatePickerSingle(
                                            id="date-picker",
                                            min_date_allowed=dt(2013, 1, 1),
                                            max_date_allowed=dt(2016, 12, 30),
                                            initial_visible_month=dt(2016, 1, 1),
                                            date=dt(2015, 10, 9).date(),
                                            display_format="MMMM D, YYYY",
                                            style={"border": "0px solid black", },
                                        ),
                                        dcc.Dropdown(
                                            id="station-dropdown",
                                            value ='Broadway & W 32 St',
                                            placeholder='Broadway & W 32 St',
                                            clearable=False,
                                        ),
                                        dcc.Loading(
                                            id="loading-1", 
                                            children=[html.Div(id="loading-output-1")], 
                                            type="default",
                                            style={"font-size": "15px"},
                                        ),
                                        html.Div(
                                            className="div-nearby",
                                            children=[
                                                html.P(
                                                    html.H3("Nearby Supply Stations:")
                                                ),
                                                html.Div(
                                                    id=('supply-station-list')
                                                ),

                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),

                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="eight columns div-for-charts bg-grey",
                    children=[
                        dcc.Graph(id="map-graph"),
                        html.Div(
                            className="text-padding",
                            children=[
                                "Select any of the bars on the histogram to section data by time."
                            ],
                        ),
                        dcc.Graph(id="histogram"),
                    ],
                ),
            ],
        )
    ]
)

#########################################  map part  ###########################################
@app.callback(
    Output("map-graph", "figure"),
    [Input("station-dropdown", "value"),
     Input("supply-station-list",'children'),
     Input("date-picker", "date"),],
)
def update_graph(selectedLocation,stations,selected_date):
    zoom = 12.0
    latInitial = 40.7272
    lonInitial = -73.991251
    bearing = 0


    supply_station_list =[]
    #decompose station children
    if stations is not None:
        station_data = stations['props']['children']
        if station_data is not None:
            for i in range(len(station_data)):
                station_name = station_data[i]['props']['children']['props']['children']
                name,lat,lon = realtime.get_location(station_name)
                supply_station_list.append([lat,lon])
    if selectedLocation:
        zoom = 15.0
        nameInitial,latInitial,lonInitial = realtime.get_location(selectedLocation)

##get all shorage and filled station of that day
    all_lat = []
    all_lon = []
    all_name = []
    overld_lat = []
    overld_lon = []
    overld_name = []
    if selected_date:
        station_list = realtime.get_stations(selected_date)
        over_station_list = realtime.get_over_stations(selected_date)
        for i in station_list:
            tempname,templat,templon = realtime.get_location(i)
            all_lat.append(templat)
            all_lon.append(templon)
            all_name.append(tempname)

        for i in over_station_list:
            tempname, templat, templon = realtime.get_location(i)
            overld_lat.append(templat)
            overld_lon.append(templon)
            overld_name.append(tempname)

## get selected station
    all_lat = []
    all_lon = []
    if selected_date:
        station_list = realtime.get_stations(selected_date)
        for i in station_list:
            tempname,templat,templon = realtime.get_location(i)
            all_lat.append(templat)
            all_lon.append(templon)

# draw the map
    fig =  go.Figure(
        data=[
            Scattermapbox(
                lat=[latInitial,supply_station_list[i][0]],
                lon=[lonInitial,supply_station_list[i][1]],
                mode="lines",
                hoverinfo="lat+lon+text",
                marker=dict(size=5, color="#3d9154"),
            )for i in range(len(supply_station_list))
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
    fig.add_trace(
        go.Scattermapbox(
            lat=all_lat,
            lon=all_lon,
            mode="markers",
            hoverinfo="text",
            text=all_name,
            marker=dict(size=10, color="#f4511e"),
        ),
    )
    fig.add_trace(
        go.Scattermapbox(
            lat=overld_lat,
            lon=overld_lon,
            mode="markers",
            hoverinfo="text",
            text=overld_name,
            marker=dict(size=10, color="#3d9154"),
        ),
    )

    return fig

############################# div-nearby ###############################################
#when client click a bar in the histogram, it update the nearby list
#take the clicked bar, date, hour
#output a html.div
@app.callback(
Output('supply-station-list', 'children'),
    [Input('histogram', 'clickData'),
     Input("date-picker", "date"),
     Input("station-dropdown", "value")])
def display_click_data(clickData,clicked_date,clicked_station):
    #todo: function 3
    supply_station_list=[]
    if clickData is not None:
        hour = clickData['points'][0]['x']
        supply_station_list = get_nearby_stations(str(clicked_date), int(hour),clicked_station)
    if supply_station_list is not None:
        DYNAMIC_CONTROLS = {}
        top_five = 5
        if len(supply_station_list) < 5:
            top_five = len(supply_station_list)
        for i in range(top_five):
            DYNAMIC_CONTROLS[i] = html.Div(
                html.Button(
                    str(supply_station_list[i]),
                    id='nearby-station' + str(i),
                    value=supply_station_list[i]
                ),
            )
        List = []
        for i in range(top_five):
            List.append(DYNAMIC_CONTROLS[i])
        return html.Div(
            List
        )



########################################  date and shoratage  ########################################
#show shortage station in selected date
@app.callback(
    Output('station-dropdown','options'),
    [Input("date-picker", "date")]
)
def shortage_station_list(selected_date):
    station_list = realtime.get_stations(selected_date)
    if selected_date is not None:
        return [{'label': i, 'value': i} for i in station_list]

#recieve the selected shortage station
@app.callback(
    Output('station-dropdown','value'),
    [Input('station-dropdown','options')]
)
def set_station(avaliable_options):
    return avaliable_options[0]['value']

@app.callback(Output("loading-output-1", "children"), [Input("date-picker", "date")])
def input_triggers_spinner(date):
    if date == '2016-09-22':
        return ''
    time.sleep(12)
    return 'Shortage Stations Updated'

#########################################  historgram part  ###########################################
#draw histogram
@app.callback(
    Output("histogram", "figure"),
    [Input("date-picker", "date"),Input("station-dropdown", "value")],
)
def update_histogram(date_picked,station_picked):
    #get trips data according to date_picked
    shortage_hours_infos = realtime.get_hourly_storage(date_picked,station_picked)
    y_axis = shortage_hours_infos[0]
    shortage_hours = shortage_hours_infos[1]
    x_axis = [x for x in range(23)]

    #set color of historgram bar, if shortage, make it red
    y_color = []

    for i in range(23):
         if shortage_hours[i]:
             y_color.append("#f4511e")
         else:
             y_color.append("#3d9154")

    #set layout
    layout = go.Layout(
        bargap=0.01,
        bargroupgap=0,
        barmode="group",
        margin=go.layout.Margin(l=10, r=0, t=0, b=50),
        showlegend=False,
        plot_bgcolor="#323130",
        paper_bgcolor="#323130",
        dragmode="select",
        font=dict(color="white"),
        xaxis=dict(
            showgrid=False,
            fixedrange=True,
            ticksuffix=":00",
        ),
        yaxis=dict(
            showticklabels=False,
            showgrid=False,
            fixedrange=True,
            #rangemode="nonnegative",
            zeroline=True,
        ),
        annotations=[
            dict(
                x=xi,
                y=yi,
                text=str(yi),
                xanchor="center",
                yanchor="bottom",
                showarrow=False,
                font=dict(color="white"),
            )
            for xi, yi in zip(x_axis, y_axis)
        ],
    )

    #set graph
    return go.Figure(
        data=[ go.Bar(x=x_axis, y=y_axis, marker=dict(color=y_color), hoverinfo="x"),
            go.Scatter(
                opacity=0,
                x=x_axis,
                y=y_axis,
                hoverinfo="none",
                mode="markers",
                marker=dict(color="rgb(66, 134, 244, 0)", symbol="square", size=40),
                visible=True,
            ),
        ],
        layout=layout,
    )

if __name__ == "__main__":
    app.run_server(debug=True,port=8051)
