#--------------------------------------------------------------------------Imports--------------------------------------------------------------------------
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import json
import numpy as np
from datetime import datetime


#--------------------------------------------------------------------------read data--------------------------------------------------------------------------
df = pd.read_csv("LustreExampleData/df.csv")
directory_stats = pd.read_csv("LustreExampleData/pool_data_directory_stats.csv")

with open("LustreExampleData/pool_data_getstripe_infos.json") as dd:
    directory_data = json.load(dd)


#--------------------------------------------------------------------------prepare graphs--------------------------------------------------------------------------
#------main overview------
number_of_files = []
dff = df[(df["partition"] == "/pool/data") & (df["storage_type"] == "OST")]
# count the number of osts in this partition
for ost in range(len(dff)):
    number_of_files.append(0)
# and how many files from our queried directory are in each   
for files in directory_data:
    # non-PFL files
    if len(files) == 1:
        for file in files:
            if "osts" in file:
                for ost in file["osts"]:
                    number_of_files[int(ost["obdidx"])] += 1
    # PFL-files
    else:
        for file_layout in files:
            if "osts" in file_layout:
                for ost in file_layout["osts"]:
                    number_of_files[int(ost["l_ost_idx"])] += 1
               


#--------------------------------------------------------------------------Layout--------------------------------------------------------------------------
app = Dash(__name__)
app.config['suppress_callback_exceptions'] = True
app.layout = html.Div([

    #main selection bar chart
    html.Div([
        dcc.Graph(figure = px.bar(dff, x='id', y='use_percent', color = number_of_files, color_continuous_scale="bluered"), id='overview'),
    ], style={'width': '99%', 'display': 'inline-block', 'padding': '0 20'}),

    #selection menus to query the stats
    html.Div([
        html.Div([
            dcc.RadioItems(
                ['atime', 'mtime', 'ctime'],
                'atime',
                id='time_filter',
                labelStyle={'display': 'inline-block', 'marginTop': '5px'}
            )
        ],
        style={'width': '33%', 'display': 'inline-block', 'float':'left'}),

        html.Div([
            dcc.RadioItems(
                ['User', 'Group'],
                'User',
                id='owner_filter',
                labelStyle={'display': 'inline-block', 'marginTop': '5px'}
            )
        ], style={'width': '33%', 'float': 'right', 'display': 'inline-block', 'float':'center'})
    ], style={'padding': '10px 5px'}),

    #charts for the time, ownership, and size statistics
    html.Div([
        html.Div([dcc.Graph(id='time_scatter')], style={'width': '33%', 'float': 'left', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='owner_scatter')], style={'width': '33%', 'float': 'center', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='size_scatter')], style={'width': '33%', 'float': 'right', 'display': 'inline-block'})
    ], style={'display': 'inline-block', 'width': '99%'})
])



#--------------------------------------------------------------------------Callback--------------------------------------------------------------------------
@callback(
    Output('time_scatter', 'figure'),
    Output('owner_scatter', 'figure'),
    Output('size_scatter', 'figure'),    
    Input('time_filter', 'value'),
    Input('owner_filter', 'value'),
    Input('overview', 'selectedData'),
)
def update_graph(x_time, ownership, selected_osts):
    osts = []
    selected_files = []
    if selected_osts is None:
        dff = directory_stats
    else:
        for ost in selected_osts["points"]:
            osts.append(ost["pointNumber"])
        for files in directory_data:
            # non-PFL files
            if len(files) == 1:
                for file in files:
                    if "osts" in file:
                        for ost in file["osts"]:
                            if int(ost["obdidx"]) in osts:
                                selected_files.append(file["filename"])
        # PFL-files
        else:
            for file_layout in files:
                if "filename"  in file_layout:
                    fname = file_layout["filename"]
                elif "osts"  in file_layout:
                    for ost in file_layout["osts"]:
                        if int(ost["obdidx"]) in osts:
                                selected_files.append(fname)
        dff = directory_stats.loc[directory_stats["name"].isin(selected_files)]
        

    # times
    match x_time:
        case "atime":
            dates = []
            for date in dff.atime.to_list():
                dates.append(datetime.fromtimestamp(date).strftime('%Y-%m-%d'))
            a_values, a_counts = np.unique(dates, return_counts=True)
            time_fig = px.scatter(x=a_values, y=a_counts)
        case "mtime":
            dates = []
            for date in dff.mtime.to_list():
                dates.append(datetime.fromtimestamp(date).strftime('%Y-%m-%d'))
            m_values, m_counts = np.unique(dates, return_counts=True)  
            time_fig = px.scatter(x=m_values, y=m_counts)
        case "ctime":
            dates = []
            for date in dff.ctime.to_list():
                dates.append(datetime.fromtimestamp(date).strftime('%Y-%m-%d'))
            c_values, c_counts = np.unique(dates, return_counts=True)  
            time_fig = px.scatter(x=c_values, y=c_counts)

    # ownership
    match ownership:
        case "User":
            owners = []
            for user in dff.user_id.to_list():
                owners.append(user)
            user, user_files = np.unique(owners, return_counts=True)
            ownership_fig = px.pie(values=user_files, names=user, title='number of files belonging to User_ids')
        case "Group":
            owners = []
            for group in dff.group_id.to_list():
                owners.append(group)
            group, group_files = np.unique(owners, return_counts=True)
            ownership_fig = px.pie(values=group_files, names=group, title='number of files belonging to Group_ids')

    # size
    size_fig = px.histogram(dff, x='size', nbins=40, log_y=True)

    return time_fig, ownership_fig, size_fig

if __name__ == '__main__':
    app.run_server(debug=True)