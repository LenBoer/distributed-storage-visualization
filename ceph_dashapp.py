#--------------------------------------------------------------------------Imports--------------------------------------------------------------------------
from pydoc import visiblename
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import json
import numpy as np
from datetime import datetime


#--------------------------------------------------------------------------read data--------------------------------------------------------------------------
tree = open("tree.txt", "r")
pg_dump = open("pg.dump.txt", "r")


#--------------------------------------------------------------------------prepare graphs--------------------------------------------------------------------------
#------main overview------
names = []
parents = []
hierarchy = []
h_weights = []

weight = []
clas = []
color = []

# pop header and add root
tree.readline()
start = tree.readline().split()
names.append(start[2]+start[3])
parents.append("")
hierarchy.append(start[2]+start[3])
h_weights.append(float(start[1]))
weight.append(float(start[1]))
clas.append("n/a")
color.append("grey")

for line in tree:
    attributes = line.split()
    

    # Buckets = [ID, WEIGHT, TYPE, NAME]
    if len(attributes) <5:
        
        # skip empty buckets that were already deleted
        if float(attributes[1])==0:
            continue
        bucket = attributes[2] + attributes[3]
        names.append(bucket)
            
        # buckets where we already read all contents are removed
        while int(h_weights[-1]) < 0.1:
            h_weights.pop(-1)
            hierarchy.pop(-1)
        # otherwise we add this bucket into the hierarchy, removing its weight from its parent
        parents.append(hierarchy[-1])
        hierarchy.append(bucket)
        h_weights[-1]-= float(attributes[1])
        h_weights.append(float(attributes[1]))
        weight.append(float(attributes[1]))
        clas.append("n/a")
        color.append("grey")
            
    # Leafs = [ID, CLASS, WEIGHT, NAME, STATUS, REWEIGHT, PRI-AFF]
    else:
        names.append(attributes[3])
        parents.append(hierarchy[-1])
        h_weights[-1]-=float(attributes[2])
        weight.append(float(attributes[2]))
        clas.append(attributes[1])
        color.append("green")
               
d = {"name": names, "parent": parents, "class": clas, "weight": weight, "color": color}
df = pd.DataFrame(data = d)


#------pg-dump------
# skipping the first 5 lines (header)
for i in range(5):
    pg_dump.readline()

lines = pg_dump.readlines()

pg_stat = []
pool_stat = []
osd_stat = []
current_block = 0

for line in lines:
    cells = line.split()
    # empty lines signal a new block: 1. PG, 2. Pool, 3. OSD , 4. unnecessary information
    if len(cells) <2:
        current_block += 1
        continue
        
    match current_block:
        # placement groups
        case 0:
            pool, pg_name = cells[0].split(".")
            tupel = (pool, pg_name, int(cells[1]), int(cells[6]), int(cells[10]), cells[11], cells[12], cells[15], cells[16])
            pg_stat.append(tupel)
        # pools
        case 1:
            tupel = (cells[0], int(cells[1]), int(cells[6]), int(cells[7]), int(cells[8]), int(cells[9]), int(cells[10]))
            pool_stat.append(tupel)
        # osds
        case 2:
            if len(cells) != 12:
                continue
            if cells[2] == "TiB":
                used = float(cells[1])*1000
            else:
                used = int(cells[1])
            if cells[4] == "TiB":
                avail = float(cells[3])*1000
            else:
                avail = int(cells[3])
            if cells[8] == "TiB":
                total = float(cells[7])*1000
            else:
                total = int(cells[7])
            tupel = (int(cells[0]), used, avail, total, cells[9], int(cells[10]), int(cells[11]))
            osd_stat.append(tupel)
            
        case 3:
            break

# placement groups
Bytes = []
Objects = []
pool = []
pg_number = []
Up = []
Up_primary = []
color = []
state = []

for pg in pg_stat:
    pool.append(pg[0])
    pg_number.append(pg[1])
    Objects.append(pg[2])
    Bytes.append(pg[3])
    state.append(pg[5])
    Up.append(pg[7])
    Up_primary.append(pg[8])
    color.append("n/a")
d = {"Bytes": Bytes, "Objects": Objects, "pool": pool, "pg_number": pg_number, "state": state, "Up": Up, "Up_primary": Up_primary, "color": color} 
pg_df = pd.DataFrame(data=d)

pg_state_colormap = {"active+clean": "forestgreen", "active+clean+scrubbing": "orange", "active+clean+scrubbing+deep": "orange"}

# pools
sorted_pool = sorted(pool_stat, key = lambda pool_stat: int(pool_stat[0]))
pool_number = []
number_of_objects = []
number_of_bytes = []
omap_bytes = []
omap_keys = []
log = []
disk_log = []

for individual_pool in sorted_pool:
    pool_number.append(individual_pool[0])
    number_of_objects.append(individual_pool[1])
    number_of_bytes.append(individual_pool[2])
    omap_bytes.append(individual_pool[3])
    omap_keys.append(individual_pool[4])
    log.append(individual_pool[5])
    disk_log.append(individual_pool[6])
d = {"pool": pool_number, "objects": number_of_objects, "bytes": number_of_bytes, "omap_bytes": omap_bytes, "omap_keys": omap_keys, "log": log, "disk_log": disk_log}
pool_df = pd.DataFrame(data=d)

# osds
num_osd = []
used = []
avail = []
total = []
hb_peers = []
pg_sum = []
primary_pg_sum = []
percentage_used = []

for osd in osd_stat:
    num_osd.append(osd[0])
    used.append(osd[1])
    avail.append(osd[2])
    total.append(osd[3])
    hb_peers.append(osd[4])
    pg_sum.append(osd[5])
    primary_pg_sum.append(osd[6])
    percentage_used.append(osd[1]/osd[3])

d = {"number":num_osd, "used": used, "available": avail, "total": total, "percentage_used": percentage_used, "hb_peers": hb_peers, "pg_sum": pg_sum, "primary_pg_sum":primary_pg_sum}
osd_df = pd.DataFrame(data=d)


#--------------------------------------------------------------------------Layout--------------------------------------------------------------------------
app = Dash(__name__)
app.config['suppress_callback_exceptions'] = True
app.layout = html.Div([

    html.Div([dcc.Graph(figure = px.treemap(df, names = "name", parents = "parent", color = "class", 
                                            color_discrete_sequence = px.colors.qualitative.Dark2,
                                            title = "OSD hierarchy, colored depending on class", hover_data = ["class", "weight"]),id='treemap')], 
             style={'width': '99%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Dropdown(
                ['used', 'available', 'total', "percentage_used", "pg_sum", "primary_pg_sum"],
                'total', id='osd_filter'
            )
        ], style={'width': '33%', 'display': 'inline-block', 'float':'left'}),

    html.Div([
        dcc.Dropdown(
                ['pool', 'state'],
                'pool', id='pg_filter'
            )
        ], style={'width': '33%', 'display': 'inline-block', 'float':'left'}),


    html.Div([
            dcc.Dropdown(
                ['objects', 'bytes', 'omap_bytes', "omap_keys", "log", "disk_log"],
                'objects', id='pool_filter'
            ),
            dcc.RadioItems(
                ["Linear", "Logarithmic"],
                'Logarithmic', id='pool_lin_log'
            )
        ], style={'width': '33%', 'display': 'inline-block', 'float':'right'}),


    html.Div([
        html.Div([dcc.Graph(figure = px.bar(x = num_osd, y = total, color = percentage_used, range_color=(0,1),
                                                labels={"x": "OSD", "y": "total size", "color": "fill %"}, 
                                                title = "fill level of osds"),id='osd_bar')], style={'width': '33%', 'float': 'left', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure = px.scatter(pg_df, x = "Bytes", y = "Objects", color = "pool", custom_data=["Up"],
                                                labels={"x": "Bytes", "y": "Number of Objects", "color": "Pool"}, 
                                                title = "size and number of PGs of different pools"),id='pg_scatter')], style={'width': '33%', 'float': 'center', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure = px.bar(x = pool_number, y = number_of_objects,
                                            labels={"x": "Pool", "y": "number of objects"},
                                            title = "Pool Data", log_y=True), id='pool_bar')], style={'width': '33%', 'float': 'right', 'display': 'inline-block'})
    ], style={'display': 'inline-block', 'width': '99%'})
])



#--------------------------------------------------------------------------Callback--------------------------------------------------------------------------
#------OSD------
@callback(
    Output("treemap", "figure", allow_duplicate=True),
    Output("pg_scatter", "figure"),
    Output("pool_bar", "figure"),
    Output("osd_bar", "figure"),
    Input("pg_filter", "value"),
    Input("pool_filter", "value"),
    Input("pool_lin_log", "value"),
    Input("osd_filter", "value"),
    Input("osd_bar", "clickData"),prevent_initial_call=True
)
def update_osd_click(pg_filter, pool_filter, pool_lin_log, osd_filter, clicked_osd):
    if clicked_osd is None:
        dff = df.copy()
        treemap = px.treemap(dff, names = "name", parents = "parent", color = "color", color_discrete_map={"grey": "lightgrey", "green": "green", "yellow": "gold"},
                              title = "OSD hierarchy, selected OSD in yellow, heart beat peers in blue, other in green",hover_data = ["class", "weight"])
        osd_bar = px.bar(osd_df, x = "number", y = osd_filter, color = "percentage_used", range_color=(0,1),
                                                labels={"x": "OSD", "y": osd_filter, "color": "fill %"}, 
                                                title = "fill level of osds")
        
        if pg_filter == "state":
            pg_scatter = px.scatter(pg_df, x = "Bytes", y = "Objects", color = pg_filter, custom_data=["Up"], color_discrete_map=pg_state_colormap,
                                                    labels={"x": "Bytes", "y": "Number of Objects", "color": "Pool"}, title = "size and number of PGs of different pools")
        else:
            pg_scatter = px.scatter(pg_df, x = "Bytes", y = "Objects", color = pg_filter, custom_data=["Up"],
                                        labels={"x": "Bytes", "y": "Number of Objects", "color": "Pool"}, title = "size and number of PGs of different pools")
        if pool_lin_log == "Linear": log_scale = False
        else: log_scale = True
        pool_bar = px.bar(pool_df, x = "pool", y = pool_filter,
                                            labels={"x": "Pool", "y": pool_filter}, title = "Pool Data", log_y=log_scale)
        
    else:
        selected_osd = "osd." + str(clicked_osd["points"][0]["x"])
        dff = df.copy()
        # osd
        peered_osds = osd_df[osd_df["number"] == clicked_osd["points"][0]["x"]]["hb_peers"].values[0]
        peered_osds = peered_osds[1:-1].split(",")
        dff.loc[dff.name == selected_osd, "color"] = "yellow"
        for peer in peered_osds:
            name = "osd." + str(peer)
            dff.loc[dff.name == name, "color"] = "blue"
        treemap = px.treemap(dff, names = "name", parents = "parent", color = "color", color_discrete_map={"grey": "lightgrey", "green": "green", "yellow": "gold", "blue": "blue"},
                             title = "OSD hierarchy, selected OSD in yellow, heart beat peers in blue, other in green", hover_data = ["class", "weight"])
        
        osd_bar = px.bar(osd_df, x = "number", y = osd_filter, color = "percentage_used", range_color=(0,1),
                                                labels={"x": "OSD", "y": osd_filter, "color": "fill %"}, 
                                                title = "fill level of osds")
        
        # PG
        pg_dff = pg_df.copy()
        for index, row in pg_dff.iterrows():
            stored_osds = row["Up"][1:-1].split(",")
            if str(clicked_osd["points"][0]["x"]) in stored_osds:
                if str(clicked_osd["points"][0]["x"]) == row["Up_primary"]:
                    pg_dff.at[index, "color"] = "primary"
                else:
                    pg_dff.at[index, "color"] = "secondary"
        pg_scatter = px.scatter(pg_dff, x = "Bytes", y = "Objects", color = pg_filter, custom_data=["Up"],
                                                labels={"x": "Bytes", "y": "Number of Objects", "color": "Pool"}, 
                                                title = "size and number of PGs of different pools")
        # pool
        if pool_lin_log == "Linear":
            log_scale = False
        else:
            log_scale = True
        pool_bar = px.bar(pool_df, x = "pool", y = pool_filter,
                                            labels={"x": "Pool", "y": pool_filter}, title = "Pool Data", log_y=log_scale)
        
    return treemap, pg_scatter, pool_bar, osd_bar

#------OSD------
@callback(
    Output("treemap", "figure", allow_duplicate=True),
    Input("pg_scatter", "clickData"),prevent_initial_call=True
)
def update_pg_click(clicked_pg):
    # the custom data transmitted from the placement group scatter plot are the osds this placement group is currently up on
    corresponding_osds = clicked_pg["points"][0]["customdata"][0][1:-1].split(",")
    corresponding_osds = list(map(lambda num_osd: "osd." + num_osd, corresponding_osds))
    # we then color the corresponding primary and seconday osds respectivly
    dff = df.copy()
    dff.loc[dff['name'].isin(corresponding_osds), 'color'] = 'blue'
    dff.loc[dff.name == corresponding_osds[0], "color"] = "yellow"
    updated_treemap = px.treemap(dff, names = "name", parents = "parent", color = "color", color_discrete_map={"grey": "lightgrey", "green": "green", "yellow": "gold", "blue": "blue"},
                              title = "Primary and Secondary OSDs for placement group, in yellow and blue respectively"  ,hover_data = ["class", "weight"])
    return updated_treemap

if __name__ == '__main__':
    app.run_server(debug=True)