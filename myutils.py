import requests as rq
import json
import os
import pandas as pd
import osmnx as ox
import numpy as np
import random

DATA_BASE = "data"
TIMETABLE_FILE = "timetable"
DF_FILE = "df"
STOPS_FILE = "stops"
PATHS_FILE = "paths"
FILE_EXT = ".json"

ROUTE_API = lambda id: f"http://apicms.ebms.vn/businfo/getroutebyid/{id}"
TIMETABLE_API = lambda route: f"http://apicms.ebms.vn/businfo/gettimetablebyroute/{route}"
ROUTEVAR_API = lambda route: f"http://apicms.ebms.vn/businfo/getvarsbyroute/{route}"
STOPS_API = lambda id, varId: f"http://apicms.ebms.vn/businfo/getstopsbyvar/{id}/{varId}"
PATHS_API = lambda id, varId: f"http://apicms.ebms.vn/businfo/getpathsbyvar/{id}/{varId}"

utn = ox.settings.useful_tags_node
oxna = ox.settings.osm_xml_node_attrs
oxnt = ox.settings.osm_xml_node_tags
utw = ox.settings.useful_tags_way
oxwa = ox.settings.osm_xml_way_attrs
oxwt = ox.settings.osm_xml_way_tags
utn = list(set(utn + oxna + oxnt))
utw = list(set(utw + oxwa + oxwt))
ox.settings.all_oneway = False
ox.settings.useful_tags_node = utn
ox.settings.useful_tags_way = utw
ox.settings.timeout=1200

def save_path_data(route, var, df):
    source = DF_FILE + str(var) + ".csv"
    dir_path = os.path.join(os.getcwd(), DATA_BASE, str(route))
    file_path = os.path.join(os.getcwd(), DATA_BASE, str(route), source)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    df.to_csv(file_path, index=False, encoding="utf-8")


def load_path_data(route, source):
    source += ".csv"
    file_path = os.path.join(os.getcwd(), DATA_BASE, str(route), source)
    if not os.path.exists(file_path):
        assert FileNotFoundError
    return pd.read_csv(file_path, encoding="utf-8")
    

def load_data(route, source, api):
    source += FILE_EXT
    dir_path = os.path.join(os.getcwd(), DATA_BASE, str(route))
    file_path = os.path.join(os.getcwd(), DATA_BASE, str(route), source)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    if not os.path.exists(file_path):
        response = rq.get(api)
        with open(file_path, "w+", encoding="utf-8") as f:
            string = json.dumps(response.json(), ensure_ascii=False)
            f.write(string)
            
    return pd.read_json(file_path, encoding="utf-8")


def load_map():
    HCM_NETWORK = "./data/hcm.graphml"
    if not os.path.exists(HCM_NETWORK):
        G = ox.graph_from_place("Ho Chi Minh City", retain_all=True, truncate_by_edge=True, buffer_dist=1000)
        ox.save_graphml(G, HCM_NETWORK)
    else:
        G = ox.load_graphml(HCM_NETWORK)
    ox.add_edge_speeds(G)
    ox.add_edge_travel_times(G)
    return G


def get_routes_from_paths(agrs):
    id, paths, stations, G = agrs
    paths["node"] = ox.nearest_nodes(G, X=paths["lng"], Y=paths["lat"])
    paths.drop_duplicates(subset="node", inplace=True, ignore_index=True)

    paths["edge"] = ox.nearest_edges(G, X=paths["lng"], Y=paths["lat"])
    paths.drop_duplicates(subset="edge", inplace=True, ignore_index=True)

    def find_dup(path):
        filt = paths.apply(lambda path: path["edge"][0], axis="columns") == path["edge"][0]
        try:
            return filt.iloc[path.name] and type(G.edges[path["edge"]]["name"]) == list
        except KeyError:
            return False

    stations["node"] = ox.nearest_nodes(G, X=stations["lng"], Y=stations["lat"])

    match_node = pd.merge(paths["node"], stations["node"], on=["node"], how='left', indicator='exist')
    paths['station'] = np.where(match_node["exist"] == 'both', True, False)

    dup_edges = paths[paths.apply(find_dup, axis="columns") & ~paths['station']]
    paths.drop(dup_edges.index, inplace=True)
    paths.reset_index(inplace=True, drop=True)

    def get_routes(path):
        if path.name + 1 != paths.shape[0]:
            return ox.shortest_path(G, path["edge"][0], paths.loc[path.name + 1, "edge"][0], cpus=4)
        else:
            return list(path["edge"][:2])

    paths["route"] = paths.apply(get_routes, axis="columns")
    paths.reset_index(drop=True, inplace=True)

    def find_loop(path):
        index = path.name
        return len(paths[paths.apply(lambda path:
            len(set(path['route']).intersection(set(paths.loc[index, "route"]))) 
            > 1 if path.name != index else False
            , axis="columns"
        )].index) != 0

    loop_filt = paths.apply(find_loop, axis="columns")
    loop_routes = paths[loop_filt & ~paths['station']]

    while loop_routes.shape[0] > 0:
        paths.drop(loop_routes.index, inplace=True)

        paths.reset_index(inplace=True, drop=True)
        paths.drop("route", axis="columns", inplace=True)
        paths["route"] = paths.apply(get_routes, axis="columns")

        loop_filt = paths.apply(find_loop, axis="columns")
        loop_routes = paths[loop_filt]
    
    paths["travel_times"] = paths["route"].apply(lambda route: sum(ox.utils_graph.get_route_edge_attributes(G, route, "travel_time")))
    
    return id, paths


def graph_folium_all_routes(buses, G):
    route_map = None
    for bus in buses:
        for _, path in bus.paths_df.items():
            routes = path["route"]
            r = lambda: random.randint(0,255)
            color = f"#{r():02x}{r():02x}{r():02x}"
            for route in routes:
                try: route_map = ox.plot_route_folium(G, route, route_map, color=color, tiles="openstreetmap", zoom=5)
                except: continue
    return route_map


def graph_all_routes(buses, G):
    ax = None
    fig = None
    for bus in buses:
        for _, path in bus.paths_df.items():
            routes = path["route"]
            r = lambda: random.randint(0,255)
            color = f"#{r():02x}{r():02x}{r():02x}"
            if ax == None:
                fig, ax = ox.plot_graph_routes(G, routes, route_colors=color, route_linewidths=10, orig_dest_size=1, node_size=0, edge_linewidth=1, show=False)
            else:
                fig, ax = ox.plot_graph_routes(G, routes, route_colors=color, route_linewidths=10, ax=ax, orig_dest_size=1, node_size=0, edge_linewidth=1, show=False)
    return fig, ax

