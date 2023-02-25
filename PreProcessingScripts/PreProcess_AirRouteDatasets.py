import numpy as np
import pandas as pd

def fetch_AirRouteDatasets():

    # Flights Data
    print("Opening AirRouteDatasets - Flights")
    network_data = pd.read_excel("./Datasets/AirRouteDatasets/FlightConnectionsData_Flights.ods")
    network_data.fillna(0, inplace = True)
    network_data = network_data.melt(
        id_vars = ['From', 'To', 'Distance', 'Time', 'Cheapest Price'],
        value_vars = ['Narrow Body', 'Turbo-prop', 'Regional Jet', 'Others'],
        var_name = "Aircraft Type",
        value_name = "Number of Flights"
    )
    network_data = network_data[network_data['Number of Flights'] > 0]
    network_data.to_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_Flights.csv", index = None)
    network_airports = set(list(network_data['From'].values) + list(network_data['To'].values))

    # Airports Data
    print("Opening AirRouteDatasets - Airports")
    airport_data = pd.read_excel("./Datasets/AirRouteDatasets/FlightConnectionsData_Airports.ods")
    airport_data.to_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_Airports.csv", index = None)
    airports = set(list(airport_data['Name'].values))

    print("Airports which are included in routes but not in airport list are -")
    if(len(network_airports - airports) > 0):
        print(network_airports - airports)
    else:
        print("None")

# Testing
fetch_AirRouteDatasets()