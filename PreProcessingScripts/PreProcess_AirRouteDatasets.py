import numpy as np
import pandas as pd

def fetch_AirRouteDatasets():

    # Airport/City Data
    airport_city_data = pd.read_csv("./Datasets/CityMapping.csv")
    airport_city_data.to_csv("./PreProcessed_Datasets/CityMapping.csv", index = None)

    # Flights Data
    print("Opening AirRouteDatasets - Flights")
    network_data = pd.read_excel("./Datasets/AirRouteDatasets/FlightConnectionsData_Flights.ods")
    network_data.fillna(0, inplace = True)
    network_data = network_data.melt(
        id_vars = ['From', 'To', 'Distance', 'Time', 'Cheapest Price'],
        value_vars = ['Narrow Body', 'Turbo-prop', 'Regional Jet', 'Others', 'Wide Body'],
        var_name = "Aircraft Type",
        value_name = "Number of Flights"
    )
    network_data = network_data[network_data['Number of Flights'] > 0]

    def duration_to_mins(duration):
        duration_lower = duration.lower()
        hr = 0; min = 0;
        if('h' in duration_lower):
            hr = duration_lower.split('h')[0]
            if('m' in duration_lower):
                min = duration_lower.split('h')[1].split('m')[0]
            else:
                min = 0
        else:
            hr = 0
            min = duration_lower.split('m')[0]
        hr = int(hr)
        min = int(min)
        return hr * 60 + min
    network_data['Time'] = network_data['Time'].apply(duration_to_mins)

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

# # Testing
# fetch_AirRouteDatasets()

def fetch_SampleAirRouteDatasets(sample_num = 1):

    # Sample Flights Data
    print("Opening Sample AirRouteDatasets")
    connections_data = pd.read_csv(f"./Datasets/SampleAirRouteDatasets/Sample{sample_num}.csv", header = 0)
    
    network_list = []
    for idx, row in connections_data.iterrows():
        source_airport = row['Source']
        dest_idx = 1
        while(True):
            if(pd.isnull(row[str(dest_idx)])):
                break
            destination_airport = row[str(dest_idx)]
            network_list.append([source_airport, destination_airport, -1])
            dest_idx += 1
    network_data = pd.DataFrame(network_list, columns = ['From', 'To', 'Dummy'])

    network_data.to_csv(f"./PreProcessed_Datasets/SampleAirRouteDatasets/Sample{sample_num}.csv", index = None)

# # Testing
# fetch_SampleAirRouteDatasets()