import numpy as np
import pandas as pd

# Dataset Sources:
# -> All flight/airport data manually collected from https://www.flightconnections.com/
# -> Flight info data collection assumptions: (1) all airlines considered, (2) prices are for one-way economy for 1 adult on May 1, 2023, (3) non-stop flights only
# -> Sample network data manually created

##################################################
# fetch_AirRouteDatasets
##################################################
# -> Used for fetching Indian flights & airport
#    datasets, along with the CityMapping file
# -> CityMapping is used to map cities with keys
#    from other datasets
##################################################
def fetch_AirRouteDatasets():

    # City Mapping Data
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

    # Convert text to duration in minutes
    def duration_to_mins(duration):
        duration_lower = duration.lower()
        hr = 0; min = 0
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

    # To check whether all airports in flights data are covered in airport data
    print("Airports which are included in routes but not in airport list are -")
    if(len(network_airports - airports) > 0):
        print(network_airports - airports)
    else:
        print("None")

# # Testing
# fetch_AirRouteDatasets()
    
##################################################
# fetch_IntlAirRouteDatasets
##################################################
# -> Used for fetching APAC flights & airport
#    datasets, along with the CityMapping file
# -> CityMapping is used to map APAC cities with
#    coordinates
##################################################
def fetch_IntlAirRouteDatasets():

    # City-to-coordinate Mapping Data
    airport_city_data = pd.read_csv("./Datasets/IntlCityMapping.csv")
    airport_city_data.to_csv("./PreProcessed_Datasets/IntlCityMapping.csv", index = None)

    # Flights Data
    print("Opening International AirRouteDatasets - Flights")
    network_data = pd.read_excel("./Datasets/AirRouteDatasets/FlightConnectionsData_IntlFlights.ods")
    network_data.fillna(0, inplace = True)
    network_data = network_data.melt(
        id_vars = ['From', 'To', 'Distance', 'Time', 'Cheapest Price'],
        value_vars = ['Narrow Body', 'Others', 'Wide Body'],
        var_name = "Aircraft Type",
        value_name = "Number of Flights"
    )
    network_data = network_data[network_data['Number of Flights'] > 0]

    # Convert text to duration in minutes
    def duration_to_mins(duration):
        duration_lower = duration.lower()
        hr = 0; min = 0
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

    network_data.to_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_IntlFlights.csv", index = None)
    network_airports = set(list(network_data['From'].values) + list(network_data['To'].values))

    # Airports Data
    print("Opening AirRouteDatasets - IntlAirports")
    airport_data = pd.read_excel("./Datasets/AirRouteDatasets/FlightConnectionsData_IntlAirports.ods")
    airport_data.to_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_IntlAirports.csv", index = None)
    airports = set(list(airport_data['Name'].values))

    # To check whether all airports in flights data are covered in airport data
    print("Airports which are included in routes but not in airport list are -")
    if(len(network_airports - airports) > 0):
        print(network_airports - airports)
    else:
        print("None")

# # Testing
# fetch_IntlAirRouteDatasets()

##################################################
# fetch_SampleAirRouteDatasets
##################################################
# -> Used for fetching data about the sample
#    air networks for which route development
#    will be done
# -> sample_names determines which samples are
#    need to be loaded
##################################################
def fetch_SampleAirRouteDatasets(sample_names = []):

    # Sample Flights Data
    print("Opening Sample AirRouteDatasets")
    for sample_name in sample_names:
        connections_data = pd.read_csv(f"./Datasets/SampleAirRouteDatasets/{sample_name}.csv", header = 0)
        
        # Convert data to same From-To format as FlightConnectionsData_Flights
        # -> FromHub: Represents whether 'From' airport is a hub for network
        # -> Dummy: Dummy variable just to keep same format as FlightConnectionsData_Flights
        network_list = []
        for _, row in connections_data.iterrows():
            source_airport = row['Source']
            is_hub = row['IsHub']
            dest_idx = 1
            while(True):
                if(pd.isnull(row[str(dest_idx)])):
                    break
                destination_airport = row[str(dest_idx)]
                network_list.append([source_airport, is_hub, destination_airport, -1])
                dest_idx += 1
        network_data = pd.DataFrame(network_list, columns = ['From', 'FromHub', 'To', 'Dummy'])

        network_data.to_csv(f"./PreProcessed_Datasets/SampleAirRouteDatasets/{sample_name}.csv", index = None)

# # Testing
# fetch_SampleAirRouteDatasets()