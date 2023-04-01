import pandas as pd
import json

import shutil
import os

from PreProcessingScripts.PreProcess_AirRouteDatasets import fetch_AirRouteDatasets, fetch_IntlAirRouteDatasets, fetch_SampleAirRouteDatasets
from PreProcessingScripts.PreProcess_CityPairWiseDomesticPassengers import fetch_CityPairWiseDomesticPassengers
from PreProcessingScripts.PreProcess_IndianRailwaysData import fetch_IndianRailwaysData
from PreProcessingScripts.PreProcess_IndiaSocioEconomicData import fetch_IndiaSocioEconomicData
from PreProcessingScripts.PreProcess_IndiaTourismData import fetch_IndiaTourismData

##################################################
# PreProcessor
##################################################
# -> Used for processing all the raw datasets
#    into processed datasets
# -> Further used to store the preprocessed data
##################################################
class PreProcessor:
    
    # sample_names -> All sample air network datasets to be loaded
    # preprocess_all_raw_data -> Whether to preprocess all raw datasets or load the preprocessed datasets (should be True first time, then False thereafter)
    def __init__(self, sample_names = [], preprocess_all_raw_data = False):

        self.sample_names = sample_names

        if(preprocess_all_raw_data):

            # Deleting previously stored preprocessed datasets & recreating folder structure
            shutil.rmtree(f'./PreProcessed_Datasets/', ignore_errors = True)
            new_dirs = [
                'AirRouteDatasets',
                'CityPairWiseDomesticPassengers',
                'IndiaEconomicData',
                'IndiaEducationalData',
                'IndiaSocialData',
                'IndiaTourismData',
                'OtherTransportModes/Railways/IndianRailwayRoutes',
                'OtherTransportModes/Railways/IndianRailwayStations',
                'SampleAirRouteDatasets',
                'Models/PCA',
                'Models/Present_Features'
            ]
            for new_dir in new_dirs:
                os.makedirs(f'./PreProcessed_Datasets/{new_dir}/')

            # Converting raw datasets to processed datasets
            print("**************************************")
            print("Converting Raw Datasets into PreProcessed Datasets")
            fetch_AirRouteDatasets()                                                                        # Preprocess Indian flights & airports data
            fetch_IntlAirRouteDatasets()                                                                    # Preprocess APAC flights & airports data
            fetch_SampleAirRouteDatasets(sample_names = self.sample_names)                                  # Preprocess given sample air networks
            fetch_CityPairWiseDomesticPassengers()                                                          # Preprocess city-pair wise domestic passenger data
            fetch_IndiaSocioEconomicData()                                                                  # Preprocess Social, Economic & Educational data
            fetch_IndiaTourismData()                                                                        # Preprocess Tourism data
            # For preprocessing railways data, we need list of cities for which data is required. else dataset could become very large
            # Hence, providing the city & district info via CityMapping
            self.city_mapping = pd.read_csv('./PreProcessed_Datasets/CityMapping.csv')
            district_to_city_map = dict(zip(self.city_mapping['StationCodeData_District'].values, self.city_mapping['City'].values))
            fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = True)             # Load info about which station lies in which district
            fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = False)            # Preprocess railway data
            print("**************************************")

        # Fetching all processed datasets
        print("**************************************")
        print("Loading PreProcessed Datasets")
        self.fetch_PreProcessed_AirRouteDatasets()
        self.fetch_PreProcessed_CityPairWiseDomesticPassengers()
        self.fetch_PreProcessed_IndianRailwaysData()
        self.fetch_PreProcessed_SocioEconomicData()
        self.fetch_PreProcessed_IndiaTourismData()
        print("**************************************")

    def fetch_PreProcessed_AirRouteDatasets(self):
        # Datasets Loaded:
        # 1. CityMapping: Mapping cities to respective keys of other datasets
        # 2. IntlCityMapping: Same as above but for APAC dataset
        # 3. FlightConnectionsData_Flights: Data about all Indian flights
        # 4. FlightConnectionsData_IntlFlights: Data about all APAC flights
        # 5. FlightConnectionsData_Airports: Info about Indian airports
        # 6. FlightConnectionsData_IntlAirports: Info about APAC airports
        # 7. SampleAirRouteDatasets: Data about the different sample air networks to find new routes for
        self.city_mapping = pd.read_csv('./PreProcessed_Datasets/CityMapping.csv')
        self.intl_city_mapping = pd.read_csv('./PreProcessed_Datasets/IntlCityMapping.csv')
        self.all_network_data = pd.read_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_Flights.csv")
        self.all_airport_data = pd.read_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_Airports.csv")
        self.intl_network_data = pd.read_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_IntlFlights.csv")
        self.intl_airport_data = pd.read_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_IntlAirports.csv")
        self.network_data = {}
        for sample_name in self.sample_names:
            self.network_data[sample_name] = pd.read_csv(f"./PreProcessed_Datasets/SampleAirRouteDatasets/{sample_name}.csv")
            self.network_data[sample_name].drop('Dummy', axis = 1, inplace = True)
        print("Loaded AirRouteDatasets")

    def fetch_PreProcessed_CityPairWiseDomesticPassengers(self):
        # Datasets Loaded:
        # 1. CityPairWiseDomesticPassengers: Data about city-pair wise domestic passengers
        self.total_domestic_data = pd.read_csv("./PreProcessed_Datasets/CityPairWiseDomesticPassengers/CityPairWiseDomesticPassengers.csv")
        print("Loaded Domestic Passenger Data")

    def fetch_PreProcessed_IndianRailwaysData(self):
        # Datasets Loaded:
        # 1. all_station_districts: Info about the district each station belongs to
        # 2. CityToCityRoutes: Data about different city-to-city trains
        self.all_station_districts_data = pd.read_csv("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayStations/all_station_districts.csv")
        with open("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayRoutes/CityToCityRoutes.json", "r") as load_file:
            self.city_to_city_train_dict = json.load(load_file)
        print("Loaded Indian Railways Data")
    
    def fetch_PreProcessed_SocioEconomicData(self):
        # Datasets Loaded:
        # 1. EconomicData: Data about city's GDP
        # 2. Pop_Area_Household: Data aIndiaPopulation23WithCoordsbout city's population, area, number of households, etc.
        # 3. IndiaPopulation23WithCoords: Data about latest 2023 population of Indian cities
        # 4. EducationData: Data about number of graduates in a city
        # 5. PopulationHistory: Data about historical populations of cities to identify trends
        self.economic_data = pd.read_csv("./PreProcessed_Datasets/IndiaEconomicData/EconomicData.csv")
        self.pop_area_household_data = pd.read_csv("./PreProcessed_Datasets/IndiaSocialData/Pop_Area_Household.csv")
        self.latest_population_data = pd.read_csv("./PreProcessed_Datasets/IndiaSocialData/IndiaPopulation23WithCoords.csv")
        self.education_data = pd.read_csv("./PreProcessed_Datasets/IndiaEducationalData/EducationData.csv")
        with open('./PreProcessed_Datasets/IndiaSocialData/PopulationHistory.json', 'r') as load_file:
            self.population_history_data = json.load(load_file)
        print("Loaded Socio-Economic Data")
    
    def fetch_PreProcessed_IndiaTourismData(self):
        # Datasets Loaded:
        # 1. MonumentVisitors: Data about number of visitors visiting different Indian monuments
        # 2. TouristLocationsCoords: Data about coordinates of different Indian monuments
        self.monument_visitors_data = pd.read_csv("./PreProcessed_Datasets/IndiaTourismData/MonumentVisitors.csv")
        self.tourist_loc_coords_data = pd.read_csv("./PreProcessed_Datasets/IndiaTourismData/TouristLocationsCoords.csv")
        print("Loaded Monument Visitors Data")