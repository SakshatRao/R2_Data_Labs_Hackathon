import numpy as np
import pandas as pd
import json

from PreProcessingScripts.PreProcess_AirRouteDatasets import fetch_AirRouteDatasets, fetch_SampleAirRouteDatasets
from PreProcessingScripts.PreProcess_CityPairWiseDomesticPassengers import fetch_CityPairWiseDomesticPassengers
from PreProcessingScripts.PreProcess_IndianRailwaysData import fetch_IndianRailwaysData
from PreProcessingScripts.PreProcess_IndiaSocioEconomicData import fetch_IndiaSocioEconomicData
from PreProcessingScripts.PreProcess_IndiaTourismData import fetch_IndiaTourismData

class PreProcessor:
    
    def __init__(self, sample_num = 1, preprocess_all_raw_data = False):

        self.sample_num = sample_num

        if(preprocess_all_raw_data):
            # Converting raw datasets to processed datasets
            print("**************************************")
            print("Converting Raw Datasets into PreProcessed Datasets")
            fetch_AirRouteDatasets()
            fetch_SampleAirRouteDatasets(sample_num = self.sample_num)
            fetch_CityPairWiseDomesticPassengers()
            self.city_mapping = pd.read_csv('./PreProcessed_Datasets/CityMapping.csv')
            district_to_city_map = dict(zip(self.city_mapping['StationCodeData_District'].values, self.city_mapping['City'].values))
            fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = True)
            fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = False)
            fetch_IndiaSocioEconomicData()
            fetch_IndiaTourismData()
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
        self.city_mapping = pd.read_csv('./PreProcessed_Datasets/CityMapping.csv')
        self.all_network_data = pd.read_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_Flights.csv")
        self.all_airport_data = pd.read_csv("./PreProcessed_Datasets/AirRouteDatasets/FlightConnectionsData_Airports.csv")
        self.network_data = pd.read_csv(f"./PreProcessed_Datasets/SampleAirRouteDatasets/Sample{self.sample_num}.csv")
        self.network_data.drop('Dummy', axis = 1, inplace = True)
        print("Loaded AirRouteDatasets")

    def fetch_PreProcessed_CityPairWiseDomesticPassengers(self):
        self.total_domestic_data = pd.read_csv("./PreProcessed_Datasets/CityPairWiseDomesticPassengers/CityPairWiseDomesticPassengers.csv")
        print("Loaded Domestic Passenger Data")

    def fetch_PreProcessed_IndianRailwaysData(self):
        self.all_station_districts_data = pd.read_csv("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayStations/all_station_districts.csv")
        with open("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayRoutes/CityToCityRoutes.json", "r") as load_file:
            self.city_to_city_train_dict = json.load(load_file)
        print("Loaded Indian Railways Data")
    
    def fetch_PreProcessed_SocioEconomicData(self):
        self.economic_data = pd.read_csv("./PreProcessed_Datasets/IndiaEconomicData/EconomicData.csv")
        self.pop_area_household_data = pd.read_csv("./PreProcessed_Datasets/IndiaSocialData/Pop_Area_Household.csv")
        self.latest_population_data = pd.read_csv("./PreProcessed_Datasets/IndiaSocialData/IndiaPopulation23WithCoords.csv")
        self.education_data = pd.read_csv("./PreProcessed_Datasets/IndiaEducationalData/EducationData.csv")
        with open('./PreProcessed_Datasets/IndiaSocialData/PopulationHistory.json', 'r') as load_file:
            self.population_history_data = json.load(load_file)
        print("Loaded Socio-Economic Data")
    
    def fetch_PreProcessed_IndiaTourismData(self):
        self.monument_visitors_data = pd.read_csv("./PreProcessed_Datasets/IndiaTourismData/MonumentVisitors.csv")
        self.tourist_loc_coords_data = pd.read_csv("./PreProcessed_Datasets/IndiaTourismData/TouristLocationsCoords.csv")
        print("Loaded Monument Visitors Data")