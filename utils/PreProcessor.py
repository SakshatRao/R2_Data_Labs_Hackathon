import numpy as np
import pandas as pd

from PreProcessingScripts.PreProcess_AirRouteDatasets import fetch_AirRouteDatasets
from PreProcessingScripts.PreProcess_CityPairWiseDomesticPassengers import fetch_CityPairWiseDomesticPassengers

class PreProcessor:
    
    def __init__(self):

        # Converting raw datasets to processed datasets
        fetch_AirRouteDatasets()
        fetch_CityPairWiseDomesticPassengers()

        # Fetching all processed datasets
        self.fetch_PreProcessed_AirRouteDatasets()
        self.fetch_PreProcessed_CityPairWiseDomesticPassengers()

    def fetch_PreProcessed_AirRouteDatasets(self):
        pass

    def fetch_PreProcessed_CityPairWiseDomesticPassengers(self):
        pass