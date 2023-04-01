import numpy as np
import pandas as pd
import os

# Dataset Sources:
# -> City-pair wise Domestic Passengers Data 2022: https://www.dgca.gov.in/digigov-portal/?page=monthlyStatistics/259/4751/html&main259/4184/servicename

##################################################
# fetch_CityPairWiseDomesticPassengers
##################################################
# -> Used for fetching data about city-pair wise
#    domestic passenger data for year 2022
# -> This data will be used to predict traffic of
#    different routes
##################################################
def fetch_CityPairWiseDomesticPassengers():

    # City-pair wise Domestic Passengers Data
    print("Opening City-pair wise Domestic Passengers Data")
    total_domestic_data = pd.DataFrame()
    all_datasets = os.listdir("./Datasets/CityPairWiseDomesticPassengers/")

    # Iterate through all months to collect total year's data
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
    for dataset in all_datasets:
        month = dataset.split('2022')[0].split(',')[-1].strip().lower()
        days_in_month = pd.Timestamp(2022, months.index(month) + 1, 1).daysinmonth
        domestic_data = pd.read_excel("./Datasets/CityPairWiseDomesticPassengers/" + dataset, skiprows = [0, 1], engine = 'openpyxl')
        domestic_data = domestic_data.assign(MONTH = np.repeat([month], domestic_data.shape[0]))
        domestic_data = domestic_data.assign(DAYS = np.repeat([days_in_month], domestic_data.shape[0]))
        domestic_data.columns = [x.replace('\n', '').strip() for x in domestic_data.columns]
        domestic_data_rev = domestic_data.copy()
        # Separate data into city1->city2 and city2->city1 data
        domestic_data.rename({'PASSENGERS TO CITY 2': 'PASSENGERS', 'FREIGHT TO CITY 2': 'FREIGHT', 'MAIL TO CITY 2': 'MAIL', 'CITY 1': 'FROM', 'CITY 2': 'TO'}, axis = 1, inplace = True)
        domestic_data.drop(['PASSENGERS FROM CITY 2', 'FREIGHT FROM CITY 2', 'MAIL FROM CITY 2', 'S.No.'],axis = 1, inplace = True)
        domestic_data['FROM'] = domestic_data['FROM'].replace({'KALABURAGI, KARNATAKA': 'KALABURAGI'})
        domestic_data['TO'] = domestic_data['TO'].replace({'KALABURAGI, KARNATAKA': 'KALABURAGI'})
        domestic_data_rev.rename({'PASSENGERS FROM CITY 2': 'PASSENGERS', 'FREIGHT FROM CITY 2': 'FREIGHT', 'MAIL FROM CITY 2': 'MAIL', 'CITY 2': 'FROM', 'CITY 1': 'TO'}, axis = 1, inplace = True)
        domestic_data_rev.drop(['PASSENGERS TO CITY 2', 'FREIGHT TO CITY 2', 'MAIL TO CITY 2', 'S.No.'],axis = 1, inplace = True)
        domestic_data_rev['FROM'] = domestic_data_rev['FROM'].replace({'KALABURAGI, KARNATAKA': 'KALABURAGI'})
        domestic_data_rev['TO'] = domestic_data_rev['TO'].replace({'KALABURAGI, KARNATAKA': 'KALABURAGI'})
        total_domestic_data = pd.concat([total_domestic_data, domestic_data, domestic_data_rev], axis = 0)
    
    total_domestic_data['Route'] = total_domestic_data[['FROM', 'TO']].apply(lambda x: f"{x['FROM']}-{x['TO']}", axis = 1)
    total_domestic_data['PASSENGERS'] = total_domestic_data['PASSENGERS'].replace({'-': '0'})
    total_domestic_data['FREIGHT'] = total_domestic_data['FREIGHT'].replace({'-': '0'})
    total_domestic_data['MAIL'] = total_domestic_data['MAIL'].replace({'-': '0'})
    total_domestic_data = total_domestic_data.astype({'PASSENGERS': 'float64', 'FREIGHT': 'float64', 'MAIL': 'float64'})
    total_domestic_data = total_domestic_data.groupby(['Route', 'FROM', 'TO'])[['PASSENGERS', 'FREIGHT', 'MAIL', 'DAYS']].aggregate('sum').reset_index(drop = False)

    total_domestic_data.to_csv("./PreProcessed_Datasets/CityPairWiseDomesticPassengers/CityPairWiseDomesticPassengers.csv", index = None)

# # Testing
# fetch_CityPairWiseDomesticPassengers()