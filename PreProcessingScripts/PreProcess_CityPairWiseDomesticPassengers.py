import numpy as np
import pandas as pd
import os

def fetch_CityPairWiseDomesticPassengers():

    # City-pair wise Domestic Passengers Data
    print("Opening City-pair wise Domestic Passengers Data")
    total_domestic_data = pd.DataFrame()
    all_datasets = os.listdir("./Datasets/CityPairWiseDomesticPassengers/")
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
    for dataset in all_datasets:
        month = dataset.split('2022')[0].split(',')[-1].strip().lower()
        days_in_month = pd.Timestamp(2022, months.index(month) + 1, 1).daysinmonth
        domestic_data = pd.read_excel("./Datasets/CityPairWiseDomesticPassengers/" + dataset, skiprows = [0, 1], engine = 'openpyxl')
        domestic_data = domestic_data.assign(MONTH = np.repeat([month], domestic_data.shape[0]))
        domestic_data = domestic_data.assign(DAYS = np.repeat([days_in_month], domestic_data.shape[0]))
        domestic_data.columns = [x.replace('\n', '').strip() for x in domestic_data.columns]
        domestic_data_rev = domestic_data.copy()
        domestic_data.rename({'PASSENGERS TO CITY 2': 'PASSENGERS', 'FREIGHT TO CITY 2': 'FREIGHT', 'MAIL TO CITY 2': 'MAIL', 'CITY 1': 'FROM', 'CITY 2': 'TO'}, axis = 1, inplace = True)
        domestic_data.drop(['PASSENGERS FROM CITY 2', 'FREIGHT FROM CITY 2', 'MAIL FROM CITY 2', 'S.No.'],axis = 1, inplace = True)
        domestic_data_rev.rename({'PASSENGERS FROM CITY 2': 'PASSENGERS', 'FREIGHT FROM CITY 2': 'FREIGHT', 'MAIL FROM CITY 2': 'MAIL', 'CITY 2': 'FROM', 'CITY 1': 'TO'}, axis = 1, inplace = True)
        domestic_data_rev.drop(['PASSENGERS TO CITY 2', 'FREIGHT TO CITY 2', 'MAIL TO CITY 2', 'S.No.'],axis = 1, inplace = True)
        total_domestic_data = pd.concat([total_domestic_data, domestic_data, domestic_data_rev], axis = 0)
    
    total_domestic_data['Route'] = total_domestic_data[['FROM', 'TO']].apply(lambda x: f"{x['FROM']}-{x['TO']}", axis = 1)
    total_domestic_data['PASSENGERS'] = total_domestic_data['PASSENGERS'].replace({'-': '0'})
    total_domestic_data['FREIGHT'] = total_domestic_data['FREIGHT'].replace({'-': '0'})
    total_domestic_data['MAIL'] = total_domestic_data['MAIL'].replace({'-': '0'})
    total_domestic_data = total_domestic_data.astype({'PASSENGERS': 'float64', 'FREIGHT': 'float64', 'MAIL': 'float64'})
    total_domestic_data = total_domestic_data.groupby(['Route', 'FROM', 'TO'])[['PASSENGERS', 'FREIGHT', 'MAIL', 'DAYS']].aggregate('sum').reset_index(drop = False)

    total_domestic_data.to_csv("./PreProcessed_Datasets/CityPairWiseDomesticPassengers/CityPairWiseDomesticPassengers.csv", index = None)

# Testing
fetch_CityPairWiseDomesticPassengers()