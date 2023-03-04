import numpy as np
import pandas as pd
import os
import json
import locale

def fetch_IndiaSocioEconomicData():

    # Economic Data
    print("Opening Economic Data")
    total_economic_data = pd.DataFrame()
    all_economic_data_paths = os.listdir("./Datasets/IndiaEconomicData/")
    all_economic_data_paths = [x for x in all_economic_data_paths if x.startswith('gdp')]
    for economic_data_path in all_economic_data_paths:
        economic_data = pd.read_csv("./Datasets/IndiaEconomicData/" + economic_data_path)
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        for col in economic_data.columns:
            if(col not in ['Year', 'Description']):
                if(economic_data[col].dtype == object):
                    economic_data[col] = economic_data[col].apply(lambda x: locale.atof(str(x)) if str(x) not in ['NaN', 'NA', 'nan'] else np.nan)
                    economic_data[col] = economic_data[col].astype('float64')
            else:
                economic_data[col] = economic_data[col].apply(lambda x: x.strip())
        economic_data = economic_data.melt(id_vars = ['Year', 'Description'], var_name = 'District', value_name = 'Value')
        economic_data = pd.pivot_table(economic_data, values = 'Value', index = ['Year', 'District'], columns = ['Description'], aggfunc = np.mean).reset_index(drop = False).reset_index(drop = True)
        economic_data['District'] = economic_data['District'].apply(lambda x: x.strip().upper())
        economic_data['State'] = np.repeat(economic_data_path.split('_')[-1].split('.')[0].replace("1", "").replace("2", ""), economic_data.shape[0])
        economic_data.rename({'GDP (in Rs. Cr.)': 'GDP', 'Growth Rate % (YoY)': 'GrowthRate'}, axis = 1, inplace = True)
        total_economic_data = pd.concat([total_economic_data, economic_data], axis = 0)
    
    # For easier analysis
    total_economic_data = total_economic_data.groupby(['State', 'District', 'Year'])['GDP'].aggregate('mean').reset_index(drop = False)
    total_economic_data['District'].replace({'KAMRUP (METROPOLITAN)': 'KAMRUP METROPOLITAN', 'DEHRADIN': 'DEHRADUN'}, inplace = True)
    
    total_economic_data.to_csv("./PreProcessed_Datasets/IndiaEconomicData/EconomicData.csv", index = None)

    # Population, Area & Household Data
    print("Opening Population, Area & Household Data")
    pop_area_household_data = pd.read_excel("./Datasets/IndiaSocialData/A-1_NO_OF_VILLAGES_TOWNS_HOUSEHOLDS_POPULATION_AND_AREA.xlsx", header = [0, 1, 2, 3])
    pop_area_household_data = pop_area_household_data.drop(pop_area_household_data.columns[:3], axis = 1)
    pop_area_household_data.columns = ['_'.join([str(x) for x in col]) for col in pop_area_household_data.columns.values]
    pop_area_household_data.rename({
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_India/ State/ Union Territory/ District/ Sub-district_Unnamed: 3_level_2_4': 'IsDistrict',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Name_Unnamed: 4_level_2_5': 'District',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Total/\nRural/\nUrban_Unnamed: 5_level_2_6': 'IsTotal',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Number of villages_Inhabited_7': 'InhabitedVillages',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Number of villages_Uninhabited_8': 'UninhabitedVillages',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Number of towns_Unnamed: 8_level_2_9': 'Towns',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Number of households_Unnamed: 9_level_2_10': 'Households',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Population_Persons_11': 'Population',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Population_Males_12': 'MalePopulation',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Population_Females_13': 'FemalePopulation',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Area\n (In sq. km)_Unnamed: 13_level_2_13': 'Area',
        'A-1 NUMBER OF VILLAGES, TOWNS, HOUSEHOLDS, POPULATION AND AREA_Population per sq. km._Unnamed: 14_level_2_14': 'PopulationPerSqKm'
    }, axis = 1, inplace = True)
    pop_area_household_data = pop_area_household_data.loc[(pop_area_household_data['IsDistrict'] == 'DISTRICT') & (pop_area_household_data['IsTotal'] == 'Total')]
    pop_area_household_data.to_csv("./PreProcessed_Datasets/IndiaSocialData/Pop_Area_Household.csv", index = None)

    # Population Trend Data
    print("Opening Population Trend Data")
    all_population_data_files = os.listdir("./Datasets/IndiaSocialData/")
    all_population_data_files = [x for x in all_population_data_files if ('A-2' in x) and (x.startswith('.') == False)]
    
    district_pop_info = {}
    for population_file in all_population_data_files:
        population_data = pd.read_excel("./Datasets/IndiaSocialData/" + population_file)
        population_data.columns = [str(x) for x in range(population_data.shape[1])]
        
        state = ""
        data_start_point = False
        district_data_start_point = False
        district_pop_data_start_point = False
        for idx, row in population_data.iterrows():
            if(district_data_start_point == True):
                if(district_pop_data_start_point == True):
                    if((pd.isnull(row['3']) == False) and (pd.isnull(row['0']) == True)):
                        year = str(row['3'])
                        district_pop_info[district]['history'][year] = [row['4'], row['7'], row['8']]
                    else:
                        district_pop_data_start_point = False
                if(pd.isnull(row['0']) == False):
                    district = row['2'].lower()
                    if(district in district_pop_info):
                        if(district_pop_info[district]['state'] == state):
                            print("ALERT! Found copy district ", district, "in state ", state)
                    district_pop_info[district] = {'state': state, 'history': {}}
                    district_pop_data_start_point = True
            if((district_data_start_point == False) and (data_start_point == True)):
                if(pd.isnull(row['0']) == False):
                    state = row['2'].lower()
                    district_data_start_point = True
            if((data_start_point == False) and (row['0'] == 1)):
                data_start_point = True
    
    with open('./PreProcessed_Datasets/IndiaSocialData/PopulationHistory.json', 'w') as save_file:
        json.dump(district_pop_info, save_file)

    # Latest Population Data
    print("Opening Latest Population Data")
    pop_data = pd.read_csv("./Datasets/IndiaSocialData/IndiaPopulation23WithCoords.csv", usecols = ['city', 'pop2023', 'latitude', 'longitude'])
    pop_data.to_csv("./PreProcessed_Datasets/IndiaSocialData/IndiaPopulation23WithCoords.csv", index = None)

# Testing
fetch_IndiaSocioEconomicData()