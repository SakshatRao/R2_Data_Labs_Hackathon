import numpy as np
import pandas as pd
import os
import json
import locale
import re

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
    
    # Format: district -> {state, history -> {year = [TotalPopulation, MalePopulation, FemalePopulation]}}
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
                if(pd.isnull(row['0']) == False):
                    district = row['2'].lower()
                    if(district in district_pop_info):
                        if(district_pop_info[district]['state'] == state):
                            print("ALERT! Found copy district ", district, "in state ", state)
                    district_pop_info[district.strip()] = {'state': state, 'history': {}}
                    district_pop_data_start_point = True
                if(district_pop_data_start_point == True):
                    if(pd.isnull(row['3']) == False):
                        year = str(row['3'])
                        district_pop_info[district.strip()]['history'][year] = [row['4'], row['7'], row['8']]
                    else:
                        district_pop_data_start_point = False
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

    # Education Data
    print("Opening Education Data")
    all_education_data_df = pd.DataFrame()
    
    for year in ['2011', '2001', '1991']:
        education_yearly_data_files = os.listdir(f"./Datasets/IndiaEducationalData/{year}/")
        education_yearly_data_files = [x for x in education_yearly_data_files if x.startswith(".") == False]
        for education_yearly_data_file in education_yearly_data_files:
            if(year != '1991'):
                education_yearly_data = pd.read_excel(f"./Datasets/IndiaEducationalData/{year}/" + education_yearly_data_file, header = [0, 1, 2, 3, 4, 5], skiprows = [0])
            else:
                education_yearly_data = pd.read_excel(f"./Datasets/IndiaEducationalData/{year}/" + education_yearly_data_file, header = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], skiprows = [0, 1])
            education_yearly_data.columns = ['_'.join([str(x) for x in col]) for col in education_yearly_data.columns.values]
            if(year == '2011'):
                education_data_col_map = {
                    'Area Name_Unnamed: 3_level_1_Unnamed: 3_level_2_Unnamed: 3_level_3_Unnamed: 3_level_4_Unnamed: 3_level_5': 'District',
                    'Total/_Rural/_Urban/_Unnamed: 4_level_3_Unnamed: 4_level_4_Unnamed: 4_level_5': 'IsTotal',
                    'Age-group_Unnamed: 5_level_1_Unnamed: 5_level_2_Unnamed: 5_level_3_1_Unnamed: 5_level_5': 'AgeGroup',
                    'Total population_Unnamed: 6_level_1_Unnamed: 6_level_2_Persons_2_Unnamed: 6_level_5': 'TotalPopulation',
                    'Total population_Unnamed: 7_level_1_Unnamed: 7_level_2_Males_3_Unnamed: 7_level_5': 'TotalMalePopulation',
                    'Total population_Unnamed: 8_level_1_Unnamed: 8_level_2_Females_4_Unnamed: 8_level_5': 'TotalFemalePopulation',
                    '          Educational level_ Graduate and above_Unnamed: 9_level_2_Persons_5_Unnamed: 9_level_5': 'Graduates',
                    '          Educational level_ Graduate and above_Unnamed: 10_level_2_Males_6_Unnamed: 10_level_5': 'MaleGraduates',
                    '          Educational level_ Graduate and above_Unnamed: 11_level_2_Females_7_Unnamed: 11_level_5': 'FemaleGraduates'
                }
            elif(year == '2001'):
                education_data_col_map = {
                    'Area Name_Unnamed: 4_level_1_Unnamed: 4_level_2_Unnamed: 4_level_3_Unnamed: 4_level_4_Unnamed: 4_level_5': 'District',
                    'Total/_Rural/_Urban/_Unnamed: 5_level_3_Unnamed: 5_level_4_Unnamed: 5_level_5': 'IsTotal',
                    'Age-group_Unnamed: 6_level_1_Unnamed: 6_level_2_Unnamed: 6_level_3_1_Unnamed: 6_level_5': 'AgeGroup',
                    'Total _Unnamed: 7_level_1_Unnamed: 7_level_2_Persons_2_Unnamed: 7_level_5': 'TotalPopulation',
                    'Total _Unnamed: 8_level_1_Unnamed: 8_level_2_Males_3_Unnamed: 8_level_5': 'TotalMalePopulation',
                    'Total _Unnamed: 9_level_1_Unnamed: 9_level_2_Females_4_Unnamed: 9_level_5': 'TotalFemalePopulation',
                    '          Educational level_ Graduate and above_Unnamed: 10_level_2_Persons_5_Unnamed: 10_level_5': 'Graduates',
                    '          Educational level_ Graduate and above_Unnamed: 11_level_2_Males_6_Unnamed: 11_level_5': 'MaleGraduates',
                    '          Educational level_ Graduate and above_Unnamed: 12_level_2_Females_7_Unnamed: 12_level_5': 'FemaleGraduates'
                }
            else:
                education_data_col_map = {
                    'Name_Unnamed: 3_level_1_Unnamed: 3_level_2_Unnamed: 3_level_3_Unnamed: 3_level_4_Unnamed: 3_level_5_Unnamed: 3_level_6_Unnamed: 3_level_7_Unnamed: 3_level_8_Unnamed: 3_level_9_Unnamed: 3_level_10_Unnamed: 3_level_11_4': 'District',
                    'Total/Rural/Urban_Unnamed: 4_level_1_Unnamed: 4_level_2_Unnamed: 4_level_3_Unnamed: 4_level_4_Unnamed: 4_level_5_Unnamed: 4_level_6_Unnamed: 4_level_7_Unnamed: 4_level_8_Unnamed: 4_level_9_Unnamed: 4_level_10_Unnamed: 4_level_11_5': 'IsTotal',
                    'Age Group_Unnamed: 5_level_1_Unnamed: 5_level_2_Unnamed: 5_level_3_Unnamed: 5_level_4_Unnamed: 5_level_5_Unnamed: 5_level_6_Unnamed: 5_level_7_Unnamed: 5_level_8_Unnamed: 5_level_9_Unnamed: 5_level_10_Unnamed: 5_level_11_6': 'AgeGroup',
                    'Total population_Unnamed: 6_level_1_Unnamed: 6_level_2_Unnamed: 6_level_3_Unnamed: 6_level_4_Unnamed: 6_level_5_Unnamed: 6_level_6_Unnamed: 6_level_7_Unnamed: 6_level_8_Unnamed: 6_level_9_Unnamed: 6_level_10_P_7': 'TotalPopulation',
                    'Total population_Unnamed: 7_level_1_Unnamed: 7_level_2_Unnamed: 7_level_3_Unnamed: 7_level_4_Unnamed: 7_level_5_Unnamed: 7_level_6_Unnamed: 7_level_7_Unnamed: 7_level_8_Unnamed: 7_level_9_Unnamed: 7_level_10_M_8': 'TotalMalePopulation',
                    'Total population_Unnamed: 8_level_1_Unnamed: 8_level_2_Unnamed: 8_level_3_Unnamed: 8_level_4_Unnamed: 8_level_5_Unnamed: 8_level_6_Unnamed: 8_level_7_Unnamed: 8_level_8_Unnamed: 8_level_9_Unnamed: 8_level_10_F_9': 'TotalFemalePopulation',
                    'Educational level_Unnamed: 27_level_1_Unnamed: 27_level_2_Graduate and above_Unnamed: 27_level_4_Unnamed: 27_level_5_Unnamed: 27_level_6_Unnamed: 27_level_7_Unnamed: 27_level_8_Unnamed: 27_level_9_Unnamed: 27_level_10_M_28': 'MaleGraduates',
                    'Educational level_Unnamed: 28_level_1_Unnamed: 28_level_2_Graduate and above_Unnamed: 28_level_4_Unnamed: 28_level_5_Unnamed: 28_level_6_Unnamed: 28_level_7_Unnamed: 28_level_8_Unnamed: 28_level_9_Unnamed: 28_level_10_F_29': 'FemaleGraduates'
                }
            education_yearly_data.rename(education_data_col_map, axis = 1, inplace = True)
            education_yearly_data = education_yearly_data[[*education_data_col_map.values()]]
            if(year == '1991'):
                education_yearly_data['Graduates'] = education_yearly_data['MaleGraduates'] + education_yearly_data['FemaleGraduates']
            
            if(year != '1991'):
                state = education_yearly_data[education_yearly_data['District'].apply(lambda x: x.startswith('State')) == True].iloc[0]['District'].split('State - ')[1]
            else:
                state = education_yearly_data[education_yearly_data['District'].apply(lambda x: x.startswith('State')) == True].iloc[0]['District'].split('State-')[1]
            
            if(year == '2001'):
                state = state.split('  ')[0]
            else:
                state = state.strip()
            state = re.sub('[^A-Za-z ]+', '', state).strip()
            
            education_yearly_data = education_yearly_data[(education_yearly_data['District'].apply(lambda x: x.startswith('District')) == True) & (education_yearly_data['IsTotal'] == 'Total')]
            if(year != '1991'):
                education_yearly_data['District'] = education_yearly_data['District'].apply(lambda x: x.split('District - ')[1])
            else:
                education_yearly_data['District'] = education_yearly_data['District'].apply(lambda x: x.split('District-')[1])
            education_yearly_data['District'] = education_yearly_data['District'].apply(lambda x: re.sub('[^A-Za-z ]+', '', x).strip())
            
            education_yearly_data.drop('IsTotal', axis = 1, inplace = True)
            for district in education_yearly_data['District'].unique():
                district_data_df = pd.DataFrame()
                district_education_data = education_yearly_data[education_yearly_data['District'] == district]
                age_wise_cols = ['TotalPopulation', 'TotalMalePopulation', 'TotalFemalePopulation', 'Graduates', 'MaleGraduates', 'FemaleGraduates']
                if(year == '1991'):
                    district_education_data.reset_index(drop = True, inplace = True)
                    district_education_data_35_59 = district_education_data[(district_education_data['AgeGroup'] == '35-39') | (district_education_data['AgeGroup'] == '40-44') | (district_education_data['AgeGroup'] == '45-49') | (district_education_data['AgeGroup'] == '50-54') | (district_education_data['AgeGroup'] == '55-59')]
                    to_remove_idx = district_education_data_35_59.index
                    district_education_data_35_59 = district_education_data_35_59.groupby(['District'])[age_wise_cols].aggregate('sum').reset_index(drop = False).rename({'index': 'District'})
                    district_education_data_35_59['AgeGroup'] = ['35-59']
                    district_education_data = district_education_data.drop(to_remove_idx, axis = 0)
                    district_education_data = pd.concat([district_education_data, district_education_data_35_59], axis = 0)

                    district_education_data.reset_index(drop = True, inplace = True)
                    district_education_data_60_plus = district_education_data[(district_education_data['AgeGroup'] == '60-64') | (district_education_data['AgeGroup'] == '65-69') | (district_education_data['AgeGroup'] == '70-74') | (district_education_data['AgeGroup'] == '75-79') | (district_education_data['AgeGroup'] == '80+')]
                    to_remove_idx = district_education_data_60_plus.index
                    district_education_data_60_plus = district_education_data_60_plus.groupby(['District'])[age_wise_cols].aggregate('sum').reset_index(drop = False).rename({'index': 'District'})
                    district_education_data_60_plus['AgeGroup'] = ['60+']
                    district_education_data = district_education_data.drop(to_remove_idx, axis = 0)
                    district_education_data = pd.concat([district_education_data, district_education_data_60_plus], axis = 0)
                for age_group in ['15-19', '20-24', '25-29', '30-34', '35-59', '60+']:
                    age_group_data = district_education_data[district_education_data['AgeGroup'] == age_group][age_wise_cols].sum()
                    age_group_data = pd.DataFrame([age_group_data.values], columns = [f"{age_group}_{x}" for x in age_wise_cols])
                    district_data_df = pd.concat([district_data_df, age_group_data], axis = 1)
                district_data_df['District'] = [district.upper()]
                district_data_df['State'] = [state.upper()]
                district_data_df['Year'] = [year]
                all_education_data_df = pd.concat([all_education_data_df, district_data_df], axis = 0)
    
    all_education_data_df = all_education_data_df.sort_values(['State', 'District', 'Year'])
    all_education_data_df = all_education_data_df.drop([x for x in all_education_data_df.columns if x.startswith('15-19')], axis = 1)
    all_education_data_df.to_csv("./PreProcessed_Datasets/IndiaEducationalData/EducationData.csv", index = None)

# # Testing
# fetch_IndiaSocioEconomicData()