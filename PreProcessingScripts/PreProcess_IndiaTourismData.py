import numpy as np
import pandas as pd

# Dataset Sources:
# -> Domestic & Foreign visitors to Centrally Protected Ticketed Monuments Data: Data manually collected from https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwiY35yy0f39AhVlSWwGHXL6CwIQFnoECBIQAQ&url=https%3A%2F%2Ftourism.gov.in%2Fsites%2Fdefault%2Ffiles%2F2022-09%2FIndia%2520Tourism%2520Statistics%25202022%2520%2528English%2529.pdf&usg=AOvVaw1-4KC-GKJIlotlsIxFxFMe, https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwjTk-rA0f39AhVGT2wGHcHyDEEQFnoECAwQAQ&url=https%3A%2F%2Ftourism.gov.in%2Fsites%2Fdefault%2Ffiles%2F2022-09%2FIndia%2520Tourism%2520Statistics%25202021%2520%25281%2529.pdf&usg=AOvVaw3h3V-3AQKvZHtdbq5QSM7j, https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwjShrDE0f39AhWJSGwGHVQxBooQFnoECBAQAQ&url=https%3A%2F%2Ftourism.gov.in%2Fsites%2Fdefault%2Ffiles%2F2021-05%2FINDIA%2520TOURISM%2520STATISTICS%25202020.pdf&usg=AOvVaw0INamqon9LOB2ASVUCEIb0
# -> Tourism Location Coordinates: Data manually collected from https://www.google.com/maps

##################################################
# fetch_IndiaTourismData
##################################################
# -> Used for fetching tourism data related to
#    several Centrally Protected Ticketed
#    Monuments from 2018 to 2022
# -> Also loads the coordinates of each monument
#    to map its nearest cities
##################################################
def fetch_IndiaTourismData():

    # Monument Visitors
    print("Opening Monument Visitors Data - Visitors Data")
    monument_visitors_file = open('./Datasets/IndiaTourismData/MonumentVisitors.txt', 'r')

    year = 2023
    years = []
    data = []
    year_data = []
    city_data = []
    cnt = 0
    while(True):
        line = monument_visitors_file.readline().strip()
        if(line == ''):
            data.append(year_data)
            break
        elif(line == str(year - 1)):
            # Record year
            # -> End recording of year's data
            if(len(year_data) > 0):
                data.append(year_data)
            
            # -> Recording starts - initialize variables
            year -= 1
            year_data = []
            years.append(year)
            city_data = []
            num_monuments_count = 1
        elif(line.startswith("Total")):
            # Record data
            # -> Collect the monument visitor info
            data_points = line.split(' ')[1:-2]
            data_points = [float(x) if x!='-' else np.nan for x in data_points]
            city_data.extend(data_points)
        else:
            # Record city
            # -> Collect number of monuments info and end recording
            num_monuments_count_new = int(line.split(' ')[-1])
            num_monuments = num_monuments_count_new - num_monuments_count
            num_monuments_count = num_monuments_count_new
            if(num_monuments > 0):
                city_data.append(num_monuments)
                year_data.append(city_data)

            # -> Start recording next city's data
            city = " ".join(line.split(' ')[:-1])
            city_data = [city]
        cnt += 1
    
    # Deciding on column names
    cols = ['City', 'n-2:n-1, Domestic', 'n-2:n-1, Foreign', 'n-1:n, Domestic', 'n-1:n, Foreign', 'Number of Monuments']
    monument_visitors_data = pd.concat([pd.DataFrame(data[x], index = np.repeat(years[x], len(data[x])), columns = cols) for x in range(len(data))], axis = 0).reset_index(drop = False).rename({'index': 'Year'}, axis = 1)

    monument_visitors_data = pd.pivot_table(monument_visitors_data, values = ['n-2:n-1, Domestic', 'n-2:n-1, Foreign', 'n-1:n, Domestic', 'n-1:n, Foreign', 'Number of Monuments'], index = 'City', columns = 'Year', aggfunc = np.mean)

    monument_visitors_data.drop([('Number of Monuments', 2021), (  'n-2:n-1, Domestic', 2021), (  'n-2:n-1, Domestic', 2022), (   'n-2:n-1, Foreign', 2021), (   'n-2:n-1, Foreign', 2022)], axis = 1, inplace = True)
    monument_visitors_data.columns = ['_'.join([str(x) for x in col]) for col in monument_visitors_data.columns.values]
    monument_visitors_data.rename({
        'Number of Monuments_2020': 'NumMonuments2020',
        'Number of Monuments_2022': 'NumMonuments2022',
        'n-2:n-1, Domestic_2020': 'Domestic2018-19',
        'n-1:n, Domestic_2020': 'Domestic2019-20',
        'n-1:n, Domestic_2021': 'Domestic2020-21',
        'n-1:n, Domestic_2022': 'Domestic2021-22',
        'n-2:n-1, Foreign_2020': 'Foreign2018-19',
        'n-1:n, Foreign_2020': 'Foreign2019-20',
        'n-1:n, Foreign_2021': 'Foreign2020-21',
        'n-1:n, Foreign_2022': 'Foreign2021-22',
    }, axis = 1, inplace = True)
    monument_visitors_data = monument_visitors_data.reset_index(drop = False).rename({'index': 'City'}, axis = 1)

    # If values for certain rows are missing, fill them in by using neighbouring values (assuming linear trend)
    for idx in range(monument_visitors_data.shape[0]):
        row = monument_visitors_data.iloc[idx]
        if(pd.isnull(row['NumMonuments2020'])):
            monument_visitors_data.loc[idx, 'NumMonuments2020'] = monument_visitors_data.iloc[idx]['NumMonuments2022']
        if(pd.isnull(row['Domestic2018-19'])):
            monument_visitors_data.loc[idx, 'Domestic2018-19'] = int(3 * monument_visitors_data.iloc[idx]['Domestic2021-22']- 2 * monument_visitors_data.iloc[idx]['Domestic2020-21'])
        if(pd.isnull(row['Domestic2019-20'])):
            monument_visitors_data.loc[idx, 'Domestic2019-20'] = int(2 * monument_visitors_data.iloc[idx]['Domestic2021-22']- monument_visitors_data.iloc[idx]['Domestic2020-21'])
        if(pd.isnull(row['Domestic2020-21'])):
            monument_visitors_data.loc[idx, 'Domestic2020-21'] = int((monument_visitors_data.iloc[idx]['Domestic2019-20'] + monument_visitors_data.iloc[idx]['Domestic2021-22']) / 2)
        if(pd.isnull(row['Foreign2018-19'])):
            monument_visitors_data.loc[idx, 'Foreign2018-19'] = int(3 * monument_visitors_data.iloc[idx]['Foreign2021-22']- 2 * monument_visitors_data.iloc[idx]['Foreign2020-21'])
        if(pd.isnull(row['Foreign2019-20'])):
            monument_visitors_data.loc[idx, 'Foreign2019-20'] = int(2 * monument_visitors_data.iloc[idx]['Foreign2021-22']- monument_visitors_data.iloc[idx]['Foreign2020-21'])
        if(pd.isnull(row['Domestic2020-21'])):
            monument_visitors_data.loc[idx, 'Foreign2020-21'] = int((monument_visitors_data.iloc[idx]['Foreign2019-20'] + monument_visitors_data.iloc[idx]['Foreign2021-22']) / 2)
    
    monument_visitors_data.to_csv("./PreProcessed_Datasets/IndiaTourismData/MonumentVisitors.csv", index = None)

    # Monument Visitors
    print("Opening Monument Visitors Data - Tourist Location Coordinates")

    tourist_loc_coords = pd.read_csv("./Datasets/IndiaTourismData/TouristLocationsCoords.csv")
    tourist_loc_coords.to_csv("./PreProcessed_Datasets/IndiaTourismData/TouristLocationsCoords.csv", index = None)

# # Testing
# fetch_IndiaTourismData()