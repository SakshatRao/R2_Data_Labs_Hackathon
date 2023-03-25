import numpy as np
import pandas as pd
import json
import time
from collections import OrderedDict

def fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = False):

    # Railways Data
    print("Opening Indian Railways Data")
    stations_data = json.load(open("./Datasets/OtherTransportModes/Railways/IndianRailwaysData/stations.json"))['features']
    trains_data = json.load(open("./Datasets/OtherTransportModes/Railways/IndianRailwaysData/trains.json"))['features']
    stops_data = json.load(open("./Datasets/OtherTransportModes/Railways/IndianRailwaysData/schedules.json"))

    if(save_station_to_district_map == True):    # Record & save station-to-district mapping info

        # District Centroids
        print("Opening District Centroid Data")
        district_centroid_df1 = pd.read_csv("./Datasets/IndianDistrictsCentroid/district wise centroids.csv")
        district_centroid_df2 = pd.read_csv("./Datasets/IndianDistrictsCentroid/district wise population and centroids.csv", usecols = ['State', 'District', 'Latitude', 'Longitude'])
        district_centroid_df = pd.concat([district_centroid_df1, district_centroid_df2], axis = 0)
        district_centroid_df = district_centroid_df.groupby(['State', 'District'])[['Latitude', 'Longitude']].aggregate('mean').reset_index(drop = False)

        # Station Codes
        print("Opening Station Codes Data")
        station_codes_file = open("./Datasets/OtherTransportModes/Railways/IndianRailwayStations/StationCodes.txt")
        station_codes = station_codes_file.read()
        station_codes = station_codes.split("<tr>")[2:]
        station_codes = [x.split('</tr>')[0] for x in station_codes]
        station_codes = [x.split('<td>')[1:4] for x in station_codes]
        station_codes = [[y.split('</td>')[0] for y in x] for x in station_codes]
        station_codes_df = pd.DataFrame(station_codes, columns = ['Name', 'StationCode', 'State'])
        station_codes_df.dropna(inplace = True)

        def extract_name_from_html(html):
            if('<' in html):
                return extract_name_from_html(html.split('>')[1].split('<')[0])
            else:
                return html.strip()
        station_codes_df['Name'] = station_codes_df['Name'].apply(extract_name_from_html)
        station_codes_df['StationCode'] = station_codes_df['StationCode'].apply(extract_name_from_html)
        station_codes_df['State'] = station_codes_df['State'].apply(extract_name_from_html)

        # States in Railways Data to states in District Centroid Data - mapping corrections
        station_to_district_centroid_state_none = ['', 'Bangladesh', None]
        station_to_district_centroid_state_map = {
            'Odisha': 'Orissa',
            'Uttarakhand': 'Uttaranchal',
            'Delhi NCT': 'Delhi'
        }

        # States in Station Code Data to states in District Centroid Data - mapping corrections
        station_code_to_district_centroid_state_none = ['']
        station_code_to_district_centroid_state_map = {
            'Tamil nadu': 'Tamil Nadu',
            'WEST BENGAL': 'West Bengal',
            'Odisha': 'Orissa',
            'Jammu &amp; Kashmir': 'Jammu and Kashmir',
            'Uttarakhand': 'Uttaranchal',
            'bihar': 'Bihar',
            'Telangana': 'Andhra Pradesh',
            'Madhya Pardesh': 'Madhya Pradesh',
            'Pondicherry': 'Puducherry',
            'Andhra pradesh': 'Andhra Pradesh',
            'Uttar pradesh': 'Uttar Pradesh',
            'Delhi NCT': 'Delhi'
        }

        # Function to find district of a given station
        def find_station_district(station_attr):
            station_code = station_attr['properties']['code']
            
            # Step 1 - Try to find the State of station
            station_state = station_attr['properties']['state']    # Finding state from Railways Data
            found_station_state = True
            if(station_state == None):    # If state not present in Railways Data
                station_state_df_info = station_codes_df[station_codes_df['StationCode'] == station_code]    # Trying to find whether we have the station in Station Code Data
                if(station_state_df_info.shape[0] == 0):    # If we do not have station in Station Code Data
                    found_station_state = False
                else:    # If we do have station in Station Code Data
                    if(station_state_df_info.shape[0] > 1):    # If we have found multiple instances of given station in Station Code Data
                        all_same_code_stations = set(station_state_df_info['State'].values)    # Finding whether the states for multiple instances are the same
                        if(len(all_same_code_stations) == 1):    # If the multiple instances have same states
                            station_state = [*all_same_code_stations][0]
                        else:    # If states of multiple instances are different
                            found_station_state = False
                    else:    # If we have single instance of station code in Station Code Data
                        station_state = station_state_df_info.iloc[0]['State']
                    if(found_station_state == True):    # If we have found the station in Station Code Data in previous step
                        if(pd.isnull(station_state) == True):    # If the station info doesn't exist in Station Code Data
                            found_station_state = False
                        else:    # If station info exists in Station Code Data
                            # Apply Station Code to District Centroid state mapping correction
                            if(station_state in station_code_to_district_centroid_state_none):
                                found_station_state = False
                            elif(station_state in station_code_to_district_centroid_state_map):
                                station_state = station_code_to_district_centroid_state_map[station_state]
            else:    # If state is present in Railways Data
                # Apply Railways Data to District Centroid state mapping correction
                if(station_state in station_to_district_centroid_state_none):
                    found_station_state = False
                elif(station_state in station_to_district_centroid_state_map):
                    station_state = station_to_district_centroid_state_map[station_state]
            
            station_coord = np.array([station_attr['geometry']['coordinates'][1], station_attr['geometry']['coordinates'][0]])
            # If state of station found, narrow down districts to search in, else use all districts
            if(found_station_state == True):
                state_district_centroids = district_centroid_df[district_centroid_df['State'] == station_state]
            else:
                state_district_centroids = district_centroid_df.copy()
            state_district_centroids_coords = state_district_centroids[['Latitude', 'Longitude']].values
            assert(state_district_centroids.shape[0] > 0)
            # Function to search closest district centroid to station coordinates
            def closest_node(node, nodes):
                dist = np.sum((nodes - node)**2, axis=1)
                return np.argmin(dist)
            closest_district_idx = closest_node(station_coord, state_district_centroids_coords)
            district = state_district_centroids.iloc[closest_district_idx]['District']
            state = state_district_centroids.iloc[closest_district_idx]['State']
            return district, state
        
        all_station_districts = {}
        for station in stations_data:
            station_code = station['properties']['code']
            if((station['geometry'] != None) and (len(station['geometry']['coordinates']) == 2)):
                station_district, station_state = find_station_district(station)
                assert(station_code not in all_station_districts)
                all_station_districts[station_code] = {}
                all_station_districts[station_code]['District'] = station_district
                all_station_districts[station_code]['State'] = station_state
        all_station_districts_df = pd.DataFrame.from_dict(all_station_districts, orient = 'index').reset_index(drop = False)
        all_station_districts_df = all_station_districts_df.rename({'index': 'StationCode'}, axis = 1)

        def duplicate_districts(row):
            if(row['District'] in ['Bilaspur', 'Raigarh', 'Aurangabad']):
                return row['District'] + '_' + row['State']
            else:
                return row['District']
        all_station_districts_df['District'] = all_station_districts_df.apply(duplicate_districts, axis = 1)

        all_station_districts_df.to_csv("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayStations/all_station_districts.csv", index = None)
    
    else:
        
        all_station_districts_df = pd.read_csv("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayStations/all_station_districts.csv")
        all_station_districts_df = all_station_districts_df.set_index("StationCode")
        all_station_districts = all_station_districts_df.to_dict(orient = "index")

        # Format: {From station -> {To station -> {distance -> <>, other train info -> <>}}
        city_to_city_train_info = {}
        
        for train_idx, train in enumerate(trains_data):
            if(train_idx % 500 == 0):
                print(f"{train_idx} Trains Scanned out of {len(trains_data)}!")
            train_num = train['properties']['number']
            all_stops = [x for x in stops_data if x['train_number'] == train_num]
            all_stops = [x for x in all_stops if x['station_code'] in all_station_districts]    # Remove stops which are not mapped to a district
            
            all_stop_station_codes = [x['station_code'] for x in all_stops]
            all_stop_districts = [all_station_districts[x]['District'] for x in all_stop_station_codes]

            common_districts = set(all_stop_districts).intersection(set(district_to_city_map.keys()))
            if(len(common_districts) >= 2):    # If the given train route passes through atleast 2 known districts, we can collect info about this route

                # Remove duplicate stops
                def remove_same_stops(raw_stops):
                    stops = [*raw_stops]
                    known_stops = []
                    to_remove_idx = []
                    for stop_idx, stop in enumerate(stops):
                        if(stop['station_code'] in known_stops):
                            to_remove_idx.append(stop_idx)
                        known_stops.append(stop['station_code'])
                    for num_removed, idx in enumerate(to_remove_idx):
                        stops.pop(idx - num_removed)
                    return stops
                all_stops = remove_same_stops(all_stops)
                
                # Function to calculate average time when train reaches stop (avg of arrival & departure)
                def calculate_timing(arrival, departure):
                    if((arrival == 'None') and (departure == 'None')):
                        return np.nan
                    elif(arrival == 'None'):
                        return time.mktime(time.strptime(f"2023:{departure}", "%Y:%H:%M:%S"))
                    elif(departure == 'None'):
                        return time.mktime(time.strptime(f"2023:{arrival}", "%Y:%H:%M:%S"))
                    else:
                        arrival_time = time.mktime(time.strptime(f"2023:{arrival}", "%Y:%H:%M:%S"))
                        departure_time = time.mktime(time.strptime(f"2023:{departure}", "%Y:%H:%M:%S"))
                        # If arrival is at end of one day and departure is at start of next, then add one day to departure timing
                        # Assumption is late arrival would be post 7 PM and early departure would be before 5 AM
                        if((time.localtime(arrival_time).tm_hour in [19, 20, 21, 22, 23]) and (time.localtime(departure_time).tm_hour in [0, 1, 2, 3, 4])):
                            return (arrival_time + (departure_time + 24 * 60 * 60)) / 2
                        else:
                            return (arrival_time + departure_time) / 2

                # Sort all stops in ascending order of timing, this is essential for further analyses
                def sort_stops(raw_stops):
                    stops = [*raw_stops]
                    to_remove_idx = []
                    timings = []
                    for stop_idx, stop in enumerate(stops):
                        day = stop['day']
                        if(day != None):    # If day info is not missing
                            arrival_time = stop['arrival']
                            departure_time = stop['departure']
                            timing = calculate_timing(arrival_time, departure_time) / 60
                            if(pd.isnull(timing)):    # If both arrival & departure timings are not given, skip this stop
                                to_remove_idx.append(stop_idx)
                            else:
                                timing += ((day - 1) * 24 * 60 * 60) / 60    # Add 'n' number of days to timings based on schedule
                                timings.append(timing)
                        else:    # If day info is missing, skip this stop
                            to_remove_idx.append(stop_idx)
                    for num_removed, idx in enumerate(to_remove_idx):
                        stops.pop(idx - num_removed)    # Remove stops to be skipped
                    sort_idx = np.argsort(timings)    # Sort as per the timings
                    new_stops = []
                    new_timings = []
                    for idx in sort_idx:
                        new_stops.append(stops[idx])
                        new_timings.append(timings[idx])
                    return new_stops, new_timings
                # Sort stops & also collect timing info for each stop
                all_stops, all_stop_timings = sort_stops(all_stops)

                # Recalculate station codes & districts since some stops would have been skipped
                all_stop_station_codes = [x['station_code'] for x in all_stops]
                all_stop_districts = [all_station_districts[x]['District'] for x in all_stop_station_codes]

                # Test to verify stops are indeed in sequence
                # 1. Departure >= Arrival
                # 2. Arrival/departure >= Previous arrival/departure
                def ensure_stops_sequential(stops):
                    previous_arrival_time = 0
                    previous_departure_time = 0
                    previous_arrival_idx = -1; previous_departure_idx = -1
                    for stop_idx, stop in enumerate(stops):
                        day = stop['day']
                        if(day != None):
                            # Calculate and check for arrival time
                            arrival_time = stop['arrival']
                            if(arrival_time != 'None'):
                                arrival_time = time.mktime(time.strptime(f"2023:{arrival_time}", "%Y:%H:%M:%S")) / 60
                                arrival_time += ((day - 1) * 24 * 60 * 60) / 60
                                assert(arrival_time >= previous_departure_time)
                                assert(arrival_time >= previous_arrival_time)
                            # Calculate and check for departure time
                            departure_time = stop['departure']
                            if(departure_time != 'None'):
                                departure_time = time.mktime(time.strptime(f"2023:{departure_time}", "%Y:%H:%M:%S")) / 60
                                departure_time += ((day - 1) * 24 * 60 * 60) / 60
                                # If arrival is at end of one day and departure is at start of next, then add one day to departure timing
                                # Assumption is late arrival would be post 7 PM and early departure would be before 5 AM
                                if(arrival_time != 'None'):
                                    if((time.localtime(arrival_time * 60).tm_hour in [19, 20, 21, 22, 23]) and (time.localtime(departure_time * 60).tm_hour in [0, 1, 2, 3, 4])):
                                        departure_time += (24 * 60 * 60) / 60
                                assert(departure_time >= previous_departure_time)
                                assert(departure_time >= previous_arrival_time)
                                if(arrival_time != 'None'):
                                    assert(departure_time >= arrival_time)
                            if(arrival_time != 'None'):
                                previous_arrival_time = arrival_time
                                previous_arrival_idx = stop_idx
                            if(departure_time != 'None'):
                                previous_departure_time = departure_time
                                previous_departure_idx = stop_idx
                ensure_stops_sequential(all_stops)

                # Finding city-to-city route info
                # Step 1 -> Go through all stops sequentially and record info about what time the train reaches each district
                # Step 2 -> Once the train leaves a known district, find out how much long did it take from previous known districts to this known district
                # Step 3 -> Add this known district to previously known districts and continue anlaysis
                known_stops_district = []
                known_stops_timings = []
                current_timing_sum = 0
                current_timing_count = 0
                previous_district = ""; current_district = ""
                for idx in range(len(all_stops) + 1):    # Adding one extra iteration at end-of-loop (to collect info about last district)
                    if(idx < len(all_stops)):    # For districts before the last district
                        current_district = all_stop_districts[idx]
                    else:    # For the last district
                        current_district = ""
                    
                    if((current_district != previous_district) and (previous_district in district_to_city_map)):    # If the train has passed a known district
                        # RECORD THE ROUTE DURATION & INFO
                        # Iterating through all previously known stops
                        for known_stop_idx in range(len(known_stops_district)):
                            known_district = known_stops_district[known_stop_idx]
                            known_timing = known_stops_timings[known_stop_idx]
                            if(current_timing_count > 0):    # If timing info for current district was collected, else skip data recording
                                duration_mins = (current_timing_sum / current_timing_count) - known_timing    # Duration = Average current timing - timing of other previously known district
                                route_info = {
                                    'duration': duration_mins,
                                    'train_num': train_num,
                                    'third_ac': train['properties']['third_ac'],
                                    'chair_car': train['properties']['chair_car'],
                                    'first_class': train['properties']['first_class'],
                                    'sleeper': train['properties']['sleeper'],
                                    'second_ac': train['properties']['second_ac'],
                                    'type': train['properties']['type'],
                                    'first_ac': train['properties']['first_ac']
                                }
                                if(known_district not in city_to_city_train_info):
                                    city_to_city_train_info[known_district] = {}
                                if(previous_district not in city_to_city_train_info[known_district]):
                                    city_to_city_train_info[known_district][previous_district] = []
                                city_to_city_train_info[known_district][previous_district].append(route_info)    # Data recording
                            
                        # Add current district to previously known districts in route
                        if(current_timing_count > 0):    # If timing info for current district was collected, else skip this
                            known_stops_district.append(previous_district)
                            known_stops_timings.append(current_timing_sum / current_timing_count)
                        
                        # Reset
                        current_timing_sum = 0
                        current_timing_count = 0

                    if(idx < len(all_stops)):    # If not the last iteration
                        current_timing = all_stop_timings[idx]
                        if(current_district in district_to_city_map):    # If district is known
                            # Continue aggregation of data
                            current_timing_sum += current_timing
                            current_timing_count += 1
                        else:
                            pass

                        previous_district = current_district

        with open("./PreProcessed_Datasets/OtherTransportModes/Railways/IndianRailwayRoutes/CityToCityRoutes.json", "w") as save_file:
            json.dump(city_to_city_train_info, save_file)
        
# # Testing
# city_mapping = pd.read_csv('./Datasets/CityMapping.csv')
# district_to_city_map = dict(zip(city_mapping['District (Station Code)'].values, city_mapping['City'].values))
# #fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = True)
# fetch_IndianRailwaysData(district_to_city_map, save_station_to_district_map = False)