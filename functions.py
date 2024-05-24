# list of functions used in the main script
import pandas as pd 
import os
import datetime as dt
import calendar
from dtype_dictionaries import route_id_mapping

def split_multiple_block_id(df):
    # 1. Extract rows with multiple block_id substrings and save their indexes
    multiple_blocks_indexes = df[df['block_id'].str.contains(',')].index
    df2 = df.loc[multiple_blocks_indexes].copy()

    # 2. Split block_id strings into a list of strings
    df2['block_id'] = df2['block_id'].str.split(', ')

    # 3. Group by service_date, direction_id, and departure_time
    groups = df2.groupby(['service_date', 'direction_id', 'actual'], observed=True)

    # 4. Add a dummy column to each group
    for _, group in groups:
        group['dummy_column'] = range(len(group))
        for i, row in group.iterrows():
            row['block_id'] = row['block_id'][row['dummy_column']]
            # replace the original row with the modified one
            group.loc[i] = row
        # Assign the new block_id values to the original dataframe
        df.loc[group.index, 'block_id'] = group['block_id']
    return df

# From the folder, import calendar_csv
def import_calendar_csv(foldername, filename):
    calendar_csv_path = os.path.join(foldername, filename)
    calendar_df = pd.read_csv(calendar_csv_path, sep=',')
    # Convert the date column to datetime
    calendar_df['date'] = pd.to_datetime(calendar_df['date'], format='%Y-%m-%d')
    # Convert day_of_week to category
    calendar_df['day_of_week'] = calendar_df['day_of_week'].astype('category')
    # Convert service_ids to dictionary
    calendar_df['service_ids'] = calendar_df['service_ids'].apply(eval)
    return calendar_df

def reduce_df_size(df):
    #Drop headway and scheduled_headway columns
    df = df.drop(columns=['headway', 'scheduled_headway'])
    # Remove rows whose point_type is Midpoint
    df = df[df['point_type'] != 'Midpoint']
    # Filter the df to only include the first and last stops of each half_trip_id
#    end_points_df = (df.groupby
#                        (['half_trip_id'], observed=True, as_index=False)
#                        ['time_point_order']
#                        .transform('max') #with transform I can get all the max occurrences while also preserving their original row index
#                    )
#    df = df.loc[(df.loc[:,'time_point_order'] == end_points_df)|(df.loc[:,'time_point_order'] == 1)]
    return df

def adjust_adt_df_settings(df, routes, feed_info_start_date, feed_info_end_date):
    arr_dep_df = df.copy()
    #Rename depareture_time to actual
    arr_dep_df = arr_dep_df.rename(columns={'departure_time': 'actual'})
    # Convert service_date, scheduled and actual columns to datetime objects
    arr_dep_df['service_date'] = pd.to_datetime(arr_dep_df['service_date'], format='%Y-%m-%d')
    arr_dep_df = arr_dep_df.loc[(arr_dep_df['service_date'] >= feed_info_start_date) & (arr_dep_df['service_date'] <= feed_info_end_date)]
    arr_dep_df['scheduled'] = pd.to_datetime(arr_dep_df['scheduled'], format='ISO8601', utc=True).dt.time
    arr_dep_df['actual'] = pd.to_datetime(arr_dep_df['actual'], format='ISO8601', utc=True).dt.time
    # Replace inbound and outbound entries with 1 and 0
    arr_dep_df['direction_id'] = arr_dep_df['direction_id'].cat.rename_categories({'Inbound': 1, 'Outbound': 0})
    # Replace nan entries in the actual column with the scheduled values
    arr_dep_df.loc[:,'actual'] = arr_dep_df.loc[:,'actual'].fillna(arr_dep_df.loc[:,'scheduled'])
    # Strip undesired characters in the strings of route_ids
    arr_dep_df['route_id'] = arr_dep_df['route_id'].str.lstrip('0')
    arr_dep_df['route_id'] = arr_dep_df['route_id'].str.rstrip('_')
    # Replace values in the route_id column with the corresponding dict.values() in route_id_mapping
    arr_dep_df['route_id'] = arr_dep_df['route_id'].replace(route_id_mapping)

    arr_dep_df['block_id'] = ''
    arr_dep_df['service_id'] = ''   
    return arr_dep_df

def get_compatible_files(MBTA_ArrivalDeparture_path, feed_start_date, feed_end_date):
    # Fetch the names of the MBTA Arrival and Departure times subdirectories
    yearlyFolders = os.listdir(MBTA_ArrivalDeparture_path)
    numYears = len(yearlyFolders)
    # Create empty list where to store the filenames
    file_list = []

    for year in range(numYears):
        num_files = len(os.listdir(os.path.join(MBTA_ArrivalDeparture_path, yearlyFolders[year])))
        files_path = os.path.join(MBTA_ArrivalDeparture_path, yearlyFolders[year])

        for month in range(num_files):
            #print(f'files: {os.listdir(files_path)}')
            filename = (os.path.join(files_path, os.listdir(files_path)[month]))
            file_list.append(filename)

    # This list will be used to filter the files to be imported
    compatibleFiles = []

    for file in file_list:
        # Extract the date from the filename
        date = file.split('_')[-1].split('.')[0]
        # Convert the date to a datetime object
        date = dt.datetime.strptime(date, '%Y-%m')
        # Check if the date is within the feed_start_date and feed_end_date. Apply the control only to year and month
        lower_date = dt.date(year=feed_start_date.year, month=feed_start_date.month, day=1)
        upper_date = dt.date(year=feed_end_date.year, month=feed_end_date.month, day=calendar.monthrange(feed_end_date.year, feed_end_date.month)[1])
        if lower_date <= date.date() <= upper_date:
                compatibleFiles.append(file)
    return compatibleFiles

def handle_24h_time(series):
    return series.str.replace(r'^24', '00', regex=True) \
                 .str.replace(r'^25', '01', regex=True) \
                 .str.replace(r'^26', '02', regex=True) \
                 .str.replace(r'^27', '03', regex=True) \
                 .str.replace(r'^28', '04', regex=True) \
                 .str.replace(r'^29', '05', regex=True) \
                 .str.replace(r'^30', '06', regex=True) \

# Parse datetime strings to datetime objects
def parse_datetime_strings(df):
    df['scheduled'] = handle_24h_time(df['scheduled'])
    df['scheduled'] = pd.to_datetime(df['scheduled'], format='%H:%M:%S')
    return df

def get_gtfs_post_rating_txt_files(folderpath, list_of_txt_files, gtfs_dtypes):
    # Parse txt files as dataframes
    df_names = [txt_file.rstrip('.txt') for txt_file in list_of_txt_files]
    # Read txt files into dataframes and assign them the names in df_names, and the dtypes in gtfs_cols using the keys with the same name as the dataframe
    dfs = [pd.read_csv(os.path.join(folderpath, gtfs_file), sep=',', low_memory=False, dtype=gtfs_dtypes[df_name]) for df_name, gtfs_file in zip(df_names, list_of_txt_files)]
    #dfs = [pd.read_csv(os.path.join(txt_path, gtfs_file), sep=',', low_memory=False) for gtfs_file in txt_list]
    # create a dictionary of dataframes
    gtfsSchedule = dict(zip(df_names, dfs))
    # Assign the dataframes to variables
    calendar = gtfsSchedule['calendar']
    calendar_attributes = gtfsSchedule['calendar_attributes']
    calendar_dates = gtfsSchedule['calendar_dates']
    feed_info = gtfsSchedule['feed_info']
    routes = gtfsSchedule['routes']
    stop_times = gtfsSchedule['stop_times']
    grouped = stop_times.groupby('trip_id')
    #stop_times = stop_times.loc[(end_points_df)|(stop_times.loc[:,'stop_sequence'] == 1)]
    # Fetch the indexes of the first and last rows of each group
    first_indices = grouped.head(1).index
    last_indices = grouped.tail(1).index
    # Combine the indices
    combined_indices = first_indices.union(last_indices)
    # Extract the rows using the combined indices
    stop_times = stop_times.loc[combined_indices]
    # Drop arrival_time and rename departure_time to scheduled
    stop_times = stop_times.drop(columns=['arrival_time'])
    stop_times = stop_times.rename(columns={'departure_time': 'scheduled'})
    stops = gtfsSchedule['stops']
    trips = gtfsSchedule['trips']
    # Filter routes and trips to only include bus routes, i.e., those whose route_id is a string of digits
    routes, trips = routes[routes['route_id'].str.isdigit()], trips[trips['route_id'].str.isdigit()]
    # Convert datetime strings to proper datetime objects
    calendar['start_date'] = pd.to_datetime(calendar['start_date'], format='%Y%m%d')
    calendar['end_date'] = pd.to_datetime(calendar['end_date'], format='%Y%m%d')
    calendar_attributes['rating_start_date'] = pd.to_datetime(calendar_attributes['rating_start_date'], format='%Y%m%d')
    calendar_attributes['rating_end_date'] = pd.to_datetime(calendar_attributes['rating_end_date'], format='%Y%m%d')
    feed_info['feed_start_date'] = pd.to_datetime(feed_info['feed_start_date'], format='%Y%m%d')
    feed_info['feed_end_date'] = pd.to_datetime(feed_info['feed_end_date'], format='%Y%m%d')
    calendar_dates['date'] = pd.to_datetime(calendar_dates['date'], format='%Y%m%d')
    # Add the service_id field from the calendar dataframe to the trips dataframe, without including the other fields
    trips = pd.merge(trips[['service_id','trip_id', 'route_id', 'direction_id', 'block_id']], calendar_attributes[['service_id']], on='service_id')
    # Merge stop_times with trips
    schedule = pd.merge(trips,stop_times[['trip_id','scheduled','stop_id','stop_sequence']], on='trip_id', how='left')
    schedule['scheduled'] = handle_24h_time(schedule['scheduled'])
    schedule['scheduled'] = pd.to_datetime(schedule['scheduled'], format='%H:%M:%S')
    #schedule = parse_datetime_strings(schedule)
    return calendar, calendar_attributes, calendar_dates, feed_info, routes, stop_times, stops, trips, schedule

#---------------------------------#
# Functions to map calendar days to their respective service_ids
def parse_calendar_file(gtfs_calendar): 
    calendar_data = {}
    for index, row in gtfs_calendar.iterrows():
        service_id = row['service_id']
        start_date = row['start_date']
        end_date = row['end_date']
        days = row[['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']]
        for date in pd.date_range(start_date, end_date):
            if date not in calendar_data:
                calendar_data[date] = set()
            if days.iloc[date.dayofweek] == 1:
                calendar_data[date].add(service_id)
    return calendar_data

def parse_calendar_dates_file(gtfs_calendar_dates, calendar_data_dict):

    for index, row in gtfs_calendar_dates.iterrows():
        service_id = row['service_id']
        date = row['date']
        exception_type = row['exception_type']
        if date not in calendar_data_dict:
            calendar_data_dict[date] = set()
        if exception_type == 1:
            calendar_data_dict[date].add(service_id)
        elif exception_type == 2:
            if service_id in calendar_data_dict[date]:
                calendar_data_dict[date].remove(service_id)
    return calendar_data_dict

def generate_schedule(feed_start_date, feed_end_date, calendar_data):
    schedule = []
    for date in pd.date_range(feed_start_date, feed_end_date):
        day_of_week = date.strftime('%A')
        service_ids = calendar_data.get(date, set()) #| calendar_dates_data.get(date, set())
        schedule.append((date, day_of_week, service_ids))
    return schedule

#---------------------------------#
# Functions to map realtime data to gtfs schedule to assign block_ids and service_ids
def map_realtime_to_gtfs_schedule(df, start_date, end_date, calendar_schedule, gtfs_schedule):
    
    export_df = df.copy()
    unmatched_names = []
    unmatched_groups = []
    # Also create a destination folder: this will be named as mapped_realtime_data_start_date_end_date.csv
    # Create a folder to store the csv files
    foldername = 'mapped_realtime_data' + start_date.strftime('%Y%m%d') + '_' + end_date.strftime('%Y%m%d')
    if not os.path.exists(foldername):
        os.makedirs(foldername)

    route_ids = df['route_id'].unique()
    calendar_df = pd.DataFrame(calendar_schedule, columns=['date', 'day_of_week', 'service_ids'])
    for route in route_ids:
        print(f'Processing route {route}...')
        # Fetch subset of the ArrivalDepartureTimes dataframe for the current route and stop_sequence ==1
        adt_route = df.loc[(df['route_id'] == route) & (df['point_type'] == 'Startpoint')]

        # Print the feed_start_date and feed_end_date
        print(f'Feed start date: {start_date}, Feed end date: {end_date}')
        #Filter adt_df to keep only the rows that lie within the feed_start_date and feed_end_date range
        date_filter = (adt_route['service_date'] >= start_date) & (adt_route['service_date'] <= end_date)
        adt_date_filtered = adt_route[date_filter]

        # Fetch subset of the GTFS schedule dataframe for the current route
        schedule_route = gtfs_schedule[(gtfs_schedule['route_id'] == route)&(gtfs_schedule['stop_sequence'] == 1)]

        adt_grouped = adt_date_filtered.groupby(['direction_id', 'scheduled'], observed=True)
        schedule_grouped = schedule_route.groupby(['direction_id', 'scheduled'], observed=True)
    
        for name, group in adt_grouped:
            # print the group name
            #print(f'{name}...')
            if name in schedule_grouped.groups:
                # extract the corresponding group from schedule_route10_grouped
                schedule_group = schedule_grouped.get_group(name)
                schedule_services = set(schedule_group['service_id'])
                # This is a series whose index is the service_id and the values are the block_ids
                schedule_service_block_ids = schedule_group.groupby(['service_id'], observed=True, as_index=False)['block_id'].apply(list)

                # extract the subset of the calendar_df that matches the service_date
                service_days_orig = calendar_df.loc[calendar_df.date.isin(group.service_date)]

                # loop through the service_days
                # Step 1: Compute the intersections
                service_days = service_days_orig.copy()
                service_days.loc[:,'adt_service_ids'] = service_days['service_ids'].apply(lambda ids: schedule_services.intersection(ids))
                service_days.loc[:,'adt_service_ids_str'] = service_days['adt_service_ids'].apply(lambda ids: ', '.join(ids))

                # Step 2: Merge service_days with group on date 
                merged = pd.merge(group, service_days, left_on='service_date', right_on='date')
                # Step 3: Merge the merged dataframe with schedule_service_block_ids and keep the index of the group
                merged = pd.merge(merged, schedule_service_block_ids, left_on='adt_service_ids_str', right_on='service_id')
                # Print the name of the group if the length of the two indexes is different to spot potential errors
                if len(merged.index) != len(group.index):
                    print(f'Route: {route}\n Length of indexes for group {name} is different: {len(merged.index)} vs {len(group.index)}')
                    # Drop group rows whose service_date is not in the service_date of merged
                    group = group[group['service_date'].isin(merged['service_date'])]

                merged = merged.set_index(group.index)
                # Step 4: recast the service_id and block_id in the adt_df
                #df.loc[merged.index, 'service_id'] = merged['service_id'] 
                #df.loc[merged.index, 'block_id'] = merged['block_id']
                df.loc[merged.index, 'service_id'] = merged['service_id_y'] 
                df.loc[merged.index, 'block_id'] = merged['block_id_y']

            else:
                #print(f'No match found for group {name}...')
                unmatched_names.append(name)
                unmatched_groups.append(group)

        export_df = add_info_to_endpoint_rows(df.loc[df.route_id==route])

        # Save the route-specific df to a csv file
        export_filename = route + '.csv'
        filepath = os.path.join(foldername, export_filename)
        export_df.to_csv(filepath, index=False)
        # Keep the index in the exported csv file and its name half_trip_id
    return df

def add_info_to_endpoint_rows(df):
    endpoint_df = df.loc[(df['point_type'] == 'Endpoint')]
    # Drop block_id and service_id columns
    endpoint_df = endpoint_df.drop(columns=['block_id', 'service_id'])
    startpoint_df = df.loc[(df['point_type'] == 'Startpoint')]
    endpoint_df = endpoint_df.merge(startpoint_df[['half_trip_id','block_id', 'service_id']], left_on='half_trip_id', right_on='half_trip_id')
    # Concatenate the two dataframes horizontally
    final_df = pd.concat([startpoint_df,endpoint_df])
    return final_df