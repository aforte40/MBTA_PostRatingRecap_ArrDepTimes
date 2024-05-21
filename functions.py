# list of functions used in the main script
import pandas as pd 
import os
import datetime as dt
import calendar

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
    # Use the routes file to replace route_ids that are not numerical with their numerical equivalents (eg SL-1 with 701)
    arr_dep_df = pd.merge(arr_dep_df, routes[['route_id', 'route_short_name']], on='route_id')
    # Drop the original route_id column, and rename the route_short_name column to route_id
    arr_dep_df = arr_dep_df.drop(columns=['route_id'])
    arr_dep_df = arr_dep_df.rename(columns={'route_short_name': 'route_id'})
    # Move the route_id column to the front
    arr_dep_df = arr_dep_df[['route_id'] + [col for col in arr_dep_df.columns if col != 'route_id']]
    # Add block_id and service_id columns as categories
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
    end_points_df = (stop_times.groupby
                        (['trip_id'], observed=True, as_index=False)
                        ['stop_sequence']
                        .transform('max') #with transform I can get all the max occurrences while also preserving their original row index
                    )
    stop_times = stop_times.loc[(stop_times.loc[:,'stop_sequence'] == end_points_df)|(stop_times.loc[:,'stop_sequence'] == 1)]
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

def parse_calendar_dates_file(gtfs_calendar_dates):

    calendar_dates_data = {}
    for index, row in gtfs_calendar_dates.iterrows():
        service_id = row['service_id']
        date = row['date']
        exception_type = row['exception_type']
        if date not in calendar_dates_data:
            calendar_dates_data[date] = set()
        if exception_type == 1:
            calendar_dates_data[date].add(service_id)
        elif exception_type == 2:
            if service_id in calendar_dates_data[date]:
                calendar_dates_data[date].remove(service_id)
    return calendar_dates_data

def generate_schedule(feed_start_date, feed_end_date, calendar_data, calendar_dates_data):
    schedule = []
    for date in pd.date_range(feed_start_date, feed_end_date):
        day_of_week = date.strftime('%A')
        service_ids = calendar_data.get(date, set()) | calendar_dates_data.get(date, set())
        schedule.append((date, day_of_week, service_ids))
    return schedule