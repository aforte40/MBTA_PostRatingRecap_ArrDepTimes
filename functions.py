# list of functions used in the main script
import pandas as pd 
import os

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

def handle_24h_time(series):
    return series.str.replace(r'^24', '00', regex=True) \
                 .str.replace(r'^25', '01', regex=True) \
                 .str.replace(r'^26', '02', regex=True) \
                 .str.replace(r'^27', '03', regex=True) \
                 .str.replace(r'^28', '04', regex=True)

# Parse datetime strings to datetime objects
def parse_datetime_strings(df):
    df['scheduled'] = handle_24h_time(df['scheduled'])
    df['scheduled'] = pd.to_datetime(df['scheduled'], format='%H:%M:%S').dt.time
    return df

def reduce_df_size(df):
    #Drop headway and scheduled_headway columns
    df = df.drop(columns=['headway', 'scheduled_headway'])
    # Filter the df to only include the first and last stops of each half_trip_id
    end_points_df = (df.groupby
                        (['half_trip_id'], observed=True, as_index=False)
                        ['time_point_order']
                        .transform('max') #with transform I can get all the max occurrences while also preserving their original row index
                    )
    df = df.loc[(df.loc[:,'time_point_order'] == end_points_df)|(df.loc[:,'time_point_order'] == 1)]
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