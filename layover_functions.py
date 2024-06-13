import pandas as pd
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Compute layover times from real-time dataframe 
def compute_layover_times(original_df):
    # Group by service_date and block_id
    grouped = original_df.groupby(['route_id','service_date', 'block_id'], observed=True)
    # Calculate scheduled and actual layover times using diff()
    original_df['scheduled_layover'] = grouped['scheduled'].diff().dt.total_seconds() / 60
    original_df['actual_layover'] = grouped['actual'].diff().dt.total_seconds() / 60
    # Initialize the trip_duration column
    original_df['trip_duration'] = np.nan
    # Shift values to trip_duration for Endpoint rows
    endpoint_mask = original_df['point_type'] == 'Endpoint'
    original_df.loc[endpoint_mask, 'trip_duration'] = original_df.loc[endpoint_mask, 'actual_layover']
    original_df.loc[endpoint_mask, 'actual_layover'] = np.nan
    # Replace the first row of each group with null timedelta
    original_df.loc[grouped.head(1).index, ['scheduled_layover', 'actual_layover']] = 0
    original_df.loc[grouped.head(1).index, ['actual_layover']] = 0
    # Replace negative values with their 24h complement
    original_df.loc[original_df.scheduled_layover < 0, 'scheduled_layover'] = 1440 + original_df.scheduled_layover
    original_df.loc[original_df.actual_layover < 0, 'actual_layover'] = 1440 + original_df.actual_layover
    #original_df.loc[original_df.point_type != 'Endpoint', ['scheduled_layover', 'actual_layover']] = np.nan
    original_df.scheduled = original_df.scheduled.dt.time
    original_df.actual = original_df.actual.dt.time
    return original_df

# Reshape the dataframe so as to have layover times displayed for every service_date and block_id,
# and for the stops belonging to the route_id currently under analysis
# Create a pivot table
def reshape_for_layover(df_with_layover_times):
    pivot_table = pd.pivot_table(
    df_with_layover_times.loc[df_with_layover_times.point_type == 'Startpoint'],
    values=['actual_layover'],
    index=['service_date', 'block_id', 'actual', 'direction_id', 'stop_id'],
    columns=['route_id'],
    aggfunc='first',
    observed=True
)
    # Assign stop_id values to column headers
    pivot_table.columns = pivot_table.columns.get_level_values(1)
    # Save the values of stop_id as a list
    route_stop_ids = pivot_table.columns.tolist()
    # Reset index to turn multi-index into columns
    pivot_table.reset_index(inplace=True)
    # Remove index names
    pivot_table.index.name = None
    pivot_table.columns.name = None
    return route_stop_ids, pivot_table

# Define the function to update layover_dict
def update_layover_dict(layover_dict, route_id, my_layover_df):
    route_id_key = f'route_{route_id}'
    stop_id_key = f'stop_ids_route{route_id}'
    layover_df_key = f'layover_df_route{route_id}'

    # If the route_id is not in the dictionary, add it
    if route_id_key not in layover_dict:
        layover_dict[route_id_key] = {stop_id_key: [], layover_df_key: pd.DataFrame()}
        
    # Append stop_ids to the stop_ids list in the dictionary
    current_stop_ids = layover_dict[route_id_key][stop_id_key]
    new_stop_ids = [stop_id for stop_id in my_layover_df.stop_id.unique() if stop_id not in current_stop_ids]
    layover_dict[route_id_key][stop_id_key].extend(new_stop_ids)

    # Update the DataFrame in the dictionary
    existing_df = layover_dict[route_id_key][layover_df_key]
    #if not existing_df.empty and new_stop_ids:
        # Add columns for new stop_ids
    #    for stop_id in new_stop_ids:
    #        existing_df[stop_id] = np.nan
    # Concatenate the new DataFrame
    updated_df = pd.concat([existing_df, my_layover_df], axis=0)
    layover_dict[route_id_key][layover_df_key] = updated_df


# Add the column with startpoint/endpoint cluster
def cluster_stop_id_combinations(layover_dataframe):
    # Step 1: Filter Startpoints and Endpoints
    startpoints = layover_dataframe[layover_dataframe['point_type'] == 'Startpoint'][['half_trip_id', 'stop_id']].rename(columns={'stop_id': 'start_stop_id'})
    endpoints = layover_dataframe[layover_dataframe['point_type'] == 'Endpoint'][['half_trip_id', 'stop_id']].rename(columns={'stop_id': 'end_stop_id'})

    # Step 2: Merge Startpoints and Endpoints
    start_end_points = pd.merge(startpoints, endpoints, on='half_trip_id', how='inner')
    
    # Debug: View the start and end points to ensure correctness
    #print("Start and End Points:")
    #print(start_end_points)
    
    # Step 3: Create Cluster Column
    start_end_points['cluster'] = start_end_points.apply(
    lambda row: frozenset([row['start_stop_id'], row['end_stop_id']]), axis=1)

    # Debug: View the clusters created to ensure correctness
    #print("Clusters Created:")
    #print(start_end_points)

    # Step 4: Merge Cluster Column back to Original DataFrame
    layover_dataframe = pd.merge(layover_dataframe, start_end_points[['half_trip_id', 'cluster']], on='half_trip_id', how='left')
    # Debug: View the final dataframe to ensure correctness
    #print("Final DataFrame:")
    #print(layover_dataframe)
    # Optional Step 5: Encode Clusters (if necessary)
    # This step encodes the 'cluster' column to a float with the format 'startstop.endstop'
    #layover_dataframe['cluster_encoded'] = layover_dataframe['cluster'].apply(lambda x: float(x.replace('_', '.')))
    return layover_dataframe


# Plot statistical distribution of layover times
def plot_layover_distribution(layover_dataframe, start_date, end_date):
    # Group data by route_id and cluster
    #grouped = layover2.groupby(['route_id', 'cluster'], observed=True)
    grouped = layover_dataframe.loc[layover_dataframe.actual_layover <= 60].groupby(['route_id', 'cluster'], observed=True)
    # Plotting
    for (route_id, cluster), group in grouped:
        # Revome both nan and zeros
        layover_times = group['actual_layover'].dropna()
        layover_times = layover_times[layover_times != 0]
        if len(layover_times) == 0:
            continue
        
        mean_layover = layover_times.mean()
        std_layover = layover_times.std()
        
        # Create the plot
        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = ax1.twinx()
        nbins = 60
        # Plot vertical bands for expected value ± 1 std deviation
        #plt.axvspan(mean_layover - std_layover, mean_layover + std_layover, color='yellow', alpha=0.3)
        
        #Plot histogram of layover times distribution
        sns.histplot(layover_times, bins=nbins, color='cyan', label='Histogram', ax=ax1)
        sns.kdeplot(layover_times, color='blue', label='KDE', ax=ax2)
        # Add grid for histogram
        ax1.grid(axis='x', linestyle='--', alpha=0.5)
        # Plot vertical line for the value with the highest frequency
        #plt.axvline(layover_times.mode().values[0], color='green', linestyle='--', linewidth=1)   
        
        # Annotate with text box
        in_range_count = ((layover_times >= (mean_layover - std_layover)) & 
                        (layover_times <= (mean_layover + std_layover))).sum()
        stats_text = (f"Service week: {start_date.date()} - {end_date.date()}\n"
                    f"Expected value (mean): {mean_layover:.2f} min\n"
                    f"Standard deviation: {std_layover:.2f} min\n"
                    f"Evaluated trips: {len(layover_times)}\n"
                    f"Entries within ±1 std dev: {in_range_count}")
        
        plt.gca().text(0.95, 0.95, stats_text, horizontalalignment='right', verticalalignment='top', 
                    transform=plt.gca().transAxes, bbox=dict(facecolor='white', alpha=0.5))
        
        # Show only positive x values
        ax1.set_xlim(left=0, right=60)
        # Set ticks for ax1 every 5 minutes
        ax1.set_xticks(np.arange(0, layover_times.max() + 5, 5))
        # display ax2 line and labels in blue
        ax2.spines['right'].set_color('blue')
        ax2.tick_params(axis='y', colors='blue')

        # Plot title
        figtitle = f'Route {route_id} - Cluster {sorted(list(cluster))}'
        plt.title(figtitle)
        ax1.set_xlabel('Actual Layover Time [min]')
        ax1.set_ylabel('Number of trips')
        ax2.set_ylabel('KDE', color = 'blue')  # Label for the secondary y-axis
        
        # Show the plot
        #plt.show()

        # Export routine
        export_folder = 'StatisticsPlots_2023'
        export_path = os.path.join(export_folder, figtitle + '.png')
        plt.savefig(export_path)
