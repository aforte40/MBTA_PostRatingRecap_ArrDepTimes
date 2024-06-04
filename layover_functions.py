import pandas as pd
import datetime as dt

# Compute layover times from real-time dataframe 
def compute_layover_times(original_df):
    # Group by service_date and block_id
    grouped = original_df.groupby(['route_id','service_date', 'block_id'], observed=True)
    # Calculate theoretical and actual layover times using diff()
    original_df['theoretical_layover'] = grouped['scheduled'].diff().dt.total_seconds() / 60
    original_df['actual_layover'] = grouped['actual'].diff().dt.total_seconds() / 60
    # Replace the first row of each group with null timedelta
    original_df.loc[grouped.head(1).index, ['theoretical_layover', 'actual_layover']] = 0
    # Replace negative values with their 24h complement
    original_df.loc[original_df.theoretical_layover < 0, 'theoretical_layover'] = 1440 + original_df.theoretical_layover
    original_df.loc[original_df.actual_layover < 0, 'actual_layover'] = 1440 + original_df.actual_layover
    #original_df.loc[original_df.point_type != 'Endpoint', ['theoretical_layover', 'actual_layover']] = np.nan
    original_df.actual = original_df.actual.dt.time
    return original_df

# Reshape the dataframe so as to have layover times displayed for every service_date and block_id,
# and for the stops belonging to the route_id currently under analysis
# Create a pivot table
def reshape_for_layover(df_with_layover_times):
    pivot_table = pd.pivot_table(
    df_with_layover_times.loc[df_with_layover_times.point_type == 'Startpoint'],
    values=['actual_layover'],
    index=['service_date', 'block_id', 'actual'],
    columns=['stop_id'],
    aggfunc='first',
    observed=True
)
    # Assign stop_id values to column headers
    pivot_table.columns = pivot_table.columns.get_level_values(1)
    # Save the values of stop_id as a list
    route_stop_ids = pivot_table.columns.tolist()
    # Reset index to turn multi-index into columns
    pivot_table.reset_index(inplace=True)
    return route_stop_ids, pivot_table