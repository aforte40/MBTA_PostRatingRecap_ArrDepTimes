import pandas as pd

gtfs_cols = {
    'calendar': {
    'service_id': 'category',
    'monday': bool,
    'tuesday': bool,
    'wednesday': bool,
    'thursday': bool,
    'friday': bool,
    'saturday': bool,
    'sunday': bool,
    'start_date': 'string',
    'end_date': 'string'
    },

    'calendar_attributes':{
    'service_id': 'category',
    'service_description': 'category',
    'service_schedule_name': 'category',
    'service_schedule_type': 'category',
    'service_schedule_typicality': pd.Int8Dtype(),
    'rating_start_date': 'string',
    'rating_end_date': 'string',
    'rating_description': 'category'
    },

    'calendar_dates':{
    'service_id': 'category',
    'date': 'string',
    'exception_type': pd.Int8Dtype(),
    'holiday_name': 'category'
    },

    'feed_info': {
    'feed_publisher_name': 'category',
    'feed_publisher_url': 'category',
    'feed_lang': 'category',
    'feed_start_date': 'string',
    'feed_end_date': 'string',
    'feed_version': 'category',
    'feed_contact_email': 'category',
    'feed_contact_url': 'category'
    },

    'routes':{
    'route_id': 'category',
    'agency_id': 'category',
    'route_short_name': 'category',
    'route_long_name': 'category',
    'route_desc': 'category',
    'route_type': 'category',
    'route_url': 'category',
    'route_color': 'category',
    'route_text_color': 'category',
    'route_sort_order': pd.Int16Dtype(),
    'route_fare': 'category',
    'line_id': 'category',
    'listed_route': 'category'
    },

    'stop_times':{
    'trip_id': 'category',
    'arrival_time': 'string',
    'departure_time': 'string',
    'stop_id': 'category',
    'stop_sequence': pd.Int16Dtype(),
    'stop_headsign': 'category',
    'pickup_type': 'category',
    'drop_off_type': 'category',
    'timepoint': pd.Int16Dtype(),
    'checkpoint_id': 'category',
    'continuous_pickup': 'category',
    'continuous_drop_off': 'category'
    },

    'stops':{
    'stop_id': 'category',
    'stop_code': 'category',
    'stop_name': 'category',
    'stop_desc': 'category',
    'stop_lat': pd.Float32Dtype(),
    'stop_lon': pd.Float32Dtype(),
    'zone_id': 'category',
    'stop_url': 'category',
    'level_id': 'category',
    'location_type': 'category',
    'municipality': 'category',
    'on_street': 'category',
    'at_street': 'category',
    'parent_station': 'category',
    'stop_timezone': 'category',
    'wheelchair_boarding': 'category',
    'platform_code': 'category',
    'platofrm_name': 'category',
    'stop_address': 'category',
    'stop_city': 'category',
    'stop_region': 'category',
    'stop_postal_code': 'category',
    'stop_country': 'category',
    'stop_phone': 'category',
    'stop_url': 'category',
    'stop_contact_name': 'category',
    'stop_contact_phone': 'category',
    'stop_contact_url': 'category',
    'stop_contact_email': 'category',
    'vehicle_type': 'category'
    },

    'trips':{
    'route_id': 'category',
    'service_id': 'category',
    'trip_id': 'category',
    'trip_headsign': 'category',
    'trip_short_name': 'category',
    'direction_id': 'category',
    'block_id': 'category',
    'shape_id': 'category',
    'wheelchair_accessible': 'category',
    'trip_route_type': 'category',
    'route_pattern_id': 'category',
    'bikes_allowed': 'category'}
}

#Arrival adn Departure time columns
adt_dtype_map = {
    "service_date": "string",
    "route_id": "category",
    "direction_id": "category",
    "half_trip_id": "category",
    "stop_id": "string",
    "time_point_id": "category", 
    "time_point_order": pd.Int16Dtype(),
    "point_type": "category", 
    "standard_type": "category",  
    "scheduled": "string",  # Consider converting to datetime later
    "actual": "string",  # Consider converting to datetime later
    "scheduled_headway": pd.Int32Dtype(),
    "headway": pd.Int32Dtype()
    }