# -*- coding: utf-8 -*-
"""
Created on Thu Jan 25 13:15:02 2024

@author: eevee
"""

import pandas as pd
from multiprocessing import Pool
import os
from collections import Counter

def process_file(file_path):
    try:
        df = pd.read_csv(file_path, compression='gzip')

        # Group by timestamp
        pixels_per_second = df.groupby('timestamp').size()

        # Group by user
        pixels_per_user = df.groupby('user').size()
        unique_colors_by_user = df.groupby('user')['pixel_color'].nunique()
        unique_coords_by_user = df.groupby('user')['coordinate'].nunique()
        common_coord_per_user = df.groupby('user')['coordinate'].agg(lambda x: Counter(x).most_common(1)[0][0])
        common_color_per_user = df.groupby('user')['pixel_color'].agg(lambda x: Counter(x).most_common(1)[0][0])

        # Group by coordinate
        changes_per_coordinate = df.groupby('coordinate').size()

        # Group by color
        count_per_color = df.groupby('pixel_color').size()
        
        #time difference calculations
        df['timestamp'] = df['timestamp'].str.slice(0, 19)  # Keep only up to seconds
        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d %H:%M:%S")
        
        # Sort the DataFrame by 'user' and 'timestamp'
        df = df.sort_values(['user', 'timestamp'])
        
        # Calculate the time difference per user
        df['time_diff'] = df.groupby('user')['timestamp'].diff()
        
        # Calculate the time difference in minutes
        df['time_diff_minutes'] = df['time_diff'].dt.total_seconds() / 60
        
        # Calculate the average time difference per user
        avg_time_diff_per_user = df.groupby('user')['time_diff_minutes'].mean()
        
        # Calculate the variance of time differences per user
        variance_time_diff_per_user = df.groupby('user')['time_diff_minutes'].var()

        return pixels_per_second, pixels_per_user, common_coord_per_user, common_color_per_user, changes_per_coordinate, count_per_color, unique_colors_by_user, unique_coords_by_user, avg_time_diff_per_user, variance_time_diff_per_user
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

def process_files_parallel(file_paths, num_processes=None):
    with Pool(num_processes) as pool:
        results = pool.map(process_file, file_paths)
    return results

if __name__ == "__main__":
    gzip_csv_directory = r'C:\Users\alexa\OneDrive - Cal Poly\Cloud Computing\44-52'
    file_paths = [os.path.join(gzip_csv_directory, f) for f in os.listdir(gzip_csv_directory) if f.endswith('.csv.gzip')]

    all_results = process_files_parallel(file_paths)

    # Initialize empty DataFrames or Series for aggregation
    final_pixels_per_second = pd.Series(dtype='int')
    final_pixels_per_user = pd.Series(dtype='int')
    final_common_coord_per_user = pd.Series(dtype='object')
    final_common_color_per_user = pd.Series(dtype='object')
    final_changes_per_coordinate = pd.Series(dtype='int')
    final_count_per_color = pd.Series(dtype='int')
    final_unique_colors_by_user = pd.Series(dtype='int')
    final_unique_coords_by_user = pd.Series(dtype='int')
    final_time_diff = pd.Series(dtype='float')
    final_variance_time_diff =pd.Series(dtype='float')

    # Aggregate results from all files
    for result in all_results:
        if result:
            final_pixels_per_second = final_pixels_per_second.add(result[0], fill_value=0)
            final_pixels_per_user = final_pixels_per_user.add(result[1], fill_value=0)
            final_common_coord_per_user = final_common_coord_per_user.add(result[2], fill_value='')
            final_common_color_per_user = final_common_color_per_user.add(result[3], fill_value='')
            final_changes_per_coordinate = final_changes_per_coordinate.add(result[4], fill_value=0)
            final_count_per_color = final_count_per_color.add(result[5], fill_value=0)
            final_unique_colors_by_user = final_unique_colors_by_user.add(result[6], fill_value = 0)
            final_unique_coords_by_user = final_unique_coords_by_user.add(result[7],fill_value=0)
            final_time_diff = final_time_diff.add(result[8],fill_value = 0)
            final_variance_time_diff = final_variance_time_diff.add(result[9],fill_value = 0)
            

    # Print or save the final DataFrames
    print(final_pixels_per_second)
    final_pixels_per_second.to_csv('pixpersec5.csv')

    print(final_pixels_per_user)
    final_pixels_per_user.to_csv('pixperuser5.csv')
    
    print(final_common_coord_per_user)
    final_common_coord_per_user.to_csv('coorduser5.csv')
    
    print(final_common_color_per_user)
    final_common_color_per_user.to_csv('coloruser5.csv')
    
    print(final_changes_per_coordinate)
    final_changes_per_coordinate.to_csv('changescoord5.csv')
    
    print(final_count_per_color)
    final_count_per_color.to_csv('countbycolor5.csv')
    
    print(final_unique_colors_by_user)
    final_unique_colors_by_user.to_csv('uniquecoloruser5.csv')
    
    print(final_unique_coords_by_user)
    final_unique_coords_by_user.to_csv('uniquecoorduser5.csv')
    
    print(final_time_diff)
    final_time_diff.to_csv('timediff5.csv')
    
    print(final_variance_time_diff)
    final_variance_time_diff.to_csv('vartimediff5.csv')
