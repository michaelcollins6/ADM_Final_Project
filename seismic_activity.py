from pyspark.sql.functions import col, from_unixtime, unix_timestamp
from pyspark.sql import SparkSession
import tkinter as tk
from tkinter import ttk
from geopy.geocoders import Nominatim
from math import cos, radians
from datetime import datetime

spark = SparkSession.builder.appName("SeismicData").getOrCreate()

# Load the data
seismic_df = spark.read.csv("Global_Earthquake_Data.csv", header=True, inferSchema=True)

# Initialize geolocator object
geolocator = Nominatim(user_agent="app")

def earthquakes(longitude, latitude, lon_tol, lat_tol, year_min_value, year_max_value):
    
    # Filter the seismic events based on the user's input
    nearby_seismic_df = seismic_df.filter(
        (col("longitude") >= longitude - lon_tol) &
        (col("longitude") <= longitude + lon_tol) &
        (col("latitude") >= latitude - lat_tol) &
        (col("latitude") <= latitude + lat_tol)
    )

    # Sort by date and time, and remove unwanted columns
    sorted_seismic_df = nearby_seismic_df.filter((col('time') >= year_min_value) & 
                                                 (col('time') <= year_max_value))
    sorted_seismic_df = sorted_seismic_df.orderBy(nearby_seismic_df["time"].desc())
    sorted_seismic_df = sorted_seismic_df.select('time', 'latitude', 'longitude', 'mag', 'place', 'type')

    # Create a new window to display the table
    table_window = tk.Toplevel(root)
    
    # Create a table widget and add it to the new window
    columns = ("#1", "#2", "#3", "#4", "#5", "#6")
    table = ttk.Treeview(table_window, columns=columns, show="headings")
    table.pack(fill='both', expand=True)


    # Add table widget
    table.heading("#1", text="Location")
    table.heading("#2", text="Time")
    table.heading("#3", text="Magnitude")
    table.heading("#4", text="Latitude")
    table.heading("#5", text="Longitude")
    table.heading("#6", text="Type")
    table.grid(row=5, column=0, columnspan=2, padx=padding, pady=padding)
    
    sorted_seismic_df = sorted_seismic_df.withColumn("unix_time", unix_timestamp("time")) \
       .withColumn("time_str", from_unixtime("unix_time", "yyyy-MM-dd HH:mm:ss"))
    
    sorted_seismic_df = sorted_seismic_df.drop('time').withColumnRenamed('time_str', 'time')
    sorted_seismic_df = sorted_seismic_df.drop('unix_time')
    
    for row in sorted_seismic_df.collect():
        table.insert("", "end", text="", values=(row.place, row.time, row.mag, row.latitude, row.longitude, row.type))


# Define function to get coordinates from location name
def get_coordinates(location_name):
    location = geolocator.geocode(location_name)
    return (location.latitude, location.longitude)

# Define function to get location from latitude and longitude values
def get_location(latitude, longitude):
    location = geolocator.reverse((latitude, longitude))
    return location.address

# Define function to convert radius in miles to a tolerance in degrees of latitude and longitude
def get_tolerance(radius_miles, latitude):
    lat_tol = radius_miles / 69
    lon_tol = radius_miles / (cos(radians(latitude)) * 69.172)
    return lat_tol, lon_tol

# Define function to handle button click
def handle_click():
    try:
        if location_text.get():
            location_name = location_text.get()
            latitude, longitude = get_coordinates(location_name)
            result_label.config(text=f"Latitude: {latitude}\nLongitude: {longitude}")
        elif lat_text.get() and long_text.get():
            latitude = float(lat_text.get())
            longitude = float(long_text.get())
            location_name = get_location(latitude, longitude)
            result_label.config(text=f"Location: {location_name}")
        else:
            result_label.config(text="Please enter a location or latitude/longitude values.")
        
        radius_miles = float(radius.get())
        lat_tol, lon_tol = get_tolerance(radius_miles=radius_miles, latitude=latitude)
        

        year_min_value = datetime(int(year_min.get()), 1, 1)
        year_max_value = datetime(int(year_max.get()), 12, 31)
        
        earthquakes(longitude=longitude, latitude=latitude, lon_tol=lon_tol, lat_tol=lat_tol, 
                    year_min_value=year_min_value, year_max_value=year_max_value)
    except:
        result_label.config(text="ERROR: Please verify input fields.")
        
# Initialize UI
root = tk.Tk()
root.title("Seismic Activity")
root.geometry("650x170")

# Add padding and spacing
padding = 5
spacing = 5

# Create input widgets
location_label = tk.Label(root, text="Location:")
location_text = tk.Entry(root)
lat_label = tk.Label(root, text="Latitude:")
lat_text = tk.Entry(root)
long_label = tk.Label(root, text="Longitude:")
long_text = tk.Entry(root)

year_min = tk.Entry(root)
year_min.insert(1, "1906")
year_min_label = tk.Label(root, text="From Year:")
year_max = tk.Entry(root)
year_max.insert(1, "2022")
year_max_label = tk.Label(root, text="To Year:")

radius = tk.Entry(root)
radius.insert(1, "25")
radius_label = tk.Label(root, text="Radius in Miles:")

# Create button widget
search_button = tk.Button(root, text="Search", command=handle_click)

# Create output widget
result_label = tk.Label(root, text="Enter a location or latitude/longitude values.")

# Add padding and spacing to UI elements
location_label.grid(row=0, column=0, padx=padding, pady=padding)
location_text.grid(row=0, column=1, padx=padding, pady=padding)
lat_label.grid(row=1, column=0, padx=padding, pady=padding)
lat_text.grid(row=1, column=1, padx=padding, pady=padding)
long_label.grid(row=2, column=0, padx=padding, pady=padding)
long_text.grid(row=2, column=1, padx=padding, pady=padding)

year_min_label.grid(row=0, column=2, padx=padding, pady=padding)
year_min.grid(row=0, column=3, padx=padding, pady=padding)
year_max_label.grid(row=1, column=2, padx=padding, pady=padding)
year_max.grid(row=1, column=3, padx=padding, pady=padding)
radius_label.grid(row=2, column=2, padx=padding, pady=padding)
radius.grid(row=2, column=3, padx=padding, pady=padding)

search_button.grid(row=3, column=1, columnspan=2, pady=(padding, spacing))
result_label.grid(row=4, column=0, columnspan=2, padx=padding, pady=padding)

# Start UI event loop
root.mainloop()
