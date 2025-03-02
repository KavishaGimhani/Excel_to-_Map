import os
import time
import pickle
import datetime
import threading
import pandas as pd
import folium
from flask import Flask, render_template, request, redirect, flash
from flask_socketio import SocketIO, emit
from geopy.geocoders import GoogleV3
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from queue import Queue

app = Flask(__name__)
socketio = SocketIO(app)
app.secret_key = "your_secret_key"
UPLOAD_FOLDER = "uploads"
CACHE_FILE = "geocode_cache.pkl"
UPDATED_CSV_FILE_PATH = "updated_addresses_google.csv"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

google_maps_api_key = "AIzaSyDBdab0Cv-Ct2MA71rKrJBLGi7uykxA_fM"
geolocator = GoogleV3(api_key=google_maps_api_key)

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Load cache
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "rb") as f:
        geocode_cache = pickle.load(f)
else:
    geocode_cache = {}

def get_lat_long(address):
    if address in geocode_cache:
        return geocode_cache[address]
    try:
        location = geolocator.geocode(address, timeout=5)
        if location:
            geocode_cache[address] = (location.latitude, location.longitude)
            with open(CACHE_FILE, "wb") as f:
                pickle.dump(geocode_cache, f)
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Error fetching coordinates for {address}: {e}")
    return None, None

def update_map_async(queue, csv_path):
    def process_csv():
        if not os.path.exists(csv_path):
            print("CSV file not found!")
            return
        
        df = pd.read_csv(csv_path)
        if "Latitude" not in df.columns or "Longitude" not in df.columns:
            df["Latitude"], df["Longitude"] = None, None

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(get_lat_long, df["address"]))
        
        df["Latitude"], df["Longitude"] = zip(*results)
        df.to_csv(UPDATED_CSV_FILE_PATH, index=False)
        
        if os.path.exists("templates/employee_map.html"):
            os.remove("templates/employee_map.html")
        
        map_sri_lanka = folium.Map(location=[7.8731, 80.7718], zoom_start=8)
        for _, row in df.iterrows():
            if pd.notnull(row["Latitude"]) and pd.notnull(row["Longitude"]):
                folium.Marker(
                    location=[row["Latitude"], row["Longitude"]],
                    popup=row["address"],
                    icon=folium.Icon(color="blue", icon="info-sign")
                ).add_to(map_sri_lanka)
        
        map_sri_lanka.save("templates/employee_map.html")
        queue.put("CSV successfully updated and Map created!")
        socketio.emit("update", {"message": "CSV successfully updated and Map created!"})
    
    threading.Thread(target=process_csv).start()

last_modified_time = 0
DEBOUNCE_TIME = 5

class CSVFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global last_modified_time
        if event.src_path.endswith("ActiveTrainees_with_City.csv"):
            current_time = time.time()
            if current_time - last_modified_time > DEBOUNCE_TIME:
                last_modified_time = current_time
                print("CSV file updated. Refreshing map...")
                update_map_async(queue, event.src_path)

observer = Observer()
observer.schedule(CSVFileHandler(), path=UPLOAD_FOLDER, recursive=False)
observer.start()
queue = Queue()

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        file = request.files.get("file")
        if file and file.filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file_path = os.path.join(app.config["UPLOAD_FOLDER"], f"trainees_{timestamp}.csv")
            file.save(file_path)
            print("File uploaded successfully. Processing in background...")
            update_map_async(queue, file_path)
            flash("File uploaded successfully. Processing started!", "info")
            return redirect(request.url)
    return render_template("index.html")

@app.route("/map")
def show_map():
    return render_template("employee_map.html")

@app.before_request
def check_queue():
    if not queue.empty():
        flash(queue.get(), "success")

if __name__ == "__main__":
    socketio.run(app, debug=True)