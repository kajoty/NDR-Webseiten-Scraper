import asyncio
import json
import csv
import aiohttp  # Import hinzufügen
from datetime import datetime, timedelta
from asyncio import Semaphore

from functions.fetch_data import fetch_playlist
from functions.scrape_playlist import scrape_playlist
from functions.influxdb import initialize_influxdb, write_to_influxdb

# Lade Konfiguration und Stationen
def load_config_and_stations():
    with open('config/config.json', 'r') as config_file:  # Pfad zur config.json geändert
        config = json.load(config_file)

    required_keys = ['influx_host', 'influx_port', 'influx_user', 'influx_password', 'influx_db', 'num_days']
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required configuration: {key}")
    
    stations = []
    with open('stations.csv', 'r') as stations_file:
        csv_reader = csv.DictReader(stations_file)
        for row in csv_reader:
            stations.append({"station_name": row['Station'], "url": row['URL']})
    
    return config, stations

# Daten für einen Tag und eine Station abrufen
async def fetch_data_for_day(session, semaphore, client, station, date):
    for hour in range(24):
        hour_str = str(hour).zfill(2)
        formatted_url = f"{station['url']}?date={date}&hour={hour_str}"
        
        html = await fetch_playlist(session, formatted_url, semaphore)
        if html:
            data = scrape_playlist(html, station['station_name'])
            if data:
                write_to_influxdb(client, data)
                print(f"Data written to InfluxDB for {station['station_name']} on {date}")
            else:
                print(f"No data found for {station['station_name']} on {date}")
        else:
            print(f"Failed to fetch data for {station['station_name']} on {date}")

# Hauptfunktion zum Abrufen von Daten für einen längeren Zeitraum
async def fetch_data():
    config, stations = load_config_and_stations()
    num_days = config.get('num_days', 7)

    # Berechnung der letzten num_days Tage direkt in der Hauptfunktion
    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]

    client = initialize_influxdb(config)

    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)
        tasks = []
        for date in dates:
            for station in stations:
                tasks.append(fetch_data_for_day(session, semaphore, client, station, date))
        
        await asyncio.gather(*tasks)
    
    client.close()

# Asynchrone Ausführung starten
if __name__ == "__main__":
    try:
        asyncio.run(fetch_data())
    finally:
        print("Program finished.")
