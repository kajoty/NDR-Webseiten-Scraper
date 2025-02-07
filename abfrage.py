import asyncio
import aiohttp
import json
import csv
import datetime
from bs4 import BeautifulSoup
from influxdb import InfluxDBClient
from urllib.parse import urlencode, urlparse, parse_qs
from asyncio import Semaphore

# Lade Konfiguration und Stationen
def load_config_and_stations():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    # Überprüfen, ob alle erforderlichen Konfigurationswerte vorhanden sind
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

# InfluxDB-Client initialisieren
def initialize_influxdb(config):
    return InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_user'],
        password=config['influx_password'],
        database=config['influx_db']
    )

# Asynchrone HTTP-Anfrage durchführen
async def fetch_playlist(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

# URL mit Datum und Uhrzeit formatieren
def format_url(base_url, date, hour):
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)
    query_params['date'] = [date]
    query_params['hour'] = [hour]

    new_query_string = urlencode(query_params, doseq=True)
    new_url = parsed_url._replace(query=new_query_string).geturl()

    return new_url

# Playlist-Daten extrahieren
def scrape_playlist(html, station_name):
    soup = BeautifulSoup(html, 'html.parser')
    programs = soup.find_all('li', class_='program')

    data = []
    for program in programs:
        time = program.find('strong', class_='time').text.strip()
        artist_title = program.find('h3').text.strip().split(' - ')

        # Sicherstellen, dass das Format korrekt ist
        if len(artist_title) != 2:
            continue  # Überspringe, wenn das Format nicht stimmt
        
        artist, title = artist_title

        # Fallback für Titel, falls er leer ist
        title = title if title else "Unbekannt"

        date_str = datetime.datetime.now().strftime('%Y-%m-%d')

        data.append({
            "measurement": "music_playlist",  # Measurement geändert
            "tags": {
                "station": station_name  # Station als Tag hinzufügen
            },
            "fields": {
                "date": date_str,
                "time": time,
                "artist": artist,
                "title": title
            }
        })
    return data

# Daten in InfluxDB schreiben
def write_to_influxdb(client, data):
    try:
        client.write_points(data)
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

# Daten für einen Tag und eine Station abrufen
async def fetch_data_for_day(session, semaphore, client, station, date):
    for hour in range(24):
        hour_str = str(hour).zfill(2)
        formatted_url = format_url(station['url'], date, hour_str)
        
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

# Funktion zum Abrufen der letzten N Tage
def get_dates_for_past_days(num_days):
    now = datetime.datetime.now()
    dates = [(now - datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]
    return dates

# Hauptfunktion zum Abrufen von Daten für einen längeren Zeitraum
async def fetch_data():
    config, stations = load_config_and_stations()
    num_days = config.get('num_days', 7)  # Anzahl der Tage aus der config.json laden (Standard: 7 Tage)
    dates = get_dates_for_past_days(num_days)

    client = initialize_influxdb(config)

    # Tasks für alle Stationen und Tage parallel ausführen
    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)
        tasks = []
        for date in dates:
            for station in stations:
                tasks.append(fetch_data_for_day(session, semaphore, client, station, date))
        
        await asyncio.gather(*tasks)
    
    # InfluxDB-Client schließen
    client.close()

# Asynchrone Ausführung starten
if __name__ == "__main__":
    try:
        asyncio.run(fetch_data())
    finally:
        print("Program finished.")
