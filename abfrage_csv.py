import asyncio
import aiohttp
import json
import csv
import datetime
from bs4 import BeautifulSoup
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
            "station": station_name,
            "date": date_str,
            "time": time,
            "artist": artist,
            "title": title
        })
    return data

# Daten in eine CSV-Datei speichern
def save_to_csv(entries, filename='music_playlist.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'time', 'station', 'artist', 'title']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Kopfzeile schreiben
        writer.writeheader()

        # Einträge in die CSV schreiben
        for entry in entries:
            writer.writerow(entry)

    print(f"Data saved to {filename}")

# Daten für einen Tag und eine Station abrufen
async def fetch_data_for_day(session, semaphore, entries, station, date):
    for hour in range(24):
        hour_str = str(hour).zfill(2)
        formatted_url = format_url(station['url'], date, hour_str)
        
        async with semaphore:
            try:
                async with session.get(formatted_url) as response:
                    response.raise_for_status()
                    html = await response.text()
                    data = scrape_playlist(html, station['station_name'])
                    if data:
                        entries.extend(data)
                        print(f"Data fetched for {station['station_name']} on {date} at hour {hour_str}")
            except Exception as e:
                print(f"Error fetching data for {station['station_name']} on {date} at hour {hour_str}: {e}")

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

    # Liste für alle abgerufenen Daten
    entries = []

    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)  # Limit für gleichzeitige Anfragen
        tasks = []
        
        # Tasks für alle Stationen und Tage parallel ausführen
        for date in dates:
            for station in stations:
                tasks.append(fetch_data_for_day(session, semaphore, entries, station, date))
        
        await asyncio.gather(*tasks)

    # Speichern der gesammelten Daten in einer CSV-Datei
    save_to_csv(entries)

# Asynchrone Ausführung starten
if __name__ == "__main__":
    try:
        asyncio.run(fetch_data())
    finally:
        print("Program finished.")
