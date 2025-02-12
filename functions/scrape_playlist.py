# scrape_playlist.py

import hashlib
import datetime
from bs4 import BeautifulSoup
import json
import csv

def generate_checksum(station_name, played_date, played_time, artist, title):
    """
    Erzeugt eine eindeutige SHA256-Checksumme aus den übergebenen Daten.
    Diese dient zur Duplikaterkennung.
    """
    combined_string = f"{station_name}_{played_date}_{played_time}_{artist}_{title}"
    checksum = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
    return checksum

def scrape_playlist(html, station_name, date_str=None):
    """
    Parst den HTML-Inhalt einer Radioplaylist und extrahiert Informationen wie Uhrzeit,
    Künstler und Titel. Die gewonnenen Daten werden als Dictionary für InfluxDB vorbereitet.
    
    :param html: Der HTML-Content der Playlist
    :param station_name: Name der Radiostation
    :param date_str: Optionaler String im Format 'YYYY-MM-DD'. Wird genutzt als abgespieltes Datum.
                     Falls nicht angegeben, wird das aktuelle Datum verwendet.
    :return: Liste von Dictionaries, die die einzelnen Messpunkte darstellen.
    """
    soup = BeautifulSoup(html, 'html.parser')
    programs = soup.find_all('li', class_='program')
    
    # Verwende das übergebene Datum oder das aktuelle Datum
    if date_str is None:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    
    data = []
    for program in programs:
        time_elem = program.find('strong', class_='time')
        h3_elem = program.find('h3')
        if time_elem is None or h3_elem is None:
            continue  # Falls Elemente fehlen, diesen Eintrag überspringen
        
        played_time = time_elem.text.strip()
        artist_title_text = h3_elem.text.strip()
        
        # Erwartetes Format: "Künstler - Titel"
        artist_title = artist_title_text.split(' - ')
        if len(artist_title) != 2:
            print(f"Unerwartetes Format in '{artist_title_text}', überspringe diesen Eintrag.")
            continue
        
        artist, title = artist_title
        title = title if title else "Unbekannt"
        
        checksum = generate_checksum(station_name, date_str, played_time, artist, title)
        
        data.append({
            "measurement": "music_playlist",
            "tags": {
                "station": station_name
            },
            "fields": {
                "checksum": checksum,
                "played_date": date_str,
                "played_time": played_time,
                "artist": artist,
                "title": title
            }
        })
    return data

def load_config_and_stations():
    """
    Lädt die Konfigurationsdatei (config/config.json) und die Stationsliste (stations.csv).
    Überprüft, ob alle erforderlichen Konfigurationsschlüssel vorhanden sind.
    """
    with open('config/config.json', 'r') as config_file:
        config = json.load(config_file)
    
    required_keys = ['influx_host', 'influx_port', 'influx_user', 'influx_password', 'influx_db', 'num_days']
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Fehlende erforderliche Konfiguration: {key}")
    
    stations = []
    with open('stations.csv', 'r') as stations_file:
        csv_reader = csv.DictReader(stations_file)
        for row in csv_reader:
            stations.append({"station_name": row['Station'], "url": row['URL']})
    
    return config, stations
