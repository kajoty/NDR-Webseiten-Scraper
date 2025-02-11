import hashlib
import datetime
from bs4 import BeautifulSoup
import json
import csv

def generate_checksum(station_name, played_date, played_time, artist, title):
    """
    Erstellt eine eindeutige Checksumme durch Verknüpfung von Stationsnamen, 
    Abspieldatum, Uhrzeit, Künstler und Titel. Die Checksumme dient zur Duplikaterkennung.
    """
    # Kombiniere die relevanten Informationen zu einem einzigen String
    combined_string = f"{station_name}_{played_date}_{played_time}_{artist}_{title}"
    
    # Erzeuge eine SHA256-Checksumme basierend auf dem kombinierten String
    checksum = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
    return checksum

def scrape_playlist(html, station_name):
    """
    Analysiert die HTML-Daten einer Radioplaylist und extrahiert Informationen wie Uhrzeit,
    Künstler und Titel. Die Daten werden für das Schreiben in die InfluxDB vorbereitet.
    """
    # Erzeuge ein BeautifulSoup-Objekt zur Analyse des HTML-Inhalts
    soup = BeautifulSoup(html, 'html.parser')
    
    # Suche alle Playlist-Elemente anhand des Klassenattributs 'program'
    programs = soup.find_all('li', class_='program')

    data = []
    for program in programs:
        # Extrahiere die Uhrzeit des abgespielten Titels
        played_time = program.find('strong', class_='time').text.strip()
        
        # Extrahiere Künstler und Titel aus dem HTML-Element
        artist_title = program.find('h3').text.strip().split(' - ')

        # Falls das Format nicht den Erwartungen entspricht, überspringe den Eintrag
        if len(artist_title) != 2:
            continue
        
        # Trenne Künstler und Titel; wenn Titel fehlt, setze einen Standardwert
        artist, title = artist_title
        title = title if title else "Unbekannt"
        
        # Erhalte das aktuelle Datum als Abspieldatum
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # Generiere eine eindeutige Checksumme für die gespeicherten Daten
        checksum = generate_checksum(station_name, date_str, played_time, artist, title)

        # Erstelle die InfluxDB-Daten
        data.append({
            "measurement": "music_playlist",
            "tags": {
                "station": station_name  # Station als Tag
            },
            "fields": {
                "checksum": checksum,          # Checksumme als eindeutiger Identifier
                "played_date": date_str,       # Datum des Spielens
                "played_time": played_time,    # Uhrzeit des Spielens
                "artist": artist,              # Künstler
                "title": title                 # Titel
            }
        })
    return data

def load_config_and_stations():
    """
    Lädt die Konfigurationseinstellungen aus einer JSON-Datei und die Stationsdaten aus einer CSV-Datei.
    Überprüft, ob alle erforderlichen Konfigurationsschlüssel vorhanden sind und gibt beides zurück.
    """
    # Öffne und lade die Konfigurationsdatei im JSON-Format
    with open('config/config.json', 'r') as config_file:
        config = json.load(config_file)

    # Überprüfe, ob alle erforderlichen Konfigurationsschlüssel vorhanden sind
    required_keys = ['influx_host', 'influx_port', 'influx_user', 'influx_password', 'influx_db', 'num_days']
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Fehlende erforderliche Konfiguration: {key}")
    
    # Lade die Stationsdaten aus der CSV-Datei
    stations = []
    with open('stations.csv', 'r') as stations_file:
        csv_reader = csv.DictReader(stations_file)
        for row in csv_reader:
            # Füge jede Station mit ihrem Namen und der URL zur Liste hinzu
            stations.append({"station_name": row['Station'], "url": row['URL']})
    
    return config, stations
