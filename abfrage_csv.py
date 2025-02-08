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
    """
    Lädt die Konfiguration aus der config.json-Datei und die Stationen aus der stations.csv-Datei.
    Überprüft auch, ob alle erforderlichen Konfigurationswerte vorhanden sind.
    """
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
        # Die CSV-Datei wird gelesen und in ein Dictionary umgewandelt
        for row in csv_reader:
            stations.append({"station_name": row['Station'], "url": row['URL']})
    
    return config, stations

# URL mit Datum und Uhrzeit formatieren
def format_url(base_url, date, hour):
    """
    Formatiert die URL für die Playlist basierend auf dem Datum und der Stunde.
    """
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)
    query_params['date'] = [date]
    query_params['hour'] = [hour]

    # Die Query-Parameter werden neu formatiert und der vollständigen URL hinzugefügt
    new_query_string = urlencode(query_params, doseq=True)
    new_url = parsed_url._replace(query=new_query_string).geturl()

    return new_url

# Playlist-Daten extrahieren
def scrape_playlist(html, station_name):
    """
    Extrahiert die Playlist-Daten (Artist, Titel, Zeit) aus dem HTML-Inhalt der Playlist-Seite.
    Die Daten werden in einem Dictionary gespeichert und zurückgegeben.
    """
    soup = BeautifulSoup(html, 'html.parser')
    programs = soup.find_all('li', class_='program')

    data = []
    for program in programs:
        # Zeit auslesen
        time = program.find('strong', class_='time').text.strip()
        artist_title = program.find('h3').text.strip().split(' - ')

        # Sicherstellen, dass das Format korrekt ist (Artist und Title)
        if len(artist_title) != 2:
            continue  # Überspringen, wenn das Format nicht stimmt
        
        artist, title = artist_title

        # Fallback für Titel, falls er leer ist
        title = title if title else "Unbekannt"

        date_str = datetime.datetime.now().strftime('%Y-%m-%d')

        # Daten für die CSV-Datei sammeln
        data.append({
            "station": station_name,  # Der Name der Station wird hinzugefügt
            "date": date_str,         # Aktuelles Datum
            "time": time,             # Die Zeit des Programms
            "artist": artist,         # Der Künstler
            "title": title            # Der Titel des Songs
        })
    return data

# Daten in eine CSV-Datei speichern
def save_to_csv(entries, filename='music_playlist.csv'):
    """
    Speichert die gesammelten Daten in eine CSV-Datei.
    """
    # Öffnen oder Erstellen der CSV-Datei und Schreiben der Daten
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'time', 'station', 'artist', 'title']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Kopfzeile der CSV-Datei schreiben
        writer.writeheader()

        # Einträge in die CSV-Datei schreiben
        for entry in entries:
            writer.writerow(entry)

    print(f"Data saved to {filename}")

# Daten für einen Tag und eine Station abrufen
async def fetch_data_for_day(session, semaphore, entries, station, date):
    """
    Ruft die Playlist-Daten für einen bestimmten Tag und eine Station ab.
    Für jede Stunde (0-23) wird eine Anfrage gestellt, um die Playlist zu erhalten.
    """
    for hour in range(24):
        hour_str = str(hour).zfill(2)  # Stellt sicher, dass die Stunde immer 2-stellig ist
        formatted_url = format_url(station['url'], date, hour_str)
        
        async with semaphore:
            try:
                # Senden der HTTP-Anfrage, um die Playlist für eine bestimmte Stunde abzurufen
                async with session.get(formatted_url) as response:
                    response.raise_for_status()  # Überprüfen, ob die Antwort erfolgreich war
                    html = await response.text()  # Den HTML-Inhalt der Seite holen
                    data = scrape_playlist(html, station['station_name'])  # Daten extrahieren
                    if data:
                        entries.extend(data)  # Die extrahierten Daten der Liste hinzufügen
                        print(f"Data fetched for {station['station_name']} on {date} at hour {hour_str}")
            except Exception as e:
                print(f"Error fetching data for {station['station_name']} on {date} at hour {hour_str}: {e}")

# Funktion zum Abrufen der letzten N Tage
def get_dates_for_past_days(num_days):
    """
    Gibt eine Liste der letzten `num_days` Tage zurück.
    """
    now = datetime.datetime.now()
    dates = [(now - datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]
    return dates

# Hauptfunktion zum Abrufen von Daten für einen längeren Zeitraum
async def fetch_data():
    """
    Lädt die Konfiguration und Stationen, ruft die Playlist-Daten für die letzten `num_days` Tage ab
    und speichert sie in einer CSV-Datei.
    """
    # Laden der Konfiguration und Stationen
    config, stations = load_config_and_stations()
    num_days = config.get('num_days', 7)  # Anzahl der Tage aus der config.json laden (Standard: 7 Tage)
    dates = get_dates_for_past_days(num_days)

    # Liste für alle abgerufenen Daten
    entries = []

    # Erstellen einer asynchronen ClientSession, um HTTP-Anfragen zu stellen
    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)  # Limit für gleichzeitige Anfragen (10 gleichzeitig)
        tasks = []
        
        # Tasks für alle Stationen und Tage parallel ausführen
        for date in dates:
            for station in stations:
                tasks.append(fetch_data_for_day(session, semaphore, entries, station, date))
        
        # Warten, bis alle Tasks abgeschlossen sind
        await asyncio.gather(*tasks)

    # Speichern der gesammelten Daten in einer CSV-Datei
    save_to_csv(entries)

# Asynchrone Ausführung starten
if __name__ == "__main__":
    try:
        # Die asynchrone fetch_data-Funktion ausführen
        asyncio.run(fetch_data())
    finally:
        print("Program finished.")
