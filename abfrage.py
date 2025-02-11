import asyncio
import json
import csv
import aiohttp  # Für asynchrone HTTP-Anfragen
from datetime import datetime, timedelta
from asyncio import Semaphore

from functions.fetch_data import fetch_playlist
from functions.scrape_playlist import scrape_playlist
from functions.influxdb import initialize_influxdb, write_to_influxdb


def load_config_and_stations():
    """
    Lädt die Konfigurationsdatei und die Liste der Radiostationen.
    Überprüft, ob alle benötigten Konfigurationswerte vorhanden sind.
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
            stations.append({"station_name": row['Station'], "url": row['URL']})  # Stationen aus CSV-Datei laden
    
    return config, stations


def select_stations(stations):
    """
    Erlaubt dem Benutzer, entweder alle Stationen auszuwählen oder bestimmte über Nummern.
    Der Benutzer kann eine Range (z.B. 1-3) oder einzelne Nummern (z.B. 1,3,5) eingeben.
    """
    print("Möchten Sie alle Stationen abfragen oder nur bestimmte?")
    print(f"Verfügbare Stationen: {[f'{i+1}. {station['station_name']}' for i, station in enumerate(stations)]}")
    choice = input("Geben Sie 'alle' für alle Stationen oder eine durch Komma getrennte Liste von Zahlen oder eine Range (z.B. 1-3) an: ").strip().lower()

    if choice == "alle":
        return stations  # Alle Stationen zurückgeben
    
    # Überprüfen, ob der Benutzer eine Range oder einzelne Stationen gewählt hat
    selected_stations = []
    try:
        # Überprüfen, ob eine Range angegeben wurde
        if '-' in choice:
            start, end = map(int, choice.split('-'))
            selected_stations = stations[start-1:end]  # Index anpassen, da Stationen bei 1 beginnen
        else:
            # Wenn keine Range, dann einzelne Zahlen (z.B. 1,3,5)
            chosen_indexes = [int(station.strip())-1 for station in choice.split(',')]  # Index anpassen
            selected_stations = [stations[i] for i in chosen_indexes if 0 <= i < len(stations)]
        
        if not selected_stations:
            print("Keine gültigen Stationen ausgewählt.")
            return []
        
        return selected_stations
    except ValueError:
        print("Ungültige Eingabe. Bitte geben Sie gültige Zahlen oder eine gültige Range ein.")
        return []


async def fetch_data_for_day(session, semaphore, client, station, date):
    """
    Ruft die Playlist-Daten für eine bestimmte Radiostation und ein bestimmtes Datum ab.
    Die Anzahl gleichzeitiger Anfragen wird durch eine Semaphore begrenzt.
    """
    async with semaphore:  # Begrenzung auf maximal 10 gleichzeitige Anfragen
        for hour in range(24):
            hour_str = str(hour).zfill(2)  # Stundenformat (z. B. '01', '02', ...)
            formatted_url = f"{station['url']}?date={date}&hour={hour_str}"

            html = await fetch_playlist(session, formatted_url, semaphore)
            if html:
                data = scrape_playlist(html, station['station_name'])
                if data:
                    write_to_influxdb(client, data)
                    print(f"Daten in InfluxDB geschrieben für {station['station_name']} am {date}")
                else:
                    print(f"Keine Daten für {station['station_name']} am {date}")
            else:
                print(f"Fehler beim Abrufen der Daten für {station['station_name']} am {date}")


async def fetch_data():
    """
    Hauptfunktion zur Erfassung von Playlisten-Daten über mehrere Tage und Stationen hinweg.
    """
    config, stations = load_config_and_stations()
    num_days = config.get('num_days', 7)  # Anzahl der Tage aus der Konfiguration

    # Benutzerabfrage für Auswahl der Stationen
    stations_to_query = select_stations(stations)
    if not stations_to_query:
        print("Keine Stationen ausgewählt. Beende das Programm.")
        return

    # Erzeuge eine Liste der letzten 'num_days' Tage
    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]

    client = initialize_influxdb(config)  # InfluxDB-Verbindung initialisieren

    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)  # Maximale gleichzeitige Anfragen auf 10 begrenzen
        tasks = []
        for date in dates:
            for station in stations_to_query:  # Verwende die gefilterte Liste von Stationen
                tasks.append(fetch_data_for_day(session, semaphore, client, station, date))
        
        await asyncio.gather(*tasks)  # Alle Aufgaben asynchron ausführen
    
    client.close()  # Datenbankverbindung schließen


if __name__ == "__main__":
    """
    Startet das asynchrone Abrufen der Daten.
    """
    try:
        asyncio.run(fetch_data())
    finally:
        print("Programm beendet.")
