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

    # Erzeuge eine Liste der letzten 'num_days' Tage
    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]

    client = initialize_influxdb(config)  # InfluxDB-Verbindung initialisieren

    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)  # Maximale gleichzeitige Anfragen auf 10 begrenzen
        tasks = []
        for date in dates:
            for station in stations:
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
