# abfrage.py

import asyncio
import json
import csv
import aiohttp  # Für asynchrone HTTP-Anfragen
from datetime import datetime, timedelta
from asyncio import Semaphore

from functions.fetch_data import fetch_playlist  # Neu: Erwartet keinen Semaphore-Parameter mehr
from functions.scrape_playlist import scrape_playlist
from functions.influxdb import initialize_influxdb, write_to_influxdb


def load_config_and_stations():
    """
    Lädt die Konfigurationsdatei und die Liste der Radiostationen.
    Überprüft, ob alle benötigten Konfigurationswerte vorhanden sind.
    """
    with open('config/config.json', 'r') as config_file:
        config = json.load(config_file)

    required_keys = [
        'influx_host', 'influx_port',
        'influx_user', 'influx_password',
        'influx_db', 'num_days'
    ]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Fehlende erforderliche Konfiguration: {key}")

    stations = []
    with open('stations.csv', 'r') as stations_file:
        csv_reader = csv.DictReader(stations_file)
        for row in csv_reader:
            stations.append({
                "station_name": row['Station'],
                "url": row['URL']
            })
    return config, stations


def parse_station_selection(choice, stations):
    """
    Parst die vom Benutzer eingegebene Auswahl und gibt eine Liste der ausgewählten Stationen zurück.
    Unterstützt sowohl einfache Zahlen als auch Bereiche (z.B. 1-3, 2,4).
    """
    selected_stations = []
    for part in choice.split(','):
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                selected_stations.extend(range(start - 1, end))
            except ValueError:
                print(f"Ungültige Range: {part}.")
        else:
            try:
                index = int(part.strip()) - 1
                if 0 <= index < len(stations):
                    selected_stations.append(index)
                else:
                    print(f"Ungültige Station: {part}.")
            except ValueError:
                print(f"Ungültige Eingabe: {part}.")
    return list(sorted(set(selected_stations)))


def select_stations(stations):
    """
    Erlaubt dem Benutzer, entweder alle Stationen auszuwählen oder bestimmte über Nummern.
    Der Benutzer kann eine Range (z.B. 1-3) oder einzelne Nummern (z.B. 1,3,5) eingeben.
    """
    print("Möchten Sie alle Stationen abfragen oder nur bestimmte?")
    print("Verfügbare Stationen:")
    for i, station in enumerate(stations):
        print(f"{i + 1}. {station['station_name']}")

    choice = input("Geben Sie 'alle' für alle Stationen oder eine durch Komma getrennte Liste von Zahlen bzw. eine Range (z.B. 1-3) an: ").strip().lower()

    if choice == "alle":
        return stations

    selected_indexes = parse_station_selection(choice, stations)
    if not selected_indexes:
        print("Keine gültigen Stationen ausgewählt.")
        return []

    selected_stations = [stations[i] for i in selected_indexes]
    return selected_stations


async def fetch_data_for_day(session, semaphore, client, station, date):
    """
    Ruft die Playlist-Daten für eine bestimmte Radiostation und ein bestimmtes Datum ab.
    Für jede Stunde des Tages wird eine HTTP-Anfrage durchgeführt, wobei die Anzahl gleichzeitiger
    Anfragen durch den Semaphore begrenzt wird.
    """
    for hour in range(24):
        hour_str = str(hour).zfill(2)  # Format z.B. '01', '02', ...
        formatted_url = f"{station['url']}?date={date}&hour={hour_str}"

        # Begrenze die gleichzeitigen Requests über den Semaphore
        async with semaphore:
            html = await fetch_playlist(session, formatted_url)

        if html:
            # Übergibt das Datum als Parameter, damit scrape_playlist den korrekten Tag verwendet
            data = scrape_playlist(html, station['station_name'], date_str=date)
            if data:
                write_to_influxdb(client, data)
                print(f"Daten in InfluxDB geschrieben für {station['station_name']} am {date}")
            else:
                print(f"Keine Daten gefunden für {station['station_name']} am {date}")
        else:
            print(f"Fehler beim Abrufen der Daten für {station['station_name']} am {date}")


async def fetch_data():
    """
    Hauptfunktion zur Erfassung von Playlisten-Daten über mehrere Tage und Stationen hinweg.
    """
    config, stations = load_config_and_stations()
    num_days = config.get('num_days', 7)

    # Auswahl der Stationen durch den Benutzer
    stations_to_query = select_stations(stations)
    if not stations_to_query:
        print("Keine Stationen ausgewählt. Beende das Programm.")
        return

    # Erzeuge eine Liste der letzten 'num_days' Tage (im Format YYYY-MM-DD)
    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]

    # InfluxDB-Verbindung initialisieren
    client = initialize_influxdb(config)

    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(10)  # Begrenze gleichzeitige HTTP-Requests auf 10
        tasks = []
        for date in dates:
            for station in stations_to_query:
                tasks.append(fetch_data_for_day(session, semaphore, client, station, date))
        await asyncio.gather(*tasks)

    client.close()


if __name__ == "__main__":
    """
    Startet das asynchrone Abrufen der Daten.
    """
    try:
        asyncio.run(fetch_data())
    finally:
        print("Programm beendet.")
