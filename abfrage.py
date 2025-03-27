import asyncio
import json
import csv
import aiohttp  # Für asynchrone HTTP-Anfragen
from datetime import datetime, timedelta
from asyncio import Semaphore
import threading
from tqdm import tqdm

from functions.fetch_data import fetch_playlist
from functions.scrape_playlist import scrape_playlist
from functions.influxdb import initialize_influxdb, write_to_influxdb


def load_config_and_stations():
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


def input_with_timeout(prompt, timeout=5):
    user_input = [None]

    def get_input():
        user_input[0] = input(prompt)

    thread = threading.Thread(target=get_input)
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        print(f"\nKeine Eingabe nach {timeout} Sekunden. Starte mit 'alle'.")
        return "alle"
    return user_input[0]


def parse_station_selection(choice, stations):
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
    print("Möchten Sie alle Stationen abfragen oder nur bestimmte?")
    print("Verfügbare Stationen:")
    for i, station in enumerate(stations):
        print(f"{i + 1}. {station['station_name']}")

    choice = input_with_timeout(
        "Geben Sie 'alle' für alle Stationen oder eine durch Komma getrennte Liste von Zahlen bzw. eine Range (z.B. 1-3) an: ",
        timeout=5
    ).strip().lower()

    if choice == "alle":
        return stations

    selected_indexes = parse_station_selection(choice, stations)
    if not selected_indexes:
        print("Keine gültigen Stationen ausgewählt.")
        return []

    selected_stations = [stations[i] for i in selected_indexes]
    return selected_stations


async def fetch_data_for_day(session, semaphore, client, station, date):
    for hour in range(24):
        hour_str = str(hour).zfill(2)
        formatted_url = f"{station['url']}?date={date}&hour={hour_str}"

        async with semaphore:
            html = await fetch_playlist(session, formatted_url)

        if html:
            data = scrape_playlist(html, station['station_name'], date_str=date)
            if data:
                write_to_influxdb(client, data)
        # Debug-Ausgabe kann bei Bedarf hier bleiben oder entfernt werden


async def fetch_data():
    config, stations = load_config_and_stations()
    num_days = config.get('num_days', 7)
    max_parallel = config.get('max_parallel_requests', 10)

    stations_to_query = select_stations(stations)
    if not stations_to_query:
        print("Keine Stationen ausgewählt. Beende das Programm.")
        return

    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]

    client = initialize_influxdb(config)

    total_tasks = len(dates) * len(stations_to_query)
    progress = tqdm(total=total_tasks, desc="Fortschritt")

    async with aiohttp.ClientSession() as session:
        semaphore = Semaphore(max_parallel)

        async def wrapped_task(station, date):
            await fetch_data_for_day(session, semaphore, client, station, date)
            progress.update(1)

        tasks = [wrapped_task(station, date) for date in dates for station in stations_to_query]
        await asyncio.gather(*tasks)

    progress.close()
    client.close()


if __name__ == "__main__":
    try:
        asyncio.run(fetch_data())
    finally:
        print("Programm beendet.")
