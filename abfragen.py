import requests
from bs4 import BeautifulSoup
import json
from influxdb import InfluxDBClient
import datetime
import csv

# Lade Konfiguration aus der config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# InfluxDB Verbindung herstellen
client = InfluxDBClient(
    host=config['influx_host'],
    port=config['influx_port'],
    username=config['influx_user'],
    password=config['influx_password'],
    database=config['influx_db']
)

# Funktion zur Berechnung von gestern
def get_yesterday():
    return (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')

# Liste der Radiostationen aus der CSV-Datei lesen
stations = []

with open('stations.csv', mode='r', newline='', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Überspringe die Kopfzeile
    for row in reader:
        station_name, url = row
        stations.append((station_name, url))

# Datum für gestern
yesterday = get_yesterday()

# Durchlaufe jede Station und hole die Titel
for station_name, url in stations:
    # Schleife über alle Stunden von 00 bis 23
    for hour in range(24):
        full_url = f"{url}?date={yesterday}&hour={hour:02d}"  # Stunde als zweiziffrige Zahl
        print(f"Rufe Daten von {full_url} für {station_name} ab...")

        # Hole die HTML-Seite
        response = requests.get(full_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Suche nach den Programmpunkten
        programs = soup.find_all("li", class_="program")

        # Wenn keine Titel gefunden wurden
        if not programs:
            print(f"Keine Datenpunkte für {station_name} um {hour:02d} Uhr gefunden.")
            continue

        # Für jede Programmpost einen Eintrag in die InfluxDB hinzufügen
        for program in programs:
            time = program.find("strong", class_="time").get_text(strip=True)
            artist = program.find("span", class_="artist").get_text(strip=True)
            title = program.find("span", class_="title").get_text(strip=True)

            # Datenpunkt erstellen
            json_body = [
                {
                    "measurement": "radio_play",
                    "tags": {
                        "station": station_name,
                        "day": yesterday,
                        "hour": time.split(':')[0],  # Extrahiere nur die Stunde
                    },
                    "fields": {
                        "artist": artist,
                        "title": title,
                    }
                }
            ]

            # Sende den Datenpunkt an die InfluxDB
            try:
                client.write_points(json_body)
                print(f"Erfolgreich Daten für {artist} - {title} gespeichert.")
            except Exception as e:
                print(f"Fehler beim Speichern der Daten: {e}")
