import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import time
from influxdb import InfluxDBClient
import os

# Lade die Konfiguration aus der JSON-Datei
config_file = "config.json"

if not os.path.exists(config_file):
    raise FileNotFoundError(f"Konfigurationsdatei '{config_file}' nicht gefunden. Bitte erstelle sie mit den Zugangsdaten.")

with open(config_file, "r") as file:
    config = json.load(file)

influx_host = config["influx_host"]
influx_port = config["influx_port"]
influx_user = config["influx_user"]
influx_password = config["influx_password"]
influx_db = config["influx_db"]

# Berechne das heutige Datum und das Datum vor 10 Wochen
today = datetime.now()
weeks_ago = today - timedelta(weeks=10)

# JSON-Dateipfad für Zwischenspeicher
backup_file = "songs_backup.json"

# Lade vorhandenen Zwischenspeicher, falls die Datei existiert
if os.path.exists(backup_file):
    with open(backup_file, "r") as file:
        song_buffer = json.load(file)
else:
    song_buffer = []

# InfluxDB-Client initialisieren
client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password)
client.switch_database(influx_db)

# Zähler für den Index
index = 1

# Schleife für jede Woche der letzten 10 Wochen
current_date = weeks_ago
while current_date <= today:
    print(f"Verarbeite Daten für das Datum: {current_date.strftime('%Y-%m-%d')}")

    formatted_date = current_date.strftime("%Y-%m-%d")

    # Für jede Stunde von 00 bis 23 die URL abfragen
    for hour in range(24):
        formatted_hour = f"{hour:02}"
        url = f"https://www.ndr.de/wellenord/programm/titelliste1204.html?date={formatted_date}&hour={formatted_hour}"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Fehler beim Abrufen von {url}: {e}")
            continue

        soup = BeautifulSoup(response.content, "html.parser")

        for item in soup.find_all("li", class_="program"):
            time_element = item.find("strong", class_="time")
            artist_element = item.find("span", class_="artist")
            title_element = item.find("span", class_="title")

            if time_element and artist_element and title_element:
                entry = {
                    "time": time_element.text.strip(),
                    "artist": artist_element.text.strip(),
                    "title": title_element.text.strip()
                }

                datetime_str = f"{formatted_date} {entry['time']}"
                dt_object = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
                timestamp = dt_object.isoformat()

                date_only = dt_object.strftime("%Y-%m-%d")
                time_only = dt_object.strftime("%H:%M")

                query = f"SELECT * FROM songs WHERE date = '{date_only}' AND hour = '{time_only}' LIMIT 1"
                result = client.query(query)

                if not result.get_points():
                    json_body = {
                        "measurement": "songs",
                        "tags": {"artist": entry["artist"]},
                        "time": timestamp,
                        "fields": {
                            "index": index,
                            "date": date_only,
                            "hour": time_only,
                            "artist": entry["artist"],
                            "title": entry["title"],
                        }
                    }

                    song_buffer.append(json_body)
                    index += 1

        # Eine Sekunde warten, bevor der nächste Request ausgeführt wird
        time.sleep(1)

    current_date += timedelta(days=7)

# Daten in InfluxDB schreiben und Backup sichern
if song_buffer:
    client.write_points(song_buffer)
    print(f"{len(song_buffer)} Songs in die Datenbank geschrieben.")
    
    # Backup speichern
    with open(backup_file, "w") as file:
        json.dump(song_buffer, file, indent=4)
    print(f"Daten wurden in {backup_file} gesichert.")
else:
    print("Keine neuen Daten zum Schreiben.")

print("Prozess abgeschlossen.")
