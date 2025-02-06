import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import time
from influxdb import InfluxDBClient
import os

# Konfigurationen
INFLUX_CONFIG = {
    "host": "192.168.178.101",
    "port": 8087,
    "username": "admin",
    "password": "admin",
    "database": "influx"
}
BACKUP_FILE = "songs_backup.json"
REQUEST_DELAY = 1  # Sekunden zwischen Anfragen


def init_influx_client():
    client = InfluxDBClient(
        host=INFLUX_CONFIG["host"],
        port=INFLUX_CONFIG["port"],
        username=INFLUX_CONFIG["username"],
        password=INFLUX_CONFIG["password"]
    )
    client.switch_database(INFLUX_CONFIG["database"])
    return client


def load_backup():
    if os.path.exists(BACKUP_FILE):
        with open(BACKUP_FILE, "r") as file:
            return json.load(file)
    return []


def save_backup(data):
    with open(BACKUP_FILE, "w") as file:
        json.dump(data, file, indent=4)
    print(f"Daten wurden in {BACKUP_FILE} gesichert.")


def fetch_songs_for_date(client, date_str, hour):
    """Daten von der Webseite abrufen und Songs parsen."""
    url = f"https://www.ndr.de/wellenord/programm/titelliste1204.html?date={date_str}&hour={hour:02}"
    response = requests.get(url)

    # Abbruchbedingung: 404 Seite
    if response.status_code == 404:
        print(f"404 Seite für {date_str} {hour:02} Uhr. Abbruch.")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    songs = []

    for item in soup.find_all("li", class_="program"):
        time_element = item.find("strong", class_="time")
        artist_element = item.find("span", class_="artist")
        title_element = item.find("span", class_="title")

        if time_element and artist_element and title_element:
            songs.append({
                "time": time_element.text.strip(),
                "artist": artist_element.text.strip(),
                "title": title_element.text.strip()
            })

    # Abbruchbedingung: Keine Songs gefunden
    if not songs:
        print(f"Keine Daten gefunden für {date_str} {hour:02} Uhr. Abbruch.")
        return None

    return songs


def main():
    client = init_influx_client()
    song_buffer = load_backup()

    current_date = datetime.now() - timedelta(days=1)  # Starte mit dem letzten Tag

    while True:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Verarbeite Daten für das Datum: {date_str}")

        for hour in range(24):
            songs = fetch_songs_for_date(client, date_str, hour)
            if songs is None:
                return  # Abbruch bei fehlenden Daten oder 404 Fehler

            for song in songs:
                timestamp_str = f"{date_str} {song['time']}"
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M").isoformat()

                entry = {
                    "measurement": "songs",
                    "tags": {"artist": song["artist"]},
                    "time": timestamp,
                    "fields": {
                        "date": date_str,
                        "hour": song["time"],
                        "artist": song["artist"],
                        "title": song["title"]
                    }
                }
                song_buffer.append(entry)

            time.sleep(REQUEST_DELAY)

        current_date -= timedelta(days=1)

    if song_buffer:
        client.write_points(song_buffer)
        print(f"{len(song_buffer)} Songs in die Datenbank geschrieben.")
        save_backup(song_buffer)
    else:
        print("Keine neuen Daten zum Schreiben.")

    print("Prozess abgeschlossen.")


if __name__ == "__main__":
    main()
