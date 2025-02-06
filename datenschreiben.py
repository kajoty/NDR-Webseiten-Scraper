import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import time
from influxdb import InfluxDBClient

# InfluxDB-Verbindungsdetails
influx_host = "192.168.178.101"
influx_port = 8087
influx_user = "admin"
influx_password = "admin"
influx_db = "influx"

# Berechne das heutige Datum und das Datum vor 10 Wochen
today = datetime.now()
weeks_ago = today - timedelta(weeks=10)

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
        # Stunde als String mit führender Null, falls nötig (z.B. '00', '01' bis '23')
        formatted_hour = f"{hour:02}"

        # URL für die jeweilige Stunde anpassen
        url = f"https://www.ndr.de/wellenord/programm/titelliste1204.html?date={formatted_date}&hour={formatted_hour}"

        # HTTP-Anfrage senden
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Daten extrahieren
        song_data = []

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

                # Kombiniere Datum und Uhrzeit und konvertiere in ein datetime-Objekt
                datetime_str = f"{formatted_date} {entry['time']}"  # z.B. '2025-02-05 12:34'
                dt_object = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")  # datetime-Objekt
                timestamp = dt_object.isoformat()  # ISO 8601 Format: '2025-02-05T12:34:00'

                # Extrahiere Datum und Uhrzeit für getrennte Felder
                date_only = dt_object.strftime("%Y-%m-%d")  # z.B. '2025-02-05'
                time_only = dt_object.strftime("%H:%M")    # z.B. '12:34'

                # Escape Sonderzeichen in Künstlername und Titel (falls vorhanden)
                artist_escaped = entry["artist"].replace("'", "\\'").replace('"', '\\"')
                title_escaped = entry["title"].replace("'", "\\'").replace('"', '\\"')

                # Prüfe, ob dieser Song bereits existiert
                query = f"SELECT * FROM songs WHERE date = '{date_only}' AND hour = '{time_only}' AND artist = '{artist_escaped}' LIMIT 1"
                result = client.query(query)

                # Wenn der Song nicht vorhanden ist, schreibe ihn in die Datenbank
                if not result.get_points():
                    json_body = [
                        {
                            "measurement": "songs",
                            "tags": {
                                "artist": entry["artist"],
                            },
                            "time": timestamp,  # Zeitstempel
                            "fields": {
                                "index": index,  # Index
                                "date": date_only,  # Datum als Feld
                                "hour": time_only,  # Uhrzeit als Feld
                                "artist": entry["artist"],  # Artist
                                "title": entry["title"],  # Titel
                            }
                        }
                    ]

                    # Schreibe die Daten in InfluxDB
                    client.write_points(json_body)
                    index += 1  # Erhöhe den Index für den nächsten Datensatz
                else:
                    print(f"Song '{entry['title']}' von {entry['artist']} bereits vorhanden, überspringe...")

        # Eine Sekunde warten, bevor der nächste Request ausgeführt wird
        time.sleep(1)

    # Zur nächsten Woche wechseln
    current_date += timedelta(days=7)

print("Daten erfolgreich in die InfluxDB geschrieben.")
