import json
from influxdb import InfluxDBClient

# Konfigurationsdatei laden
with open("config.json") as config_file:
    config = json.load(config_file)

# Verbindung zur InfluxDB herstellen
client = InfluxDBClient(
    host=config["influx_host"],
    port=config["influx_port"],
    username=config["influx_user"],
    password=config["influx_password"]
)
client.switch_database(config["influx_db"])

# Abfrage der letzten 20 Einträge
query = "SELECT * FROM songs ORDER BY time DESC LIMIT 20"
result = client.query(query)

# Ergebnisse ausgeben
print("Letzte 20 Einträge (Index | Tag | Uhrzeit | Artist | Song):")
for point in result.get_points():
    index = point.get('index', 'N/A')  # Sicherstellen, dass der Index vorhanden ist
    date = point.get('date', 'Unbekannt')
    hour = point.get('hour', 'Unbekannt')
    artist = point.get('artist', 'Unbekannt')
    title = point.get('title', 'Unbekannt')
    print(f"{index} | {date} | {hour} | {artist} | {title}")
