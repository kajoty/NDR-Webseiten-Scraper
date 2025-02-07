import json
from influxdb import InfluxDBClient

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

# Beispielabfrage, um alle gespeicherten "radio_play" Daten anzuzeigen
query = 'SELECT * FROM "radio_play" LIMIT 10'

# Führe die Abfrage aus
result = client.query(query)

# Überprüfe, ob Daten vorhanden sind
if result:
    print("Daten in der Datenbank gefunden:")
    for point in result.get_points():
        print(point)
else:
    print("Keine Daten gefunden.")
