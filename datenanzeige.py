from influxdb import InfluxDBClient

# InfluxDB-Verbindungsdetails
influx_host = "192.168.178.101"
influx_port = 8087
influx_user = "admin"
influx_password = "admin"
influx_db = "influx"

# Verbindung zur InfluxDB herstellen
client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password)
client.switch_database(influx_db)

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
