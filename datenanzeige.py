import json
import datetime
from influxdb import InfluxDBClient

# Lade die Konfiguration aus der config.json
with open('config/config.json') as config_file:
    config = json.load(config_file)

# InfluxDB-Verbindung
client = InfluxDBClient(
    host=config['influx_host'],
    port=config['influx_port'],
    username=config['influx_user'],
    password=config['influx_password'],
    database=config['influx_db']
)

# Funktion zur Formatierung des Zeitstempels
def format_timestamp(time_string, station):
    # Beachte die Mikrosekunden im Zeitstempel
    timestamp = datetime.datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S.%fZ')

    # Falls mehrere Stationen denselben Titel zur gleichen Zeit spielen, gebe jedem Sender eine kleine zeitliche Differenz
    if station == 'wellenord':
        timestamp += datetime.timedelta(milliseconds=100)
    elif station == 'NDR1 Niedersachsen':
        timestamp += datetime.timedelta(milliseconds=200)
    elif station == 'NDR 90.3':
        timestamp += datetime.timedelta(milliseconds=300)
    
    # Konvertiere Zeitstempel in lokale Zeit und gebe ihn im gewünschten Format aus
    local_timestamp = timestamp.astimezone(datetime.timezone(datetime.timedelta(hours=1)))  # Beispiel: UTC+1
    return local_timestamp.strftime('%Y-%m-%d %H:%M:%S')

# Funktion, um die letzten Einträge aus der Datenbank zu holen
def fetch_last_entries():
    query = f'''
        SELECT * FROM "music_playlist" 
        WHERE time > now() - {config["num_days"]}d 
        ORDER BY time DESC LIMIT {config["num_entries"]}
    '''
    result = client.query(query)
    entries = list(result.get_points(measurement='music_playlist'))
    return entries

# Funktion zur Anzeige der letzten Einträge
def display_last_entries():
    entries = fetch_last_entries()
    
    print(f"{'Zeitpunkt':<25} {'Station':<25} {'Datum':<15} {'Künstler':<30} {'Titel':<30}")
    print("="*100)
    
    for entry in entries:
        print(f"Entry Data: {entry}")
        
        # Formatierter Zeitstempel
        formatted_time = format_timestamp(entry['time'], entry['station'])
        
        # Anzeige der Daten
        print(f"{formatted_time} {entry['station']:<25} {entry['played_date']}  {entry['artist']:<30} {entry['title']}")

if __name__ == "__main__":
    display_last_entries()
