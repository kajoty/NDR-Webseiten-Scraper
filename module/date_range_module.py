import influxdb_module
import config_module
from datetime import datetime

def get_data_within_date_range(client, start_date, end_date):
    # InfluxDB-Abfrage: Daten innerhalb des Zeitraums abfragen
    query = f'''
    SELECT "artist", "title", "date", "time" 
    FROM "music_playlist"
    WHERE "date" >= '{start_date}' AND "date" <= '{end_date}'
    '''
    result = client.query(query)

    # Ergebnisse extrahieren
    data = [{"artist": point['artist'], "title": point['title'], "date": point['date'], "time": point['time']} 
            for point in result.get_points()]
    return data

def print_data_within_date_range(data):
    print("\nData within the selected date range:")
    for entry in data:
        print(f"Artist: {entry['artist']}, Title: {entry['title']}, Date: {entry['date']}, Time: {entry['time']}")

def run():
    # Konfiguration laden
    config = config_module.load_config()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Benutzer nach Start- und Enddatum fragen
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")

    # Daten im Zeitraum abfragen
    data = get_data_within_date_range(client, start_date, end_date)
    print_data_within_date_range(data)

    # InfluxDB-Client schließen
    client.close()
