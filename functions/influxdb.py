from influxdb import InfluxDBClient

def initialize_influxdb(config):
    return InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_user'],
        password=config['influx_password'],
        database=config['influx_db']
    )

def write_to_influxdb(client, data):
    try:
        client.write_points(data)
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
