import yaml
import csv
import time
from mqtt_spb_wrapper import *

_APP_VER = "01.220505"

_DEBUG = True   # Enable debug messages

print("--- Sparkplug B - Simulated End Of Node Device CSV player - Version " + _APP_VER + "  JFG ---")

# Load application configuration file
try:
    with open("data/config_csv_player.yml") as fr:
        config = yaml.load(fr, yaml.FullLoader)
    print("Configuration file loaded")
except:
    print("ERROR - Could not load config file")
    exit()

# Create the spB entity object
device = MqttSpbEntityDevice(spb_domain_name=config['sparkplugb']['domain_name'],
                             spb_eon_name=config['sparkplugb']['edge_node_name'],
                             spb_eon_device_name=config['sparkplugb']['device_name'],
                             retain_birth=True,
                             debug_enabled=_DEBUG)

# Load data from CSV file -----------------------------------------------------------------------
print("Loading data")

csv_data = []
with open(config['data']['file'], newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        csv_data.append(row)

config_replay_interval = config['data']['replay_interval']
print("Loading data - finished")

# Remap spB device ATTR, DATA, CMD fields as specified in the config file -----------------------

# MAP the telemetry fields - references to the CSV columns
telemetry = {}
for k in config['data']['data']:
    value = config['data']['data'][k]
    ref = value

    # Map the CSV column to the telemetry field
    if isinstance(value, str) and "file." in value:
        try:
            field = value.split(".")[1]
            ref = [row[field] for row in csv_data]
        except KeyError:
            print(f"WARNING - Could not map telemetry field - {k} : {value}")

    telemetry[k] = ref  # Save the reference

# MAP the attributes fields - static data - get data from CSV columns
attributes = {}
for k in config['data']['attributes']:
    value = config['data']['attributes'][k]
    ref = value

    if isinstance(value, str) and "file." in value:
        try:
            field = value.split(".")[1]
            ref = csv_data[0][field]
        except KeyError:
            print(f"WARNING - Could not map attribute field - {k} : {value}")

    attributes[k] = ref  # Save the reference

# MAP the commands fields - static data
commands = {}
for k in config['data']['commands']:
    value = config['data']['commands'][k]
    ref = value

    if isinstance(value, str) and "file." in value:
        try:
            field = value.split(".")[1]
            ref = csv_data[0][field]
        except KeyError:
            print(f"WARNING - Could not map commands field - {k} : {value}")

    commands[k] = ref  # Save the reference

# Fill out the device fields and console print values -----------------------------------------------

print("--- ATTRIBUTES")
for k in attributes:
    print(f"  {k} - {attributes[k]}")
    device.attributes.set_value(k, attributes[k])

print("--- COMMANDS")
for k in commands:
    print(f"  {k} - {commands[k]}")
    device.commands.set_value(k, commands[k])

print("--- TELEMETRY")
for k in telemetry:
    if isinstance(telemetry[k], list):
        value = telemetry[k][0]
    else:
        value = telemetry[k]
    print(f"  {k} - {value}")
    device.data.set_value(k, value)

# Reply spB device data on the MQTT server ----------------------------------------------------------

# Connect to the MQTT broker
_connected = False
while not _connected:
    print(f"Connecting to data broker {config['mqtt']['host']}:{config['mqtt']['port']} ...")
    _connected = device.connect(config['mqtt']['host'],
                                config['mqtt']['port'],
                                config['mqtt']['user'],
                                config['mqtt']['pass'])
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

device.publish_birth()  # Send birth message

# Iterate device data
for i in range(100):
    # Update the field telemetry data
    for k in device.data.get_names():
        value = telemetry[k]
        if isinstance(value, list):
            device.data.set_value(k, value[i])
        else:
            device.data.set_value(k, value)

    # Send data values
    device.publish_data()

    # Get next field data from file
    values = ""
    for v in telemetry.values():
        if values != "":
            values += "; "
        if isinstance(v, list):
            values += str(v[i]) # From CSV list
        else:
            values += str(v)    # Static value

    print(f"  {values}")

    # Wait some time, replay time
    time.sleep(config_replay_interval)