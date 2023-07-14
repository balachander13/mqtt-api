import json
import paho.mqtt.client as mqtt
import requests

# MQTT configuration
mqtt_broker = 'iot.salieabs.in'
mqtt_port = 1883
mqtt_topic = 'baypo/level'
#mqtt_client_id = 'mqtt_client'

# API configuration
api_url = 'http://sensordatawebhook-7ttvfjytkq-el.a.run.app'
api_headers = {'Content-Type': 'application/json'}

def on_connect(client, userdata, flags, rc):
    print('Connected to MQTT broker')
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    try:
        json_data = json.loads(payload)
        send_data(json_data)
    except json.JSONDecodeError:
        print('Received payload is not in valid JSON format')

def send_data(data):
    try:
        response = requests.post(api_url, json=data, headers=api_headers)
        response.raise_for_status()
        print('Data sent to the API successfully')
    except requests.exceptions.RequestException as e:
        print(f'Error sending data to the API: {e}')

# Create MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker and start loop
client.connect(mqtt_broker, mqtt_port, keepalive=60)
client.loop_forever()
