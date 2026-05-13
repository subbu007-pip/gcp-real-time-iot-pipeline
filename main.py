import base64
import json
from google.cloud import bigquery

client = bigquery.Client()
table_id = "iot-data-pipeline-495413.iot_dataset.sensor_data"

def process_iot_data(event, context):
    message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(message)

    row = [{
        "device_id": data.get("device_id"),
        "temperature": data.get("temperature"),
        "humidity": data.get("humidity"),
        "motion": data.get("motion")
    }]

    errors = client.insert_rows_json(table_id, row)

    if errors:
        print("Error:", errors)
    else:
        print("Data inserted successfully")