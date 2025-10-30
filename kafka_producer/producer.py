from confluent_kafka import Producer
import time
import csv
import json
import sys
import os

def report(err, msg):
    if err is not None:
        print(f"Error: {err}")
    else:
        print(f"Message sent test data: {msg.key().decode('utf-8')} -> {msg.value().decode('utf-8')}")

if __name__ == "__main__":

    print("-----Producer-----")

    kafka_config = {
        'bootstrap.servers': 'kafka:9092',
    }

    fcd_topic = "fcd_topic"
    emission_topic = "emission_topic"

    fcd_path = os.path.join(os.path.dirname(__file__), "fcd1.csv")
    emission_path = os.path.join(os.path.dirname(__file__), "emission1.csv")

    producer = Producer(kafka_config)

    try:
        with open(fcd_path, 'r') as file:
            lane_data = csv.DictReader(file, delimiter=';')
            for row in lane_data:
                try:
                    vehicle_info = {
                        'timestep_time': float(row['timestep_time']),
                        'vehicle_angle': float(row['vehicle_angle']) if row['vehicle_angle'] else 0.0,
                        'vehicle_id': row['vehicle_id'],
                        'vehicle_lane': row['vehicle_lane'],
                        'vehicle_pos': float(row['vehicle_pos']) if row['vehicle_pos'] else 0.0,
                        'vehicle_slope': float(row['vehicle_slope']) if row['vehicle_slope'] else 0.0,
                        'vehicle_speed': float(row['vehicle_speed']) if row['vehicle_speed'] else 0.0,
                        'vehicle_type': row['vehicle_type'],
                        'vehicle_x': float(row['vehicle_x']) if row['vehicle_x'] else 0.0,
                        'vehicle_y': float(row['vehicle_y']) if row['vehicle_y'] else 0.0,
                    }

                    producer.produce(fcd_topic, key='fcd_key', value=json.dumps(vehicle_info), callback=report)
                except Exception as e:
                    print(f"FCD ---- Greška kod reda: {row}. Detalji: {e}")

            producer.flush()
            time.sleep(0.2)

        with open(emission_path, 'r') as file:
            emission_data = csv.DictReader(file, delimiter=';')
            for row in emission_data:
                try:
                    emission_info = {
                        'timestep_time': float(row['timestep_time']),
                        'vehicle_CO': float(row['vehicle_CO']) if row['vehicle_CO'] else 0.0,
                        'vehicle_CO2': float(row['vehicle_CO2']) if row['vehicle_CO2'] else 0.0,
                        'vehicle_HC': float(row['vehicle_HC']) if row['vehicle_HC'] else 0.0,
                        'vehicle_NOx': float(row['vehicle_NOx']) if row['vehicle_NOx'] else 0.0,
                        'vehicle_PMx': float(row['vehicle_PMx']) if row['vehicle_PMx'] else 0.0,
                        'vehicle_angle': float(row['vehicle_angle']) if row['vehicle_angle'] else 0.0,
                        'vehicle_eclass': row['vehicle_eclass'],
                        'vehicle_electricity': float(row['vehicle_electricity']) if row['vehicle_electricity'] else 0.0,
                        'vehicle_fuel': float(row['vehicle_fuel']) if row['vehicle_fuel'] else 0.0,
                        'vehicle_id': row['vehicle_id'],
                        'vehicle_lane': row['vehicle_lane'],
                        'vehicle_noise': float(row['vehicle_noise']) if row['vehicle_noise'] else 0.0,
                        'vehicle_pos': float(row['vehicle_pos']) if row['vehicle_pos'] else 0.0,
                        'vehicle_route': row['vehicle_route'],
                        'vehicle_speed': float(row['vehicle_speed']) if row['vehicle_speed'] else 0.0,
                        'vehicle_type': row['vehicle_type'],
                        'vehicle_waiting': float(row['vehicle_waiting']) if row['vehicle_waiting'] else 0.0,
                        'vehicle_x': float(row['vehicle_x']) if row['vehicle_x'] else 0.0,
                        'vehicle_y': float(row['vehicle_y']) if row['vehicle_y'] else 0.0,
                    }

                    producer.produce(emission_topic, key='emission_key', value=json.dumps(emission_info), callback=report)
                except Exception as e:
                    print(f"EMISSION ---- Greška kod reda: {row}. Detalji: {e}")

            producer.flush()
            time.sleep(0.2)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        

