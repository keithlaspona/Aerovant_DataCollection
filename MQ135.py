#!/usr/bin/env python3
"""
AEROVANT: Real-Time Data Collection for MQ-135 Sensor
Collects and logs raw sensor data from a Raspberry Pi for the MQ-135 gas sensor.
"""

import board
import busio
import digitalio
import adafruit_mcp3xxx.mcp3008 as MCP
from adafruit_mcp3xxx.analog_in import AnalogIn
import adafruit_dht
import pandas as pd
import time
import argparse
from datetime import datetime
from typing import Dict, Any

# Simple logger setup
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CALIBRATION CONSTANTS ---
TRUE_PPM_VALUE = 100  # Set the known gas concentration for the experiment

class DataCollector:
    """Collects real-time sensor data for the MQ-135 sensor."""
    def __init__(self):
        # Initialize SPI and MCP3008
        self.spi = busio.SPI(clock=board.SCK, MISO=board.MISO, MOSI=board.MOSI)
        self.cs = digitalio.DigitalInOut(board.D8)
        self.mcp = MCP.MCP3008(self.spi, self.cs)

        # Initialize DHT sensor
        self.dht = adafruit_dht.DHT11(board.D4)

        # Define the specific sensor ADC channel
        self.sensor_channel = {'MQ135_adc': AnalogIn(self.mcp, MCP.P4)}
        logger.info("Initialized hardware for MQ-135 sensor.")

    def _read_sensors(self) -> Dict[str, Any]:
        """Reads raw data from the MQ-135 and DHT sensors."""
        # Read the single MQ sensor
        sensor_name, channel = next(iter(self.sensor_channel.items()))
        raw_adc_reading = {sensor_name: channel.value // 64} # Convert 16-bit to 10-bit

        try:
            temperature = self.dht.temperature
            humidity = self.dht.humidity
        except RuntimeError as error:
            logger.warning(f"DHT sensor reading failed: {error.args[0]}")
            temperature, humidity = None, None

        return {
            'raw_adc_reading': raw_adc_reading,
            'temperature': temperature,
            'humidity': humidity
        }

    def collect_and_log_data(self, log_file: str, sample_interval: int = 10):
        """Continuously collects and logs sensor data to a CSV file."""
        logger.info(f"Starting MQ-135 data collection. Logging to {log_file}")
        header_needed = not pd.io.common.file_exists(log_file)

        while True:
            try:
                sensor_data = self._read_sensors()
                raw_adc_reading = sensor_data['raw_adc_reading']
                temperature = sensor_data['temperature']
                humidity = sensor_data['humidity']

                if temperature is None or humidity is None:
                    logger.warning("Skipping sample due to failed DHT reading.")
                    time.sleep(sample_interval)
                    continue

                # Create the data point for logging
                data_point = {
                    'timestamp': datetime.now().isoformat(),
                    'true_ppm': TRUE_PPM_VALUE,
                    'temp_c': float(temperature),
                    'hum_pct': float(humidity),
                    **raw_adc_reading
                }

                # Log a summary to the console
                mq_reading = next(iter(raw_adc_reading.values()))
                log_summary = f"MQ135_adc: {mq_reading}, Temp: {data_point['temp_c']}°C, Hum: {data_point['hum_pct']}%"
                logger.info(f"Logged new data point. {log_summary}")

                # Save to CSV
                df_point = pd.DataFrame([data_point])
                df_point.to_csv(log_file, mode='a', header=header_needed, index=False)
                if header_needed:
                    header_needed = False

            except Exception as e:
                logger.error(f"An error occurred during data collection: {e}")

            time.sleep(sample_interval)

def main():
    parser = argparse.ArgumentParser(description="AEROVANT MQ-135 Data Collection")
    parser.add_argument("--output", type=str, default="mq135_sensor_data.csv", help="Output CSV file path.")
    parser.add_argument("--interval", type=int, default=10, help="Time interval between samples in seconds.")
    args = parser.parse_args()

    logger.info("AEROVANT MQ-135 Real-Time Data Collection Started")
    logger.info("=" * 50)
    logger.info(f"True Gas Concentration set to: {TRUE_PPM_VALUE} ppm")
    logger.info("=" * 50)

    collector = DataCollector()
    collector.collect_and_log_data(log_file=args.output, sample_interval=args.interval)

if __name__ == "__main__":
    main()