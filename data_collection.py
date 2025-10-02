#!/usr/bin/env python3
"""
AEROVANT: Real-Time Data Collection Module
Collects and logs raw sensor data from Raspberry Pi hardware for 5 gas sensors.
"""

import board
import busio
import digitalio
import adafruit_mcp3xxx.mcp3008 as MCP
import adafruit_dht
import pandas as pd
import time
import argparse
from datetime import datetime
from typing import Dict, Any

# Assuming a simple logger setup if config.py is removed
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- SET YOUR TRUE PPM VALUE HERE ---
TRUE_PPM_VALUE = 100

class DataCollector:
    """
    Collects real-time sensor data from a Raspberry Pi
    """
    def __init__(self):
        # Initialize ADC for MQ sensors
        self.spi = busio.SPI(clock=board.SCK, MISO=board.MISO, MOSI=board.MOSI)
        self.cs = digitalio.DigitalInOut(board.D5)  # Chip Select pin
        self.mcp = MCP.MCP3008(self.spi, self.cs)

        # Initialize DHT sensor for temperature/humidity
        self.dht = adafruit_dht.DHT11(board.D4)  # Data pin
        
        # Mapping of sensor names to ADC channels
        self.sensor_channels = {
            'MQ2_adc': self.mcp.channels[0],
            'MQ4_adc': self.mcp.channels[1],
            'MQ5_adc': self.mcp.channels[2],
            'MQ9_adc': self.mcp.channels[3],
            'MQ135_adc': self.mcp.channels[4]
        }
    
    def _read_sensors(self) -> Dict[str, Any]:
        """Reads raw data from all connected sensors"""
        raw_readings = {}
        # The .value attribute gives a 16-bit number (0-65535)
        for name, channel in self.sensor_channels.items():
            raw_readings[name] = channel.value
            
        try:
            temperature = self.dht.temperature
            humidity = self.dht.humidity
        except RuntimeError as error:
            logger.warning(f"DHT sensor reading failed: {error.args[0]}")
            temperature = None
            humidity = None
            
        return {
            'raw_adc_readings': raw_readings,
            'temperature': temperature,
            'humidity': humidity
        }

    def collect_and_log_data(self, log_file: str, sample_interval: int = 10):
        """
        Continuously collects and logs sensor data to a file
        
        Args:
            log_file: Path to the CSV file for data logging.
            sample_interval: Time in seconds between each data sample.
        """
        logger.info(f"Starting real-time data collection. Logging to {log_file}")
        
        while True:
            try:
                # Read raw sensor data
                sensor_data = self._read_sensors()
                raw_adc_readings = sensor_data['raw_adc_readings']
                temperature = sensor_data['temperature']
                humidity = sensor_data['humidity']

                if temperature is None or humidity is None:
                    logger.warning("Skipping sample due to failed environmental sensor reading.")
                    time.sleep(sample_interval)
                    continue

                # Create the data point with the specified column names
                data_point = {
                    'timestamp': datetime.now().isoformat(),
                    'true_ppm': TRUE_PPM_VALUE,
                    'temp_c': temperature,
                    'hum_pct': humidity,
                    **raw_adc_readings
                }
                
                # Convert to DataFrame and append to file
                df_point = pd.DataFrame([data_point])
                try:
                    header = not pd.io.common.file_exists(log_file)
                except Exception:
                    header = True
                
                df_point.to_csv(log_file, mode='a', header=header, index=False)
                
                logger.info(f"Logged new data point.")

            except Exception as e:
                logger.error(f"An error occurred during data collection: {e}")
                
            time.sleep(sample_interval)

def main():
    """Main function for running data collection"""
    parser = argparse.ArgumentParser(description="AEROVANT Custom Real-Time Data Collection")
    parser.add_argument("--output", type=str, default="custom_sensor_data.csv",
                        help="Output file path for data logging")
    parser.add_argument("--interval", type=int, default=10,
                        help="Time interval between samples in seconds")
    
    args = parser.parse_args()
    
    logger.info("AEROVANT Custom Real-Time Data Collection Started")
    logger.info("=" * 50)
    
    collector = DataCollector()
    collector.collect_and_log_data(
        log_file=args.output,
        sample_interval=args.interval
    )

if __name__ == "__main__":
    main()
