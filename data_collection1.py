#!/usr/bin/env python3
"""
AEROVANT: Real-Time Data Collection Module
Collects and logs raw sensor data from Raspberry Pi hardware for 5 gas sensors.

CRITICAL ASSUMPTIONS FOR CALIBRATION (R_S/R_0 METHOD):
1. R_L (Load Resistor) is set to 1.0 kOhm (Confirmed by user).
2. Div_Ratio is 1.47, derived from the 470 Ohm (R1) and 1 kOhm (R2) voltage divider.

*** IMPORTANT SAFETY/ACCURACY NOTE ***
The 470 Ohm / 1 kOhm voltage divider setup results in a maximum voltage of ~3.4V 
at the ADC input, which slightly exceeds the MCP3008's 3.3V reference. This 
could lead to clipped readings or long-term component degradation. A 10k/10k 
divider (Ratio 2.0) is safer and recommended if possible.
"""

import board
import busio
import digitalio
import adafruit_mcp3xxx.mcp3008 as MCP
import adafruit_dht
import pandas as pd
import time
import argparse
import math
from datetime import datetime
from typing import Dict, Any

# Assuming a simple logger setup
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CALIBRATION CONSTANTS (FINALIZED) ---
# Load Resistance (R_L) for the voltage divider circuit on the MQ module.
RL_KOHM = 1.0 # <--- FINALIZED: 1 kOhm

# Voltage Divider Ratio (Div_Ratio) to correct for VOUT scaling.
# Div_Ratio = (R1 + R2) / R2 = (470 + 1000) / 1000 = 1.47
DIV_RATIO = 1.47

# True 10-bit max value for the MCP3008 (used for V_ref conversion)
ADC_MAX_10BIT = 1023.0 

# --- EXPERIMENT CONSTANTS ---
# Set the known gas concentration (e.g., Ammonia) for the experiment run
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
        
        for name, channel in self.sensor_channels.items():
            # CRITICAL: Convert 16-bit value (0-65535) to 10-bit (0-1023)
            raw_readings[name] = channel.value // 64
            
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
        """
        logger.info(f"Starting real-time data collection. Logging to {log_file}")
        
        header_needed = not pd.io.common.file_exists(log_file)
        
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

                # Create the data point
                data_point = {
                    'timestamp': datetime.now().isoformat(),
                    'true_ppm': TRUE_PPM_VALUE,
                    'temp_c': temperature,
                    'hum_pct': humidity,
                    **raw_adc_readings
                }
                
                # Log a summary of the data
                log_summary = f"MQ2_adc: {data_point['MQ2_adc']}, Temp: {temperature}°C, Hum: {humidity}%"
                logger.info(f"Logged new data point. {log_summary}")

                # Convert to DataFrame and append to file
                df_point = pd.DataFrame([data_point])
                df_point.to_csv(log_file, mode='a', header=header_needed, index=False)
                
                if header_needed:
                    header_needed = False

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
    logger.info(f"Load Resistance (RL) FINALIZED at: {RL_KOHM} kOhm")
    logger.info(f"Voltage Divider Ratio FINALIZED at: {DIV_RATIO} (for 470/1k divider)")
    logger.info(f"True Gas Concentration set to: {TRUE_PPM_VALUE} ppm")
    logger.info("=" * 50)
    
    collector = DataCollector()
    collector.collect_and_log_data(
        log_file=args.output,
        sample_interval=args.interval
    )

if __name__ == "__main__":
    main()