# AirQ_M Logger (M5Paper S3)

This turns an M5Stack PaperS3 into an ESP-NOW air-quality data logger for up to 10 M5Stack AirQ sensor nodes.

## What it does

- Receives sensor packets over ESP-NOW (CO2, PM, temperature, humidity, VOC, NOx, battery, sequence).
- Shows live sensor status in a touch UI (list view, detail view, analysis plot, Wi-Fi, settings, sleep/off).
- Logs sensor data to microSD as TSV with timestamps and sensor identity.
- Supports two log modes:
  - `Single`: append to one continuous file (`/airq_data_log.tsv`)
  - `Per Boot`: create one file per boot session (`/airq_boot_<id>.tsv`)
- Queues cloud payloads and uploads to HTTPS endpoint when Wi-Fi/cloud mode is enabled.
- Prioritizes ESP-NOW reliability for logging; Wi-Fi is paused outside Wi-Fi setup/upload flows to protect packet reception.
- Supports menu icon customization from SD card: add your preferred PNG icon files in `/icon`.  
  Example icon files are provided in the `icon` folder of this project.

## Device role

Primary role is robust, long-running local data logging.  
Cloud upload is possible but is secondary and it is intended not to interrupt ESP-NOW collection.

## Storage format

TSV header:

`time_iso epoch_s elapsed_s wifi_sync label mac seq batt_pct pm1 pm25 pm4 pm10 tempC rh voc nox co2`

## Hardware

- Target: M5Stack PaperS3 (touch panel GT911)
- Communication: ESP-NOW (AirQ sensor -> PaperS3 logger)
- Storage: microSD
- Optional: Wi-Fi for NTP time and cloud upload
