# AirQ_M Logger (M5Stack PaperS3 v1.2)

AirQ_M Logger turns an M5Stack PaperS3 into an ESP-NOW air-quality logger for AirQ sensor nodes.  
The main goal is reliable local logging to SD card. Cloud upload is supported, but local collection is the priority.

## What it does

- Receives AirQ packets over ESP-NOW (CO2, PM, temperature, humidity, VOC, NOx, battery, sequence).
- Shows live data on touch UI screens (menu, AirQ list/detail, analysis, Wi-Fi, settings, sleep/off).
- Logs sensor data to microSD as TSV with timestamp and sensor identity.
- Supports two log modes:
  - `Single`: append to `/airq_data_log.tsv`
  - `Per Boot`: write `/airq_boot_<id>.tsv`
- Queues cloud payloads and uploads to HTTPS when cloud config is present.
- Supports custom menu icons from SD in `/icon` (PNG files).

## Device role

This firmware is built for long-running, stable local logging first.  
Cloud upload is secondary and should not break ESP-NOW capture.

## Time behavior (current)

- Time is based on manual/RTC/system clock path.
- Wi-Fi NTP sync is disabled by default in current firmware.
- If valid time is not available, UI shows `NO TIME SET`.
- While time is not valid, logging is paused (no mixed `NO_TIME` rows in main log).
- After time is set, new rows are logged with valid `time_iso` and `epoch_s`.

## Logging format

Default TSV columns:

`time_iso	epoch_s	uptime_seconds	wifi_sync	label	mac	seq	batt_pct	pm1	pm25	pm4	pm10	tempC	rh	voc	nox	co2`

Notes:
- New files are created with a header.
- Analysis parser also supports headerless rows (for older files).

## Analysis mode

- Select sensors, day filter, and time window filter.
- `Window` means time-of-day blocks:
  - `All`, `08-12`, `12-16`, `16-20`, `20-24`
- `Zoom` controls recent range (`10m`, `1h`, `1d`, `All`).

## Wi-Fi and cloud config (from SD `config.txt`)

Cloud credentials should be hardcoded in `config.txt` in your microSD card:  
If Wi-Fi credentials are provided, it will remember the password for that provided WiFi SSID but not auto connect at boot (reason below):
Since ESP-NOW + Wi‑Fi share the same 2.4GHz radio/channel, so sensor reception can degrade while Wi‑Fi is active and the priority of this AirQ_M Logger is to ensure local sensor logging first:
WiFi can be used to upload the .tsv file to your desired Cloud address:

```txt
cloud_ingest_url = "https://your-host-address"
cloud_api_key = "your-secret-api-key"

wifi_ssid = "YOUR_WIFI_NAME"
wifi_pwd = "YOUR_WIFI_PASSWORD"
