#include <M5Unified.h>
#include <WiFi.h>
#include <esp_now.h>
#include <FS.h>
#include <SD.h>
#include <time.h>
#include <esp_sleep.h>
#include <Preferences.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>

#include "airq_types.h"

// ============================
// Global state
// ============================
static bool asleep = false;
static bool offLatch = false;
static uint32_t bootBlockOffUntil = 0;
static uint32_t lastEspNowRxMs = 0;

// Paper S3 BtnA often GPIO0; adjust if your wake pin differs
static constexpr gpio_num_t WAKE_PIN = GPIO_NUM_0;

static constexpr uint32_t LONGPRESS_MS = 900;
static constexpr uint32_t LIST_REDRAW_MIN_MS = 2000;
static constexpr uint32_t DEVICE_STALE_MS = 90000;

// ============================
// Optional Wi-Fi for NTP time
// ============================
// Leave SSID empty "" to skip Wi-Fi time completely.
static const char* WIFI_SSID = "";
static const char* WIFI_PASS = "";

// ============================
// Devices
// ============================
static constexpr int MAX_DEVS = 16;
static DeviceState devs[MAX_DEVS];
static bool devDirty[MAX_DEVS];
static int devCount = 0;
static int selectedIdx = 0;

// ============================
// Metrics
// ============================
static bool analysisSel[MAX_DEVS] = {false};
static SummaryMetric listMetrics[3] = {
  SummaryMetric::CO2,
  SummaryMetric::PM25,
  SummaryMetric::VOC
};
static int listMetricCount = 3;
static SummaryMetric analysisMetric = SummaryMetric::CO2;

enum class ZoomRange : uint8_t { ZOOM_10M, ZOOM_1H, ZOOM_1D, ZOOM_ALL };
static ZoomRange analysisZoom = ZoomRange::ZOOM_1H;

// ============================
// Logging
// ============================
enum class LogMode : uint8_t { CONTINUOUS, PER_BOOT };
static LogMode logMode = LogMode::CONTINUOUS;
static uint32_t logIntervalMs = 20000;
static uint32_t lastLogMs[MAX_DEVS] = {0};
static bool sdOk = false;
static uint32_t bootId = 0;
static bool espNowActive = false;
static File logFile;
static String logFilePath;
static String logFiles[16];
static int logFileCount = 0;
static int logFileIdx = 0;
static bool hasNtpTime = false;
static uint32_t hubStartMs = 0;

// Cloud upload (optional)
static const char* CLOUD_INGEST_URL = "https://n8n.srv1311843.hstgr.cloud/airq/ingest";
static const char* CLOUD_API_KEY = "19e76cb6d1762535e8c00a7b6f547ed18de929966499d872a72ffb95a45b0c2e";  // set your VPS token here
static bool cloudUploadEnabled = true;
static constexpr int CLOUD_QSIZE = 24;
static String cloudQ[CLOUD_QSIZE];
static int cloudHead = 0;
static int cloudTail = 0;
static uint32_t cloudDropped = 0;
static uint32_t cloudSent = 0;
static uint32_t cloudFail = 0;
static uint32_t cloudLastTryMs = 0;
static constexpr uint32_t CLOUD_RETRY_MS = 4000;
static constexpr uint32_t CLOUD_UPLOAD_PERIOD_MS = 3UL * 60UL * 60UL * 1000UL;
static bool cloudScheduleEnabled = false;
static uint32_t cloudLastScheduleMs = 0;


// ============================
// Wi-Fi UI state
// ============================
static Preferences prefs;
static bool kbShift = false;
static String uiWifiSsid;
static String uiWifiPass;
static bool wifiConnected = false;
static String wifiStatusMsg = "";
static uint32_t wifiLastReconnectTryMs = 0;
static uint8_t wifiReconnectFailCount = 0;
static constexpr uint32_t WIFI_RECONNECT_BASE_MS = 10000;
static constexpr uint32_t WIFI_RECONNECT_MAX_MS = 120000;

static constexpr int WIFI_LIST_MAX = 12;
static String wifiSsids[WIFI_LIST_MAX];
static int wifiCount = 0;
static int wifiPage = 0;

// ============================
// Screen modes
// ============================
enum class Screen : uint8_t {
  MENU,
  WIFI_LIST,
  WIFI_PASS,
  AIRQ_LIST,
  AIRQ_DETAIL,
  METRIC_SELECT,
  ANALYSIS,
  ANALYSIS_SENSORS,
  SETTINGS,
  SLEEP
};
static Screen screen = Screen::MENU;

// ============================
// Ring buffer (avoid SD in callback)
// ============================
static constexpr int QSIZE = 32;
struct QItem { AirQPacket p; };
static volatile uint16_t qHead = 0, qTail = 0;
static QItem q[QSIZE];

static bool qPush(const AirQPacket& p) {
  uint16_t next = (qHead + 1) % QSIZE;
  if (next == qTail) return false;
  q[qHead].p = p;
  qHead = next;
  return true;
}

static bool qPop(AirQPacket& out) {
  if (qTail == qHead) return false;
  out = q[qTail].p;
  qTail = (qTail + 1) % QSIZE;
  return true;
}


// Forward declarations
static void scanLogFiles();
static void goOffDeepSleep();
static void cloudUploadPump();
static void enqueueCloudRow(const AirQPacket& p, const char* label);
static bool wifiConnectSilent(uint32_t timeoutMs = 10000);
static void wifiDisconnectSilent();
static void cloudScheduledUploadTick();
static void wifiAutoReconnectTick();
static void maintainEspNowPriority();

// ============================
// Helpers
// ============================
static void setupZurichTZ() {
  setenv("TZ", "CET-1CEST,M3.5.0/2,M10.5.0/3", 1);
  tzset();
}

static String macToStr(const uint8_t m[6]) {
  char b[18];
  snprintf(b, sizeof(b), "%02X:%02X:%02X:%02X:%02X:%02X",
           m[0], m[1], m[2], m[3], m[4], m[5]);
  return String(b);
}
static String macTail(const uint8_t m[6]) {
  char b[9];
  snprintf(b, sizeof(b), "%02X%02X%02X", m[3], m[4], m[5]);
  return String(b);
}

static int findOrCreateDevice(const uint8_t mac[6]) {
  for (int i = 0; i < MAX_DEVS; i++) {
    if (devs[i].used && memcmp(devs[i].mac, mac, 6) == 0) return i;
  }
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!devs[i].used) {
      devs[i].used = true;
      memcpy(devs[i].mac, mac, 6);
      devs[i].pktCount = 0;
      devs[i].lastSeenMs = 0;
      devs[i].last = AirQPacket{};
      devs[i].label[0] = char('A' + devCount);
      devs[i].label[1] = '\0';
      devCount++;
      return i;
    }
  }
  return -1;
}

static bool isDeviceActiveIdx(int idx) {
  if (idx < 0 || idx >= MAX_DEVS) return false;
  if (!devs[idx].used) return false;
  if (devs[idx].lastSeenMs == 0) return false;
  return (millis() - devs[idx].lastSeenMs) <= DEVICE_STALE_MS;
}

static int firstActiveDevice() {
  for (int i = 0; i < MAX_DEVS; i++) {
    if (isDeviceActiveIdx(i)) return i;
  }
  return -1;
}

static int activeDeviceCount() {
  int c = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (isDeviceActiveIdx(i)) c++;
  }
  return c;
}


static const char* metricLabelFn(SummaryMetric m) {
  switch (m) {
    case SummaryMetric::CO2:  return "CO2";
    case SummaryMetric::PM25: return "PM2.5";
    case SummaryMetric::PM10: return "PM10";
    case SummaryMetric::VOC:  return "VOC";
    case SummaryMetric::NOX:  return "NOx";
    case SummaryMetric::TEMP: return "Temp";
    case SummaryMetric::RH:   return "RH";
  }
  return "";
}

static const char* metricUnitFn(SummaryMetric m) {
  switch (m) {
    case SummaryMetric::CO2:  return "ppm";
    case SummaryMetric::PM25: return "ug/m3";
    case SummaryMetric::PM10: return "ug/m3";
    case SummaryMetric::VOC:  return "idx";
    case SummaryMetric::NOX:  return "idx";
    case SummaryMetric::TEMP: return "C";
    case SummaryMetric::RH:   return "%";
  }
  return "";
}

static String metricValueString(const AirQPacket& p, SummaryMetric m) {
  char buf[16];
  switch (m) {
    case SummaryMetric::CO2:
      if (p.co2_ppm == 0) return "--";
      snprintf(buf, sizeof(buf), "%u", (unsigned)p.co2_ppm);
      return String(buf);
    case SummaryMetric::PM25: {
      float v = x10_to_f(p.pm25_x10);
      if (isnan(v)) return "--";
      snprintf(buf, sizeof(buf), "%.1f", v);
      return String(buf);
    }
    case SummaryMetric::PM10: {
      float v = x10_to_f(p.pm10_x10);
      if (isnan(v)) return "--";
      snprintf(buf, sizeof(buf), "%.1f", v);
      return String(buf);
    }
    case SummaryMetric::VOC: {
      int v = idx_to_i(p.voc_idx);
      if (v < 0) return "--";
      return String(v);
    }
    case SummaryMetric::NOX: {
      int v = idx_to_i(p.nox_idx);
      if (v < 0) return "--";
      return String(v);
    }
    case SummaryMetric::TEMP: {
      float v = x10_to_f(p.tC_x10);
      if (isnan(v)) return "--";
      snprintf(buf, sizeof(buf), "%.1f", v);
      return String(buf);
    }
    case SummaryMetric::RH: {
      float v = x10_to_f(p.rh_x10);
      if (isnan(v)) return "--";
      snprintf(buf, sizeof(buf), "%.1f", v);
      return String(buf);
    }
  }
  return "--";
}

// Hub battery
static float hubBatteryV() {
  float mv = (float)M5.Power.getBatteryVoltage();
  if (mv > 20.0f) return mv / 1000.0f;
  return mv;
}
static int hubBatteryPct() {
  return (int)M5.Power.getBatteryLevel();
}

// Timestamp
static String timestampString() {
  if (hasNtpTime) {
    struct tm t;
    if (getLocalTime(&t, 10)) {
      char buf[32];
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &t);
      return String(buf);
    }
  }
  uint32_t mins = (millis() - hubStartMs) / 60000UL;
  return String("t+") + String(mins) + "m";
}

static uint32_t nowEpoch() {
  if (!hasNtpTime) return 0;
  time_t now;
  time(&now);
  return (uint32_t)now;
}

static int labelIndexFromChar(char c) {
  if (c < 'A' || c > 'P') return -1;
  return c - 'A';
}

static String elapsedString() {
  uint32_t s = (millis() - hubStartMs) / 1000UL;
  return String(s);
}

// ============================

static String makeLogFilePath() {
  if (logMode == LogMode::PER_BOOT) {
    if (bootId == 0) bootId = 1;
    return String("/airq_boot_") + String(bootId) + ".tsv";
  }
  return "/airq_data_log.tsv";
}

// CSV logging (TSV)
// ============================
static void openLogFile() {
  if (!sdOk) return;

  logFilePath = makeLogFilePath();
  if (logFile) logFile.close();

  // Use append mode to avoid truncating existing logs on reopen/remount.
  logFile = SD.open(logFilePath, FILE_APPEND);
  if (!logFile) {
    // Fallback for older cores/filesystems.
    logFile = SD.open(logFilePath, FILE_WRITE);
    if (!logFile) return;
  }

  if (logFile.size() == 0) {
    logFile.println("time_iso	epoch_s	elapsed_s	wifi_sync	label	mac	seq	batt_pct	pm1	pm25	pm4	pm10	tempC	rh	voc	nox	co2");
    logFile.flush();
  }
}

static void appendTsv(const AirQPacket& p, const char* label) {
  if (!sdOk) return;
  if (!logFile) {
    openLogFile();
    if (!logFile) return;
  }

  auto x10 = [](int16_t v)->String {
    if (v == (int16_t)0x7FFF) return "";
    char b[16];
    snprintf(b, sizeof(b), "%.1f", v / 10.0f);
    return String(b);
  };
  auto idx = [](int16_t v)->String {
    if (v == (int16_t)0x7FFF) return "";
    return String((int)v);
  };

  String line;
  line.reserve(240);
  String t = hasNtpTime ? timestampString() : (String("t+") + elapsedString() + "s");
  String ep = hasNtpTime ? String((unsigned long)nowEpoch()) : "";
  line += t; line += "\t";
  line += ep; line += "\t";
  line += elapsedString(); line += "	";
  line += (hasNtpTime ? "1" : "0"); line += "	";
  line += String(label); line += "	";
  line += macToStr(p.srcMac); line += "\t";
  line += String((unsigned long)p.seq); line += "\t";
  line += String((unsigned)p.batt_pct); line += "\t";
  line += x10(p.pm1_x10);  line += "\t";
  line += x10(p.pm25_x10); line += "\t";
  line += x10(p.pm4_x10);  line += "\t";
  line += x10(p.pm10_x10); line += "\t";
  line += x10(p.tC_x10);   line += "\t";
  line += x10(p.rh_x10);   line += "\t";
  line += idx(p.voc_idx);  line += "\t";
  line += idx(p.nox_idx);  line += "\t";
  line += String((unsigned)p.co2_ppm);
  line += "\n";

  size_t wr = logFile.print(line);
  if (wr != line.length()) {
    // Recover from a stale file handle after card remount.
    if (logFile) logFile.close();
    openLogFile();
    if (!logFile) return;
    logFile.print(line);
  }
  logFile.flush();
}

static bool cloudQPush(const String& payload) {
  int next = (cloudHead + 1) % CLOUD_QSIZE;
  if (next == cloudTail) {
    cloudDropped++;
    return false;
  }
  cloudQ[cloudHead] = payload;
  cloudHead = next;
  return true;
}

static bool cloudQPop(String& out) {
  if (cloudTail == cloudHead) return false;
  out = cloudQ[cloudTail];
  cloudQ[cloudTail] = "";
  cloudTail = (cloudTail + 1) % CLOUD_QSIZE;
  return true;
}

static bool cloudQPeek(String& out) {
  if (cloudTail == cloudHead) return false;
  out = cloudQ[cloudTail];
  return true;
}

static String jsonNumOrNull(int16_t raw, bool isX10) {
  if (raw == (int16_t)0x7FFF) return "null";
  if (isX10) {
    char b[24];
    snprintf(b, sizeof(b), "%.1f", raw / 10.0f);
    return String(b);
  }
  return String((int)raw);
}

static void enqueueCloudRow(const AirQPacket& p, const char* label) {
  if (!cloudUploadEnabled) return;
  if (strlen(CLOUD_INGEST_URL) == 0 || strlen(CLOUD_API_KEY) == 0) return;

  String timeIso = hasNtpTime ? timestampString() : (String("t+") + elapsedString() + "s");
  uint32_t ep = hasNtpTime ? nowEpoch() : 0;

  String js;
  js.reserve(320);
  js += "{";
  js += "\"time_iso\":\"" + timeIso + "\",";
  js += "\"epoch_s\":" + String((unsigned long)ep) + ",";
  js += "\"elapsed_s\":" + elapsedString() + ",";
  js += "\"wifi_sync\":" + String(hasNtpTime ? 1 : 0) + ",";
  js += "\"label\":\"" + String(label) + "\",";
  js += "\"mac\":\"" + macToStr(p.srcMac) + "\",";
  js += "\"seq\":" + String((unsigned long)p.seq) + ",";
  js += "\"batt_pct\":" + String((unsigned)p.batt_pct) + ",";
  js += "\"pm1\":" + jsonNumOrNull(p.pm1_x10, true) + ",";
  js += "\"pm25\":" + jsonNumOrNull(p.pm25_x10, true) + ",";
  js += "\"pm4\":" + jsonNumOrNull(p.pm4_x10, true) + ",";
  js += "\"pm10\":" + jsonNumOrNull(p.pm10_x10, true) + ",";
  js += "\"tempC\":" + jsonNumOrNull(p.tC_x10, true) + ",";
  js += "\"rh\":" + jsonNumOrNull(p.rh_x10, true) + ",";
  js += "\"voc\":" + jsonNumOrNull(p.voc_idx, false) + ",";
  js += "\"nox\":" + jsonNumOrNull(p.nox_idx, false) + ",";
  js += "\"co2\":" + String((unsigned)p.co2_ppm);
  js += "}";

  cloudQPush(js);
}

static void cloudUploadPump() {
  if (!cloudUploadEnabled) return;
  if (strlen(CLOUD_INGEST_URL) == 0 || strlen(CLOUD_API_KEY) == 0) return;
  if (!wifiConnected || WiFi.status() != WL_CONNECTED) return;
  if (cloudTail == cloudHead) return;

  uint32_t nowMs = millis();
  if (nowMs - cloudLastTryMs < CLOUD_RETRY_MS) return;
  cloudLastTryMs = nowMs;

  String payload;
  if (!cloudQPeek(payload)) return;

  WiFiClientSecure client;
  client.setInsecure();  // Use proper CA pinning in production
  HTTPClient http;
  if (!http.begin(client, CLOUD_INGEST_URL)) {
    cloudFail++;
    return;
  }

  http.setConnectTimeout(2500);
  http.setTimeout(3000);
  http.addHeader("Content-Type", "application/json");
  http.addHeader("X-API-Key", CLOUD_API_KEY);

  int code = http.POST(payload);
  http.end();

  if (code >= 200 && code < 300) {
    String dropped;
    cloudQPop(dropped);
    cloudSent++;
  } else {
    cloudFail++;
  }
}

// ============================
// ESP-NOW callback (IDF v5)
// ============================
static void onRecv(const esp_now_recv_info_t* info, const uint8_t* data, int len) {
  (void)info;
  if (!data) return;
  if (len != (int)sizeof(AirQPacket)) return;

  AirQPacket p;
  memcpy(&p, data, sizeof(p));
  if (p.ver != 1) return;

  lastEspNowRxMs = millis();
  qPush(p);
}

// ============================
// NTP once
// ============================
static void tryNtpOnce() {
  if (strlen(WIFI_SSID) == 0) return;

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 8000) delay(100);

  if (WiFi.status() == WL_CONNECTED) {
    setupZurichTZ();
    configTime(0, 0, "pool.ntp.org", "time.google.com");
    struct tm t;
    if (getLocalTime(&t, 5000) && (t.tm_year + 1900) >= 2023) hasNtpTime = true;
    else hasNtpTime = false;
  }
  WiFi.disconnect(true, true);
}

// ============================
// SD helper
// ============================
static void refreshSdState() {
  if (sdOk) {
    bool ok = (SD.cardType() != CARD_NONE);
    if (ok) {
      File root = SD.open("/");
      ok = (bool)root;
      if (root) root.close();
    }
    if (!ok) {
      sdOk = false;
      if (logFile) logFile.close();
      logFileCount = 0;
      logFileIdx = 0;
      logFilePath = "";
    } else if (!logFile) {
      openLogFile();
    }
    return;
  }

  if (SD.begin()) {
    if (SD.cardType() != CARD_NONE) {
      sdOk = true;
      openLogFile();
      scanLogFiles();
    }
  }
}

// ============================
// ESP-NOW start/stop
// ============================
static void espnowStart() {
  WiFi.mode(WIFI_STA);
  if (!wifiConnected) {
    WiFi.disconnect(true, true);
  }
  if (!espNowActive) {
    if (esp_now_init() == ESP_OK) {
      espNowActive = true;
      esp_now_register_recv_cb(onRecv);
    }
  }
}
static void espnowStop() {
  if (espNowActive) {
    esp_now_deinit();
    espNowActive = false;
  }
  WiFi.disconnect(true, true);
}

// ============================
// UI helpers
// ============================
static bool aggressiveRefresh = true;
static constexpr uint32_t DEEP_CLEAN_EVERY = 5;
static uint32_t fullDrawCount = 0;
static bool deepCleanPending = false;

static void present() {
  if (aggressiveRefresh) {
    M5.Display.display();
    M5.Display.display();
    if (deepCleanPending) {
      M5.Display.display();
      deepCleanPending = false;
    }
  } else {
    M5.Display.display();
  }
}

static void setEpdQuality() {
#ifdef M5GFX_USE_EPD
  M5.Display.setEpdMode(epd_mode_t::EPD_QUALITY);
#endif
}

static Rect topBarRect{0, 0, 540, 90};
static Rect menuAirQ{40, 140, 200, 200};
static Rect menuWifi{300, 140, 200, 200};
static Rect menuAnalysis{40, 380, 200, 200};
static Rect menuSleep{300, 380, 200, 200};
static Rect menuOff{40, 620, 200, 200};
static Rect menuSettings{300, 620, 200, 200};

static Rect listBackBtn{20, 870, 140, 70};
static Rect listMetricBtn{180, 870, 180, 70};
static Rect listMenuBtn{380, 870, 140, 70};

static Rect detailBackBtn{20, 870, 140, 70};
static Rect detailPrevBtn{180, 870, 180, 70};
static Rect detailNextBtn{380, 870, 140, 70};

static Rect wifiBackBtn{20, 870, 140, 70};
static Rect wifiUploadBtn{200, 870, 140, 70};
static Rect wifiScanBtn{380, 870, 140, 70};

static Rect passCancelBtn{20, 870, 180, 70};
static Rect passOkBtn{340, 870, 180, 70};
static Rect passShiftBtn{430, 160, 90, 60};

static Rect metricDoneBtn{340, 870, 180, 70};
static Rect metricBackBtn{20, 870, 180, 70};

static Rect analysisBackBtn{20, 870, 110, 70};
static Rect analysisMetricBtn{140, 870, 110, 70};
static Rect analysisZoomBtn{260, 870, 110, 70};
static Rect analysisSensorsBtn{380, 870, 140, 70};
static Rect settingsBackBtn{20, 870, 140, 70};
static Rect settingsModeRow{20, 660, 500, 60};
static Rect settingsRefreshRow{20, 740, 500, 60};

static void fullClear() {
  if (aggressiveRefresh) {
    fullDrawCount++;
    deepCleanPending = (DEEP_CLEAN_EVERY > 0) && (fullDrawCount % DEEP_CLEAN_EVERY == 0);
    if (deepCleanPending) {
      M5.Display.fillScreen(TFT_BLACK);
      M5.Display.display();
      M5.Display.fillScreen(TFT_WHITE);
      M5.Display.display();
    }
    M5.Display.fillScreen(TFT_BLACK);
    M5.Display.display();
    M5.Display.fillScreen(TFT_WHITE);
    M5.Display.display();
  } else {
    M5.Display.fillScreen(TFT_WHITE);
  }
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
}

static void setScreen(Screen s) {
  if (screen != s) {
    screen = s;
  }
}

static void clearRect(int x, int y, int w, int h) {
  M5.Display.fillRect(x, y, w, h, TFT_WHITE);
  if (aggressiveRefresh) {
    M5.Display.display();
  }
}

static void drawTopBar(const char* title) {
  clearRect(0, 0, 540, 90);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 20);
  M5.Display.print(title);

  int bp = hubBatteryPct();
  float bv = hubBatteryV();
  String bat = "";
  bat += (bp >= 0 ? String(bp) + "%" : String("--%"));
  if (bv > 0.5f) bat += " " + String(bv, 2) + "V";

  int wBat = M5.Display.textWidth(bat);
  int batX = 540 - 20 - wBat;
  M5.Display.setCursor(batX, 20);
  M5.Display.print(bat);

  bool wifiNow = (WiFi.status() == WL_CONNECTED);
  wifiConnected = wifiNow;
  String sdLabel = sdOk ? "SD" : "noSD";
  String wifiLabel = wifiNow ? "WiFi" : "noWiFi";
  String status = sdLabel + " " + wifiLabel;
  if (strlen(CLOUD_API_KEY) > 0) {
    status += " CL:" + String(cloudSent);
    if (cloudHead != cloudTail) status += "(+" + String((cloudHead - cloudTail + CLOUD_QSIZE) % CLOUD_QSIZE) + ")";
  }
  int statusW = M5.Display.textWidth(status);
  int statusX = batX - 12 - statusW;
  if (statusX < 20) statusX = 20;
  M5.Display.setCursor(statusX, 20);
  M5.Display.print(status);
}

static void drawButton(const Rect& r, const char* label, bool filled) {
  uint32_t bg = filled ? TFT_BLACK : TFT_WHITE;
  uint32_t fg = filled ? TFT_WHITE : TFT_BLACK;
  M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 14, bg);
  M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 14, TFT_BLACK);
  M5.Display.setTextColor(fg, bg);
  M5.Display.setTextSize(2);
  int tw = M5.Display.textWidth(label);
  int tx = r.x + (r.w - tw) / 2;
  int ty = r.y + (r.h / 2) - 8;
  M5.Display.setCursor(tx, ty);
  M5.Display.print(label);
}

static bool drawIconFromSd(const char* path, int x, int y, int maxW, int maxH) {
  if (!sdOk) return false;
  File f = SD.open(path, FILE_READ);
  if (!f) return false;
  size_t sz = f.size();
  if (sz == 0 || sz > 200000) { f.close(); return false; }
  uint8_t* buf = (uint8_t*)malloc(sz);
  if (!buf) { f.close(); return false; }
  size_t rd = f.read(buf, sz);
  f.close();
  if (rd != sz) { free(buf); return false; }
  bool ok = M5.Display.drawPng(buf, sz, x, y, maxW, maxH);
  free(buf);
  return ok;
}

static void drawMenuTile(const Rect& r, const char* label, const char* iconPath) {
  M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 14, TFT_WHITE);
  M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 14, TFT_BLACK);

  int iconW = 102;
  int iconH = 102;
  int ix = r.x + (r.w - iconW) / 2;
  int iy = r.y + (r.h - iconH) / 2;
  bool iconOk = drawIconFromSd(iconPath, ix, iy, iconW, iconH);

  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);
  int tw = M5.Display.textWidth(label);
  int ty = iconOk ? (r.y + r.h - 28) : (r.y + (r.h / 2) - 8);
  M5.Display.setCursor(r.x + (r.w - tw) / 2, ty);
  M5.Display.print(label);
}

static void drawMenu() {
  setEpdQuality();
  fullClear();
  drawTopBar("AirQ_M Logger");

  drawMenuTile(menuAirQ, "AirQ", "/icon/airq_icon.png");
  drawMenuTile(menuWifi, "WiFi", "/icon/wifi_icon.png");
  drawMenuTile(menuAnalysis, "Analysis", "/icon/graph_icon.png");
  drawMenuTile(menuSleep, "Sleep", "/icon/sleepmode_icon.png");
  drawMenuTile(menuOff, "OFF", "/icon/off_icon.png");
  drawMenuTile(menuSettings, "Settings", "/icon/setting_icon.png");

  present();
}

static void drawSleepScreen() {
  setEpdQuality();
  fullClear();
  M5.Display.setTextSize(3);
  M5.Display.setCursor(190, 420);
  M5.Display.print("SLEEP");
  M5.Display.setTextSize(2);
  M5.Display.setCursor(120, 480);
  M5.Display.print("Tap to wake");
  present();
}

static void drawListFooter() {
  clearRect(0, 860, 540, 120);
  drawButton(listBackBtn, "Back", false);
  drawButton(listMetricBtn, "Metrics", false);
  drawButton(listMenuBtn, "Menu", false);
}

static void drawListHeader() {
  drawTopBar("AirQ Sensors");
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 70);
  M5.Display.print("Metrics: ");
  for (int i = 0; i < listMetricCount; i++) {
    if (i > 0) M5.Display.print(", ");
    M5.Display.print(metricLabelFn(listMetrics[i]));
  }
}

static Rect listRowRect(int row) {
  int y0 = 120;
  int rowH = 110;
  return Rect{20, y0 + row * rowH, 500, 100};
}

static void drawListRow(int row, int devIdx, bool selected) {
  Rect r = listRowRect(row);
  uint32_t bg = selected ? TFT_BLACK : TFT_WHITE;
  uint32_t fg = selected ? TFT_WHITE : TFT_BLACK;

  M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 12, bg);
  M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 12, selected ? TFT_WHITE : TFT_BLACK);
  M5.Display.setTextColor(fg, bg);

  M5.Display.setTextSize(2);
  M5.Display.setCursor(r.x + 16, r.y + 10);
  M5.Display.printf("%s  %s  Bat:%u%%", devs[devIdx].label, macTail(devs[devIdx].mac).c_str(), (unsigned)devs[devIdx].last.batt_pct);

  uint32_t ageS = devs[devIdx].lastSeenMs ? (millis() - devs[devIdx].lastSeenMs) / 1000UL : 9999;

  int valY = r.y + 42;
  int boxW = 150;
  int boxH = 44;
  int gap = 10;

  for (int i = 0; i < listMetricCount; i++) {
    Rect box{r.x + 16 + i * (boxW + gap), valY, boxW, boxH};
    M5.Display.fillRoundRect(box.x, box.y, box.w, box.h, 8, bg);
    M5.Display.drawRoundRect(box.x, box.y, box.w, box.h, 8, fg);

    String mval = metricValueString(devs[devIdx].last, listMetrics[i]);
    M5.Display.setTextSize(2);
    M5.Display.setCursor(box.x + 8, box.y + 8);
    M5.Display.print(mval);
    M5.Display.setTextSize(1);
    M5.Display.setCursor(box.x + 70, box.y + 14);
    M5.Display.print(metricLabelFn(listMetrics[i]));
  }

  String age = String(ageS) + "s";
  M5.Display.setTextSize(1);
  int w = M5.Display.textWidth(age);
  M5.Display.setCursor(r.x + r.w - 16 - w, r.y + 12);
  M5.Display.print(age);
}

static void drawList() {
  setEpdQuality();
  fullClear();
  drawListHeader();

  if (!isDeviceActiveIdx(selectedIdx)) {
    int first = firstActiveDevice();
    if (first >= 0) selectedIdx = first;
  }

  int row = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!isDeviceActiveIdx(i)) continue;
    bool sel = (i == selectedIdx);
    drawListRow(row, i, sel);
    row++;
    if (row >= 6) break;
  }

  if (row == 0) {
    M5.Display.setTextSize(2);
    M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
    M5.Display.setCursor(40, 360);
    M5.Display.print("No active ESP-NOW sensors");
  }

  drawListFooter();
  present();
}

static void updateListDirtyRows() {
  if (!isDeviceActiveIdx(selectedIdx)) {
    drawList();
    return;
  }
  setEpdQuality();
  int row = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!isDeviceActiveIdx(i)) continue;
    if (devDirty[i]) {
      bool sel = (i == selectedIdx);
      drawListRow(row, i, sel);
      devDirty[i] = false;
    }
    row++;
    if (row >= 6) break;
  }
  present();
}

static void drawDetailHeader(const DeviceState& d) {
  drawTopBar("Sensor Detail");

  String macs = macToStr(d.mac);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 70);
  M5.Display.printf("Sensor %s", d.label);

  int w = M5.Display.textWidth(macs);
  M5.Display.setCursor(540 - 20 - w, 70);
  M5.Display.print(macs);
}

static Rect detailCard(int col, int row) {
  int x0 = 20;
  int y0 = 140;
  int w = 240;
  int h = 150;
  int gapX = 20;
  int gapY = 20;
  return Rect{x0 + col * (w + gapX), y0 + row * (h + gapY), w, h};
}

static void drawDetailCard(const Rect& r, const char* label, const String& value, const char* unit) {
  M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 12, TFT_WHITE);
  M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 12, TFT_BLACK);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(r.x + 16, r.y + 12);
  M5.Display.print(label);
  M5.Display.setTextSize(3);
  M5.Display.setCursor(r.x + 16, r.y + 60);
  M5.Display.print(value);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(r.x + 16, r.y + 110);
  M5.Display.print(unit);
}

static void drawDetail(const DeviceState& d) {
  setEpdQuality();
  fullClear();
  drawDetailHeader(d);

  auto f1 = [](int16_t v)->String {
    if (isMissing(v)) return "--";
    char b[16]; snprintf(b, sizeof(b), "%.1f", v / 10.0f);
    return String(b);
  };
  auto idx = [](int16_t v)->String {
    int r = idx_to_i(v);
    return (r < 0) ? String("--") : String(r);
  };

  String co2 = d.last.co2_ppm ? String((unsigned)d.last.co2_ppm) : String("--");

  drawDetailCard(detailCard(0, 0), "PM1",  f1(d.last.pm1_x10),  "ug/m3");
  drawDetailCard(detailCard(1, 0), "PM2.5",f1(d.last.pm25_x10), "ug/m3");
  drawDetailCard(detailCard(0, 1), "PM10", f1(d.last.pm10_x10), "ug/m3");
  drawDetailCard(detailCard(1, 1), "Temp", f1(d.last.tC_x10),   "C");
  drawDetailCard(detailCard(0, 2), "RH",   f1(d.last.rh_x10),   "%");
  drawDetailCard(detailCard(1, 2), "CO2",  co2,                "ppm");
  drawDetailCard(detailCard(0, 3), "VOC",  idx(d.last.voc_idx), "idx");
  drawDetailCard(detailCard(1, 3), "NOx",  idx(d.last.nox_idx), "idx");

  clearRect(0, 860, 540, 120);
  drawButton(detailBackBtn, "Back", false);
  drawButton(detailPrevBtn, "Prev", false);
  drawButton(detailNextBtn, "Next", false);

  present();
}

static void drawMetricSelect() {
  setEpdQuality();
  fullClear();
  drawTopBar("Select Metrics (max 3)");

  SummaryMetric allMetrics[7] = {
    SummaryMetric::CO2,
    SummaryMetric::PM25,
    SummaryMetric::PM10,
    SummaryMetric::VOC,
    SummaryMetric::NOX,
    SummaryMetric::TEMP,
    SummaryMetric::RH
  };

  int y0 = 140;
  int rowH = 90;
  for (int i = 0; i < 7; i++) {
    Rect r{20, y0 + i * rowH, 500, 70};
    M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 10, TFT_WHITE);
    M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 10, TFT_BLACK);
    M5.Display.setTextSize(2);
    M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
    M5.Display.setCursor(r.x + 20, r.y + 20);
    M5.Display.print(metricLabelFn(allMetrics[i]));

    bool selected = false;
    for (int k = 0; k < listMetricCount; k++) {
      if (listMetrics[k] == allMetrics[i]) selected = true;
    }
    if (selected) {
      M5.Display.setTextSize(2);
      M5.Display.setCursor(r.x + r.w - 80, r.y + 20);
      M5.Display.print("ON");
    }
  }

  clearRect(0, 860, 540, 120);
  clearRect(0, 860, 540, 120);
  drawButton(metricBackBtn, "Back", false);
  drawButton(metricDoneBtn, "Done", false);
  present();
}


static const uint32_t intervalOptionsMs[] = {60000, 180000, 300000, 600000, 900000, 1800000, 3600000};
static const char* intervalOptionsLabel[] = {"1m", "3m", "5m", "10m", "15m", "30m", "1h"};

static void drawSettings() {
  setEpdQuality();
  fullClear();
  drawTopBar("Settings");

  M5.Display.setTextSize(2);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setCursor(20, 110);
  M5.Display.print("Log Interval");

  int y0 = 160;
  int rowH = 70;
  for (int i = 0; i < 7; i++) {
    Rect r{20, y0 + i * rowH, 500, 60};
    M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 10, TFT_WHITE);
    M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 10, TFT_BLACK);
    M5.Display.setTextSize(2);
    M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
    M5.Display.setCursor(r.x + 20, r.y + 18);
    M5.Display.print(intervalOptionsLabel[i]);
    if (logIntervalMs == intervalOptionsMs[i]) {
      M5.Display.setCursor(r.x + r.w - 80, r.y + 18);
      M5.Display.print("ON");
    }
  }

  // Log mode toggle
  M5.Display.fillRoundRect(settingsModeRow.x, settingsModeRow.y, settingsModeRow.w, settingsModeRow.h, 10, TFT_WHITE);
  M5.Display.drawRoundRect(settingsModeRow.x, settingsModeRow.y, settingsModeRow.w, settingsModeRow.h, 10, TFT_BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setCursor(settingsModeRow.x + 20, settingsModeRow.y + 18);
  M5.Display.print("Log Mode");
  M5.Display.setCursor(settingsModeRow.x + 300, settingsModeRow.y + 18);
  M5.Display.print(logMode == LogMode::CONTINUOUS ? "Single" : "Per Boot");

  M5.Display.fillRoundRect(settingsRefreshRow.x, settingsRefreshRow.y, settingsRefreshRow.w, settingsRefreshRow.h, 10, TFT_WHITE);
  M5.Display.drawRoundRect(settingsRefreshRow.x, settingsRefreshRow.y, settingsRefreshRow.w, settingsRefreshRow.h, 10, TFT_BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setCursor(settingsRefreshRow.x + 20, settingsRefreshRow.y + 18);
  M5.Display.print("Cloud Net");
  M5.Display.setCursor(settingsRefreshRow.x + 260, settingsRefreshRow.y + 18);
  M5.Display.print(cloudScheduleEnabled ? "Burst 3h" : "Always On");

  clearRect(0, 860, 540, 120);
  drawButton(settingsBackBtn, "Back", false);
  present();
}


static void scanLogFiles() {
  logFileCount = 0;
  logFileIdx = 0;
  if (!sdOk) return;
  File root = SD.open("/");
  if (!root) return;
  File f = root.openNextFile();
  while (f) {
    String name = String(f.name());
    if (!name.startsWith("/")) name = "/" + name;
    if (name.startsWith("/.") || name.startsWith("/._")) {
      f = root.openNextFile();
      continue;
    }
    if (!f.isDirectory() && name.endsWith(".tsv")) {
      if (logFileCount < 16) {
        logFiles[logFileCount++] = name;
      }
    }
    f = root.openNextFile();
  }
  // Prefer main continuous log when present
  for (int i = 0; i < logFileCount; i++) {
    if (logFiles[i].endsWith("/airq_data_log.tsv")) { logFileIdx = i; break; }
  }
  root.close();
}

static void selectNextLogFile() {
  if (logFileCount == 0) return;
  logFileIdx = (logFileIdx + 1) % logFileCount;
}

static String currentLogFile() {
  if (logFileCount == 0) return logFilePath;
  return logFiles[logFileIdx];
}

// ============================
// Analysis
// ============================
static uint32_t zoomSeconds() {
  switch (analysisZoom) {
    case ZoomRange::ZOOM_10M: return 10 * 60;
    case ZoomRange::ZOOM_1H:  return 60 * 60;
    case ZoomRange::ZOOM_1D:  return 24 * 60 * 60;
    case ZoomRange::ZOOM_ALL: return 0;
  }
  return 0;
}

static const char* zoomLabel() {
  switch (analysisZoom) {
    case ZoomRange::ZOOM_10M: return "10m";
    case ZoomRange::ZOOM_1H:  return "1h";
    case ZoomRange::ZOOM_1D:  return "1d";
    case ZoomRange::ZOOM_ALL: return "All";
  }
  return "";
}

static int analysisSelectedCount() {
  int c = 0;
  for (int i = 0; i < MAX_DEVS; i++) if (analysisSel[i]) c++;
  return c;
}

static void nextZoom() {
  analysisZoom = (ZoomRange)((((int)analysisZoom) + 1) % 4);
}

static bool scanMaxX(uint32_t& maxX, bool& useEpoch) {
  maxX = 0;
  useEpoch = false;
  if (!sdOk || !logFilePath.length()) return false;
  File f = SD.open(logFilePath, FILE_READ);
  if (!f) return false;

  int lineIdx = -1;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() == 0) continue;
    lineIdx++;
    if (lineIdx == 0) continue;

    int col = 0;
    int start = 0;
    uint32_t epoch = 0;
    uint32_t elapsed = 0;

    for (int i = 0; i <= line.length(); i++) {
      if (i == line.length() || line[i] == '\t') {
        String token = line.substring(start, i);
        if (col == 1 && token.length()) epoch = (uint32_t)token.toInt();
        if (col == 2 && token.length()) elapsed = (uint32_t)token.toInt();
        col++;
        start = i + 1;
      }
    }

    if (epoch > 0) {
      useEpoch = true;
      if (epoch > maxX) maxX = epoch;
    } else {
      if (elapsed > maxX) maxX = elapsed;
    }
  }

  f.close();
  return maxX > 0;
}

static bool readAnalysisData(float* xs, float* ys, uint8_t* sids, int maxPts, int& count, bool& useEpoch, uint32_t windowSec) {
  count = 0;
  useEpoch = false;
  if (!sdOk || !logFilePath.length()) return false;

  uint32_t maxX = 0;
  if (!scanMaxX(maxX, useEpoch)) return false;
  uint32_t startX = (windowSec > 0 && maxX > windowSec) ? (maxX - windowSec) : 0;

  File f = SD.open(logFilePath, FILE_READ);
  if (!f) return false;

  bool hasWifiSync = false;
  String header = "";
  if (f.available()) {
    header = f.readStringUntil('\n');

    if (header.indexOf("wifi_sync") >= 0) hasWifiSync = true;
  }

  int totalLines = 0;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() > 0) totalLines++;
  }
  f.close();

  int skip = (totalLines > maxPts) ? (totalLines / maxPts) : 1;

  f = SD.open(logFilePath, FILE_READ);
  if (!f) return false;

  int lineIdx = -1;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() == 0) continue;
    lineIdx++;
    if (lineIdx == 0) continue;
    if (skip > 1 && (lineIdx % skip) != 0) continue;

    int col = 0;
    int start = 0;
    uint32_t epoch = 0;
    float elapsed = 0;
    String labelStr;
    String valStr;
    // If row is missing wifi_sync column, adjust indices on the fly
    int colCount = 1;
    for (int i = 0; i < line.length(); i++) if (line[i] == "	"[0]) colCount++;
    bool rowHasWifiSync = hasWifiSync && (colCount >= 17);
    int labelIdx = rowHasWifiSync ? 4 : 3;
    int pm25Idx = rowHasWifiSync ? 9 : 8;
    int pm10Idx = rowHasWifiSync ? 11 : 10;
    int tempIdx = rowHasWifiSync ? 12 : 11;
    int rhIdx = rowHasWifiSync ? 13 : 12;
    int vocIdx = rowHasWifiSync ? 14 : 13;
    int noxIdx = rowHasWifiSync ? 15 : 14;
    int co2Idx = rowHasWifiSync ? 16 : 15;

    for (int i = 0; i <= line.length(); i++) {
      if (i == line.length() || line[i] == '\t') {
        String token = line.substring(start, i);
        if (col == 1) { if (token.length()) epoch = (uint32_t)token.toInt(); }
        if (col == 2) { if (token.length()) elapsed = token.toFloat(); }
        if (col == labelIdx) { if (token.length()) labelStr = token; }
        if ((analysisMetric == SummaryMetric::CO2 && col == co2Idx) ||
            (analysisMetric == SummaryMetric::PM25 && col == pm25Idx) ||
            (analysisMetric == SummaryMetric::PM10 && col == pm10Idx) ||
            (analysisMetric == SummaryMetric::VOC && col == vocIdx) ||
            (analysisMetric == SummaryMetric::NOX && col == noxIdx) ||
            (analysisMetric == SummaryMetric::TEMP && col == tempIdx) ||
            (analysisMetric == SummaryMetric::RH && col == rhIdx) ||
            (analysisMetric == SummaryMetric::CO2 && col == co2Idx)) {
          valStr = token;
        }
        col++;
        start = i + 1;
      }
    }

    float x = (epoch > 0) ? (float)epoch : elapsed;
    if (windowSec > 0 && x < (float)startX) continue;
    if (valStr.length() == 0) continue;

    int sid = -1;
    if (labelStr.length()) sid = labelIndexFromChar(labelStr[0]);
    if (sid < 0 || sid >= MAX_DEVS) continue;
    if (!analysisSel[sid]) continue;

    float val = valStr.toFloat();
    if (count < maxPts) {
      xs[count] = x;
      ys[count] = val;
      sids[count] = (uint8_t)sid;
      count++;
    }
  }

  f.close();
  return count > 0;
}

static void formatTimeLabel(char* out, size_t outSize, bool useEpoch, float value, float spanSec) {
  if (useEpoch) {
    time_t t = (time_t)((uint32_t)value);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(out, outSize, "%H:%M", &tmv);
  } else {
    int s = (int)value;
    if (spanSec >= 3600.0f) {
      int h = s / 3600;
      int m = (s % 3600) / 60;
      snprintf(out, outSize, "%dh%02d", h, m);
    } else {
      int m = s / 60;
      snprintf(out, outSize, "%dm", m);
    }
  }
}

static void drawAnalysis() {
  setEpdQuality();
  fullClear();
  drawTopBar("Analysis");

  if (!sdOk) {
    refreshSdState();
  }
  String useFile = logFilePath.length() ? logFilePath : String("/airq_data_log.tsv");
  if (!sdOk || useFile.length() == 0) {
    M5.Display.setTextSize(2);
    M5.Display.setCursor(40, 380);
    M5.Display.print("No SD card / no data");
    clearRect(0, 860, 540, 120);
  drawButton(analysisBackBtn, "Back", false);
    drawButton(analysisSensorsBtn, "Sensors", false);
    present();
    return;
  }

  static float xs[200];
  static float ys[200];
  static uint8_t sids[200];

  if (analysisSelectedCount() == 0) {
    M5.Display.setTextSize(2);
    M5.Display.setCursor(40, 380);
    M5.Display.print("Select sensors to plot");
    clearRect(0, 860, 540, 120);
  drawButton(analysisBackBtn, "Back", false);
    drawButton(analysisSensorsBtn, "Sensors", false);
    present();
    return;
  }

  int count = 0;
  bool useEpoch = false;
  uint32_t win = zoomSeconds();
  String oldPath = logFilePath;
  logFilePath = useFile;
  bool ok = readAnalysisData(xs, ys, sids, 200, count, useEpoch, win);
  logFilePath = oldPath;
  if (!ok) {
    M5.Display.setTextSize(2);
    M5.Display.setCursor(40, 380);
    M5.Display.print("No data to plot");
    clearRect(0, 860, 540, 120);
  drawButton(analysisBackBtn, "Back", false);
    drawButton(analysisSensorsBtn, "Sensors", false);
    present();
    return;
  }

  float minX = xs[0], maxX = xs[0];
  float minY = ys[0], maxY = ys[0];
  for (int i = 1; i < count; i++) {
    if (xs[i] < minX) minX = xs[i];
    if (xs[i] > maxX) maxX = xs[i];
    if (ys[i] < minY) minY = ys[i];
    if (ys[i] > maxY) maxY = ys[i];
  }
  if (maxY - minY < 1e-3f) { maxY = minY + 1.0f; }

  float span = maxX - minX;
  Rect plot{70, 140, 440, 640};
  float sum = 0.0f;
  int nsum = 0;
  for (int i = 0; i < count; i++) {
    sum += ys[i];
    nsum++;
  }
  float avg = (nsum > 0) ? (sum / nsum) : 0.0f;

  M5.Display.setTextSize(2);
  char avgBuf[60];
  snprintf(avgBuf, sizeof(avgBuf), "Avg: %.2f   Min: %.2f   Max: %.2f", avg, minY, maxY);
  int avgW = M5.Display.textWidth(avgBuf);
  int avgY = plot.y + plot.h + 28;
  M5.Display.setCursor((540 - avgW) / 2, avgY);
  M5.Display.print(avgBuf);

  M5.Display.drawRect(plot.x, plot.y, plot.w, plot.h, TFT_BLACK);

  M5.Display.setTextSize(1);
  M5.Display.setCursor(20, 100);
  M5.Display.print("Sensors: ");
  int shown = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (analysisSel[i]) {
      if (shown > 0) M5.Display.print(",");
      char c = 'A' + i;
      M5.Display.print(c);
      shown++;
      if (shown >= 10) break;
    }
  }

  M5.Display.setTextSize(1);
  M5.Display.setCursor(20, 120);
  M5.Display.print(metricLabelFn(analysisMetric));
  M5.Display.print(" (");
  M5.Display.print(metricUnitFn(analysisMetric));
  M5.Display.print(")");

  int ticks = 5;
  for (int i = 0; i <= ticks; i++) {
    int y = plot.y + plot.h - (plot.h * i / ticks);
    M5.Display.drawLine(plot.x - 4, y, plot.x, y, TFT_BLACK);
    float v = minY + (maxY - minY) * i / ticks;
    M5.Display.setTextSize(1);
    M5.Display.setCursor(8, y - 4);
    M5.Display.printf("%.1f", v);
  }

    for (int i = 0; i <= ticks; i++) {
    int x = plot.x + (plot.w * i / ticks);
    M5.Display.drawLine(x, plot.y + plot.h, x, plot.y + plot.h + 4, TFT_BLACK);
    float v = minX + (maxX - minX) * i / ticks;
    char buf[16];
    formatTimeLabel(buf, sizeof(buf), useEpoch, v, span);
    M5.Display.setTextSize(1);
    int tw = M5.Display.textWidth(buf);
    M5.Display.setCursor(x - tw / 2, plot.y + plot.h + 8);
    M5.Display.print(buf);
  }

  int lastX[MAX_DEVS];
  int lastY[MAX_DEVS];
  bool lastOk[MAX_DEVS];
  for (int i = 0; i < MAX_DEVS; i++) lastOk[i] = false;

  for (int i = 0; i < count; i++) {
    int x = plot.x + (int)((xs[i] - minX) / (maxX - minX) * plot.w);
    int y = plot.y + plot.h - (int)((ys[i] - minY) / (maxY - minY) * plot.h);
    int sid = sids[i];
    if (sid >= 0 && sid < MAX_DEVS) {
      if (lastOk[sid]) {
        M5.Display.drawLine(lastX[sid], lastY[sid], x, y, TFT_BLACK);
      }
      lastX[sid] = x;
      lastY[sid] = y;
      lastOk[sid] = true;
    }
  }

  clearRect(0, 860, 540, 120);
  drawButton(analysisBackBtn, "Back", false);
  drawButton(analysisMetricBtn, "Metric", false);
  drawButton(analysisZoomBtn, zoomLabel(), false);
  drawButton(analysisSensorsBtn, "Sensors", false);
  present();
}


static void drawAnalysisSensors() {
  setEpdQuality();
  fullClear();
  drawTopBar("Select Sensors (max 10)");

  int y0 = 140;
  int rowH = 120;
  int shown = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!devs[i].used) continue;
    Rect r{20, y0 + shown * rowH, 500, 100};
    M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 10, TFT_WHITE);
    M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 10, TFT_BLACK);
    M5.Display.setTextSize(2);
    M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
    char c = 'A' + i;
    M5.Display.setCursor(r.x + 20, r.y + 18);
    M5.Display.printf("%c  %s", c, macTail(devs[i].mac).c_str());
    if (analysisSel[i]) {
      M5.Display.setCursor(r.x + r.w - 70, r.y + 18);
      M5.Display.print("ON");
    }
    shown++;
    if (shown >= 10) break;
  }

  M5.Display.setTextSize(1);
  M5.Display.setCursor(20, 820);
  M5.Display.print("Selected: ");
  M5.Display.print(analysisSelectedCount());
  M5.Display.print(" / 10");

  clearRect(0, 860, 540, 120);
  clearRect(0, 860, 540, 120);
  drawButton(metricBackBtn, "Back", false);
  drawButton(metricDoneBtn, "Done", false);
  present();
}

// ============================
// Wi-Fi screens
// ============================
static int keyWForLen(int len, int margin = 10, int gap = 6) {
  return (540 - 2 * margin - gap * (len - 1)) / len;
}

static int keyStartX(int len, int keyW, int margin = 10, int gap = 6) {
  int total = len * keyW + gap * (len - 1);
  return (540 - total) / 2;
}

static void drawWifiList() {
  setEpdQuality();
  fullClear();
  drawTopBar("WiFi Network");

  int y = 120;
  int rowH = 70;
  for (int i = 0; i < wifiCount; i++) {
    int idx = wifiPage * WIFI_LIST_MAX + i;
    if (idx >= wifiCount) break;
    Rect r{20, y, 500, rowH - 10};
    M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 10, TFT_WHITE);
    M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 10, TFT_BLACK);
    M5.Display.setTextSize(2);
    M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
    M5.Display.setCursor(r.x + 16, r.y + 18);
    M5.Display.print(wifiSsids[idx]);
    y += rowH;
    if (y > 820) break;
  }

  M5.Display.setTextSize(1);
  M5.Display.setCursor(20, 808);
  M5.Display.print("Cloud mode: ");
  M5.Display.print(cloudScheduleEnabled ? "Burst 3h" : "Always On");
  if (wifiStatusMsg.length()) {
    M5.Display.setCursor(20, 828);
    M5.Display.print(wifiStatusMsg);
  }

  clearRect(0, 860, 540, 120);
  drawButton(wifiBackBtn, "Back", false);
  drawButton(wifiUploadBtn, "Upload", false);
  drawButton(wifiScanBtn, "Scan", false);
  present();
}

static void drawWifiPass() {
  setEpdQuality();
  fullClear();
  drawTopBar("WiFi Password");

  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 110);
  M5.Display.print("SSID:");
  M5.Display.setCursor(100, 110);
  M5.Display.print(uiWifiSsid);

  Rect box{20, 160, 390, 60};
  M5.Display.fillRoundRect(box.x, box.y, box.w, box.h, 8, TFT_WHITE);
  M5.Display.drawRoundRect(box.x, box.y, box.w, box.h, 8, TFT_BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(30, 180);
  M5.Display.print(uiWifiPass);

  M5.Display.fillRoundRect(passShiftBtn.x, passShiftBtn.y, passShiftBtn.w, passShiftBtn.h, 8, TFT_WHITE);
  M5.Display.drawRoundRect(passShiftBtn.x, passShiftBtn.y, passShiftBtn.w, passShiftBtn.h, 8, TFT_BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(passShiftBtn.x + 6, passShiftBtn.y + 18);
  M5.Display.print(kbShift ? "SHIFT" : "shift");

  const char* row1 = kbShift ? "QWERTYUIOP" : "qwertyuiop";
  const char* row2 = kbShift ? "ASDFGHJKL" : "asdfghjkl";
  const char* row3 = kbShift ? "ZXCVBNM" : "zxcvbnm";
  const char* row4 = "1234567890";
  const char* row5 = ".-_/@#*!?";

  int keyH = 58;
  int gapY = 8;
  int y0 = 250;

  auto drawRow = [&](const char* row, int y) {
    int len = strlen(row);
    int keyW = keyWForLen(len);
    int x0 = keyStartX(len, keyW);
    for (int i = 0; i < len; i++) {
      Rect r{x0 + i * (keyW + 6), y, keyW, keyH};
      M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 6, TFT_WHITE);
      M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 6, TFT_BLACK);
      M5.Display.setTextSize(2);
      M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
      M5.Display.setCursor(r.x + (keyW / 2) - 6, r.y + 18);
      M5.Display.print(row[i]);
    }
  };

  drawRow(row1, y0);
  drawRow(row2, y0 + (keyH + gapY));
  drawRow(row3, y0 + 2 * (keyH + gapY));
  drawRow(row4, y0 + 3 * (keyH + gapY));
  drawRow(row5, y0 + 4 * (keyH + gapY));

  Rect space{120, y0 + 5 * (keyH + gapY), 300, 56};
  Rect back{430, y0 + 5 * (keyH + gapY), 90, 56};
  M5.Display.fillRoundRect(space.x, space.y, space.w, space.h, 6, TFT_WHITE);
  M5.Display.drawRoundRect(space.x, space.y, space.w, space.h, 6, TFT_BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(space.x + 110, space.y + 18);
  M5.Display.print("SPACE");

  M5.Display.fillRoundRect(back.x, back.y, back.w, back.h, 6, TFT_WHITE);
  M5.Display.drawRoundRect(back.x, back.y, back.w, back.h, 6, TFT_BLACK);
  M5.Display.setCursor(back.x + 12, back.y + 18);
  M5.Display.print("DEL");

  clearRect(0, 860, 540, 120);
  drawButton(passCancelBtn, "Cancel", false);
  drawButton(passOkBtn, "OK", false);

  present();
}

// ============================
// Wi-Fi actions
// ============================
static void wifiScan() {
  wifiStatusMsg = "Scanning...";
  drawWifiList();

  espnowStop();
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true, true);
  int n = WiFi.scanNetworks();

  wifiCount = 0;
  for (int i = 0; i < n && wifiCount < WIFI_LIST_MAX; i++) {
    String s = WiFi.SSID(i);
    if (s.length()) {
      wifiSsids[wifiCount++] = s;
    }
  }

  if (wifiCount == 0) {
    wifiStatusMsg = "No networks found";
  } else {
    wifiStatusMsg = String(wifiCount) + " networks";
  }

  WiFi.disconnect(true, true);
  espnowStart();
}

static bool wifiConnect(const String& ssid, const String& pass, bool showUI = true) {
  if (showUI) {
    wifiStatusMsg = "Connecting...";
    drawWifiList();
  } else {
    wifiStatusMsg = "";
  }

  bool wasEsp = espNowActive;
  if (wasEsp) {
    espnowStop();
  }
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());

  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 10000) {
    delay(100);
  }

  bool ok = (WiFi.status() == WL_CONNECTED);
  if (ok) {
    uiWifiSsid = ssid;
    uiWifiPass = pass;
    prefs.putString("ssid", uiWifiSsid);
    prefs.putString("pass", uiWifiPass);
    wifiConnected = true;

    setupZurichTZ();
    configTime(0, 0, "pool.ntp.org", "time.google.com");
    struct tm t;
    if (getLocalTime(&t, 5000) && (t.tm_year + 1900) >= 2023) hasNtpTime = true;
    else hasNtpTime = false;
    wifiReconnectFailCount = 0;
    wifiLastReconnectTryMs = millis();
    if (cloudScheduleEnabled) {
      wifiStatusMsg = "Saved (Burst 3h)";
      wifiDisconnectSilent();
    } else {
      wifiStatusMsg = "Connected";
    }
  } else {
    wifiConnected = false;
    wifiStatusMsg = "Connect failed";
    WiFi.disconnect(true, true);
  }

  if (wasEsp) {
    espnowStart();
  }
  return ok;
}

static bool wifiConnectSilent(uint32_t timeoutMs) {
  if (!uiWifiSsid.length()) return false;
  WiFi.mode(WIFI_STA);
  WiFi.begin(uiWifiSsid.c_str(), uiWifiPass.c_str());

  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < timeoutMs) {
    delay(100);
  }

  bool ok = (WiFi.status() == WL_CONNECTED);
  wifiConnected = ok;
  if (ok) {
    setupZurichTZ();
    configTime(0, 0, "pool.ntp.org", "time.google.com");
    struct tm t;
    hasNtpTime = (getLocalTime(&t, 4000) && (t.tm_year + 1900) >= 2023);
  }
  return ok;
}

static void wifiDisconnectSilent() {
  WiFi.disconnect(true, true);
  wifiConnected = false;
}

static void cloudScheduledUploadTick() {
  if (!cloudUploadEnabled || !cloudScheduleEnabled) return;
  if (strlen(CLOUD_INGEST_URL) == 0 || strlen(CLOUD_API_KEY) == 0) return;
  if (!uiWifiSsid.length()) return;
  if (cloudHead == cloudTail) return;

  uint32_t nowMs = millis();
  if (cloudLastScheduleMs != 0 && (nowMs - cloudLastScheduleMs) < CLOUD_UPLOAD_PERIOD_MS) return;
  cloudLastScheduleMs = nowMs;

  bool wasEsp = espNowActive;
  if (wasEsp) espnowStop();

  if (wifiConnectSilent(10000)) {
    uint32_t t0 = millis();
    while (cloudHead != cloudTail && (millis() - t0) < 45000) {
      cloudUploadPump();
      delay(40);
    }
    wifiDisconnectSilent();
  }

  if (wasEsp) espnowStart();
}

static void wifiAutoReconnectTick() {
  if (cloudScheduleEnabled) return;
  if (!uiWifiSsid.length()) return;

  bool inWifiUi = (screen == Screen::WIFI_LIST || screen == Screen::WIFI_PASS);
  if (!inWifiUi) return;

  if (WiFi.status() == WL_CONNECTED) {
    wifiConnected = true;
    wifiReconnectFailCount = 0;
    return;
  }

  uint32_t interval = WIFI_RECONNECT_BASE_MS;
  for (uint8_t i = 0; i < wifiReconnectFailCount && interval < WIFI_RECONNECT_MAX_MS; i++) {
    interval *= 2;
    if (interval > WIFI_RECONNECT_MAX_MS) {
      interval = WIFI_RECONNECT_MAX_MS;
      break;
    }
  }

  uint32_t nowMs = millis();
  if (nowMs - wifiLastReconnectTryMs < interval) return;
  wifiLastReconnectTryMs = nowMs;

  bool wasEsp = espNowActive;
  if (wasEsp) espnowStop();
  bool ok = wifiConnectSilent(8000);
  if (wasEsp) espnowStart();

  if (ok) {
    wifiReconnectFailCount = 0;
    wifiStatusMsg = "WiFi reconnected";
  } else {
    if (wifiReconnectFailCount < 6) wifiReconnectFailCount++;
  }
}

static void maintainEspNowPriority() {
  // Keep ESP-NOW as primary data path except when user is actively in Wi-Fi setup screens.
  bool inWifiUi = (screen == Screen::WIFI_LIST || screen == Screen::WIFI_PASS);

  if (!inWifiUi && WiFi.status() == WL_CONNECTED) {
    wifiDisconnectSilent();
    wifiStatusMsg = "WiFi paused (ESP-NOW priority)";
  }

  if (!espNowActive) {
    espnowStart();
  }

  static uint32_t lastRxKickMs = 0;
  uint32_t nowMs = millis();
  if ((nowMs - lastEspNowRxMs) > 30000 && (nowMs - lastRxKickMs) > 30000) {
    lastRxKickMs = nowMs;
    espnowStop();
    delay(15);
    espnowStart();
  }
}


// ============================
// Touch handling
// ============================
static void handleMenuTouch(int x, int y) {
  if (menuAirQ.contains(x, y)) {
    setScreen(Screen::AIRQ_LIST);
    drawList();
  } else if (menuWifi.contains(x, y)) {
    setScreen(Screen::WIFI_LIST);
    wifiScan();
    drawWifiList();
  } else if (menuAnalysis.contains(x, y)) {
    setScreen(Screen::ANALYSIS);
    drawAnalysis();
  } else if (menuSleep.contains(x, y)) {
    asleep = true;
    setScreen(Screen::SLEEP);
    espnowStop();
    drawSleepScreen();
  } else if (menuOff.contains(x, y)) {
    goOffDeepSleep();
  } else if (menuSettings.contains(x, y)) {
    setScreen(Screen::SETTINGS);
    drawSettings();
  }
}

static void handleListTouch(int x, int y) {
  if (listBackBtn.contains(x, y)) {
    setScreen(Screen::MENU);
    drawMenu();
    return;
  }
  if (listMetricBtn.contains(x, y)) {
    setScreen(Screen::METRIC_SELECT);
    drawMetricSelect();
    return;
  }
  if (listMenuBtn.contains(x, y)) {
    setScreen(Screen::MENU);
    drawMenu();
    return;
  }

  int row = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!isDeviceActiveIdx(i)) continue;
    Rect r = listRowRect(row);
    if (r.contains(x, y)) {
      selectedIdx = i;
      screen = Screen::AIRQ_DETAIL;
      drawDetail(devs[selectedIdx]);
      return;
    }
    row++;
    if (row >= 6) break;
  }
}

static void handleDetailTouch(int x, int y) {
  if (detailBackBtn.contains(x, y)) {
    setScreen(Screen::AIRQ_LIST);
    drawList();
    return;
  }
  if (detailPrevBtn.contains(x, y)) {
    int i = selectedIdx;
    for (int k = 0; k < MAX_DEVS; k++) {
      i = (i - 1 + MAX_DEVS) % MAX_DEVS;
      if (isDeviceActiveIdx(i)) { selectedIdx = i; break; }
    }
    drawDetail(devs[selectedIdx]);
    return;
  }
  if (detailNextBtn.contains(x, y)) {
    int i = selectedIdx;
    for (int k = 0; k < MAX_DEVS; k++) {
      i = (i + 1) % MAX_DEVS;
      if (isDeviceActiveIdx(i)) { selectedIdx = i; break; }
    }
    drawDetail(devs[selectedIdx]);
    return;
  }
}

static void handleMetricSelectTouch(int x, int y) {
  SummaryMetric allMetrics[7] = {
    SummaryMetric::CO2,
    SummaryMetric::PM25,
    SummaryMetric::PM10,
    SummaryMetric::VOC,
    SummaryMetric::NOX,
    SummaryMetric::TEMP,
    SummaryMetric::RH
  };

  int y0 = 140;
  int rowH = 90;
  for (int i = 0; i < 7; i++) {
    Rect r{20, y0 + i * rowH, 500, 70};
    if (r.contains(x, y)) {
      bool selected = false;
      int selIdx = -1;
      for (int k = 0; k < listMetricCount; k++) {
        if (listMetrics[k] == allMetrics[i]) {
          selected = true;
          selIdx = k;
        }
      }
      if (selected) {
        for (int k = selIdx; k < listMetricCount - 1; k++) {
          listMetrics[k] = listMetrics[k + 1];
        }
        listMetricCount--;
      } else if (listMetricCount < 3) {
        listMetrics[listMetricCount++] = allMetrics[i];
      }
      drawMetricSelect();
      return;
    }
  }

  if (metricBackBtn.contains(x, y)) {
    setScreen(Screen::AIRQ_LIST);
    drawList();
    return;
  }
  if (metricDoneBtn.contains(x, y)) {
    setScreen(Screen::AIRQ_LIST);
    drawList();
    return;
  }
}

static void handleWifiListTouch(int x, int y) {
  if (wifiBackBtn.contains(x, y)) {
    setScreen(Screen::MENU);
    drawMenu();
    return;
  }
  if (wifiScanBtn.contains(x, y)) {
    wifiScan();
    drawWifiList();
    return;
  }
  if (wifiUploadBtn.contains(x, y)) {
    if (cloudHead == cloudTail) {
      wifiStatusMsg = "No cloud data queued";
      drawWifiList();
      return;
    }

    uint32_t sent0 = cloudSent;
    if (cloudScheduleEnabled) {
      cloudLastScheduleMs = 0;
      cloudScheduledUploadTick();
    } else {
      if (WiFi.status() != WL_CONNECTED) {
        bool wasEsp = espNowActive;
        if (wasEsp) espnowStop();
        bool ok = wifiConnectSilent(10000);
        if (wasEsp) espnowStart();
        if (!ok) {
          wifiStatusMsg = "Upload failed: no WiFi";
          drawWifiList();
          return;
        }
      }
      uint32_t t0 = millis();
      while (cloudHead != cloudTail && (millis() - t0) < 15000) {
        cloudUploadPump();
        delay(40);
      }
    }

    uint32_t sentNow = cloudSent - sent0;
    wifiStatusMsg = String("Uploaded ") + String(sentNow) + String(" rows");
    drawWifiList();
    return;
  }

  int y0 = 120;
  int rowH = 70;
  for (int i = 0; i < wifiCount; i++) {
    int idx = wifiPage * WIFI_LIST_MAX + i;
    if (idx >= wifiCount) break;
    Rect r{20, y0 + i * rowH, 500, rowH - 10};
    if (r.contains(x, y)) {
      uiWifiSsid = wifiSsids[idx];
      uiWifiPass = "";
      screen = Screen::WIFI_PASS;
      drawWifiPass();
      return;
    }
  }
}

static void handleWifiPassTouch(int x, int y) {
  if (passCancelBtn.contains(x, y)) {
    setScreen(Screen::WIFI_LIST);
    drawWifiList();
    return;
  }
  if (passOkBtn.contains(x, y)) {
    wifiConnect(uiWifiSsid, uiWifiPass);
    setScreen(Screen::WIFI_LIST);
    drawWifiList();
    return;
  }
  if (passShiftBtn.contains(x, y)) {
    kbShift = !kbShift;
    drawWifiPass();
    return;
  }

  const char* row1 = kbShift ? "QWERTYUIOP" : "qwertyuiop";
  const char* row2 = kbShift ? "ASDFGHJKL" : "asdfghjkl";
  const char* row3 = kbShift ? "ZXCVBNM" : "zxcvbnm";
  const char* row4 = "1234567890";
  const char* row5 = ".-_/@#*!?";

  int keyH = 58;
  int gapY = 8;
  int y0 = 250;

  char c = 0;
  auto hitRow = [&](const char* row, int yrow) -> char {
    int len = strlen(row);
    int keyW = keyWForLen(len);
    int x0 = keyStartX(len, keyW);
    for (int i = 0; i < len; i++) {
      Rect r{x0 + i * (keyW + 6), yrow, keyW, keyH};
      if (r.contains(x, y)) return row[i];
    }
    return 0;
  };

  c = hitRow(row1, y0);
  if (!c) c = hitRow(row2, y0 + (keyH + gapY));
  if (!c) c = hitRow(row3, y0 + 2 * (keyH + gapY));
  if (!c) c = hitRow(row4, y0 + 3 * (keyH + gapY));
  if (!c) c = hitRow(row5, y0 + 4 * (keyH + gapY));

  Rect space{120, y0 + 5 * (keyH + gapY), 300, 56};
  Rect back{430, y0 + 5 * (keyH + gapY), 90, 56};
  if (space.contains(x, y)) c = ' ';
  if (back.contains(x, y)) {
    if (uiWifiPass.length()) uiWifiPass.remove(uiWifiPass.length() - 1);
    drawWifiPass();
    return;
  }

  if (c) {
    if (uiWifiPass.length() < 32) {
      uiWifiPass += c;
      drawWifiPass();
    }
  }
}

static void handleAnalysisTouch(int x, int y) {
  if (analysisBackBtn.contains(x, y)) {
    setScreen(Screen::MENU);
    drawMenu();
    return;
  }
  if (analysisMetricBtn.contains(x, y)) {
    analysisMetric = (SummaryMetric)((((int)analysisMetric) + 1) % 7);
    drawAnalysis();
    return;
  }
  if (analysisZoomBtn.contains(x, y)) {
    nextZoom();
    drawAnalysis();
    return;
  }
  if (analysisSensorsBtn.contains(x, y)) {
    setScreen(Screen::ANALYSIS_SENSORS);
    drawAnalysisSensors();
    return;
  }
}


static void handleAnalysisSensorsTouch(int x, int y) {
  int y0 = 140;
  int rowH = 120;
  int shown = 0;
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!devs[i].used) continue;
    Rect r{20, y0 + shown * rowH, 500, 100};
    if (r.contains(x, y)) {
      if (analysisSel[i]) {
        analysisSel[i] = false;
      } else {
        if (analysisSelectedCount() < 10) {
          analysisSel[i] = true;
        }
      }
      drawAnalysisSensors();
      return;
    }
    shown++;
    if (shown >= 10) break;
  }

  if (metricBackBtn.contains(x, y) || metricDoneBtn.contains(x, y)) {
    setScreen(Screen::ANALYSIS);
    drawAnalysis();
    return;
  }
}


static void handleSettingsTouch(int x, int y) {
  if (settingsRefreshRow.contains(x, y)) {
    cloudScheduleEnabled = !cloudScheduleEnabled;
    prefs.putBool("cloud_sched", cloudScheduleEnabled);
    if (cloudScheduleEnabled) {
      cloudLastScheduleMs = 0;
      if (wifiConnected || WiFi.status() == WL_CONNECTED) {
        wifiDisconnectSilent();
      }
      wifiStatusMsg = "Burst 3h mode";
    } else {
      if (uiWifiSsid.length()) {
        wifiConnect(uiWifiSsid, uiWifiPass, false);
      }
      wifiStatusMsg = "Always-on mode";
    }
    drawSettings();
    return;
  }

  int y0 = 160;
  int rowH = 70;
  for (int i = 0; i < 7; i++) {
    Rect r{20, y0 + i * rowH, 500, 60};
    if (r.contains(x, y)) {
      logIntervalMs = intervalOptionsMs[i];
      prefs.putUInt("log_ms", logIntervalMs);
      drawSettings();
      return;
    }
  }

  if (settingsModeRow.contains(x, y)) {
    logMode = (logMode == LogMode::CONTINUOUS) ? LogMode::PER_BOOT : LogMode::CONTINUOUS;
    prefs.putUInt("log_mode", (uint8_t)logMode);
    if (logMode == LogMode::PER_BOOT) {
      bootId = prefs.getUInt("boot_id", 0) + 1;
      prefs.putUInt("boot_id", bootId);
    } else {
      bootId = 0;
    }
    if (logFile) logFile.close();
    if (sdOk) {
      openLogFile();
      scanLogFiles();
    }
    drawSettings();
    return;
  }

  if (settingsBackBtn.contains(x, y)) {
    setScreen(Screen::MENU);
    drawMenu();
    return;
  }
}

// ============================
// OFF: deep sleep
// ============================
static void goOffDeepSleep() {
  setEpdQuality();
  fullClear();
  M5.Display.setTextSize(3);
  M5.Display.setCursor(220, 420);
  M5.Display.print("OFF");
  M5.Display.setTextSize(2);
  M5.Display.setCursor(140, 480);
  M5.Display.print("Press BtnA to wake");
  present();
  delay(200);

  M5.Display.sleep();

  esp_sleep_enable_ext0_wakeup(WAKE_PIN, 0); // active low
  esp_deep_sleep_start();
}

// ============================
// Setup / loop
// ============================
void setup() {
  auto cfg = M5.config();
  cfg.clear_display = true;
  cfg.output_power = true;   // helps Paper S3 power rail
  M5.begin(cfg);

  M5.Display.wakeup();
  M5.Display.setRotation(0);

  // show boot splash immediately
  setEpdQuality();
  M5.Display.fillScreen(TFT_WHITE);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(3);
  M5.Display.setCursor(160, 300);
  M5.Display.print("BOOT OK");
  M5.Display.setTextSize(2);
  M5.Display.setCursor(120, 360);
  M5.Display.print("Starting Hub...");
  present();
  delay(600);

  hubStartMs = millis();
  bootBlockOffUntil = millis() + 2000;

  prefs.begin("airq", false);
  uiWifiSsid = prefs.getString("ssid", "");
  uiWifiPass = prefs.getString("pass", "");
  logIntervalMs = prefs.getUInt("log_ms", 20000);
  logMode = (LogMode)prefs.getUInt("log_mode", 0);
  cloudScheduleEnabled = prefs.getBool("cloud_sched", false);

  bootId = prefs.getUInt("boot_id", 0);
  if (logMode == LogMode::PER_BOOT) {
    bootId++;
    prefs.putUInt("boot_id", bootId);
  } else {
    bootId = 0;
  }

  if (uiWifiSsid.length()) {
    wifiConnect(uiWifiSsid, uiWifiPass, false);
    if (cloudScheduleEnabled && (wifiConnected || WiFi.status() == WL_CONNECTED)) {
      wifiDisconnectSilent();
    }
  } else {
    wifiConnected = false;
  }

  // SD init
  sdOk = SD.begin();
  if (sdOk) {
    openLogFile();
    scanLogFiles();
  }

  // Optional NTP
  tryNtpOnce();

  // ESP-NOW
  espnowStart();

  for (int i = 0; i < MAX_DEVS; i++) devDirty[i] = false;

  screen = Screen::MENU;
  drawMenu();
}

void loop() {
  M5.update();

  // refresh SD presence status occasionally
  static uint32_t lastSdCheck = 0;
  if (millis() - lastSdCheck > 2000) {
    lastSdCheck = millis();
    refreshSdState();
  }

  // BtnA short: Sleep/Wake
  if (M5.BtnA.wasPressed()) {
    if (!asleep) {
      asleep = true;
      screen = Screen::SLEEP;
      espnowStop();
      drawSleepScreen();
    } else {
      asleep = false;
      espnowStart();
      screen = Screen::MENU;
      drawMenu();
    }
  }

  // BtnA long: OFF (blocked for first 2s after boot)
  if (millis() > bootBlockOffUntil && M5.BtnA.pressedFor(LONGPRESS_MS)) {
    if (!offLatch) {
      offLatch = true;
      goOffDeepSleep();
    }
  } else {
    offLatch = false;
  }

  static bool touchLatch = false;

  if (asleep) {
    auto t = M5.Touch.getDetail();
    bool touchEvent = t.wasPressed() || t.wasClicked() || t.isPressed();
    if (touchEvent && !touchLatch) {
      touchLatch = true;
      asleep = false;
      screen = Screen::MENU;
      espnowStart();
      drawMenu();
    }
    if (!t.isPressed()) touchLatch = false;
    delay(30);
    return;
  }

  maintainEspNowPriority();

  // Drain queue
  AirQPacket p;
  bool anyNew = false;
  while (qPop(p)) {
    int idx = findOrCreateDevice(p.srcMac);
    if (idx >= 0) {
      devs[idx].last = p;
      devs[idx].pktCount++;
      devs[idx].lastSeenMs = millis();
      devDirty[idx] = true;
      anyNew = true;
      uint32_t nowMs = millis();
      if (lastLogMs[idx] == 0 || (nowMs - lastLogMs[idx]) >= logIntervalMs) {
        appendTsv(p, devs[idx].label);
        enqueueCloudRow(p, devs[idx].label);
        lastLogMs[idx] = nowMs;
      }
    }
  }

  // Touch handling (robust against long blocking cycles)
  auto t = M5.Touch.getDetail();
  bool touchEvent = t.wasPressed() || t.wasClicked() || t.isPressed();
  if (touchEvent && !touchLatch) {
    touchLatch = true;
    int x = t.x;
    int y = t.y;
    if (screen == Screen::MENU) handleMenuTouch(x, y);
    else if (screen == Screen::AIRQ_LIST) handleListTouch(x, y);
    else if (screen == Screen::AIRQ_DETAIL) handleDetailTouch(x, y);
    else if (screen == Screen::METRIC_SELECT) handleMetricSelectTouch(x, y);
    else if (screen == Screen::WIFI_LIST) handleWifiListTouch(x, y);
    else if (screen == Screen::WIFI_PASS) handleWifiPassTouch(x, y);
    else if (screen == Screen::ANALYSIS) handleAnalysisTouch(x, y);
    else if (screen == Screen::ANALYSIS_SENSORS) handleAnalysisSensorsTouch(x, y);
    else if (screen == Screen::SETTINGS) handleSettingsTouch(x, y);
    else if (screen == Screen::SLEEP) {
      asleep = false;
      screen = Screen::MENU;
      espnowStart();
      drawMenu();
    }
  }
  if (!t.isPressed()) touchLatch = false;

  // Auto refresh on new packets (no blinking)
  static uint32_t lastRedraw = 0;
  if (screen == Screen::AIRQ_LIST && anyNew) {
    if (millis() - lastRedraw > LIST_REDRAW_MIN_MS) {
      lastRedraw = millis();
      updateListDirtyRows();
    }
  }

  static uint32_t lastListPeriodicRedraw = 0;
  if (screen == Screen::AIRQ_LIST && millis() - lastListPeriodicRedraw > 5000) {
    lastListPeriodicRedraw = millis();
    drawList();
  }

  if (screen == Screen::AIRQ_DETAIL) {
    if (!isDeviceActiveIdx(selectedIdx)) {
      setScreen(Screen::AIRQ_LIST);
      drawList();
    } else if (anyNew && devDirty[selectedIdx]) {
      devDirty[selectedIdx] = false;
      drawDetail(devs[selectedIdx]);
    }
  }

  wifiAutoReconnectTick();
  cloudScheduledUploadTick();
  cloudUploadPump();

  delay(20);
}
