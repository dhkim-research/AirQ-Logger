#include <M5Unified.h>
#include <WiFi.h>
#include <esp_now.h>
#include <FS.h>
#include <SD.h>
#include <time.h>
#include <sys/time.h>
#include <esp_sleep.h>
#include <Preferences.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>

#include "airq_types.h"

// ============================
// Debug toggles (set to 1 to enable)
// ============================
#ifndef AIRQ_DEBUG_WIFI_SCAN
#define AIRQ_DEBUG_WIFI_SCAN 0
#endif

#ifndef AIRQ_DEBUG_DIRTY_RECTS
#define AIRQ_DEBUG_DIRTY_RECTS 0
#endif

#ifndef AIRQ_DEBUG_CL_COUNTER
#define AIRQ_DEBUG_CL_COUNTER 0
#endif

#define DBG_WIFI(fmt, ...) do { if (AIRQ_DEBUG_WIFI_SCAN) Serial.printf("[WIFI] " fmt "\n", ##__VA_ARGS__); } while (0)
#define DBG_DIRTY(fmt, ...) do { if (AIRQ_DEBUG_DIRTY_RECTS) Serial.printf("[DIRTY] " fmt "\n", ##__VA_ARGS__); } while (0)
#define DBG_CL(fmt, ...) do { if (AIRQ_DEBUG_CL_COUNTER) Serial.printf("[CL] " fmt "\n", ##__VA_ARGS__); } while (0)

// Analysis rendering style: dots reduce ghosting compared to connected lines.
static constexpr bool ANALYSIS_PLOT_DOTS = true;
static constexpr int ANALYSIS_DOT_RADIUS = 2;
static constexpr int ANALYSIS_AXIS_TEXT_SIZE = 2;

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
static constexpr bool ENABLE_PHYSICAL_SLEEP_OFF_SHORTCUTS = false;
static constexpr bool WIFI_AUTO_CONNECT_ON_BOOT = false;

// ============================
// Optional Wi-Fi for NTP time
// ============================
// Leave SSID empty "" to skip Wi-Fi time completely.
static const char* WIFI_SSID = "";
static const char* WIFI_PASS = "";
static constexpr bool ALLOW_WIFI_NTP_TIME_SYNC = false;

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
static bool manualTimeValid = false;
static uint32_t manualBaseEpoch = 0;
static uint32_t manualBaseMillis = 0;
static uint32_t manualLastPersistMs = 0;
static constexpr uint32_t MANUAL_TIME_MIN_EPOCH = 1704067200UL;  // 2024-01-01 00:00:00 UTC
static constexpr uint32_t MANUAL_TIME_MAX_EPOCH = 4102444799UL;  // 2099-12-31 23:59:59 UTC
static constexpr uint32_t MANUAL_TIME_PERSIST_MS = 60000UL;

// Cloud upload (optional)
// Loaded from SD /config.txt:
//   cloud_ingest_url = "https://your-host/airq/ingest"
//   cloud_api_key = "your-secret-key"
static String cloudIngestUrl = "";
static String cloudApiKey = "";
static String cloudRemoteName = "airq_data_log.tsv";
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
static uint32_t rxPacketCount = 0;
static bool topBarLiveDirty = true;
static uint32_t topBarPacketDirtyLastMs = 0;
static constexpr uint32_t TOPBAR_PACKET_DIRTY_MIN_MS = 1200;


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
static bool wifiScanRunning = false;
static bool wifiScanResumeEspNow = false;
static uint32_t wifiScanStartedMs = 0;
static constexpr uint32_t WIFI_SCAN_TIMEOUT_MS = 15000;
static bool wifiOpActive = false;

// Manual date/time edit state
static int setYear = 2026;
static int setMonth = 1;
static int setDay = 1;
static int setHour = 0;
static int setMinute = 0;

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
  SET_TIME,
  SLEEP
};
static Screen screen = Screen::MENU;

// Analysis filters (offline capable)
static String analysisDates[16];
static int analysisDateCount = 0;
static int analysisDateSel = -1;  // -1 = latest date, -2 = all dates, >=0 = explicit index
enum class AnalysisWindow : uint8_t { ALL, H08_12, H12_16, H16_20, H20_24 };
static AnalysisWindow analysisWindow = AnalysisWindow::ALL;

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
static void clearRect(int x, int y, int w, int h);
static void markTopBarDirty(const char* reason);
static void refreshTopBarLiveIfNeeded();
static void drawTopBar(const char* title);
static void drawListHeader();
static void drawDetailHeader(const DeviceState& d);
static void presentDirtyRegionOnly();
static void addWifiSsidUnique(const String& ssid);
static void loadCloudConfigFromSd();
static void drawSetTime();
static void handleSetTimeTouch(int x, int y);
static uint32_t currentManualEpoch();
static void loadManualTimeFromPrefs();
static void saveManualTimeToPrefs();
static void initSetTimeEditorFromCurrent();
static void scanAnalysisDates(const String& filePath);
static String selectedAnalysisDate();
static bool isAnalysisTimeInWindow(const String& timeIso);

// ============================
// Helpers
// ============================
static void setupZurichTZ() {
  setenv("TZ", "CET-1CEST,M3.5.0/2,M10.5.0/3", 1);
  tzset();
}

static bool isReasonableEpoch(uint32_t epoch) {
  return epoch >= MANUAL_TIME_MIN_EPOCH && epoch <= MANUAL_TIME_MAX_EPOCH;
}

static void setSystemClockAndRtc(uint32_t ep) {
  timeval tv{};
  tv.tv_sec = (time_t)ep;
  tv.tv_usec = 0;
  settimeofday(&tv, nullptr);

  if (M5.Rtc.isEnabled()) {
    time_t t = (time_t)ep;
    struct tm* gmt = gmtime(&t);
    if (gmt) M5.Rtc.setDateTime(gmt);
  }
}

static uint32_t currentManualEpoch() {
  if (!manualTimeValid) return 0;
  uint32_t elapsedS = (millis() - manualBaseMillis) / 1000UL;
  return manualBaseEpoch + elapsedS;
}

static void saveManualTimeToPrefs() {
  prefs.putBool("time_valid", manualTimeValid);
  prefs.putULong("time_epoch", manualBaseEpoch);
}

static void loadManualTimeFromPrefs() {
  bool savedValid = prefs.getBool("time_valid", false);
  uint32_t savedEpoch = prefs.getULong("time_epoch", 0);
  manualBaseMillis = millis();
  manualLastPersistMs = manualBaseMillis;
  if (savedValid && isReasonableEpoch(savedEpoch)) {
    // Keep as a stale suggestion for Set Time editor, but require boot-time
    // confirmation (RTC/NTP/manual) before treating clock as valid.
    manualBaseEpoch = savedEpoch;
  } else {
    manualBaseEpoch = 0;
  }
  manualTimeValid = false;
  hasNtpTime = false;
}

static void captureSystemTimeIfValid() {
  time_t now;
  time(&now);
  uint32_t ep = (uint32_t)now;
  if (!isReasonableEpoch(ep)) return;
  manualTimeValid = true;
  manualBaseEpoch = ep;
  manualBaseMillis = millis();
  manualLastPersistMs = manualBaseMillis;
  hasNtpTime = true;
  if (M5.Rtc.isEnabled()) {
    time_t t = (time_t)ep;
    struct tm* gmt = gmtime(&t);
    if (gmt) M5.Rtc.setDateTime(gmt);
  }
  saveManualTimeToPrefs();
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

static int batteryPctFromVoltage(float v) {
  if (v < 0.5f) return -1;
  if (v <= 3.20f) return 0;
  if (v >= 4.20f) return 100;
  int p = (int)(((v - 3.20f) / 1.00f) * 100.0f + 0.5f);
  if (p < 0) p = 0;
  if (p > 100) p = 100;
  return p;
}

static int hubBatteryPct() {
  int pct = (int)M5.Power.getBatteryLevel();
  float v = hubBatteryV();
  int pv = batteryPctFromVoltage(v);
  if (pct < 0 || pct > 100) return pv;
  // PaperS3 sometimes reports a spurious 0% while voltage is high.
  if (pv >= 0 && pct <= 1 && v >= 4.00f) return pv;
  if (pv >= 0 && pct >= 99 && v <= 3.45f) return pv;
  return pct;
}

// Timestamp
static String timestampString() {
  uint32_t ep = currentManualEpoch();
  if (ep > 0) {
    time_t now = (time_t)ep;
    struct tm t;
    localtime_r(&now, &t);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &t);
    return String(buf);
  }
  return String("NO TIME SET");
}

static uint32_t nowEpoch() {
  return currentManualEpoch();
}

static int labelIndexFromChar(char c) {
  if (c < 'A' || c > 'P') return -1;
  return c - 'A';
}

static int labelIndexFromToken(const String& token) {
  for (int i = 0; i < token.length(); i++) {
    char c = token[i];
    if (c >= 'a' && c <= 'p') c = (char)(c - 32);
    if (c >= 'A' && c <= 'P') return c - 'A';
  }
  return -1;
}

static bool isLikelyTsvHeaderLine(const String& line) {
  if (line.length() == 0) return false;
  if (line.startsWith("time_iso")) return true;
  if (line.indexOf("epoch_s") >= 0) return true;
  if (line.indexOf("uptime_seconds") >= 0) return true;
  if (line.indexOf("wifi_sync") >= 0) return true;
  return false;
}

static String trimWsCopy(const String& in) {
  int s = 0;
  int e = in.length() - 1;
  while (s <= e && (in[s] == ' ' || in[s] == '\t' || in[s] == '\r' || in[s] == '\n')) s++;
  while (e >= s && (in[e] == ' ' || in[e] == '\t' || in[e] == '\r' || in[e] == '\n')) e--;
  if (e < s) return "";
  return in.substring(s, e + 1);
}

static String unquoteCopy(const String& in) {
  if (in.length() >= 2) {
    char a = in[0];
    char b = in[in.length() - 1];
    if ((a == '"' && b == '"') || (a == '\'' && b == '\'')) {
      return in.substring(1, in.length() - 1);
    }
  }
  return in;
}

static bool hasCloudUploadConfig() {
  return cloudIngestUrl.length() > 0 && cloudApiKey.length() > 0;
}

static bool isConfigNoneLike(const String& in) {
  String t = trimWsCopy(in);
  if (t.length() == 0) return true;
  t.toUpperCase();
  return t == "NONE";
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
    logFile.println("time_iso	epoch_s	uptime_seconds	wifi_sync	label	mac	seq	batt_pct	pm1	pm25	pm4	pm10	tempC	rh	voc	nox	co2");
    logFile.flush();
  }
}

static void loadCloudConfigFromSd() {
  cloudIngestUrl = "";
  cloudApiKey = "";
  String cfgWifiSsid = "";
  String cfgWifiPwd = "";
  if (!sdOk) return;

  File f = SD.open("/config.txt", FILE_READ);
  if (!f) f = SD.open("/CONFIG.TXT", FILE_READ);
  if (!f) {
    markTopBarDirty("cloud_cfg_missing");
    return;
  }

  while (f.available()) {
    String line = f.readStringUntil('\n');
    line.replace("\r", "");
    String t = trimWsCopy(line);
    if (t.length() == 0) continue;
    if (t[0] == '#') continue;

    int eq = t.indexOf('=');
    if (eq < 0) continue;

    String key = trimWsCopy(t.substring(0, eq));
    key.toLowerCase();
    String value = trimWsCopy(t.substring(eq + 1));
    value = unquoteCopy(value);

    if (key == "cloud_ingest_url" || key == "cloud_upload_url" || key == "cloud_url") {
      cloudIngestUrl = value;
    } else if (key == "cloud_api_key" || key == "cloud_key" || key == "api_key") {
      cloudApiKey = value;
    } else if (key == "cloud_remote_name" || key == "cloud_filename") {
      if (!isConfigNoneLike(value)) cloudRemoteName = value;
    } else if (key == "wifi_ssid") {
      cfgWifiSsid = value;
    } else if (key == "wifi_pwd") {
      cfgWifiPwd = value;
    }
  }
  f.close();

  if (!isConfigNoneLike(cfgWifiSsid)) {
    uiWifiSsid = cfgWifiSsid;
    uiWifiPass = isConfigNoneLike(cfgWifiPwd) ? String("") : cfgWifiPwd;
  }

  markTopBarDirty("cloud_cfg_loaded");
}

static void appendTsv(const AirQPacket& p, const char* label) {
  if (!sdOk) return;
  uint32_t epNow = nowEpoch();
  if (!epNow) return;  // Strict policy: never write NO_TIME rows to main log.
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
  String t = timestampString();
  String ep = String((unsigned long)epNow);
  line += t; line += "\t";
  line += ep; line += "\t";
  line += elapsedString(); line += "\t";
  line += "1"; line += "\t";
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
    markTopBarDirty("cloud_drop");
    DBG_CL("queue full, dropped=%lu", (unsigned long)cloudDropped);
    return false;
  }
  cloudQ[cloudHead] = payload;
  cloudHead = next;
  markTopBarDirty("cloud_enqueue");
  DBG_CL("enqueue depth=%d", (cloudHead - cloudTail + CLOUD_QSIZE) % CLOUD_QSIZE);
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
  if (!hasCloudUploadConfig()) return;

  uint32_t ep = nowEpoch();
  if (!ep) return;  // Keep cloud payload timestamps strict as well.
  String timeIso = timestampString();

  String js;
  js.reserve(320);
  js += "{";
  js += "\"time_iso\":\"" + timeIso + "\",";
  js += "\"epoch_s\":" + String((unsigned long)ep) + ",";
  js += "\"elapsed_s\":" + elapsedString() + ",";
  js += "\"wifi_sync\":" + String(ep ? 1 : 0) + ",";
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
  if (!hasCloudUploadConfig()) return;
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
  if (!http.begin(client, cloudIngestUrl.c_str())) {
    cloudFail++;
    return;
  }

  http.setConnectTimeout(2500);
  http.setTimeout(3000);
  http.addHeader("Content-Type", "application/json");
  http.addHeader("X-API-Key", cloudApiKey.c_str());

  int code = http.POST(payload);
  http.end();

  if (code >= 200 && code < 300) {
    String dropped;
    cloudQPop(dropped);
    cloudSent++;
    markTopBarDirty("cloud_sent");
    DBG_CL("sent=%lu depth=%d", (unsigned long)cloudSent, (cloudHead - cloudTail + CLOUD_QSIZE) % CLOUD_QSIZE);
  } else {
    cloudFail++;
    DBG_CL("upload fail code=%d fail=%lu", code, (unsigned long)cloudFail);
  }
}

class MultipartFileStream : public Stream {
 public:
  MultipartFileStream(File* f, const String& prefix, const String& suffix)
  : file(f), pre(prefix), post(suffix), prePos(0), postPos(0) {}

  int available() override {
    int n = 0;
    if (prePos < (size_t)pre.length()) n += (int)(pre.length() - prePos);
    if (file) n += file->available();
    if (postPos < (size_t)post.length()) n += (int)(post.length() - postPos);
    return n;
  }

  int read() override {
    uint8_t c = 0;
    if (readBytes((char*)&c, 1) == 1) return (int)c;
    return -1;
  }

  int peek() override {
    if (prePos < (size_t)pre.length()) return pre[(int)prePos];
    if (file && file->available()) return file->peek();
    if (postPos < (size_t)post.length()) return post[(int)postPos];
    return -1;
  }

  void flush() override {}
  size_t write(uint8_t) override { return 0; }

  size_t readBytes(char* buffer, size_t length) override {
    size_t out = 0;
    while (out < length) {
      if (prePos < (size_t)pre.length()) {
        buffer[out++] = pre[(int)prePos++];
        continue;
      }
      if (file && file->available()) {
        int r = file->read((uint8_t*)&buffer[out], length - out);
        if (r > 0) {
          out += (size_t)r;
          continue;
        }
      }
      if (postPos < (size_t)post.length()) {
        buffer[out++] = post[(int)postPos++];
        continue;
      }
      break;
    }
    return out;
  }

 private:
  File* file;
  String pre;
  String post;
  size_t prePos;
  size_t postPos;
};

static bool uploadActiveLogFileMultipart(String& statusMsg, uint32_t& uploadedBytes) {
  uploadedBytes = 0;
  if (!sdOk) {
    statusMsg = "No SD card";
    return false;
  }
  if (!hasCloudUploadConfig()) {
    statusMsg = "No cloud config";
    return false;
  }

  if (logFile) logFile.flush();

  String sourcePath = logFilePath;
  if (sourcePath.length() == 0) sourcePath = makeLogFilePath();
  if (!sourcePath.startsWith("/")) sourcePath = "/" + sourcePath;

  File src = SD.open(sourcePath, FILE_READ);
  if (!src && sourcePath != "/airq_data_log.tsv") {
    sourcePath = "/airq_data_log.tsv";
    src = SD.open(sourcePath, FILE_READ);
  }
  if (!src) {
    statusMsg = "No log file to upload";
    return false;
  }

  size_t fileSize = src.size();
  if (fileSize == 0) {
    src.close();
    statusMsg = "Log file is empty";
    return false;
  }

  String localName = sourcePath;
  int slash = localName.lastIndexOf('/');
  if (slash >= 0 && (slash + 1) < localName.length()) localName = localName.substring(slash + 1);
  String remoteName = cloudRemoteName.length() ? cloudRemoteName : localName;

  src.close();

  auto compactResp = [&](const String& in) -> String {
    String t = in;
    t.replace("\r", " ");
    t.replace("\n", " ");
    t = trimWsCopy(t);
    if (t.length() > 72) t = t.substring(0, 72);
    return t;
  };

  const String boundary = "----AirQBoundaryM5PaperS3";
  String prefix;
  prefix.reserve(256 + remoteName.length());
  prefix += "--" + boundary + "\r\n";
  prefix += "Content-Disposition: form-data; name=\"file\"; filename=\"" + remoteName + "\"\r\n";
  prefix += "Content-Type: text/tab-separated-values\r\n\r\n";

  String suffix;
  suffix.reserve(320 + remoteName.length());
  suffix += "\r\n--" + boundary + "\r\n";
  suffix += "Content-Disposition: form-data; name=\"filename\"\r\n\r\n";
  suffix += remoteName + "\r\n";
  suffix += "--" + boundary + "\r\n";
  suffix += "Content-Disposition: form-data; name=\"overwrite\"\r\n\r\n1\r\n";
  suffix += "--" + boundary + "--\r\n";

  size_t totalMultipartLen = prefix.length() + fileSize + suffix.length();

  auto doMultipart = [&](String& respOut) -> int {
    File f = SD.open(sourcePath, FILE_READ);
    if (!f) return -10;
    WiFiClientSecure client;
    client.setInsecure();
    HTTPClient http;
    if (!http.begin(client, cloudIngestUrl.c_str())) {
      f.close();
      return -11;
    }
    http.setConnectTimeout(6000);
    http.setTimeout(120000);
    http.addHeader("Content-Type", String("multipart/form-data; boundary=") + boundary);
    http.addHeader("X-API-Key", cloudApiKey.c_str());
    http.addHeader("X-File-Name", remoteName);
    http.addHeader("X-Overwrite", "1");
    MultipartFileStream body(&f, prefix, suffix);
    int code = http.sendRequest("POST", &body, totalMultipartLen);
    respOut = http.getString();
    http.end();
    f.close();
    return code;
  };

  auto doRaw = [&](const char* method, String& respOut) -> int {
    File f = SD.open(sourcePath, FILE_READ);
    if (!f) return -20;
    WiFiClientSecure client;
    client.setInsecure();
    HTTPClient http;
    if (!http.begin(client, cloudIngestUrl.c_str())) {
      f.close();
      return -21;
    }
    http.setConnectTimeout(6000);
    http.setTimeout(120000);
    http.addHeader("Content-Type", "text/tab-separated-values");
    http.addHeader("X-API-Key", cloudApiKey.c_str());
    http.addHeader("X-File-Name", remoteName);
    http.addHeader("X-Overwrite", "1");
    int code = http.sendRequest(method, &f, fileSize);
    respOut = http.getString();
    http.end();
    f.close();
    return code;
  };

  auto onSuccess = [&](const char* mode) {
    cloudSent++;
    uploadedBytes = (uint32_t)fileSize;
    markTopBarDirty("cloud_file_sent");
    statusMsg = String("Uploaded ") + String((unsigned long)uploadedBytes) + String("B (") + String(mode) + String(")");
  };

  String resp;
  int code = doMultipart(resp);
  if (code >= 200 && code < 300) {
    onSuccess("multipart");
    return true;
  }

  // Endpoint may reject multipart schema. Retry common file APIs.
  String respPut;
  int codePut = doRaw("PUT", respPut);
  if (codePut >= 200 && codePut < 300) {
    onSuccess("PUT");
    return true;
  }

  String respPost;
  int codePost = doRaw("POST", respPost);
  if (codePost >= 200 && codePost < 300) {
    onSuccess("POST");
    return true;
  }

  cloudFail++;
  int finalCode = codePost > 0 ? codePost : (codePut > 0 ? codePut : code);
  String finalResp = codePost > 0 ? respPost : (codePut > 0 ? respPut : resp);
  if (finalCode > 0) {
    statusMsg = String("Upload failed HTTP ") + String(finalCode);
    String snippet = compactResp(finalResp);
    if (snippet.length()) statusMsg += String(" ") + snippet;
  } else {
    statusMsg = "Upload failed";
  }
  return false;
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
  if (!ALLOW_WIFI_NTP_TIME_SYNC) return;
  if (manualTimeValid) return;
  if (strlen(WIFI_SSID) == 0) return;

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 8000) delay(100);

  if (WiFi.status() == WL_CONNECTED) {
    setupZurichTZ();
    configTime(0, 0, "pool.ntp.org", "time.google.com");
    captureSystemTimeIfValid();
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
      cloudIngestUrl = "";
      cloudApiKey = "";
      markTopBarDirty("sd_removed");
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
      loadCloudConfigFromSd();
      markTopBarDirty("sd_inserted");
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
static int listLastActiveCount = -1;
static int listRenderedRows = 0;
static uint32_t listFooterLastRefreshMs = 0;
static uint32_t lastDeepCleanMs = 0;
static constexpr uint32_t PERIODIC_DEEP_CLEAN_MS = 0;
static bool screenTransitionWhitePrime = false;
static constexpr bool USE_DIRTY_REGION_COMMIT = false;
struct DirtyRegion {
  bool valid = false;
  int x1 = 0, y1 = 0, x2 = 0, y2 = 0;
};
static DirtyRegion gDirty;

static void resetDirtyRegion() {
  gDirty.valid = false;
}

static void markDirtyRegion(int x, int y, int w, int h) {
  if (w <= 0 || h <= 0) return;
  if (x < 0) { w += x; x = 0; }
  if (y < 0) { h += y; y = 0; }
  if (x >= 540 || y >= 960) return;
  if (x + w > 540) w = 540 - x;
  if (y + h > 960) h = 960 - y;
  if (w <= 0 || h <= 0) return;

  int nx1 = x;
  int ny1 = y;
  int nx2 = x + w - 1;
  int ny2 = y + h - 1;
  if (!gDirty.valid) {
    gDirty.valid = true;
    gDirty.x1 = nx1; gDirty.y1 = ny1; gDirty.x2 = nx2; gDirty.y2 = ny2;
  } else {
    if (nx1 < gDirty.x1) gDirty.x1 = nx1;
    if (ny1 < gDirty.y1) gDirty.y1 = ny1;
    if (nx2 > gDirty.x2) gDirty.x2 = nx2;
    if (ny2 > gDirty.y2) gDirty.y2 = ny2;
  }
  DBG_DIRTY("mark x=%d y=%d w=%d h=%d => [%d,%d]-[%d,%d]", x, y, w, h, gDirty.x1, gDirty.y1, gDirty.x2, gDirty.y2);
}

static void waitDisplaySafe(uint32_t timeoutMs = 12000) {
  uint32_t t0 = millis();
  while (M5.Display.displayBusy()) {
    delay(1);  // yields to avoid WDT resets during long e-ink transitions
    if ((millis() - t0) > timeoutMs) break;
  }
}

static void runDeepCleanCycle() {
  waitDisplaySafe();
  M5.Display.fillRect(0, 0, 540, 960, TFT_WHITE);
  M5.Display.display();
  waitDisplaySafe();

  lastDeepCleanMs = millis();
  resetDirtyRegion();
}

static void bootWhitePrime(uint8_t passes) {
  if (passes == 0) return;
  waitDisplaySafe();
  for (uint8_t i = 0; i < passes; i++) {
    clearRect(0, 0, 540, 960);
    M5.Display.display();
    waitDisplaySafe();
  }
  lastDeepCleanMs = millis();
  resetDirtyRegion();
}

static void present() {
  // Start one committed frame. If a previous update is still running, wait first.
  waitDisplaySafe();

  // Periodic anti-ghost deep-clean for long stays on the same screen.
  if (aggressiveRefresh && PERIODIC_DEEP_CLEAN_MS > 0 && screen != Screen::SLEEP) {
    uint32_t now = millis();
    if (lastDeepCleanMs == 0) {
      lastDeepCleanMs = now;
    } else if ((now - lastDeepCleanMs) >= PERIODIC_DEEP_CLEAN_MS) {
      runDeepCleanCycle();
    }
  }

  if (USE_DIRTY_REGION_COMMIT && gDirty.valid) {
    int w = gDirty.x2 - gDirty.x1 + 1;
    int h = gDirty.y2 - gDirty.y1 + 1;
    DBG_DIRTY("commit region x=%d y=%d w=%d h=%d", gDirty.x1, gDirty.y1, w, h);
    M5.Display.display(gDirty.x1, gDirty.y1, w, h);
  } else {
    DBG_DIRTY("commit full");
    M5.Display.display();
  }
  resetDirtyRegion();
}

static void presentDirtyRegionOnly() {
  if (!gDirty.valid) return;
  waitDisplaySafe();
  int w = gDirty.x2 - gDirty.x1 + 1;
  int h = gDirty.y2 - gDirty.y1 + 1;
  DBG_DIRTY("commit local region x=%d y=%d w=%d h=%d", gDirty.x1, gDirty.y1, w, h);
  M5.Display.display(gDirty.x1, gDirty.y1, w, h);
  resetDirtyRegion();
}

static void setEpdQuality() {
#ifdef M5GFX_USE_EPD
  M5.Display.setEpdMode(epd_mode_t::EPD_QUALITY);
#endif
}

static Rect topBarRect{0, 0, 540, 90};
static Rect topBarTitleRect{0, 0, 260, 90};
static Rect topBarDynRect{200, 0, 340, 90};
static constexpr int TOPBAR_TEXT_Y = 20;
static constexpr int TOPBAR_DYN_LEFT_SHIFT_PX = 16;
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
static Rect wifiStatusRect{0, 792, 540, 64};
static Rect wifiFooterRect{0, 860, 540, 120};

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
static Rect settingsTimeRow{20, 820, 500, 40};
static Rect setTimeBackBtn{20, 870, 140, 70};
static Rect setTimeSaveBtn{380, 870, 140, 70};
static Rect setTimeMinusBtn[5]{
  Rect{20, 240, 90, 60}, Rect{125, 240, 90, 60}, Rect{230, 240, 90, 60}, Rect{335, 240, 90, 60}, Rect{440, 240, 80, 60}
};
static Rect setTimeValueBox[5]{
  Rect{20, 320, 90, 70}, Rect{125, 320, 90, 70}, Rect{230, 320, 90, 70}, Rect{335, 320, 90, 70}, Rect{440, 320, 80, 70}
};
static Rect setTimePlusBtn[5]{
  Rect{20, 410, 90, 60}, Rect{125, 410, 90, 60}, Rect{230, 410, 90, 60}, Rect{335, 410, 90, 60}, Rect{440, 410, 80, 60}
};
static Rect analysisDateBtn{20, 94, 250, 52};
static Rect analysisWindowBtn{280, 94, 240, 52};

static String topBarTitleCache = "";
static bool topBarNeedsRedraw = true;
static char topBarStatusCache[80] = {0};
static char topBarBatteryCache[40] = {0};

static void clearWholeScreenWhite() {
  if (screenTransitionWhitePrime) {
    // One committed full-white substrate on screen transition.
    waitDisplaySafe();
    M5.Display.fillRect(0, 0, 540, 960, TFT_WHITE);
    M5.Display.display();
    waitDisplaySafe();
    screenTransitionWhitePrime = false;
    lastDeepCleanMs = millis();
    resetDirtyRegion();
  }

  clearRect(0, 0, 540, 960);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  topBarNeedsRedraw = true;
}

static void setScreen(Screen s) {
  if (screen != s) {
    screenTransitionWhitePrime = true;
    topBarNeedsRedraw = true;
    screen = s;
  }
}

static void clearRect(int x, int y, int w, int h) {
  M5.Display.fillRect(x, y, w, h, TFT_WHITE);
  markDirtyRegion(x, y, w, h);
}

static void markTopBarDirty(const char* reason) {
  topBarLiveDirty = true;
  DBG_CL("topbar dirty: %s", reason ? reason : "unknown");
}

static void refreshTopBarLiveIfNeeded() {
  if (!USE_DIRTY_REGION_COMMIT) return;
  if (!topBarLiveDirty) return;
  if (screen == Screen::SLEEP) return;

  switch (screen) {
    case Screen::MENU:
      drawTopBar("AirQ_M Logger");
      break;
    case Screen::AIRQ_LIST:
      drawListHeader();
      break;
    case Screen::AIRQ_DETAIL:
      if (selectedIdx >= 0 && selectedIdx < MAX_DEVS && devs[selectedIdx].used) {
        drawDetailHeader(devs[selectedIdx]);
      } else {
        drawTopBar("Sensor Detail");
      }
      break;
    case Screen::METRIC_SELECT:
      drawTopBar("Select Metrics (max 3)");
      break;
    case Screen::WIFI_LIST:
      drawTopBar("WiFi Network");
      break;
    case Screen::WIFI_PASS:
      drawTopBar("WiFi Password");
      break;
    case Screen::ANALYSIS:
      drawTopBar("Analysis");
      break;
    case Screen::ANALYSIS_SENSORS:
      drawTopBar("Select Sensors (max 10)");
      break;
    case Screen::SETTINGS:
      drawTopBar("Settings");
      break;
    case Screen::SET_TIME:
      drawTopBar("Set Date/Time");
      break;
    case Screen::SLEEP:
      break;
  }

  topBarLiveDirty = false;
  present();
}

static void formatCountCompact(uint32_t v, char* out, size_t outSize) {
  if (v < 1000UL) {
    snprintf(out, outSize, "%lu", (unsigned long)v);
  } else if (v < 1000000UL) {
    snprintf(out, outSize, "%.1fk", v / 1000.0f);
  } else {
    snprintf(out, outSize, "%.1fM", v / 1000000.0f);
  }
}

static void drawTopBarDynamicFields(bool forceRedraw) {
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);

  int bp = hubBatteryPct();
  char bat[40];
  if (bp >= 0) {
    snprintf(bat, sizeof(bat), "%d%%", bp);
  } else {
    snprintf(bat, sizeof(bat), "--%%");
  }

  bool wifiNow = (WiFi.status() == WL_CONNECTED);
  wifiConnected = wifiNow;
  String sdLabel = sdOk ? "SD" : "noSD";
  String wifiLabel = wifiNow ? "WiFi" : "noWiFi";
  char status[80];
  snprintf(status, sizeof(status), "%s %s", sdLabel.c_str(), wifiLabel.c_str());
  if (hasCloudUploadConfig()) {
    char sentBuf[16];
    char rxBuf[16];
    formatCountCompact(cloudSent, sentBuf, sizeof(sentBuf));
    formatCountCompact(rxPacketCount, rxBuf, sizeof(rxBuf));
    snprintf(status, sizeof(status), "%s %s CL %s(+%s)", sdLabel.c_str(), wifiLabel.c_str(), sentBuf, rxBuf);
  }

  bool redrawDyn = forceRedraw
                   || (strcmp(status, topBarStatusCache) != 0)
                   || (strcmp(bat, topBarBatteryCache) != 0);

  if (redrawDyn) {
    // Keep updates small: static title area is drawn once per screen entry.
    clearRect(topBarDynRect.x, topBarDynRect.y, topBarDynRect.w, topBarDynRect.h);

    int wBat = M5.Display.textWidth(bat);
    int batX = topBarDynRect.x + topBarDynRect.w - 20 - TOPBAR_DYN_LEFT_SHIFT_PX - wBat;
    if (batX < topBarDynRect.x + 2) batX = topBarDynRect.x + 2;

    int statusW = M5.Display.textWidth(status);
    int statusX = batX - 12 - statusW;
    if (statusX < topBarDynRect.x + 2) statusX = topBarDynRect.x + 2;
    M5.Display.setCursor(statusX, TOPBAR_TEXT_Y);
    M5.Display.print(status);

    // Keep battery visible even when status text gets long.
    M5.Display.setCursor(batX, TOPBAR_TEXT_Y);
    M5.Display.print(bat);
    snprintf(topBarStatusCache, sizeof(topBarStatusCache), "%s", status);
    snprintf(topBarBatteryCache, sizeof(topBarBatteryCache), "%s", bat);
  }
  topBarLiveDirty = false;
}

static void drawTopBarStaticBackground() {
  // Match footer behavior: hard white baseline per screen entry.
  clearRect(topBarRect.x, topBarRect.y, topBarRect.w, topBarRect.h);
}

static void drawTopBar(const char* title) {
  String newTitle = String(title);
  // Always re-establish full white header baseline (footer-style behavior).
  drawTopBarStaticBackground();
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, TOPBAR_TEXT_Y);
  M5.Display.print(title);

  topBarTitleCache = newTitle;
  topBarNeedsRedraw = false;
  topBarStatusCache[0] = '\0';
  topBarBatteryCache[0] = '\0';
  drawTopBarDynamicFields(true);
}

static void drawButton(const Rect& r, const char* label, bool filled) {
  uint32_t bg = filled ? TFT_BLACK : TFT_WHITE;
  uint32_t fg = filled ? TFT_WHITE : TFT_BLACK;
  M5.Display.fillRect(r.x - 2, r.y - 2, r.w + 4, r.h + 4, TFT_WHITE);
  M5.Display.fillRoundRect(r.x, r.y, r.w, r.h, 14, bg);
  M5.Display.drawRoundRect(r.x, r.y, r.w, r.h, 14, TFT_BLACK);
  M5.Display.setTextColor(fg, bg);
  M5.Display.setTextSize(2);
  int tw = M5.Display.textWidth(label);
  int tx = r.x + (r.w - tw) / 2;
  int ty = r.y + (r.h / 2) - 8;
  M5.Display.setCursor(tx, ty);
  M5.Display.print(label);
  markDirtyRegion(r.x - 2, r.y - 2, r.w + 4, r.h + 4);
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
  M5.Display.fillRect(r.x - 2, r.y - 2, r.w + 4, r.h + 4, TFT_WHITE);
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
  markDirtyRegion(r.x - 2, r.y - 2, r.w + 4, r.h + 4);
}

static void drawMenu() {
  setEpdQuality();
  clearWholeScreenWhite();
  drawTopBar("AirQ_M Logger");

  drawMenuTile(menuAirQ, "AirQ", "/icon/airq_icon.png");
  drawMenuTile(menuWifi, "WiFi", "/icon/wifi_icon.png");
  drawMenuTile(menuAnalysis, "Analysis", "/icon/graph_icon.png");
  drawMenuTile(menuSleep, "Sleep", "/icon/sleepmode_icon.png");
  drawMenuTile(menuOff, "OFF", "/icon/off_icon.png");
  drawMenuTile(menuSettings, "Settings", "/icon/setting_icon.png");

  clearRect(0, 840, 540, 120);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 868);
  M5.Display.print("Time: ");
  M5.Display.print(timestampString());

  present();
}

static void drawSleepScreen() {
  setEpdQuality();
  clearWholeScreenWhite();
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
  M5.Display.setCursor(20, 98);
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
  markDirtyRegion(r.x - 2, r.y - 2, r.w + 4, r.h + 4);
}

static void drawList() {
  setEpdQuality();
  clearWholeScreenWhite();
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
  listFooterLastRefreshMs = millis();
  present();

  listRenderedRows = row;
  listLastActiveCount = activeDeviceCount();
}

static void drawListSoftRefresh() {
  // User-requested behavior: redraw full white background and all list boxes each refresh.
  drawList();
}

static void updateListDirtyRows() {
  resetDirtyRegion();
  int row = 0;
  bool changed = false;
  if (topBarLiveDirty) {
    drawTopBar("AirQ Sensors");
    changed = true;
  }
  for (int i = 0; i < MAX_DEVS; i++) {
    if (!isDeviceActiveIdx(i)) continue;
    if (devDirty[i]) {
      DBG_DIRTY("list row update dev=%d row=%d", i, row);
      drawListRow(row, i, (i == selectedIdx));
      devDirty[i] = false;
      changed = true;
    }
    row++;
    if (row >= 6) break;
  }

  // If active rows decreased, clear old rows that are no longer valid.
  if (row < listRenderedRows) {
    for (int r = row; r < listRenderedRows && r < 6; r++) {
      Rect rr = listRowRect(r);
      clearRect(rr.x - 2, rr.y - 2, rr.w + 4, rr.h + 4);
      changed = true;
    }
  }
  listRenderedRows = row;
  listLastActiveCount = activeDeviceCount();

  if (changed) {
    presentDirtyRegionOnly();
  }
}

static void drawDetailHeader(const DeviceState& d) {
  drawTopBar("Sensor Detail");

  String macs = macToStr(d.mac);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 98);
  M5.Display.printf("Sensor %s", d.label);

  int w = M5.Display.textWidth(macs);
  M5.Display.setCursor(540 - 20 - w, 98);
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
  markDirtyRegion(r.x - 2, r.y - 2, r.w + 4, r.h + 4);
}

static void drawDetail(const DeviceState& d) {
  setEpdQuality();
  clearWholeScreenWhite();
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
  clearWholeScreenWhite();
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
  drawButton(metricBackBtn, "Back", false);
  drawButton(metricDoneBtn, "Done", false);
  present();
}


static const uint32_t intervalOptionsMs[] = {60000, 180000, 300000, 600000, 900000, 1800000, 3600000};
static const char* intervalOptionsLabel[] = {"1m", "3m", "5m", "10m", "15m", "30m", "1h"};

static void drawSettings() {
  setEpdQuality();
  clearWholeScreenWhite();
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

  M5.Display.fillRoundRect(settingsTimeRow.x, settingsTimeRow.y, settingsTimeRow.w, settingsTimeRow.h, 10, TFT_WHITE);
  M5.Display.drawRoundRect(settingsTimeRow.x, settingsTimeRow.y, settingsTimeRow.w, settingsTimeRow.h, 10, TFT_BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(settingsTimeRow.x + 20, settingsTimeRow.y + 10);
  M5.Display.print("Set Date/Time");
  String tNow = timestampString();
  M5.Display.setTextSize(1);
  int twNow = M5.Display.textWidth(tNow);
  M5.Display.setCursor(settingsTimeRow.x + settingsTimeRow.w - twNow - 16, settingsTimeRow.y + 13);
  M5.Display.print(tNow);

  clearRect(0, 860, 540, 120);
  drawButton(settingsBackBtn, "Back", false);
  present();
}

static int daysInMonth(int y, int m) {
  static const int dm[] = {31,28,31,30,31,30,31,31,30,31,30,31};
  if (m < 1) m = 1;
  if (m > 12) m = 12;
  int d = dm[m - 1];
  bool leap = ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0));
  if (m == 2 && leap) d = 29;
  return d;
}

static void normalizeSetTimeFields() {
  if (setYear < 2024) setYear = 2024;
  if (setYear > 2099) setYear = 2099;
  if (setMonth < 1) setMonth = 12;
  if (setMonth > 12) setMonth = 1;
  int maxD = daysInMonth(setYear, setMonth);
  if (setDay < 1) setDay = maxD;
  if (setDay > maxD) setDay = 1;
  if (setHour < 0) setHour = 23;
  if (setHour > 23) setHour = 0;
  if (setMinute < 0) setMinute = 59;
  if (setMinute > 59) setMinute = 0;
}

static void initSetTimeEditorFromCurrent() {
  uint32_t ep = nowEpoch();
  if (!ep && isReasonableEpoch(manualBaseEpoch)) ep = manualBaseEpoch;
  if (!ep) ep = MANUAL_TIME_MIN_EPOCH;
  time_t t = (time_t)ep;
  struct tm tmv;
  localtime_r(&t, &tmv);
  setYear = tmv.tm_year + 1900;
  setMonth = tmv.tm_mon + 1;
  setDay = tmv.tm_mday;
  setHour = tmv.tm_hour;
  setMinute = tmv.tm_min;
  normalizeSetTimeFields();
}

static void drawSetTime() {
  setEpdQuality();
  clearWholeScreenWhite();
  drawTopBar("Set Date/Time");

  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 120);
  M5.Display.print("Manual clock (offline)");

  const char* labels[5] = {"Year", "Mon", "Day", "Hour", "Min"};
  int vals[5] = {setYear, setMonth, setDay, setHour, setMinute};
  for (int i = 0; i < 5; i++) {
    M5.Display.setTextSize(2);
    int lw = M5.Display.textWidth(labels[i]);
    M5.Display.setCursor(setTimeValueBox[i].x + (setTimeValueBox[i].w - lw) / 2, 202);
    M5.Display.print(labels[i]);

    drawButton(setTimeMinusBtn[i], "-", false);
    M5.Display.fillRoundRect(setTimeValueBox[i].x, setTimeValueBox[i].y, setTimeValueBox[i].w, setTimeValueBox[i].h, 10, TFT_WHITE);
    M5.Display.drawRoundRect(setTimeValueBox[i].x, setTimeValueBox[i].y, setTimeValueBox[i].w, setTimeValueBox[i].h, 10, TFT_BLACK);
    drawButton(setTimePlusBtn[i], "+", false);

    char vb[12];
    if (i == 0) snprintf(vb, sizeof(vb), "%04d", vals[i]);
    else snprintf(vb, sizeof(vb), "%02d", vals[i]);
    M5.Display.setTextSize(3);
    int vw = M5.Display.textWidth(vb);
    M5.Display.setCursor(setTimeValueBox[i].x + (setTimeValueBox[i].w - vw) / 2, setTimeValueBox[i].y + 20);
    M5.Display.print(vb);
  }

  M5.Display.setTextSize(1);
  M5.Display.setCursor(20, 510);
  if (manualTimeValid) {
    M5.Display.print("Saved time is used for TSV time_iso/epoch.");
  } else {
    M5.Display.print("NO TIME SET: logging is paused until Save.");
  }

  clearRect(0, 860, 540, 120);
  drawButton(setTimeBackBtn, "Back", false);
  drawButton(setTimeSaveBtn, "Save", false);
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

static const char* analysisWindowLabel() {
  switch (analysisWindow) {
    case AnalysisWindow::ALL: return "Window:All";
    case AnalysisWindow::H08_12: return "Window:08-12";
    case AnalysisWindow::H12_16: return "Window:12-16";
    case AnalysisWindow::H16_20: return "Window:16-20";
    case AnalysisWindow::H20_24: return "Window:20-24";
  }
  return "Window:All";
}

static bool isAnalysisTimeInWindow(const String& timeIso) {
  if (analysisWindow == AnalysisWindow::ALL) return true;
  if (timeIso.length() < 16) return false;
  int hh = timeIso.substring(11, 13).toInt();
  switch (analysisWindow) {
    case AnalysisWindow::H08_12: return (hh >= 8 && hh < 12);
    case AnalysisWindow::H12_16: return (hh >= 12 && hh < 16);
    case AnalysisWindow::H16_20: return (hh >= 16 && hh < 20);
    case AnalysisWindow::H20_24: return (hh >= 20 && hh < 24);
    case AnalysisWindow::ALL: return true;
  }
  return true;
}

static String selectedAnalysisDate() {
  if (analysisDateCount <= 0) return "";
  if (analysisDateSel == -2) return "";
  if (analysisDateSel < 0 || analysisDateSel >= analysisDateCount) {
    return analysisDates[analysisDateCount - 1];
  }
  return analysisDates[analysisDateSel];
}

static void scanAnalysisDates(const String& filePath) {
  analysisDateCount = 0;
  if (!sdOk || filePath.length() == 0) return;
  File f = SD.open(filePath, FILE_READ);
  if (!f) return;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() == 0) continue;
    if (isLikelyTsvHeaderLine(line)) continue;
    int p = line.indexOf('\t');
    String tIso = (p >= 0) ? line.substring(0, p) : line;
    if (tIso.length() < 10 || tIso == "NO_TIME" || tIso == "NO TIME SET") continue;
    String d = tIso.substring(0, 10);
    bool exists = false;
    for (int i = 0; i < analysisDateCount; i++) {
      if (analysisDates[i] == d) { exists = true; break; }
    }
    if (!exists && analysisDateCount < 16) {
      analysisDates[analysisDateCount++] = d;
    }
  }
  f.close();
  if (analysisDateCount == 0) {
    analysisDateSel = -1;
  } else if (analysisDateSel >= analysisDateCount) {
    analysisDateSel = analysisDateCount - 1;
  }
}

static bool isReadableLogFile(const String& filePath) {
  if (!sdOk || filePath.length() == 0) return false;
  File f = SD.open(filePath, FILE_READ);
  if (!f) return false;
  bool ok = f.size() > 0;
  f.close();
  return ok;
}

static bool scanMaxX(const String& sourceFile, uint32_t& maxX, bool& useEpoch, const String& dateFilter) {
  maxX = 0;
  useEpoch = false;
  if (!sdOk || sourceFile.length() == 0) return false;
  File f = SD.open(sourceFile, FILE_READ);
  if (!f) return false;

  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() == 0) continue;
    if (isLikelyTsvHeaderLine(line)) continue;

    int col = 0;
    int start = 0;
    uint32_t epoch = 0;
    uint32_t elapsed = 0;
    String timeIso;

    for (int i = 0; i <= line.length(); i++) {
      if (i == line.length() || line[i] == '\t') {
        String token = line.substring(start, i);
        if (col == 0) timeIso = token;
        if (col == 1 && token.length()) epoch = (uint32_t)token.toInt();
        if (col == 2 && token.length()) elapsed = (uint32_t)token.toInt();
        col++;
        start = i + 1;
      }
    }

    if (dateFilter.length() > 0) {
      if (timeIso.length() < 10 || timeIso.substring(0, 10) != dateFilter) continue;
    }
    if (!isAnalysisTimeInWindow(timeIso)) continue;

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

static bool readAnalysisData(const String& sourceFile, float* xs, float* ys, uint8_t* sids, int maxPts, int& count, bool& useEpoch, uint32_t windowSec, const String& dateFilter) {
  count = 0;
  useEpoch = false;
  if (!sdOk || sourceFile.length() == 0) return false;

  int latestSessionStart = 0;
  if (dateFilter.length() == 0) {
    File sf = SD.open(sourceFile, FILE_READ);
    if (sf) {
      int dataLine = -1;
      float prevElapsed = -1.0f;
      while (sf.available()) {
        String line = sf.readStringUntil('\n');
        if (line.length() == 0) continue;
        if (isLikelyTsvHeaderLine(line)) continue;
        dataLine++;
        int col = 0;
        int start = 0;
        uint32_t epoch = 0;
        float elapsed = -1.0f;
        for (int i = 0; i <= line.length(); i++) {
          if (i == line.length() || line[i] == '\t') {
            String token = line.substring(start, i);
            if (col == 1 && token.length()) epoch = (uint32_t)token.toInt();
            if (col == 2 && token.length()) elapsed = token.toFloat();
            col++;
            start = i + 1;
          }
        }
        if (epoch == 0 && elapsed >= 0.0f) {
          if (prevElapsed >= 0.0f && elapsed + 30.0f < prevElapsed) {
            latestSessionStart = dataLine;
          }
          prevElapsed = elapsed;
        }
      }
      sf.close();
    }
  }

  uint32_t maxX = 0;
  if (!scanMaxX(sourceFile, maxX, useEpoch, dateFilter)) return false;

  if (!useEpoch && dateFilter.length() == 0 && latestSessionStart > 0) {
    File mf = SD.open(sourceFile, FILE_READ);
    if (mf) {
      int dataLine = -1;
      uint32_t mx = 0;
      while (mf.available()) {
        String line = mf.readStringUntil('\n');
        if (line.length() == 0) continue;
        if (isLikelyTsvHeaderLine(line)) continue;
        dataLine++;
        if (dataLine < latestSessionStart) continue;
        int col = 0;
        int start = 0;
        uint32_t elapsed = 0;
        for (int i = 0; i <= line.length(); i++) {
          if (i == line.length() || line[i] == '\t') {
            String token = line.substring(start, i);
            if (col == 2 && token.length()) elapsed = (uint32_t)token.toInt();
            col++;
            start = i + 1;
          }
        }
        if (elapsed > mx) mx = elapsed;
      }
      mf.close();
      if (mx > 0) maxX = mx;
    }
  }

  uint32_t startX = (windowSec > 0 && maxX > windowSec) ? (maxX - windowSec) : 0;

  File f = SD.open(sourceFile, FILE_READ);
  if (!f) return false;

  int totalLines = 0;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() == 0) continue;
    if (isLikelyTsvHeaderLine(line)) continue;
    totalLines++;
  }
  f.close();

  int skip = (totalLines > maxPts) ? (totalLines / maxPts) : 1;

  f = SD.open(sourceFile, FILE_READ);
  if (!f) return false;

  int dataLine = -1;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    if (line.length() == 0) continue;
    if (isLikelyTsvHeaderLine(line)) continue;
    dataLine++;
    if (!useEpoch && dateFilter.length() == 0 && dataLine < latestSessionStart) continue;
    if (skip > 1 && (dataLine % skip) != 0) continue;

    int col = 0;
    int start = 0;
    uint32_t epoch = 0;
    float elapsed = 0;
    String timeIso;
    String labelStr;
    String valStr;
    // If row is missing wifi_sync column, adjust indices on the fly
    int colCount = 1;
    for (int i = 0; i < line.length(); i++) if (line[i] == "	"[0]) colCount++;
    bool rowHasWifiSync = (colCount >= 17);
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
        if (col == 0) timeIso = token;
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

    if (dateFilter.length() > 0) {
      if (timeIso.length() < 10 || timeIso.substring(0, 10) != dateFilter) continue;
    }
    if (!isAnalysisTimeInWindow(timeIso)) continue;

    float x = (epoch > 0) ? (float)epoch : elapsed;
    if (windowSec > 0 && x < (float)startX) continue;
    if (valStr.length() == 0) continue;

    int sid = labelIndexFromToken(labelStr);
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

static void drawAnalysisFilterSelectors(const String& dateFilter) {
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setTextSize(2);

  M5.Display.fillRoundRect(analysisDateBtn.x, analysisDateBtn.y, analysisDateBtn.w, analysisDateBtn.h, 6, TFT_WHITE);
  M5.Display.drawRoundRect(analysisDateBtn.x, analysisDateBtn.y, analysisDateBtn.w, analysisDateBtn.h, 6, TFT_BLACK);
  M5.Display.setCursor(analysisDateBtn.x + 10, analysisDateBtn.y + 18);
  M5.Display.print("Day: ");
  if (analysisDateCount <= 0) {
    M5.Display.print("No timestamp");
  } else if (analysisDateSel == -2) {
    M5.Display.print("All");
  } else if (analysisDateSel == -1) {
    M5.Display.print("Latest");
  } else {
    M5.Display.print(dateFilter);
  }

  M5.Display.fillRoundRect(analysisWindowBtn.x, analysisWindowBtn.y, analysisWindowBtn.w, analysisWindowBtn.h, 6, TFT_WHITE);
  M5.Display.drawRoundRect(analysisWindowBtn.x, analysisWindowBtn.y, analysisWindowBtn.w, analysisWindowBtn.h, 6, TFT_BLACK);
  M5.Display.setCursor(analysisWindowBtn.x + 10, analysisWindowBtn.y + 18);
  M5.Display.print(analysisWindowLabel());
}

static void drawAnalysisFooterButtons() {
  clearRect(0, 860, 540, 120);
  drawButton(analysisBackBtn, "Back", false);
  drawButton(analysisMetricBtn, "Metric", false);
  drawButton(analysisZoomBtn, zoomLabel(), false);
  drawButton(analysisSensorsBtn, "Sensors", false);
}

static void drawAnalysis() {
  setEpdQuality();
  clearWholeScreenWhite();
  drawTopBar("Analysis");
  clearRect(0, 90, 540, 770);

  if (!sdOk) {
    refreshSdState();
  }
  if (sdOk) scanLogFiles();

  String candidates[20];
  int candCount = 0;
  auto addCand = [&](const String& p) {
    if (p.length() == 0) return;
    if (!isReadableLogFile(p)) return;
    for (int i = 0; i < candCount; i++) {
      if (candidates[i] == p) return;
    }
    if (candCount < 20) candidates[candCount++] = p;
  };

  // Prefer active source first, then explicit/common fallbacks.
  addCand(logFilePath);
  addCand(currentLogFile());
  addCand(String("/airq_data_log.tsv"));
  for (int i = 0; i < logFileCount; i++) addCand(logFiles[i]);

  if (!sdOk || candCount == 0) {
    drawAnalysisFilterSelectors("");
    M5.Display.setTextSize(2);
    M5.Display.setCursor(40, 380);
    M5.Display.print("No SD card / no data");
    drawAnalysisFooterButtons();
    present();
    return;
  }

  static float xs[200];
  static float ys[200];
  static uint8_t sids[200];

  if (analysisSelectedCount() == 0) {
    scanAnalysisDates(candidates[0]);
    String dateFilter0 = selectedAnalysisDate();
    if (analysisDateCount == 0) dateFilter0 = "";
    drawAnalysisFilterSelectors(dateFilter0);
    M5.Display.setTextSize(2);
    M5.Display.setCursor(40, 380);
    M5.Display.print("Select sensors to plot");
    drawAnalysisFooterButtons();
    present();
    return;
  }

  int count = 0;
  bool useEpoch = false;
  uint32_t win = zoomSeconds();
  bool ok = false;
  String dateFilter = "";
  int desiredDateSel = analysisDateSel;

  for (int i = 0; i < candCount; i++) {
    analysisDateSel = desiredDateSel;
    scanAnalysisDates(candidates[i]);
    String candidateDateFilter = selectedAnalysisDate();
    if (analysisDateCount == 0) candidateDateFilter = "";
    int candidateCount = 0;
    bool candidateUseEpoch = false;
    if (readAnalysisData(candidates[i], xs, ys, sids, 200, candidateCount, candidateUseEpoch, win, candidateDateFilter)) {
      ok = true;
      count = candidateCount;
      useEpoch = candidateUseEpoch;
      dateFilter = candidateDateFilter;
      break;
    }
  }

  if (!ok) {
    // Keep selectors meaningful even if selected file has no rows.
    analysisDateSel = desiredDateSel;
    scanAnalysisDates(candidates[0]);
    dateFilter = selectedAnalysisDate();
    if (analysisDateCount == 0) dateFilter = "";
  }

  drawAnalysisFilterSelectors(dateFilter);

  if (!ok) {
    M5.Display.setTextSize(2);
    M5.Display.setCursor(40, 380);
    M5.Display.print("No data in selected day/window");
    drawAnalysisFooterButtons();
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
  if (span < 1e-6f) span = 1.0f;
  Rect plot{86, 210, 424, 560};
  float sum = 0.0f;
  int nsum = 0;
  for (int i = 0; i < count; i++) {
    sum += ys[i];
    nsum++;
  }
  float avg = (nsum > 0) ? (sum / nsum) : 0.0f;

  M5.Display.setTextSize(2);
  char avgBuf[60];
  if (analysisMetric == SummaryMetric::CO2) {
    int avgI = (int)(avg + 0.5f);
    int minI = (int)(minY + 0.5f);
    int maxI = (int)(maxY + 0.5f);
    snprintf(avgBuf, sizeof(avgBuf), "Avg: %d   Min: %d   Max: %d", avgI, minI, maxI);
  } else {
    snprintf(avgBuf, sizeof(avgBuf), "Avg: %.2f   Min: %.2f   Max: %.2f", avg, minY, maxY);
  }
  int avgW = M5.Display.textWidth(avgBuf);
  int avgY = plot.y + plot.h + 28;
  M5.Display.setCursor((540 - avgW) / 2, avgY);
  M5.Display.print(avgBuf);

  M5.Display.drawRect(plot.x, plot.y, plot.w, plot.h, TFT_BLACK);

  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 160);
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

  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 182);
  M5.Display.print(metricLabelFn(analysisMetric));
  M5.Display.print(" (");
  M5.Display.print(metricUnitFn(analysisMetric));
  M5.Display.print(")");

  int ticks = 4;
  for (int i = 0; i <= ticks; i++) {
    int y = plot.y + plot.h - (plot.h * i / ticks);
    M5.Display.drawLine(plot.x - 4, y, plot.x, y, TFT_BLACK);
    float v = minY + (maxY - minY) * i / ticks;
    M5.Display.setTextSize(ANALYSIS_AXIS_TEXT_SIZE);
    M5.Display.setCursor(8, y - 8);
    if (analysisMetric == SummaryMetric::CO2) {
      M5.Display.printf("%d", (int)(v + 0.5f));
    } else {
      M5.Display.printf("%.1f", v);
    }
  }

  for (int i = 0; i <= ticks; i++) {
    int x = plot.x + (plot.w * i / ticks);
    M5.Display.drawLine(x, plot.y + plot.h, x, plot.y + plot.h + 4, TFT_BLACK);
    float v = minX + (maxX - minX) * i / ticks;
    char buf[16];
    formatTimeLabel(buf, sizeof(buf), useEpoch, v, span);
    M5.Display.setTextSize(ANALYSIS_AXIS_TEXT_SIZE);
    int tw = M5.Display.textWidth(buf);
    M5.Display.setCursor(x - tw / 2, plot.y + plot.h + 10);
    M5.Display.print(buf);
  }

  int lastX[MAX_DEVS];
  int lastY[MAX_DEVS];
  bool lastOk[MAX_DEVS];
  for (int i = 0; i < MAX_DEVS; i++) lastOk[i] = false;

  for (int i = 0; i < count; i++) {
    int x = plot.x + (int)((xs[i] - minX) / span * plot.w);
    int y = plot.y + plot.h - (int)((ys[i] - minY) / (maxY - minY) * plot.h);
    int sid = sids[i];
    if (sid >= 0 && sid < MAX_DEVS) {
      if (!ANALYSIS_PLOT_DOTS && lastOk[sid]) {
        M5.Display.drawLine(lastX[sid], lastY[sid], x, y, TFT_BLACK);
      }
      if (ANALYSIS_PLOT_DOTS) {
        M5.Display.fillCircle(x, y, ANALYSIS_DOT_RADIUS, TFT_BLACK);
      }
      lastX[sid] = x;
      lastY[sid] = y;
      lastOk[sid] = true;
    }
  }

  drawAnalysisFooterButtons();
  present();
}


static void drawAnalysisSensors() {
  setEpdQuality();
  clearWholeScreenWhite();
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

static void drawWifiFooterBlock() {
  clearRect(wifiStatusRect.x, wifiStatusRect.y, wifiStatusRect.w, wifiStatusRect.h);
  M5.Display.setTextSize(1);
  M5.Display.setTextColor(TFT_BLACK, TFT_WHITE);
  M5.Display.setCursor(20, 808);
  M5.Display.print("Cloud mode: ");
  M5.Display.print(cloudScheduleEnabled ? "Burst 3h" : "Always On");
  if (wifiStatusMsg.length()) {
    M5.Display.setCursor(20, 828);
    M5.Display.print(wifiStatusMsg);
  }

  clearRect(wifiFooterRect.x, wifiFooterRect.y, wifiFooterRect.w, wifiFooterRect.h);
  drawButton(wifiBackBtn, "Back", false);
  drawButton(wifiUploadBtn, "Upload", false);
  drawButton(wifiScanBtn, "Scan", false);
}

static void drawWifiList() {
  setEpdQuality();
  clearWholeScreenWhite();
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
  drawWifiFooterBlock();
  present();
}

static void drawWifiPass() {
  setEpdQuality();
  clearWholeScreenWhite();
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
static void addWifiSsidUnique(const String& ssid) {
  if (!ssid.length()) return;
  for (int i = 0; i < wifiCount; i++) {
    if (wifiSsids[i] == ssid) return;
  }
  if (wifiCount < WIFI_LIST_MAX) {
    wifiSsids[wifiCount++] = ssid;
  }
}

static void wifiScan() {
  if (wifiScanRunning || wifiOpActive) {
    wifiStatusMsg = "Scanning...";
    DBG_WIFI("scan request ignored: running=%d opActive=%d", wifiScanRunning ? 1 : 0, wifiOpActive ? 1 : 0);
    return;
  }

  wifiOpActive = true;
  wifiStatusMsg = "Scanning...";
  wifiCount = 0;
  wifiPage = 0;

  DBG_WIFI("scan start");
  wifiScanResumeEspNow = espNowActive;
  if (wifiScanResumeEspNow) espnowStop();

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.disconnect(false, true);
  delay(80);
  WiFi.scanDelete();

  // Async scan to avoid UI freeze.
  int rc = WiFi.scanNetworks(true, false, false, 120, 0);
  DBG_WIFI("scan rc=%d", rc);
  if (rc == WIFI_SCAN_FAILED) {
    wifiStatusMsg = "Scan failed";
    wifiScanRunning = false;
    wifiOpActive = false;
    WiFi.disconnect(true, true);
    if (wifiScanResumeEspNow) espnowStart();
    wifiScanResumeEspNow = false;
    DBG_WIFI("scan start failed");
    return;
  }

  wifiScanRunning = true;
  wifiScanStartedMs = millis();
}

static void wifiScanPoll() {
  if (!wifiScanRunning) return;

  int n = WiFi.scanComplete();
  DBG_WIFI("scan poll n=%d", n);
  if (n == WIFI_SCAN_RUNNING) {
    if (millis() - wifiScanStartedMs > WIFI_SCAN_TIMEOUT_MS) {
      wifiStatusMsg = "Scan timeout";
      WiFi.scanDelete();
      WiFi.disconnect(true, true);
      wifiScanRunning = false;
      wifiOpActive = false;
      if (wifiScanResumeEspNow) espnowStart();
      wifiScanResumeEspNow = false;
      DBG_WIFI("scan timeout");
      if (screen == Screen::WIFI_LIST) drawWifiList();
    }
    return;
  }

  wifiCount = 0;
  if (n > 0) {
    for (int i = 0; i < n && wifiCount < WIFI_LIST_MAX; i++) {
      addWifiSsidUnique(WiFi.SSID(i));
    }
  }

  // Fallback: if async result is empty/failed, try one blocking scan.
  if (wifiCount == 0 && n <= 0) {
    DBG_WIFI("fallback blocking scan");
    int nb = WiFi.scanNetworks(false, false, false, 120, 0);
    DBG_WIFI("fallback result=%d", nb);
    if (nb > 0) {
      for (int i = 0; i < nb && wifiCount < WIFI_LIST_MAX; i++) {
        addWifiSsidUnique(WiFi.SSID(i));
      }
      n = nb;
    }
  }

  if (n == WIFI_SCAN_FAILED) {
    wifiStatusMsg = "Scan failed";
  } else if (wifiCount == 0) {
    wifiStatusMsg = "No networks found";
  } else {
    wifiStatusMsg = String(wifiCount) + " networks";
  }
  DBG_WIFI("scan done n=%d listed=%d", n, wifiCount);

  WiFi.scanDelete();
  WiFi.disconnect(true, true);
  wifiScanRunning = false;
  wifiOpActive = false;
  if (wifiScanResumeEspNow) espnowStart();
  wifiScanResumeEspNow = false;
  if (screen == Screen::WIFI_LIST) drawWifiList();
}

static bool wifiConnect(const String& ssid, const String& pass, bool showUI = true) {
  if (wifiOpActive && !wifiScanRunning) {
    DBG_WIFI("connect blocked by active op");
    return false;
  }
  if (wifiScanRunning) {
    WiFi.scanDelete();
    wifiScanRunning = false;
    wifiOpActive = false;
    if (wifiScanResumeEspNow) {
      espnowStart();
      wifiScanResumeEspNow = false;
    }
  }

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
  wifiOpActive = true;
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
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

    if (ALLOW_WIFI_NTP_TIME_SYNC && !manualTimeValid) {
      setupZurichTZ();
      configTime(0, 0, "pool.ntp.org", "time.google.com");
      captureSystemTimeIfValid();
    }
    wifiReconnectFailCount = 0;
    wifiLastReconnectTryMs = millis();
    if (cloudScheduleEnabled) {
      wifiStatusMsg = "Saved (Burst 3h)";
      wifiDisconnectSilent();
    } else {
      wifiStatusMsg = "Connected";
    }
    markTopBarDirty("wifi_connected");
  } else {
    wifiConnected = false;
    wifiStatusMsg = "Connect failed";
    WiFi.disconnect(true, true);
    markTopBarDirty("wifi_connect_fail");
  }

  if (wasEsp) {
    espnowStart();
  }
  wifiOpActive = false;
  return ok;
}

static bool wifiConnectSilent(uint32_t timeoutMs) {
  if (!uiWifiSsid.length()) return false;
  if (wifiOpActive && !wifiScanRunning) return false;

  if (wifiScanRunning) {
    WiFi.scanDelete();
    wifiScanRunning = false;
    wifiOpActive = false;
    if (wifiScanResumeEspNow) {
      espnowStart();
      wifiScanResumeEspNow = false;
    }
  }

  wifiOpActive = true;
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(uiWifiSsid.c_str(), uiWifiPass.c_str());

  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < timeoutMs) {
    delay(100);
  }

  bool ok = (WiFi.status() == WL_CONNECTED);
  wifiConnected = ok;
  markTopBarDirty(ok ? "wifi_silent_ok" : "wifi_silent_fail");
  if (ok && ALLOW_WIFI_NTP_TIME_SYNC && !manualTimeValid) {
    setupZurichTZ();
    configTime(0, 0, "pool.ntp.org", "time.google.com");
    captureSystemTimeIfValid();
  }
  wifiOpActive = false;
  return ok;
}

static void wifiDisconnectSilent() {
  WiFi.disconnect(true, true);
  wifiConnected = false;
  markTopBarDirty("wifi_disconnected");
}

static void cloudScheduledUploadTick() {
  if (!cloudUploadEnabled || !cloudScheduleEnabled) return;
  if (!hasCloudUploadConfig()) return;
  if (!uiWifiSsid.length()) return;

  uint32_t nowMs = millis();
  if (cloudLastScheduleMs != 0 && (nowMs - cloudLastScheduleMs) < CLOUD_UPLOAD_PERIOD_MS) return;
  cloudLastScheduleMs = nowMs;

  bool wasEsp = espNowActive;
  if (wasEsp) espnowStop();

  if (wifiConnectSilent(10000)) {
    String uploadMsg;
    uint32_t uploadBytes = 0;
    uploadActiveLogFileMultipart(uploadMsg, uploadBytes);
    wifiStatusMsg = uploadMsg;
    wifiDisconnectSilent();
  }

  if (wasEsp) espnowStart();
}

static void wifiAutoReconnectTick() {
  static constexpr bool WIFI_AUTORECONNECT_ONLY_WIFI_SCREEN = true;
  if (cloudScheduleEnabled) return;
  if (!uiWifiSsid.length()) return;

  bool inWifiUi = (screen == Screen::WIFI_LIST || screen == Screen::WIFI_PASS);
  if (WIFI_AUTORECONNECT_ONLY_WIFI_SCREEN && !inWifiUi) return;
  if (wifiScanRunning || wifiOpActive) return;

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
      setScreen(Screen::AIRQ_DETAIL);
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
    if (!hasCloudUploadConfig()) {
      wifiStatusMsg = "No cloud config";
      drawWifiList();
      return;
    }

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

    uint32_t uploadBytes = 0;
    String uploadMsg;
    uploadActiveLogFileMultipart(uploadMsg, uploadBytes);
    wifiStatusMsg = uploadMsg;
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
      String chosenSsid = wifiSsids[idx];
      // Keep remembered password when user picks the same SSID.
      if (chosenSsid != uiWifiSsid) {
        uiWifiSsid = chosenSsid;
        uiWifiPass = "";
      }
      setScreen(Screen::WIFI_PASS);
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
  if (analysisDateBtn.contains(x, y)) {
    if (analysisDateCount > 0) {
      if (analysisDateSel == -1) analysisDateSel = -2;                  // Latest -> All
      else if (analysisDateSel == -2) analysisDateSel = 0;              // All -> first explicit day
      else if (analysisDateSel >= 0 && analysisDateSel < analysisDateCount - 1) analysisDateSel++;
      else analysisDateSel = -1;                                         // last explicit day -> Latest
    }
    drawAnalysis();
    return;
  }
  if (analysisWindowBtn.contains(x, y)) {
    analysisWindow = (AnalysisWindow)((((int)analysisWindow) + 1) % 5);
    drawAnalysis();
    return;
  }
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

  if (settingsTimeRow.contains(x, y)) {
    initSetTimeEditorFromCurrent();
    setScreen(Screen::SET_TIME);
    drawSetTime();
    return;
  }

  if (settingsBackBtn.contains(x, y)) {
    setScreen(Screen::MENU);
    drawMenu();
    return;
  }
}

static void handleSetTimeTouch(int x, int y) {
  if (setTimeBackBtn.contains(x, y)) {
    setScreen(Screen::SETTINGS);
    drawSettings();
    return;
  }

  if (setTimeSaveBtn.contains(x, y)) {
    struct tm tmv{};
    tmv.tm_year = setYear - 1900;
    tmv.tm_mon = setMonth - 1;
    tmv.tm_mday = setDay;
    tmv.tm_hour = setHour;
    tmv.tm_min = setMinute;
    tmv.tm_sec = 0;
    time_t t = mktime(&tmv);
    uint32_t ep = (uint32_t)t;
    if (isReasonableEpoch(ep)) {
      setSystemClockAndRtc(ep);
      manualTimeValid = true;
      manualBaseEpoch = ep;
      manualBaseMillis = millis();
      manualLastPersistMs = millis();
      hasNtpTime = true;
      saveManualTimeToPrefs();
      wifiStatusMsg = "Manual time saved";
    } else {
      manualTimeValid = false;
      manualBaseEpoch = 0;
      manualBaseMillis = millis();
      hasNtpTime = false;
      saveManualTimeToPrefs();
      wifiStatusMsg = "Invalid time";
    }
    markTopBarDirty("manual_time_set");
    setScreen(Screen::SETTINGS);
    drawSettings();
    return;
  }

  for (int i = 0; i < 5; i++) {
    if (setTimeMinusBtn[i].contains(x, y)) {
      if (i == 0) setYear--;
      if (i == 1) setMonth--;
      if (i == 2) setDay--;
      if (i == 3) setHour--;
      if (i == 4) setMinute--;
      normalizeSetTimeFields();
      drawSetTime();
      return;
    }
    if (setTimePlusBtn[i].contains(x, y)) {
      if (i == 0) setYear++;
      if (i == 1) setMonth++;
      if (i == 2) setDay++;
      if (i == 3) setHour++;
      if (i == 4) setMinute++;
      normalizeSetTimeFields();
      drawSetTime();
      return;
    }
  }
}

// ============================
// OFF: deep sleep
// ============================
static void goOffDeepSleep() {
  setEpdQuality();
  clearWholeScreenWhite();
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

static const char* resetReasonText(esp_reset_reason_t r) {
  switch (r) {
    case ESP_RST_UNKNOWN:   return "UNKNOWN";
    case ESP_RST_POWERON:   return "POWERON";
    case ESP_RST_EXT:       return "EXT_PIN";
    case ESP_RST_SW:        return "SW_RESET";
    case ESP_RST_PANIC:     return "PANIC";
    case ESP_RST_INT_WDT:   return "INT_WDT";
    case ESP_RST_TASK_WDT:  return "TASK_WDT";
    case ESP_RST_WDT:       return "WDT";
    case ESP_RST_DEEPSLEEP: return "DEEPSLEEP";
    case ESP_RST_BROWNOUT:  return "BROWNOUT";
    case ESP_RST_SDIO:      return "SDIO";
    default:                return "OTHER";
  }
}

// ============================
// Setup / loop
// ============================
void setup() {
  Serial.begin(115200);
  delay(20);
  esp_reset_reason_t rr = esp_reset_reason();
  Serial.printf("[BOOT] reset_reason=%d (%s)\n", (int)rr, resetReasonText(rr));

  auto cfg = M5.config();
  cfg.clear_display = true;
  cfg.output_power = true;   // helps Paper S3 power rail
  M5.begin(cfg);

  M5.Display.wakeup();
  M5.Display.setRotation(0);
  setupZurichTZ();

  // Startup substrate clean: same white-fill logic, repeated for a clean baseline.
  bootWhitePrime(3);

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
  lastDeepCleanMs = hubStartMs;
  bootBlockOffUntil = millis() + 2000;

  prefs.begin("airq", false);
  loadManualTimeFromPrefs();
  // Prefer hardware/system clock (RTC-backed if available) on every boot.
  captureSystemTimeIfValid();
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

  // SD init
  sdOk = SD.begin();
  if (sdOk) {
    openLogFile();
    scanLogFiles();
    loadCloudConfigFromSd();
  }

  // Optional one-time NTP fallback (only if manual time not set)
  tryNtpOnce();

  // Optional boot auto-connect (default OFF to preserve ESP-NOW priority).
  if (WIFI_AUTO_CONNECT_ON_BOOT && uiWifiSsid.length()) {
    wifiConnect(uiWifiSsid, uiWifiPass, false);
    if (cloudScheduleEnabled && (wifiConnected || WiFi.status() == WL_CONNECTED)) {
      wifiDisconnectSilent();
    }
  } else {
    wifiConnected = false;
  }

  // ESP-NOW
  espnowStart();

  for (int i = 0; i < MAX_DEVS; i++) devDirty[i] = false;

  setScreen(Screen::MENU);
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

  wifiScanPoll();

  if (ENABLE_PHYSICAL_SLEEP_OFF_SHORTCUTS) {
    // BtnA short: Sleep/Wake
    if (M5.BtnA.wasPressed()) {
      if (!asleep) {
        asleep = true;
        setScreen(Screen::SLEEP);
        espnowStop();
        drawSleepScreen();
      } else {
        asleep = false;
        espnowStart();
        setScreen(Screen::MENU);
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
  }

  static bool touchLatch = false;

  if (asleep) {
    auto t = M5.Touch.getDetail();
    bool touchEvent = t.wasClicked() || t.wasPressed();
    if (touchEvent && !touchLatch) {
      touchLatch = true;
      asleep = false;
      setScreen(Screen::MENU);
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
  bool anyPacketRx = false;
  while (qPop(p)) {
    rxPacketCount++;
    anyPacketRx = true;
    DBG_CL("packet rx count=%lu seq=%lu", (unsigned long)rxPacketCount, (unsigned long)p.seq);
    int idx = findOrCreateDevice(p.srcMac);
    if (idx >= 0) {
      devs[idx].last = p;
      devs[idx].pktCount++;
      devs[idx].lastSeenMs = millis();
      devDirty[idx] = true;
      anyNew = true;
      uint32_t nowMs = millis();
      if (lastLogMs[idx] == 0 || (nowMs - lastLogMs[idx]) >= logIntervalMs) {
        if (nowEpoch() > 0) {
          appendTsv(p, devs[idx].label);
          lastLogMs[idx] = nowMs;
        }
      }
    }
  }
  if (anyPacketRx) {
    uint32_t nowMs = millis();
    if ((nowMs - topBarPacketDirtyLastMs) >= TOPBAR_PACKET_DIRTY_MIN_MS) {
      topBarPacketDirtyLastMs = nowMs;
      markTopBarDirty("espnow_packet");
    }
  }

  // Touch handling (robust against long blocking cycles)
  auto t = M5.Touch.getDetail();
  bool touchEvent = t.wasClicked() || t.wasPressed();
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
    else if (screen == Screen::SET_TIME) handleSetTimeTouch(x, y);
    else if (screen == Screen::SLEEP) {
      asleep = false;
      setScreen(Screen::MENU);
      espnowStart();
      drawMenu();
    }
  }
  if (!t.isPressed()) touchLatch = false;

  // Auto refresh on new packets (changed rows only)
  static uint32_t lastRedraw = 0;
  if (screen == Screen::AIRQ_LIST) {
    int activeNow = activeDeviceCount();
    if ((anyNew || activeNow != listLastActiveCount || topBarLiveDirty) &&
        (millis() - lastRedraw > LIST_REDRAW_MIN_MS)) {
      lastRedraw = millis();
      updateListDirtyRows();
    }
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
  refreshTopBarLiveIfNeeded();

  if (manualTimeValid && (millis() - manualLastPersistMs) >= MANUAL_TIME_PERSIST_MS) {
    manualBaseEpoch = currentManualEpoch();
    manualBaseMillis = millis();
    manualLastPersistMs = manualBaseMillis;
    saveManualTimeToPrefs();
  }

  delay(20);
}
