package eval;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Coinalyze OKX ETH perp OI backfiller -> writes oi + oidelta into SQLite okx_ext_features (30m rows).
 *
 * Features:
 * 1) Auto-detect earliest available start date (within user range).
 * 2) Auto-detect best Coinalyze symbol (tries candidates; pick the one with the earliest data).
 * 3) Interval fallback (30min -> 1hour -> 4hour -> daily by default) and pick the best combo.
 * 4) Rate-limit resilient (HTTP 429 Retry-After supports fractional seconds).
 * 5) SQLite lock resilient (WAL + busy_timeout + retry on SQLITE_BUSY / BUSY_SNAPSHOT).
 *
 * VM options (examples):
 *   -Dcoinalyze.apiKey=YOUR_KEY
 *   -Dcoinalyze.symbol=ETHUSDT_PERP.3                 (preferred symbol; optional if using candidates)
 *   -Dcoinalyze.symbolCandidates=ETHUSDT_PERP,ETHUSDT_PERP.1,ETHUSDT_PERP.2,ETHUSDT_PERP.3
 *   -Dinterval=30min                                  (preferred interval; optional if using intervalCandidates)
 *   -DintervalCandidates=30min,1hour,4hour,daily
 *
 *   -Ddb=C:\Users\baiyu\TimeOEX\okx_candles_osc.db
 *   -Dinst=ETH-USDT-SWAP
 *   -Dbar=30m
 *   -Dstart=2022-02-01
 *   -Dend=2026-02-28
 *   -Dtz=UTC
 *
 *   -DsleepMs=800                 (base pacing between days)
 *   -DminBarsPerDay=40            (skip day if already has >= this count)
 *   -DmaxRetries=12               (for 429/5xx/IO reset)
 *   -DbackoffBaseMs=1000          (exponential base)
 *   -DbackoffMaxMs=120000         (cap sleep)
 *   -DhttpConnectionClose=false   (true=disable keep-alive, slower but sometimes more stable)
 *
 *   -DautoDetectStart=true        (default true)
 *   -DautoDetectSymbol=true       (default true)
 *   -DdetectStepDays=60           (coarse probe step)
 *   -DdetectMaxRetries=6          (retries during detect probe)
 *   -DdetectMaxProbes=500         (overall probe budget)
 *   -DdetectVerbose=true
 *
 *   -DsqliteWal=true
 *   -DsqliteBusyTimeoutMs=60000
 *   -DsqliteBusyRetries=10
 *   -DsqliteBusyBackoffBaseMs=200
 *   -DsqliteBusyBackoffMaxMs=5000
 */
public class CoinalyzeOiBackfillOkx_202202_202602 {

    // NOTE: we rebuild HttpClient if severe IO issues occur (optional), but keep a default here.
    static volatile HttpClient HTTP = newHttpClient();
    static HttpClient newHttpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .build();
    }

    static final String API_BASE = "https://api.coinalyze.net";
    static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static final DateTimeFormatter DTF_MONTH = DateTimeFormatter.ofPattern("yyyy-MM");

    static String sys(String k, String def) {
        String v = System.getProperty(k);
        if (v == null || v.trim().isEmpty()) return def;
        return v.trim();
    }
    static boolean sysBool(String k, boolean def) {
        String v = System.getProperty(k);
        if (v == null || v.trim().isEmpty()) return def;
        v = v.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("y") || v.equals("on");
    }
    static int sysInt(String k, int def) {
        try { return Integer.parseInt(sys(k, "" + def)); } catch (Exception e) { return def; }
    }
    static long sysLong(String k, long def) {
        try { return Long.parseLong(sys(k, "" + def)); } catch (Exception e) { return def; }
    }

    public static void main(String[] args) throws Exception {
        String apiKey = sys("coinalyze.apiKey", "");
        if (apiKey.isEmpty()) {
            System.err.println("Missing -Dcoinalyze.apiKey=YOUR_KEY");
            return;
        }

        String inst = sys("inst", "ETH-USDT-SWAP");
        String bar = sys("bar", "30m");

        String tzS = sys("tz", "UTC");
        ZoneId zone = ZoneId.of(tzS);

        LocalDate rangeStart = LocalDate.parse(sys("start", "2022-02-01"), DTF);
        LocalDate rangeEnd = LocalDate.parse(sys("end", "2026-02-28"), DTF);

        int sleepMs = sysInt("sleepMs", 800);
        int minBarsPerDay = sysInt("minBarsPerDay", 40);

        int maxRetries = sysInt("maxRetries", 12);
        int backoffBaseMs = sysInt("backoffBaseMs", 1000);
        int backoffMaxMs = sysInt("backoffMaxMs", 120000);
        boolean httpConnectionClose = sysBool("httpConnectionClose", false);

        boolean autoDetectStart = sysBool("autoDetectStart", true);
        boolean autoDetectSymbol = sysBool("autoDetectSymbol", true);

        int detectStepDays = sysInt("detectStepDays", 60);
        int detectMaxRetries = sysInt("detectMaxRetries", 6);
        int detectMaxProbes = sysInt("detectMaxProbes", 500);
        boolean detectVerbose = sysBool("detectVerbose", true);

        String preferredSymbol = sys("coinalyze.symbol", "ETHUSDT_PERP.3");
        String preferredInterval = normalizeInterval(sys("interval", "30min"));

        List<String> symbolCandidates = parseCandidates(
                sys("coinalyze.symbolCandidates", ""),
                preferredSymbol,
                defaultSymbolCandidates(preferredSymbol)
        );

        List<String> intervalCandidates = parseCandidates(
                sys("intervalCandidates", ""),
                preferredInterval,
                Arrays.asList("30min", "1hour", "4hour", "daily")
        );

        String dbPath = sys("db", "okx_candles_osc.db");

        // SQLite lock strategy
        boolean sqliteWal = sysBool("sqliteWal", true);
        int sqliteBusyTimeoutMs = sysInt("sqliteBusyTimeoutMs", 60000);
        int sqliteBusyRetries = sysInt("sqliteBusyRetries", 10);
        int sqliteBusyBackoffBaseMs = sysInt("sqliteBusyBackoffBaseMs", 200);
        int sqliteBusyBackoffMaxMs = sysInt("sqliteBusyBackoffMaxMs", 5000);

        System.out.println("\u6570\u636E\u5E93(DB)=" + dbPath + (new java.io.File(dbPath).exists() ? " (\u5DF2\u5B58\u5728)" : " (\u4E0D\u5B58\u5728,\u5C06\u521B\u5EFA)"));
        System.out.println("\u5408\u7EA6(INST)=" + inst + " \u7C92\u5EA6(BAR)=" + bar);
        System.out.println("\u5019\u9009SYMBOLS=" + String.join(",", symbolCandidates));
        System.out.println("\u5019\u9009intervals=" + String.join(",", intervalCandidates));
        System.out.println("\u65E5\u671F\u8303\u56F4(DATE_RANGE)=" + rangeStart + " -> " + rangeEnd + " (tz=" + zone + ")");
        System.out.println("\u8282\u594F(PACE) sleepMs=" + sleepMs + " minBarsPerDay=" + minBarsPerDay +
                " maxRetries=" + maxRetries + " backoffBaseMs=" + backoffBaseMs + " backoffMaxMs=" + backoffMaxMs +
                " httpConnectionClose=" + httpConnectionClose);
        System.out.println("\u63A2\u6D4B(AUTO-DETECT) start=" + autoDetectStart + " symbol=" + autoDetectSymbol +
                " detectStepDays=" + detectStepDays + " detectMaxRetries=" + detectMaxRetries + " detectMaxProbes=" + detectMaxProbes +
                " detectVerbose=" + detectVerbose);
        System.out.println("SQLite(LOCK) wal=" + sqliteWal + " busyTimeoutMs=" + sqliteBusyTimeoutMs +
                " busyRetries=" + sqliteBusyRetries + " busyBackoffBaseMs=" + sqliteBusyBackoffBaseMs + " busyBackoffMaxMs=" + sqliteBusyBackoffMaxMs);
        System.out.println("------------------------------------------------------");

        // Choose best (symbol, interval) combo (earliest data start) and detect earliest start date
        Combo best = new Combo(preferredSymbol, preferredInterval, null, false);
        if (autoDetectSymbol || autoDetectStart) {
            best = detectBestCombo(
                    apiKey, symbolCandidates, intervalCandidates,
                    rangeStart, rangeEnd, zone,
                    detectStepDays, detectMaxRetries, detectMaxProbes, detectVerbose,
                    httpConnectionClose
            );
            if (!best.hasData) {
                System.out.println("[AUTO-DETECT] \u8303\u56F4\u5185\u672A\u627E\u5230\u4EFB\u4F55\u6709\u6570\u636E\u7684 (symbol,interval) \u7EC4\u5408\u3002\u8BF7\u6362 symbol/interval \u6216\u6269\u5927\u65E5\u671F\u8303\u56F4\u3002");
                return;
            }
            System.out.println("[AUTO-DETECT] \u6700\u4F18\u7EC4\u5408: symbols=" + best.symbol + " interval=" + best.interval +
                    " earliest=" + best.earliest);
            if (autoDetectStart && best.earliest != null && best.earliest.isAfter(rangeStart)) {
                System.out.println("[AUTO-DETECT] \u8D77\u59CB\u65E5\u671F\u81EA\u52A8\u8C03\u6574: " + rangeStart + " -> " + best.earliest);
                rangeStart = best.earliest;
            }
        } else {
            best = new Combo(preferredSymbol, preferredInterval, null, true);
        }

        String symbols = best.symbol;
        String interval = best.interval;

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(false);
            initSqlitePragmas(conn, sqliteWal, sqliteBusyTimeoutMs);

            ensureSchema(conn);
            PreparedStatement psUpsert = prepareUpsert(conn);

            PreparedStatement psLastOiBefore = conn.prepareStatement(
                    "SELECT oi, ts FROM okx_ext_features WHERE inst_id=? AND bar=? AND ts<? AND oi IS NOT NULL ORDER BY ts DESC LIMIT 1"
            );
            PreparedStatement psCountDay = conn.prepareStatement(
                    "SELECT COUNT(1) FROM okx_ext_features WHERE inst_id=? AND bar=? AND ts>=? AND ts<=? AND oi IS NOT NULL"
            );

            long[] beforeStats = dbStats(conn, inst, bar);
            System.out.println("[DB][BEFORE] inst=" + inst + " bar=" + bar + " oiRows=" + beforeStats[0] +
                    " tsMin=" + beforeStats[1] + " tsMax=" + beforeStats[2]);

            long totalBarsPrepared = 0;
            long totalUpsertApplied = 0;
            long emptyDays = 0;
            long skippedAlready = 0;
            long dayErrors = 0;

            LocalDate d = rangeStart;
            String curMonth = null;
            long monthBars = 0;
            long monthEmpty = 0;
            long monthSkip = 0;
            long monthErr = 0;

            while (!d.isAfter(rangeEnd)) {
                String m = d.format(DTF_MONTH);
                if (!m.equals(curMonth)) {
                    if (curMonth != null) {
                        System.out.println("[MONTH] " + curMonth + " \u65B0\u589EBars=" + monthBars +
                                " \u7A7A\u6570\u636EDays=" + monthEmpty + " \u5DF2\u6709\u8DF3\u8FC7Days=" + monthSkip + " \u9519\u8BEFDays=" + monthErr);
                        monthBars = 0;
                        monthEmpty = 0;
                        monthSkip = 0;
                        monthErr = 0;
                    }
                    curMonth = m;
                }

                long dayStartMs = d.atStartOfDay(zone).toInstant().toEpochMilli();
                long dayEndMs = d.plusDays(1).atStartOfDay(zone).toInstant().toEpochMilli() - 1;

                int existing = countBars(psCountDay, inst, bar, dayStartMs, dayEndMs);
                if (existing >= minBarsPerDay) {
                    skippedAlready++;
                    monthSkip++;
                    if (sleepMs > 0) Thread.sleep(Math.min(200, sleepMs));
                    d = d.plusDays(1);
                    continue;
                }

                Double lastOi = null;
                psLastOiBefore.setString(1, inst);
                psLastOiBefore.setString(2, bar);
                psLastOiBefore.setLong(3, dayStartMs);
                try (ResultSet rs = psLastOiBefore.executeQuery()) {
                    if (rs.next()) lastOi = rs.getDouble(1);
                }

                List<Bar> bars;
                try {
                    bars = fetchOpenInterestHistoryWithRetry(
                            apiKey, symbols, interval, dayStartMs, dayEndMs,
                            maxRetries, backoffBaseMs, backoffMaxMs,
                            httpConnectionClose
                    );
                } catch (Exception e) {
                    dayErrors++;
                    monthErr++;
                    System.out.println("[DAY][\u9519\u8BEF] " + d + " -> " + e.getClass().getSimpleName() + ": " + safeMsg(e));
                    // continue on error
                    d = d.plusDays(1);
                    if (sleepMs > 0) Thread.sleep(sleepMs);
                    continue;
                }

                if (bars.isEmpty()) {
                    emptyDays++;
                    monthEmpty++;
                    d = d.plusDays(1);
                    if (sleepMs > 0) Thread.sleep(sleepMs);
                    continue;
                }

                bars.sort(Comparator.comparingLong(b2 -> b2.ts));

                long prepared = 0;
                for (Bar b : bars) {
                    if (b.ts <= 0 || Double.isNaN(b.value)) continue;
                    long aligned = align30m(b.ts);
                    double oi = b.value;
                    Double delta = null;
                    if (lastOi != null) delta = oi - lastOi;
                    lastOi = oi;

                    bindUpsert(psUpsert, inst, bar, aligned, oi, delta);
                    psUpsert.addBatch();
                    prepared++;
                    totalBarsPrepared++;
                    monthBars++;
                }

                int applied = executeBatchWithSqliteBusyRetry(
                        conn, psUpsert,
                        sqliteBusyRetries, sqliteBusyBackoffBaseMs, sqliteBusyBackoffMaxMs
                );
                totalUpsertApplied += applied;

                if (sleepMs > 0) Thread.sleep(sleepMs);
                d = d.plusDays(1);
            }

            if (curMonth != null) {
                System.out.println("[MONTH] " + curMonth + " \u65B0\u589EBars=" + monthBars +
                        " \u7A7A\u6570\u636EDays=" + monthEmpty + " \u5DF2\u6709\u8DF3\u8FC7Days=" + monthSkip + " \u9519\u8BEFDays=" + monthErr);
            }

            long[] afterStats = dbStats(conn, inst, bar);
            System.out.println("------------------------------------------------------");
            System.out.println("[DB][AFTER] inst=" + inst + " bar=" + bar + " oiRows=" + afterStats[0] +
                    " tsMin=" + afterStats[1] + " tsMax=" + afterStats[2]);
            System.out.println("\u5B8C\u6210(DONE). totalBarsPrepared=" + totalBarsPrepared +
                    " totalUpsertApplied=" + totalUpsertApplied +
                    " emptyDays=" + emptyDays +
                    " skippedAlready=" + skippedAlready +
                    " dayErrors=" + dayErrors);
        }
    }

    // ------------------------- AUTO DETECT (symbol + interval + start) -------------------------

    static class Combo {
        final String symbol;
        final String interval;
        final LocalDate earliest;
        final boolean hasData;
        Combo(String symbol, String interval, LocalDate earliest, boolean hasData) {
            this.symbol = symbol; this.interval = interval; this.earliest = earliest; this.hasData = hasData;
        }
    }

    static Combo detectBestCombo(
            String apiKey,
            List<String> symbolCandidates,
            List<String> intervalCandidates,
            LocalDate rangeStart,
            LocalDate rangeEnd,
            ZoneId zone,
            int detectStepDays,
            int detectMaxRetries,
            int detectMaxProbes,
            boolean detectVerbose,
            boolean httpConnectionClose
    ) throws Exception {

        // probe budget shared across all combos
        int probesUsed = 0;

        Combo best = new Combo(symbolCandidates.get(0), intervalCandidates.get(0), null, false);

        for (String sym : symbolCandidates) {
            for (String itv : intervalCandidates) {
                if (probesUsed >= detectMaxProbes) break;

                // quick check: probe coarse dates by step to find first HAS_DATA
                LocalDate d = rangeStart;
                LocalDate firstHas = null;

                while (!d.isAfter(rangeEnd) && probesUsed < detectMaxProbes) {
                    probesUsed++;
                    boolean has = probeHasData(apiKey, sym, itv, d, zone, detectMaxRetries, httpConnectionClose);
                    if (detectVerbose) {
                        System.out.println("[AUTO-DETECT] probe#" + probesUsed + " sym=" + sym + " itv=" + itv + " day=" + d + " -> " + (has ? "HAS_DATA" : "EMPTY"));
                    }
                    if (has) { firstHas = d; break; }
                    d = d.plusDays(detectStepDays);
                }

                if (firstHas == null) {
                    continue;
                }

                // binary search between lastEmpty..firstHas to find earliest
                LocalDate left = rangeStart;
                LocalDate right = firstHas;
                // find a left bound that is EMPTY right before firstHas by stepping back detectStepDays
                LocalDate back = firstHas.minusDays(detectStepDays);
                if (!back.isBefore(rangeStart)) left = back;
                // ensure left is EMPTY; if not, move further back (limited)
                for (int k = 0; k < 3 && !left.isEqual(rangeStart); k++) {
                    if (!probeHasData(apiKey, sym, itv, left, zone, detectMaxRetries, httpConnectionClose)) break;
                    LocalDate newLeft = left.minusDays(detectStepDays);
                    if (newLeft.isBefore(rangeStart)) { left = rangeStart; break; }
                    left = newLeft;
                }

                LocalDate earliest = binaryEarliest(apiKey, sym, itv, left, right, zone, detectMaxRetries, detectVerbose, httpConnectionClose);
                if (earliest == null) continue;

                if (!best.hasData || earliest.isBefore(best.earliest)) {
                    best = new Combo(sym, itv, earliest, true);
                    // earliest possible
                    if (earliest.isEqual(rangeStart)) return best;
                }
            }
        }
        return best;
    }

    static LocalDate binaryEarliest(
            String apiKey, String sym, String itv,
            LocalDate left, LocalDate right,
            ZoneId zone,
            int detectMaxRetries,
            boolean detectVerbose,
            boolean httpConnectionClose
    ) throws Exception {
        // we want earliest HAS_DATA in [left,right], assuming right HAS_DATA, left maybe EMPTY/HAS
        // If left already HAS_DATA, shrink toward rangeStart by binary anyway.
        LocalDate lo = left;
        LocalDate hi = right;

        // ensure hi has data
        if (!probeHasData(apiKey, sym, itv, hi, zone, detectMaxRetries, httpConnectionClose)) return null;

        while (lo.isBefore(hi)) {
            long days = Duration.between(lo.atStartOfDay(), hi.atStartOfDay()).toDays();
            if (days <= 1) {
                // check lo
                boolean loHas = probeHasData(apiKey, sym, itv, lo, zone, detectMaxRetries, httpConnectionClose);
                if (detectVerbose) System.out.println("[AUTO-DETECT][BIN] sym=" + sym + " itv=" + itv + " left=" + lo + " right=" + hi + " -> loHas=" + loHas);
                return loHas ? lo : hi;
            }
            LocalDate mid = lo.plusDays(days / 2);
            boolean midHas = probeHasData(apiKey, sym, itv, mid, zone, detectMaxRetries, httpConnectionClose);
            if (detectVerbose) System.out.println("[AUTO-DETECT][BIN] sym=" + sym + " itv=" + itv + " left=" + lo + " right=" + hi + " mid=" + mid + " -> " + (midHas ? "HAS_DATA" : "EMPTY"));
            if (midHas) {
                hi = mid;
            } else {
                lo = mid.plusDays(1);
            }
        }
        return lo;
    }

    static boolean probeHasData(String apiKey, String symbols, String interval, LocalDate day, ZoneId zone, int detectMaxRetries, boolean httpConnectionClose) throws Exception {
        long dayStartMs = day.atStartOfDay(zone).toInstant().toEpochMilli();
        long dayEndMs = day.plusDays(1).atStartOfDay(zone).toInstant().toEpochMilli() - 1;

        // lightweight retry
        int attempt = 0;
        long sleep = 800;
        while (true) {
            attempt++;
            HttpResult r;
            try {
                r = fetchOnce(apiKey, symbols, interval, dayStartMs, dayEndMs, httpConnectionClose);
            } catch (IOException ioe) {
                if (attempt >= detectMaxRetries) return false;
                // rebuild client to avoid stale connection reuse
                HTTP = newHttpClient();
                Thread.sleep(sleep);
                sleep = Math.min(sleep * 2, 10_000);
                continue;
            }

            if (r.code == 200) {
                List<Bar> bars = parseOiBars(r.bodyText);
                return !bars.isEmpty();
            }
            if (r.code == 429 || (r.code >= 500 && r.code <= 599)) {
                if (attempt >= detectMaxRetries) return false;
                long raMs = (r.code == 429) ? parseRetryAfterMs(r.retryAfter) : -1;
                long waitMs = raMs > 0 ? raMs : sleep;
                waitMs = Math.min(waitMs, 60_000);
                Thread.sleep(waitMs);
                sleep = Math.min(sleep * 2, 60_000);
                continue;
            }
            // other 4xx treat as empty for detect
            return false;
        }
    }

    static List<String> parseCandidates(String csv, String preferred, List<String> defaults) {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        if (preferred != null && !preferred.trim().isEmpty()) set.add(preferred.trim());
        if (csv != null && !csv.trim().isEmpty()) {
            for (String s : csv.split(",")) {
                String v = s.trim();
                if (!v.isEmpty()) set.add(v);
            }
        }
        if (defaults != null) for (String s : defaults) if (s != null && !s.trim().isEmpty()) set.add(s.trim());
        return new ArrayList<>(set);
    }

    static List<String> defaultSymbolCandidates(String preferred) {
        // If preferred is like ETHUSDT_PERP.3, also try ETHUSDT_PERP, .1..10
        String base = preferred == null ? "ETHUSDT_PERP" : preferred.trim();
        if (base.contains(",")) base = base.split(",")[0].trim();
        String root = base;
        int dot = base.indexOf('.');
        if (dot > 0) root = base.substring(0, dot);

        List<String> out = new ArrayList<>();
        out.add(root);
        for (int i = 1; i <= 10; i++) out.add(root + "." + i);

        // also try some common alternatives (harmless if invalid)
        out.add("ETHUSD_PERP");
        out.add("ETHUSD_PERP.1");
        out.add("ETHUSDT_PERP");
        return out;
    }

    // ------------------------- SQLite helpers -------------------------

    static void initSqlitePragmas(Connection conn, boolean wal, int busyTimeoutMs) {
        try (Statement st = conn.createStatement()) {
            if (wal) {
                try { st.execute("PRAGMA journal_mode=WAL;"); } catch (Exception ignored) {}
                try { st.execute("PRAGMA synchronous=NORMAL;"); } catch (Exception ignored) {}
            }
            if (busyTimeoutMs > 0) {
                try { st.execute("PRAGMA busy_timeout=" + busyTimeoutMs + ";"); } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
    }

    static int executeBatchWithSqliteBusyRetry(
            Connection conn, PreparedStatement ps,
            int retries, int backoffBaseMs, int backoffMaxMs
    ) throws SQLException, InterruptedException {
        int attempt = 0;
        int[] res = null;

        while (true) {
            attempt++;
            try {
                res = ps.executeBatch();
                conn.commit();
                return sumApplied(res);
            } catch (SQLException e) {
                if (!isSqliteBusy(e) || attempt > Math.max(1, retries)) {
                    try { conn.rollback(); } catch (Exception ignored) {}
                    throw e;
                }
                try { conn.rollback(); } catch (Exception ignored) {}
                long waitMs = Math.min((long) backoffBaseMs * (1L << Math.min(12, attempt - 1)), backoffMaxMs);
                // jitter
                waitMs = waitMs + (long) (Math.random() * 200);
                System.out.println("[SQLITE][LOCK] database is locked -> retry " + attempt + "/" + retries + " sleepMs=" + waitMs);
                Thread.sleep(waitMs);
            }
        }
    }

    static boolean isSqliteBusy(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) msg = "";
        msg = msg.toLowerCase();
        return msg.contains("database is locked")
                || msg.contains("sqlite_busy")
                || msg.contains("busy_snapshot")
                || msg.contains("sqlite_busy_snapshot");
    }

    static long[] dbStats(Connection conn, String inst, String bar) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(1), MIN(ts), MAX(ts) FROM okx_ext_features WHERE inst_id=? AND bar=? AND oi IS NOT NULL"
        )) {
            ps.setString(1, inst);
            ps.setString(2, bar);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long c = rs.getLong(1);
                    long mn = rs.getLong(2);
                    long mx = rs.getLong(3);
                    return new long[]{c, mn, mx};
                }
            }
        } catch (Exception ignored) {}
        return new long[]{0, 0, 0};
    }

    static int countBars(PreparedStatement ps, String inst, String bar, long start, long end) throws SQLException {
        ps.setString(1, inst);
        ps.setString(2, bar);
        ps.setLong(3, start);
        ps.setLong(4, end);
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        }
        return 0;
    }

    static long align30m(long tsMs) {
        long BAR30 = 30L * 60_000L;
        return (tsMs / BAR30) * BAR30;
    }

    static void ensureSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            safeAlter(st, "ALTER TABLE okx_ext_features ADD COLUMN oi REAL");
            safeAlter(st, "ALTER TABLE okx_ext_features ADD COLUMN oidelta REAL");
        }
    }

    static void safeAlter(Statement st, String sql) {
        try { st.executeUpdate(sql); } catch (Exception ignored) {}
    }

    static PreparedStatement prepareUpsert(Connection conn) throws SQLException {
        String sql = "INSERT INTO okx_ext_features(inst_id, bar, ts, oi, oidelta) " +
                "VALUES(?,?,?,?,?) " +
                "ON CONFLICT(inst_id, bar, ts) DO UPDATE SET " +
                "oi=excluded.oi, " +
                "oidelta=excluded.oidelta";
        return conn.prepareStatement(sql);
    }

    static void bindUpsert(PreparedStatement ps, String inst, String bar, long ts, double oi, Double delta) throws SQLException {
        ps.setString(1, inst);
        ps.setString(2, bar);
        ps.setLong(3, ts);
        ps.setDouble(4, oi);
        if (delta == null) ps.setNull(5, Types.REAL);
        else ps.setDouble(5, delta);
    }

    static int sumApplied(int[] a) {
        int s = 0;
        if (a == null) return 0;
        for (int v : a) if (v > 0) s += v;
        return s;
    }

    static String safeMsg(Throwable t) {
        try {
            String m = t.getMessage();
            if (m == null) return "";
            m = m.replace("\n", " ").replace("\r", " ");
            if (m.length() > 240) m = m.substring(0, 240) + "...";
            return m;
        } catch (Exception e) {
            return "";
        }
    }

    // ------------------------- Coinalyze HTTP -------------------------

    static class Bar {
        final long ts;
        final double value;
        Bar(long ts, double value) { this.ts = ts; this.value = value; }
    }

    static List<Bar> fetchOpenInterestHistoryWithRetry(
            String apiKey, String symbols, String interval, long fromMs, long toMs,
            int maxRetries, int backoffBaseMs, int backoffMaxMs,
            boolean httpConnectionClose
    ) throws Exception {

        int attempt = 0;
        long sleep = backoffBaseMs;

        while (true) {
            attempt++;
            HttpResult r;
            try {
                r = fetchOnce(apiKey, symbols, interval, fromMs, toMs, httpConnectionClose);
            } catch (IOException ioe) {
                // include Connection reset / timeouts
                if (attempt >= maxRetries) throw ioe;
                HTTP = newHttpClient();
                long waitMs = Math.min(sleep, backoffMaxMs);
                System.out.println("[NET] IOException(" + ioe.getClass().getSimpleName() + ") -> sleepMs=" + waitMs + " (attempt " + attempt + "/" + maxRetries + ")");
                Thread.sleep(waitMs);
                sleep = Math.min(sleep * 2, backoffMaxMs);
                continue;
            }

            if (r.code == 200) {
                return parseOiBars(r.bodyText);
            }

            if (r.code == 429) {
                long raMs = parseRetryAfterMs(r.retryAfter);
                long waitMs = (raMs > 0) ? raMs : sleep;
                waitMs = Math.min(waitMs, backoffMaxMs);
                System.out.println("[429] Too Many Requests. Retry-After=" + r.retryAfter + " -> sleepMs=" + waitMs + " (attempt " + attempt + "/" + maxRetries + ")");
                if (attempt >= maxRetries) throw new RuntimeException("HTTP 429 after maxRetries. Last body=" + r.bodyText);
                Thread.sleep(waitMs);
                sleep = Math.min(sleep * 2, backoffMaxMs);
                continue;
            }

            if (r.code >= 500 && r.code <= 599) {
                long waitMs = Math.min(sleep, backoffMaxMs);
                System.out.println("[5xx] HTTP " + r.code + " -> sleepMs=" + waitMs + " (attempt " + attempt + "/" + maxRetries + ")");
                if (attempt >= maxRetries) throw new RuntimeException("HTTP " + r.code + " after maxRetries. Last body=" + r.bodyText);
                Thread.sleep(waitMs);
                sleep = Math.min(sleep * 2, backoffMaxMs);
                continue;
            }

            throw new RuntimeException("HTTP " + r.code + " " + r.url + " => " + r.bodyText);
        }
    }

    static class HttpResult {
        final int code;
        final String url;
        final String bodyText;
        final String retryAfter;
        HttpResult(int code, String url, String bodyText, String retryAfter) {
            this.code = code; this.url = url; this.bodyText = bodyText; this.retryAfter = retryAfter;
        }
    }

    static HttpResult fetchOnce(String apiKey, String symbols, String interval, long fromMs, long toMs, boolean httpConnectionClose) throws IOException, InterruptedException {
        String path = "/v1/open-interest-history";
        String q = "symbols=" + enc(symbols)
                + "&interval=" + enc(interval)
                + "&from=" + (fromMs / 1000)
                + "&to=" + (toMs / 1000);
        String url = API_BASE + path + "?" + q;
        URI uri = URI.create(url);

        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(60))
                .header("api_key", apiKey)
                .GET();
        if (httpConnectionClose) b.header("Connection", "close");
        HttpRequest req = b.build();

        HttpResponse<byte[]> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofByteArray());
        String retryAfter = resp.headers().firstValue("Retry-After").orElse(null);

        String body = new String(resp.body(), StandardCharsets.UTF_8);
        return new HttpResult(resp.statusCode(), url, body, retryAfter);
    }

    // Expected: [{"t":1700000000,"o":12345.67}, ...]
    static List<Bar> parseOiBars(String json) {
        List<Bar> out = new ArrayList<>();
        if (json == null) return out;
        String s = json.trim();
        if (s.isEmpty() || s.equals("[]")) return out;

        int i = 0;
        while (true) {
            int tIdx = s.indexOf("\"t\"", i);
            if (tIdx < 0) break;
            int colon1 = s.indexOf(':', tIdx);
            if (colon1 < 0) break;
            int comma1 = s.indexOf(',', colon1);
            if (comma1 < 0) break;
            String tStr = s.substring(colon1 + 1, comma1).trim();
            long t = parseLongSafe(tStr);

            int oIdx = s.indexOf("\"o\"", comma1);
            if (oIdx < 0) break;
            int colon2 = s.indexOf(':', oIdx);
            if (colon2 < 0) break;

            int end = s.indexOf(',', colon2);
            int end2 = s.indexOf('}', colon2);
            if (end < 0 || (end2 >= 0 && end2 < end)) end = end2;
            if (end < 0) break;

            String oStr = s.substring(colon2 + 1, end).trim();
            double o = parseDoubleSafe(oStr);

            out.add(new Bar(t * 1000L, o));
            i = end;
        }
        return out;
    }

    static long parseLongSafe(String s) {
        try { return Long.parseLong(s.replaceAll("[^0-9\\-]", "")); } catch (Exception e) { return 0; }
    }
    static double parseDoubleSafe(String s) {
        try { return Double.parseDouble(s.replaceAll("[^0-9eE\\+\\-\\.]", "")); } catch (Exception e) { return Double.NaN; }
    }

    static String enc(String s) {
        try { return URLEncoder.encode(s == null ? "" : s.trim(), StandardCharsets.UTF_8); } catch (Exception e) { return s; }
    }

    static String normalizeInterval(String interval) {
        if (interval == null) return "30min";
        String v = interval.trim().toLowerCase();
        if (v.equals("30minute") || v.equals("30minutes")) return "30min";
        if (v.equals("15minute") || v.equals("15minutes")) return "15min";
        if (v.equals("5minute")  || v.equals("5minutes"))  return "5min";
        if (v.equals("1minute")  || v.equals("1minutes"))  return "1min";
        if (v.equals("1h") || v.equals("60min") || v.equals("60m")) return "1hour";
        if (v.equals("2h")) return "2hour";
        if (v.equals("4h")) return "4hour";
        if (v.equals("6h")) return "6hour";
        if (v.equals("12h")) return "12hour";
        if (v.equals("1d") || v.equals("day") || v.equals("1day")) return "daily";
        return v;
    }

    static long parseRetryAfterMs(String retryAfter) {
        if (retryAfter == null || retryAfter.trim().isEmpty()) return -1;
        String s = retryAfter.trim();
        // Servers often return seconds; sometimes fractional seconds (e.g., 14.879).
        try {
            double sec = Double.parseDouble(s);
            if (sec <= 0) return -1;
            return (long) Math.ceil(sec * 1000.0);
        } catch (Exception ignored) {
            // HTTP-date not handled; fallback
            return -1;
        }
    }
}
