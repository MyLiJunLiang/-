package com.xl.XAUT趋势系统;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通用 OKX K 线 SQLite 缓存（彻底解耦：任何策略包都可复用，不再互相 import）
 *
 * - 表结构：okx_candles(inst_id, bar, ts, o,h,l,c, vol, confirm)
 * - 主键： (inst_id, bar, ts)
 * - loadRange：优先读库；若发现覆盖不足/缺口，则用 fetcher 拉全区间补齐，再入库（仅 confirm=1）
 *
 * ✅ 无未来函数：只入库 confirm=1（已收盘）K。
 *
 * 设计说明：
 * - 为了不强制你把 Candle 抽成公共类，这里通过 class+反射来“创建/填充”你的 Candle 类型。
 * - 你的 Candle 只要有无参构造（默认就有）且包含字段 ts/o/h/l/c/vol/confirm（可不是 public），即可直接复用。
 */
public final class OkxCandleCache {

    @FunctionalInterface
    public interface RangeFetcher<T> {
        List<T> fetch(String instId, String bar, long startMs, long endMs) throws Exception;
    }

    private static final Map<String, OkxCandleCache> INST = new ConcurrentHashMap<>();

    public static OkxCandleCache get(String dbFile, int keepYears) {
        return INST.computeIfAbsent(dbFile, f -> new OkxCandleCache(f, keepYears));
    }

    private final String dbFile;
    private final int keepYears;

    private OkxCandleCache(String dbFile, int keepYears) {
        this.dbFile = "./okx_candles_XAUT.db";
        this.keepYears = keepYears;
    }

    // -------------------------
    // public APIs
    // -------------------------

    /** 同步最近 N 年（实际上就是 loadRange 一次）。 */
    public <T> void syncLastYears(String instId, String bar, int years, Class<T> clazz, RangeFetcher<T> fetcher) throws Exception {
        long endMs = System.currentTimeMillis();
        long startMs = endMs - years * 365L * 24 * 3600 * 1000;
        loadRange(instId, bar, startMs, endMs, clazz, fetcher);
    }

    /** 读取区间 K 线（如果库里不足，会自动从网络补齐并入库）。 */
    public <T> List<T> loadRange(String instId, String bar, long startMs, long endMs, Class<T> clazz, RangeFetcher<T> fetcher) throws Exception {
        ensureTable();

        // 1) 先读库
        List<T> db = readRange(instId, bar, startMs, endMs, clazz);

        // 2) 覆盖检查：是否基本覆盖（至少有头尾）
        if (db.isEmpty()) {
            List<T> net = fetcher.fetch(instId, bar, startMs, endMs);
            upsertClosedOnly(instId, bar, net);
            return sortByTs(net);
        }

        long minTs = getTs(db.get(0));
        long maxTs = getTs(db.get(db.size() - 1));
        boolean headOk = minTs <= startMs + 2 * 60_000L; // 允许轻微偏差
        boolean tailOk = maxTs >= endMs - 2 * 60_000L;

        if (headOk && tailOk && !hasNonIncreasingTs(db)) {
            return db;
        }

        // 3) 有缺口/覆盖不足：全拉一次（简单且稳定，避免复杂补洞逻辑导致错位/未来函数）
        List<T> net = fetcher.fetch(instId, bar, startMs, endMs);
        upsertClosedOnly(instId, bar, net);
        return sortByTs(net);
    }

    // -------------------------
    // internal: SQLite
    // -------------------------

    private Connection conn() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
    }

    private void ensureTable() throws SQLException {
        try (Connection c = conn();
             Statement st = c.createStatement()) {
            st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS okx_candles (" +
                            "inst_id TEXT NOT NULL," +
                            "bar TEXT NOT NULL," +
                            "ts INTEGER NOT NULL," +
                            "o REAL NOT NULL," +
                            "h REAL NOT NULL," +
                            "l REAL NOT NULL," +
                            "c REAL NOT NULL," +
                            "vol REAL NOT NULL," +
                            "confirm INTEGER NOT NULL," +
                            "PRIMARY KEY(inst_id, bar, ts)" +
                            ")"
            );
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_okx_candles_inst_bar_ts ON okx_candles(inst_id, bar, ts)");
        }
    }

    private <T> List<T> readRange(String instId, String bar, long startMs, long endMs, Class<T> clazz) throws Exception {
        String sql = "SELECT ts, o, h, l, c, vol, confirm FROM okx_candles " +
                     "WHERE inst_id=? AND bar=? AND ts>=? AND ts<=? ORDER BY ts ASC";

        List<T> out = new ArrayList<>();
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, instId);
            ps.setString(2, bar);
            ps.setLong(3, startMs);
            ps.setLong(4, endMs);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    T obj = clazz.getDeclaredConstructor().newInstance();
                    setField(obj, "ts", rs.getLong(1));
                    setField(obj, "o",  rs.getDouble(2));
                    setField(obj, "h",  rs.getDouble(3));
                    setField(obj, "l",  rs.getDouble(4));
                    setField(obj, "c",  rs.getDouble(5));
                    setField(obj, "vol", rs.getDouble(6));
                    setField(obj, "confirm", rs.getInt(7));
                    out.add(obj);
                }
            }
        }
        return out;
    }

    private <T> void upsertClosedOnly(String instId, String bar, List<T> candles) throws Exception {
        if (candles == null || candles.isEmpty()) return;

        long cutoff = System.currentTimeMillis() - keepYears * 365L * 24 * 3600 * 1000;

        String sql = "INSERT OR REPLACE INTO okx_candles(inst_id, bar, ts, o, h, l, c, vol, confirm) " +
                     "VALUES(?,?,?,?,?,?,?,?,?)";

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);

            for (T x : candles) {
                int confirm = getConfirm(x);
                if (confirm != 1) continue;  // 只入已收盘
                long ts = getTs(x);
                if (ts < cutoff) continue;

                ps.setString(1, instId);
                ps.setString(2, bar);
                ps.setLong(3, ts);
                ps.setDouble(4, getD(x, "o"));
                ps.setDouble(5, getD(x, "h"));
                ps.setDouble(6, getD(x, "l"));
                ps.setDouble(7, getD(x, "c"));
                ps.setDouble(8, getVol(x));
                ps.setInt(9, confirm);
                ps.addBatch();
            }
            ps.executeBatch();
            c.commit();
        }
    }

    // -------------------------
    // helpers
    // -------------------------

    private static <T> List<T> sortByTs(List<T> candles) {
        if (candles == null || candles.size() <= 1) return candles;
        candles.sort(Comparator.comparingLong(OkxCandleCache::getTs));
        return candles;
    }

    private static <T> boolean hasNonIncreasingTs(List<T> cs) {
        if (cs.size() <= 2) return false;
        long prev = getTs(cs.get(0));
        for (int i = 1; i < cs.size(); i++) {
            long cur = getTs(cs.get(i));
            if (cur <= prev) return true;
            prev = cur;
        }
        return false;
    }

    private static long getTs(Object o) {
        return ((Number) getField(o, "ts", 0L)).longValue();
    }

    private static int getConfirm(Object o) {
        Object v = getField(o, "confirm", 1);
        if (v instanceof Number) return ((Number) v).intValue();
        return 1;
    }

    private static double getVol(Object o) {
        Object v = getField(o, "vol", 0.0);
        if (v instanceof Number) return ((Number) v).doubleValue();
        return 0.0;
    }

    private static double getD(Object o, String name) {
        Object v = getField(o, name, 0.0);
        if (v instanceof Number) return ((Number) v).doubleValue();
        return 0.0;
    }

    private static Object getField(Object obj, String name, Object defVal) {
        try {
            Class<?> c = obj.getClass();
            try {
                var f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(obj);
            } catch (NoSuchFieldException e) {
                // fallback getter
                String m = "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
                var mm = c.getDeclaredMethod(m);
                mm.setAccessible(true);
                return mm.invoke(obj);
            }
        } catch (Throwable t) {
            return defVal;
        }
    }

    private static void setField(Object obj, String name, Object val) {
        try {
            Class<?> c = obj.getClass();
            var f = c.getDeclaredField(name);
            f.setAccessible(true);

            Class<?> ft = f.getType();
            if (ft == long.class) f.setLong(obj, ((Number) val).longValue());
            else if (ft == int.class) f.setInt(obj, ((Number) val).intValue());
            else if (ft == double.class) f.setDouble(obj, ((Number) val).doubleValue());
            else if (ft == float.class) f.setFloat(obj, ((Number) val).floatValue());
            else f.set(obj, val);
        } catch (Throwable ignore) {
            // ignore if field not exist
        }
    }
}
