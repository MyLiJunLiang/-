package com.xl.ETH趋势系统;

import eval.engine.candidates.ConditionFactory;
import eval.engine.candidates.PoolCondition;
import eval.engine.indicators.IndicatorSeriesCache;
import org.json.JSONArray;
import org.json.JSONObject;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.xl.ETH趋势系统.OkxPagingAlarm.alarmOnceInTick;
import static com.xl.ETH趋势系统.OkxSignalAlarmOnly.AuditDb.INV_DEPTH;

/**
 *  OKX 指标闹钟（纯提醒版，不下单）
 *
 * =========================
 *  本版：止盈全平（TP） + 出场优先级写死
 * 优先级：
 *  1) 止损 SL（最高）
 *  2) TP2 全平
 *  3) 原出场逻辑：MACD颜色反转
 *
 *  回测日志增强：每笔 trade 打印 MFE/MAE
 *
 *  新增（只加过滤，不改其它逻辑）：
 *  A)  亚洲盘前半（流动性低）不进场（默认 JST 00:00-06:00，可改）
 *  B)  大级别中性期第一根不进场
 *     1H 中性定义：满足任一：
 *       1) |hist| < X（例如 < 3）
 *       2) hist 连续 N 根在 0 附近来回（sign 混杂 且幅度小）
 *       3) hist 刚从负转正 / 正转负 第一根（sign flip 第一根）
 *     并且：从“非中性 -> 中性”的第一根 1H candle，禁止开仓（仅开仓过滤）
 *
 *  本次改动（不改任何原输出，只补逻辑一致性）：
 *  1) scanHistory / realtime 补齐与回测一致的进场过滤（含 neutral/2ndTrend/质量过滤）
 * =========================
 */
public class OkxSignalAlarmOnly {
    // v35.2: SSOT 回看（VIEW_ENGINE）+ SRMF/资金兼容 wrapper（OkxPagingAlarm）


    // ===================== BUILD / VERSION =====================
    static final String BUILD_TAG = "2026-03-01_pool2Failm8ReflectFix_bayesExpectedRQuality_calibAvgRGate_noPosteriorFallbackWhenCalibReady_v4_fix_sameTsEntryExit_hold_20260305";
    static void printBuildTag() {
        System.out.println("[BUILD] OkxSignalAlarmOnly " + BUILD_TAG);
    }

    // ===================== JVM 参数读取（用于自动下单桥接 & 可调参数） =====================
    static String sysStr(String key, String def) {
        try {
            String v = System.getProperty(key);
            return (v == null || v.isBlank()) ? def : v.trim();
        } catch (Exception e) { return def; }
    }
    static long sysLong(String key, long def) {
        try {
            String v = System.getProperty(key);
            if (v == null || v.isBlank()) return def;
            return Long.parseLong(v.trim());
        } catch (Exception e) { return def; }
    }

    // int wrapper（历史代码主要用 sysLong 强转；这里补一个轻量 wrapper 供新增参数使用）
    static int sysInt(String key, int def) {
        return (int) sysLong(key, def);
    }
    static double sysDouble(String key, double def) {
        try {
            String v = System.getProperty(key);
            if (v == null || v.isBlank()) return def;
            return Double.parseDouble(v.trim());
        } catch (Exception e) { return def; }
    }
    static boolean sysBool(String key, boolean def) {
        try {
            String v = System.getProperty(key);
            if (v == null || v.isBlank()) return def;
            String s = v.trim().toLowerCase(Locale.ROOT);
            if ("1".equals(s) || "true".equals(s) || "yes".equals(s) || "y".equals(s)) return true;
            if ("0".equals(s) || "false".equals(s) || "no".equals(s) || "n".equals(s)) return false;
            return def;
        } catch (Exception e) { return def; }
    }

    static String canonicalPath(String p) {
        try {
            if (p == null || p.isBlank()) return p;
            return new File(p).getCanonicalPath();
        } catch (Exception e) {
            try { return new File(p).getAbsolutePath(); } catch (Exception ignore) { return p; }
        }
    }


    // ===================== 自检打印（参数/过滤器/数据源一致性） =====================
    static final boolean SELF_CHECK_ON_BOOT = sysBool("okx.selfCheckOnBoot", true);
    static final boolean SELF_CHECK_VERBOSE = sysBool("okx.selfCheckVerbose", false);


    static final boolean DEBUG_POS_TRACE = sysBool("okx.debugPosTrace", true);
    static final boolean DEBUG_FILTER_TRACE = sysBool("okx.debugFilterTrace", true);


    // ===================== Bayesian 多特征过滤/仓位缩放（默认不破坏原行为：默认关闭） =====================
    // 用法（开启）：-Dokx.useBayesian=true
    static final boolean USE_BAYESIAN_FILTER = sysBool("okx.useBayesian", true);//10/3/5/20 ->10/3.3/4/10
    static final boolean BAYES_VERBOSE = sysBool("okx.bayesVerbose", true);
    static final int     BAYES_LOOKBACK_TRADES = sysInt("okx.bayesLookback", 200);
    static final double  BAYES_LAPLACE_ALPHA = sysDouble("okx.bayesAlpha", 0.5); // OPT-1: Jeffrey's Prior (was 2.0)

    // posterior 阈值：<min 直接否决；[min,med) 轻仓；[med,strong) 正常；>=strong 加仓
    static final double  BAYES_MIN_POSTERIOR = sysDouble("okx.bayesMinPosterior", 0.55);
    static final double  BAYES_MEDIUM_POSTERIOR = sysDouble("okx.bayesMedPosterior", 0.63);
    static final double  BAYES_STRONG_POSTERIOR = sysDouble("okx.bayesStrongPosterior", 0.72);
    static final boolean BAYES_PRINT_LR_REPORT = sysBool("okx.bayesPrintLrReport", true);

    // ===== BAYES（稳健版：滚动最近N笔交易分位数分箱 + 最小样本约束；默认不改原逻辑，开启后可选 GATE/BOOST）=====
// scheme: legacy(旧固定分箱) / quantile(滚动分位数分箱)
    static final String  BAYES_SCHEME = sysStr("okx.bayesScheme", "quantile");
    // mode: OFF / BOOST / GATE / BOTH / SCALE（SCALE=旧 0/0.7/1/1.3）
    static final String  BAYES_MODE = sysStr("okx.bayesMode", "BOTH");

    // 量化分箱参数（仅 scheme=quantile 时生效）
    static final int     BAYES_BIN_K = sysInt("okx.bayesBinK", 3);                 // 3(推荐) 或 5
    static final int     BAYES_MIN_TRADE_EVIDENCE = sysInt("okx.bayesMinTradeEvidence", 120); // 样本不足时不允许 GATE/BOOST（避免冷启动误判）
    static final int     BAYES_MIN_BIN_TRADES = sysInt("okx.bayesMinBinTrades", 25);          // 每个bin至少多少样本（不够则降级K）
    static final int     BAYES_MIN_BIN_EVIDENCE = sysInt("okx.bayesMinBinEvidence", 20);      // 某bin样本不足则该特征本次不出证据（LR=1）
    static final int     BAYES_FAMILY_CAP = sysInt("okx.bayesFamilyCap", 1);                // 每个信息家族最多使用几个特征（防止重复证据）
    static final int     BAYES_REFIT_EVERY = sysInt("okx.bayesRefitEvery", 10);               // 每新增多少笔交易触发一次refit
    static final double  BAYES_WINSOR_PCT = sysDouble("okx.bayesWinsorPct", 0.01);            // 分位数边界拟合前截尾比例(1%)

    // 决策阈值（GATE/BOOST）
    static final double  BAYES_GATE_POSTERIOR  = sysDouble("okx.bayesGatePosterior", 0.05);  // posterior < gate → 拦截
    static final double  BAYES_BOOST_POSTERIOR = sysDouble("okx.bayesBoostPosterior", 0.80); // posterior > boost → 小幅加仓
    static final double  BAYES_BOOST_SCALE     = sysDouble("okx.bayesBoostScale", 1.12);     // 小幅加仓倍数（推荐 1.05~1.10）

    // P3: posterior 分位数阈值（推荐：保持固定触发比例，避免“几乎不触发/触发过多”）
    static final boolean BAYES_USE_POSTERIOR_QUANTILE_THR = sysBool("okx.bayesUsePosteriorQuantileThr", true);
    static final double  BAYES_GATE_QUANTILE  = sysDouble("okx.bayesGateQuantile", 0.05);   // 拦截最差 15%
    static final double  BAYES_BOOST_QUANTILE = sysDouble("okx.bayesBoostQuantile", 0.85);  // 加仓最强 15%
    static final double  BAYES_THR_MIN_GAP    = sysDouble("okx.bayesThrMinGap", 0.02);      // gate 与 boost 最小间隔（防止阈值挤在一起）

    // P4: 极弱证据 veto（对明显负边 bin 直接禁入，可配置白名单）
    static final boolean BAYES_WEAK_VETO_ENABLED = sysBool("okx.bayesWeakVetoEnabled", false);
    static final double  BAYES_VETO_LR_THRESHOLD = sysDouble("okx.bayesVetoLrThr", 0.60);  // LR<0.70 视为强负边
    static final String  BAYES_VETO_FEATURES = sysStr("okx.bayesVetoFeatures", "");

    // P5: Gate 依据从 posterior 改为 “posterior 分桶的历史 AvgR”（Expected R 口径）
// 说明：当 posterior->AvgR 非单调时，直接用 posterior 阈值 Gate 会误杀低 posterior 但高期望的桶。
// 开启后：若校验分层样本足够，会按“当前 posterior 落入的桶”的 AvgR 来决定是否 Gate。
// 若校验样本不足，则回退为旧 posterior gate（通常阈值很低，基本不影响收益）。
    static final boolean BAYES_GATE_BY_CALIB_AVGR = sysBool("okx.bayesGateByCalibAvgR", false);
    static final double  BAYES_CALIB_GATE_R_THR  = sysDouble("okx.bayesCalibGateRThr", 0.0);
    static final int     BAYES_CALIB_BUCKETS     = sysInt("okx.bayesCalibBuckets", 5);
    static final int     BAYES_CALIB_MIN_N       = sysInt("okx.bayesCalibMinN", 150);
    static final int     BAYES_CALIB_MIN_BUCKET_N= sysInt("okx.bayesCalibMinBucketN", 30);

    // P6: 过滤“区分力过弱”的特征（max|logLR| 太小的特征当作噪声，不参与 posterior/veto）
// 建议：0.08~0.12；设为0可关闭。
    static final double  BAYES_MIN_MAX_ABS_LOGLR = sysDouble("okx.bayesMinMaxAbsLogLr", 0.08);

    // ===== OPT: 高级贝叶斯优化开关（默认渐进开启，feature flag 控制）=====
// OPT-2: Beta-Binomial 自适应权重（替代硬性 minBinEvidence 截断）
    static final boolean BAYES_USE_BETA_WEIGHTS    = sysBool("okx.bayesBetaWeights", true);
    // OPT-3: Isotonic 校准（PAVA 算法：修正 NB 过度自信问题）
    static final boolean BAYES_USE_ISOTONIC_CALIB  = sysBool("okx.bayesIsotonicCalib", false);
    // OPT-4: Kelly Criterion 仓位缩放（替代固定 0/0.7/1/1.3）
    static final boolean BAYES_USE_KELLY_SIZING    = sysBool("okx.bayesKellySizing", true);
    static final double  BAYES_KELLY_FRACTION      = sysDouble("okx.bayesKellyFraction", 0.25); // quarter-Kelly
    static final double  BAYES_AVG_WIN_R           = sysDouble("okx.bayesAvgWinR", 1.2);  // 默认盈亏比（从历史推算）
    static final double  BAYES_AVG_LOSS_R          = sysDouble("okx.bayesAvgLossR", 1.0);
    // OPT-5: 互信息（MI）特征选择（替代 max|logLR|）
    static final boolean BAYES_USE_MI_SELECTION    = sysBool("okx.bayesMiSelection", true);
    static final double  BAYES_MI_THRESHOLD        = sysDouble("okx.bayesMiThreshold", 0.005); // MI<0.005 bits → 噪声
    // OPT-6: James-Stein 收缩（防止 LR 过拟合）
    static final boolean BAYES_USE_JAMES_STEIN     = sysBool("okx.bayesJamesStein", true);
    // OPT-11: 相关性惩罚（替代粗暴 FamilyCap=1）
    static final boolean BAYES_USE_CORR_PENALTY    = sysBool("okx.bayesCorrPenalty", true);

    // 报告控制（避免刷屏）：0=不自动；1=每次refit都打印；n=每n次refit打印一次
    static final int     BAYES_REPORT_EVERY_REFIT = sysInt("okx.bayesReportEveryRefit", 1);

    // ===== BAYES 标签（从“胜率 win/loss”升级到“高质量赢/输（Expected R）”）=====
    // okx.bayesLabelMode:
    //   - PNL_SIGN   : 旧行为（pnlPoints>0 视为 win）
    //   - R_SIGN     : rMultiple>0 视为 win（需要 slPointsUsed>0）
    //   - R_QUALITY  : 仅把“高质量”交易计入训练：r>=+RGood 记为 win；r<=-RBad 记为 loss；其余为 neutral（可选跳过训练）
    static final String  BAYES_LABEL_MODE = sysStr("okx.bayesLabelMode", "R_QUALITY");
    static final double  BAYES_R_GOOD = sysDouble("okx.bayesRGood", 0.35);
    static final double  BAYES_R_BAD  = sysDouble("okx.bayesRBad", 0.35);
    static final boolean BAYES_SKIP_NEUTRAL_TRAIN = sysBool("okx.bayesSkipNeutralTrain", true);
    static final boolean BAYES_LABEL_DEBUG = sysBool("okx.bayesLabelDebug", true);
    static void printSelfCheckOnce() {
        if (!SELF_CHECK_ON_BOOT) return;
        try {
            System.out.println(" ================== [SELF-CHECK] START ==================");
            System.out.println("[SELF-CHECK] build=" + BUILD_TAG);
            System.out.println("[SELF-CHECK] now=" + ZonedDateTime.now(ZoneId.of("Asia/Tokyo")) + " | utc=" + Instant.now());
            System.out.println("[SELF-CHECK] java=" + System.getProperty("java.version") + " | os=" + System.getProperty("os.name") + " " + System.getProperty("os.version"));
            System.out.println("[SELF-CHECK] user.dir=" + System.getProperty("user.dir"));
            System.out.println("[SELF-CHECK] role=" + sysStr("okx.role", "engine"));
            System.out.println("[SELF-CHECK] instId=" + INST_ID);

            // DB path canonicalization
            String dbRaw = DB_FILE;
            String dbCan = canonicalPath(dbRaw);
            File dbf = new File(dbCan);
            System.out.println("[SELF-CHECK][DB] raw=" + dbRaw);
            System.out.println("[SELF-CHECK][DB] canonical=" + dbCan);
            System.out.println("[SELF-CHECK][DB] exists=" + dbf.exists() + " | abs=" + dbf.isAbsolute()
                    + " | sizeMB=" + (dbf.exists() ? String.format(Locale.ROOT, "%.2f", dbf.length() / 1024.0 / 1024.0) : "NA")
                    + " | lastModified=" + (dbf.exists() ? Instant.ofEpochMilli(dbf.lastModified()) : "NA"));


            System.out.println("[SELF-CHECK][AUDIT] enabled=" + AUDIT_DB_ENABLED
                    + " | raw=" + AUDIT_DB_FILE_RAW
                    + " | canonical=" + AUDIT_DB_FILE);
            System.out.println("[SELF-CHECK][STATE] restore=" + RESTORE_LIVE_STATE
                    + " | raw=" + LIVE_STATE_FILE_RAW
                    + " | canonical=" + LIVE_STATE_FILE);
            // Core SSOT / timing knobs

            // AUDIT/STATE file canonicalization
            String auditRaw = AUDIT_DB_FILE_RAW;
            String auditCan = AUDIT_DB_FILE;
            System.out.println("[SELF-CHECK][AUDIT_DB] raw=" + auditRaw);
            System.out.println("[SELF-CHECK][AUDIT_DB] canonical=" + auditCan);

            String stateRaw = LIVE_STATE_FILE_RAW;
            String stateCan = LIVE_STATE_FILE;
            System.out.println("[SELF-CHECK][STATE_FILE] raw=" + stateRaw);
            System.out.println("[SELF-CHECK][STATE_FILE] canonical=" + stateCan);

            System.out.println("[SELF-CHECK][SSOT] USE_DB_CACHE=" + USE_DB_CACHE + " strictSsot=" + STRICT_SSOT
                    + " safeCloseMs=" + SAFE_CLOSE_MS + " engineSecond=" + CHECK_SECOND
                    + " waitTries=" + WAIT_OKX_REFRESH_TRIES + " waitMs=" + WAIT_OKX_REFRESH_MS);
            System.out.println("[SELF-CHECK][HOLD] MANUAL_VIRTUAL_POS_ON_ALERT=" + MANUAL_VIRTUAL_POS_ON_ALERT
                    + " | STOPLOSS_CD=" + USE_STOPLOSS_COOLDOWN + "(" + STOPLOSS_COOLDOWN_BARS + " bars)"
                    + " | TAKEPROFIT_CD=" + USE_TAKEPROFIT_COOLDOWN + "(" + TAKEPROFIT_COOLDOWN_BARS + " bars)");

            System.out.println("[SELF-CHECK][BAYES] enabled=" + USE_BAYESIAN_FILTER + " lookback=" + BAYES_LOOKBACK_TRADES + " alpha=" + BAYES_LAPLACE_ALPHA
                    + " | thr(min/med/strong)=" + BAYES_MIN_POSTERIOR + "/" + BAYES_MEDIUM_POSTERIOR + "/" + BAYES_STRONG_POSTERIOR);
            System.out.println("[SELF-CHECK][BAYES-OPT] betaWeights=" + BAYES_USE_BETA_WEIGHTS
                    + " | isotonic=" + BAYES_USE_ISOTONIC_CALIB
                    + " | kelly=" + BAYES_USE_KELLY_SIZING + "(frac=" + BAYES_KELLY_FRACTION + ")"
                    + " | MI=" + BAYES_USE_MI_SELECTION + "(thr=" + BAYES_MI_THRESHOLD + ")"
                    + " | jamesStein=" + BAYES_USE_JAMES_STEIN
                    + " | corrPenalty=" + BAYES_USE_CORR_PENALTY);

            // Common “must match between engine & puller” knobs (only prints what exists in this class)
            // (Puller is standalone class now, but we still print the key name that must match.)
            System.out.println("[SELF-CHECK][PULLER-EXPECT] okx.candlesDb=" + dbCan + " | okx.safeCloseMs=" + SAFE_CLOSE_MS
                    + " | suggestion: puller/engine must use SAME candlesDb & safeCloseMs");

            // Print filter toggles (USE_*) and important thresholds. Reflection-based to avoid missing any.
            List<String> lines = new ArrayList<>();
            Field[] fs = OkxSignalAlarmOnly.class.getDeclaredFields();
            for (Field f : fs) {
                int mod = f.getModifiers();
                if (!Modifier.isStatic(mod)) continue;
                if (!Modifier.isFinal(mod)) continue;
                Class<?> t = f.getType();
                String name = f.getName();
                // Focus on knobs that can affect “RT != HIST”
                boolean pick = name.startsWith("USE_")
                        || name.startsWith("STRICT_")
                        || name.startsWith("VIEW_")
                        || name.startsWith("TSRND_")
                        || name.startsWith("OKX_")
                        || name.startsWith("SRMF_")
                        || name.startsWith("MANUAL_")
                        || name.startsWith("AUTO_")
                        || name.contains("COOLDOWN")
                        || name.contains("THRESH")
                        || name.contains("GATE")
                        || name.contains("BARS")
                        || name.contains("KEEP")
                        || name.contains("DB_")
                        || name.contains("SAFE_CLOSE")
                        || name.contains("CHECK_SECOND")
                        || name.contains("WAIT_OKX");
                if (!pick) continue;

                try {
                    f.setAccessible(true);
                    Object v = null;
                    if (t == boolean.class) v = f.getBoolean(null);
                    else if (t == int.class) v = f.getInt(null);
                    else if (t == long.class) v = f.getLong(null);
                    else if (t == double.class) v = f.getDouble(null);
                    else if (t == String.class) v = (String) f.get(null);
                    else continue;
                    lines.add(String.format(Locale.ROOT, "%s=%s", name, String.valueOf(v)));
                } catch (Throwable ignore) {}
            }
            Collections.sort(lines);
            System.out.println("[SELF-CHECK] knobs(" + lines.size() + "):");
            int shown = 0;
            for (String s : lines) {
                if (!SELF_CHECK_VERBOSE && shown >= 120) {
                    System.out.println("  ... (truncated, set -Dokx.selfCheckVerbose=true to show all)");
                    break;
                }
                System.out.println("  " + s);
                shown++;
            }

            // Quick “sanity rules” warnings
            if (STRICT_SSOT && !dbf.isAbsolute()) {
                System.out.println("[SELF-CHECK][WARN] strictSsot=true but candlesDb is not absolute: " + dbCan);
            }
            if (USE_DB_CACHE && STRICT_SSOT && !dbf.exists()) {
                System.out.println("[SELF-CHECK][WARN] strictSsot=true but DB file does not exist yet. Start puller first.");
            }

            System.out.println("================== [SELF-CHECK] END ================== ");
        } catch (Throwable t) {
            System.out.println("[SELF-CHECK][ERR] " + t.getMessage());
        }
    }

    // ===================== 自动下单桥接（趋势系统 ↔ OkxAutoTradeBridge） =====================
    static final boolean AUTO_TRADE_ENABLED = sysBool("okx.autoTrade", false);
    static final boolean PRINT_AUTO_ENTRY_PLAN_EACH_REFRESH = sysBool("okx.printAutoPlan", true);
    // 进场挂单价格口径：
    // - true：使用 refPx±entryRefOffset（推荐：与你测试类一致，默认0.3）
    // - false：使用旧口径 refPx±entryLegacyOffset（历史默认2.0，仅用于回退）
    static final boolean ENTRY_USE_REF_OFFSET = sysBool("okx.entryUseRefOffset", true);
    static final double ENTRY_REF_OFFSET = sysDouble("okx.entryRefOffset", 0.3);
    static final double ENTRY_LEGACY_OFFSET = sysDouble("okx.entryLegacyOffset", 2.0);


    // ===================== 手动进场模式（策略A）：响铃即视为已进场（虚拟持仓） =====================
// 默认开启：你的实际进场是手动执行，但系统需要把“响铃那一刻”视为已开仓，否则会反复响铃/机会抖动。
    static final boolean MANUAL_VIRTUAL_POS_ON_ALERT = sysBool("okx.manualVirtualPos", true);

    // 进场提醒去重：同一个 entryTs（=K0收盘=K1开盘）只允许触发一次
    static volatile long LAST_RT_ENTRY_ALERT_TS = -1L;
    static volatile long LAST_RT_SIGNAL_TS_AT_ALERT = -1L;
    static volatile long PENDING_VERIFY_ENTRY_TS = -1L;     // 等待 scanHistory 复现 RT 提醒的 entryTs

    // ===================== PROBE：指定某个 entryTs，打印该时刻为何未进场（用于定位“文档里有、刷新后无”） =====================
    // 用法：-Dokx.probeEntryTsMs=<entryTs毫秒> 例如：-Dokx.probeEntryTsMs=1770838400000
    static final long PROBE_ENTRY_TS_MS = sysLong("okx.probeEntryTsMs", -1L);
    //↓okx.btPrintBlocked=false是关闭回测市场过滤器显示3333333333333333333333
    static final boolean BT_PRINT_BLOCKED = sysBool("okx.btPrintBlocked", false);
    static boolean isProbeTs(long ts) { return PROBE_ENTRY_TS_MS > 0 && ts == PROBE_ENTRY_TS_MS; }
    static volatile long PENDING_VERIFY_SET_AT_MS = -1L;   // 记录 pending 创建时间（用于超时告警）


    // 对齐诊断：缓存 RT/HIST 的 30m/1H 列表元信息，用于 [ALIGN-CHECK][WARN] 时解释“为什么不一致”
    static class ListMeta {
        final String tag;
        final long savedAtMs;
        final int size;
        final long firstTs;
        final long lastTs;
        final long barMs;
        final long[] tailTs; // 末尾若干根 openTs（用于快速判断“ts 是否存在于列表尾部”）
        ListMeta(String tag, long savedAtMs, List<Candle> list, long barMs, int tailN) {
            this.tag = tag;
            this.savedAtMs = savedAtMs;
            this.barMs = barMs;
            if (list == null || list.isEmpty()) {
                this.size = 0;
                this.firstTs = 0L;
                this.lastTs = 0L;
                this.tailTs = new long[0];
            } else {
                this.size = list.size();
                this.firstTs = list.get(0).ts;
                this.lastTs = list.get(list.size() - 1).ts;
                int k = Math.min(tailN, list.size());
                long[] t = new long[k];
                for (int i = 0; i < k; i++) {
                    t[i] = list.get(list.size() - k + i).ts;
                }
                this.tailTs = t;
            }
        }
        boolean inRange(long ts) {
            return size > 0 && ts >= firstTs && ts <= lastTs;
        }
        boolean inTail(long ts) {
            if (tailTs == null) return false;
            for (long v : tailTs) if (v == ts) return true;
            return false;
        }
        String rangeStr() {
            if (size <= 0) return "empty";
            return fmtOpen(firstTs) + " ~ " + fmtOpen(lastTs);
        }
    }
    static volatile ListMeta META_RT_30M = null;
    static volatile ListMeta META_HIST_30M = null;
    static volatile ListMeta META_RT_1H = null;
    static volatile ListMeta META_HIST_1H = null;

    static void cacheMetaFromList(String tag, long nowMs, List<Candle> list, long barMs) {
        if (!DEBUG_ALIGN) return;
        ListMeta meta = new ListMeta(tag, nowMs, list, barMs, 12);
        if (tag.startsWith("RT-30m")) META_RT_30M = meta;
        else if (tag.startsWith("HIST-30m")) META_HIST_30M = meta;
        else if (tag.startsWith("RT-1H")) META_RT_1H = meta;
        else if (tag.startsWith("HIST-1H")) META_HIST_1H = meta;
    }

    // 进场提醒锁：只要锁定成功，就会更新 LAST_RT_AUDIT 并写入 rt_audit_last.json；
// 锁定失败（重复/倒退）则不会响铃/不会覆盖“最近一次机会”，避免机会抖动。
    static boolean rtLockEntryAlert(AlignAudit a) {
        if (a == null) return false;
        long entryTs = a.entryTs;
        long sigTs = a.signalTs;

        // 1) 去重：同一 entryTs 不允许重复触发
        if (LAST_RT_ENTRY_ALERT_TS == entryTs) {
            if (DEBUG_ALIGN) {
                System.out.println("[ENTRY-DEDUP] skip duplicate entry alert: entryTs=" + fmtOpen(entryTs)
                        + " | sig=" + fmtOpen(sigTs));
            }
            return false;
        }
        // 2) 防抖：如果 entryTs 倒退（机会跳来跳去），直接拒绝（保留更“新”的那一次）
        if (LAST_RT_ENTRY_ALERT_TS > 0 && entryTs < LAST_RT_ENTRY_ALERT_TS) {
            System.out.println("[ENTRY-OOD][WARN] entryTs goes backward: new=" + fmtOpen(entryTs)
                    + " < last=" + fmtOpen(LAST_RT_ENTRY_ALERT_TS)
                    + " | keep last");
            return false;
        }

        LAST_RT_ENTRY_ALERT_TS = entryTs;
        LAST_RT_SIGNAL_TS_AT_ALERT = sigTs;

        PENDING_VERIFY_ENTRY_TS = entryTs;
        PENDING_VERIFY_SET_AT_MS = a.nowMs;

        LAST_RT_AUDIT = a;
        saveRtAudit(a);
        alignLog(a);
        return true;
    }


    // ===================== v30: A/B/C 落地（实盘触发日志 + K线快照 + 状态持久化） =====================
    static final boolean VIEW_USE_REPLAY = sysBool("okx.viewUseReplay", true);
    // Route A：当 view 使用 replay 时，必须开启 audit DB（否则无法“实盘=回放”）
    static final boolean AUDIT_DB_ENABLED = VIEW_USE_REPLAY || sysBool("okx.auditDbEnabled", true);
    static final boolean BOOT_RESEARCH_BACKTEST = sysBool("okx.bootResearchBacktest", true);//ture开启历史

    // 启动回测窗口（默认 1 年；想跑 4 年：-Dokx.bootBacktestYears=4）
    static final int BOOT_BACKTEST_YEARS = (int) sysLong("okx.bootBacktestYears", 4);
    // 指标预热缓冲（默认 60 天）
    static final int BOOT_BACKTEST_WARMUP_DAYS = (int) sysLong("okx.bootBacktestWarmupDays", 60);

    // ========== 回测区间开关（用于样本外/时间切分验证）==========
    // 默认两个都为空 ""，此时按原逻辑（最近 BOOT_BACKTEST_YEARS 年）跑，行为不变。
    // 填上日期则只回测该区间，格式 "yyyy-MM-dd"（按 ZONE=Asia/Tokyo 解释）。
    // 例：做 2:1:1 切分验证默认配置稳健性时——
    //   训练段：BT_START_DATE="2022-04-01"  BT_END_DATE="2024-04-01"
    //   测试段：BT_START_DATE="2024-04-01"  BT_END_DATE="2026-05-31"
    // 注意：warmup 仍会在 start 之前自动多取 BOOT_BACKTEST_WARMUP_DAYS 天做指标预热，
    //       但成交统计只从 BT_START_DATE 当天起算（预热段不产生交易），避免冷启动污染。
    static final String BT_START_DATE = "";   // 空=不限制；填 "yyyy-MM-dd" 指定回测起点
    static final String BT_END_DATE   = "";   // 空=到最新；填 "yyyy-MM-dd" 指定回测终点
    // [临时诊断] 强制打印指定入场K时间的完整 [BT-DIAG] 过滤链；空=关闭，不影响任何行为。
    //   例：-Dokx.diagForceTs="2026-05-28 03:00"
    static final String DIAG_FORCE_TS = sysStr("okx.diagForceTs", "2026-06-03 02:00");
    // 启动回测是否打印“胜率/回撤/月度胜率/EQUITY”等报告
    static final boolean BOOT_BACKTEST_PRINT_REPORT = sysBool("okx.bootBacktestPrintReport", true);
    // 多币种文件隔离：默认 ETH-USDT-SWAP 沿用历史文件名(不影响现有实盘)；其它 instId 自动加后缀，便于"一币一进程"并行不撞车
    static String instFileTag() {
        String inst = sysStr("okx.instId", "ETH-USDT-SWAP");
        return "ETH-USDT-SWAP".equals(inst) ? "" : ("_" + inst);
    }
    // 是否默认 ETH 标的：用于让“其它币种”自动切到更省事的默认(单进程自缓存 + 百分比止损)，ETH 保持原行为
    static boolean isEthDefaultInst() {
        return "ETH-USDT-SWAP".equals(sysStr("okx.instId", "ETH-USDT-SWAP"));
    }
    static final String  AUDIT_DB_FILE_RAW = sysStr ("okx.auditDb", "./okx_audit" + instFileTag() + ".db");
    static final String  AUDIT_DB_FILE     = canonicalPath(AUDIT_DB_FILE_RAW);
    static final int     RT_TRADE_KEEP    = (int) sysLong("okx.rtTradeKeep", 5000);
    static final int     RT_STATE_KEEP    = (int) sysLong("okx.rtStateKeep", 2000);
    static final boolean RESEARCH_BACKTEST_PARALLEL = sysBool("okx.researchBacktestParallel", true);//true开启历史


    static final boolean SNAPSHOT_ENABLED = sysBool("okx.snapshotEnabled", true);
    static final int     SNAP_30M_N       = (int) sysLong("okx.snapshotN30m", 300);
    static final int     SNAP_1H_N        = (int) sysLong("okx.snapshotN1h",  200);
    static final int     SNAP_KEEP_BATCHES= (int) sysLong("okx.snapshotKeepBatches", 500);

    static final int     RT_TRIGGER_KEEP  = (int) sysLong("okx.rtTriggerKeep", 500);
    static final boolean RT_TRIGGER_SELFTEST = sysBool("okx.rtTriggerSelfTest", false);


    static final boolean SHOW_SIGNAL_TS = sysBool("okx.showSignalTs", false);
    static final String  LIVE_STATE_FILE_RAW = sysStr ("okx.liveState", "./livepos_state" + instFileTag() + ".json");
    static final String  LIVE_STATE_FILE     = canonicalPath(LIVE_STATE_FILE_RAW);
    static final boolean RESTORE_LIVE_STATE = sysBool("okx.restoreState", true);

    // 记录最近一次成功写入的快照 batchId（便于“时间胶囊回放”）
    static volatile long LAST_SNAP_BATCH_30M = -1L;
    static volatile long LAST_SNAP_BATCH_1H  = -1L;

    static final class RtTrigger {
        long id;
        long savedAtMs;
        String buildTag;
        String instId;
        String tf;
        long signalTs;
        long entryTs;
        Side side;
        double entryRef;
        double nextOpen;
        double dif, dea;
        double prevDif, prevDea;
        int diffInt;
        String t1h;
        String note;
    }

    static final class RtTradeRow {
        long id;
        long savedAtMs;
        String buildTag;
        String instId;
        String tf;
        long enterTs, exitTs;
        long signalTs;
        Side side;
        String json; // CompletedTrade JSON
    }

    static final class RtStateRow {
        long id;
        long savedAtMs;
        String buildTag;
        String instId;
        String reason;
        String json; // EngineState JSON
    }

    static final class AuditDb {
        static volatile boolean inited = false;
        static final ThreadLocal<Integer> INV_DEPTH = ThreadLocal.withInitial(() -> 0);

        static void init() {
            if (!AUDIT_DB_ENABLED) return;
            if (inited) return;
            synchronized (AuditDb.class) {
                if (inited) return;
                try {
                    Class.forName("org.sqlite.JDBC");
                } catch (Throwable ignore) { /* driver may auto-load */ }
                try (Connection c = conn(); Statement st = c.createStatement()) {
                    st.executeUpdate("PRAGMA journal_mode=WAL;");
                    st.executeUpdate("PRAGMA synchronous=NORMAL;");
                    st.executeUpdate("PRAGMA busy_timeout=5000;");
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS rt_trigger (" +
                            "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                            "savedAtMs INTEGER NOT NULL," +
                            "buildTag TEXT," +
                            "instId TEXT," +
                            "tf TEXT," +
                            "signalTs INTEGER," +
                            "entryTs INTEGER," +
                            "side TEXT," +
                            "entryRef REAL," +
                            "nextOpen REAL," +
                            "dif REAL, dea REAL, prevDif REAL, prevDea REAL," +
                            "diffInt INTEGER," +
                            "t1h TEXT," +
                            "note TEXT" +
                            ");");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_rt_trigger_entryTs ON rt_trigger(entryTs);");

                    st.executeUpdate("CREATE TABLE IF NOT EXISTS rt_trade (" +
                            "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                            "savedAtMs INTEGER NOT NULL," +
                            "buildTag TEXT," +
                            "instId TEXT," +
                            "tf TEXT," +
                            "enterTs INTEGER," +
                            "exitTs INTEGER," +
                            "signalTs INTEGER," +
                            "side TEXT," +
                            "json TEXT" +
                            ");");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_rt_trade_exitTs ON rt_trade(exitTs);");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_rt_trade_enterTs ON rt_trade(enterTs);");

                    st.executeUpdate("CREATE TABLE IF NOT EXISTS engine_state (" +
                            "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                            "savedAtMs INTEGER NOT NULL," +
                            "buildTag TEXT," +
                            "instId TEXT," +
                            "reason TEXT," +
                            "json TEXT" +
                            ");");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_engine_state_savedAt ON engine_state(savedAtMs);");

                    st.executeUpdate("CREATE TABLE IF NOT EXISTS snapshot_batch (" +
                            "batchId INTEGER PRIMARY KEY AUTOINCREMENT," +
                            "savedAtMs INTEGER NOT NULL," +
                            "buildTag TEXT," +
                            "instId TEXT," +
                            "tf TEXT," +
                            "snapAtTs INTEGER," +
                            "n INTEGER," +
                            "note TEXT" +
                            ");");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_snapshot_tf_batch ON snapshot_batch(tf, batchId);");

                    st.executeUpdate("CREATE TABLE IF NOT EXISTS candle_snapshot (" +
                            "batchId INTEGER NOT NULL," +
                            "idx INTEGER NOT NULL," +
                            "ts INTEGER," +
                            "o REAL,h REAL,l REAL,c REAL," +
                            "vol REAL," +
                            "confirm INTEGER," +
                            "PRIMARY KEY(batchId, idx)" +
                            ");");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_candle_snapshot_ts ON candle_snapshot(ts);");

                    inited = true;

                    // 可选自检：用于验证 rt_trigger 写入链路是否正常（默认关闭）
                    if (RT_TRIGGER_SELFTEST) {
                        try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM rt_trigger")) {
                            long n = (rs.next() ? rs.getLong(1) : 0);
                            System.out.println("[RT-TRIGGER][SELFTEST] rt_trigger rows=" + n + " (no insert)");
                        } catch (Exception e2) {
                            System.out.println("[RT-TRIGGER][SELFTEST] failed: " + e2.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.out.println("[AUDIT-DB] init failed: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        static Connection conn() throws SQLException {
            return DriverManager.getConnection("jdbc:sqlite:" + AUDIT_DB_FILE);
        }

        static long insertRtTrigger(RtTrigger t) {
            if (!AUDIT_DB_ENABLED) return -1;
            init();
            String sql = "INSERT INTO rt_trigger(savedAtMs,buildTag,instId,tf,signalTs,entryTs,side,entryRef,nextOpen,dif,dea,prevDif,prevDea,diffInt,t1h,note) " +
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            try (Connection c = conn();
                 PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, t.savedAtMs);
                ps.setString(2, t.buildTag);
                ps.setString(3, t.instId);
                ps.setString(4, t.tf);
                ps.setLong(5, t.signalTs);
                ps.setLong(6, t.entryTs);
                ps.setString(7, (t.side == null ? null : (t.side == Side.LONG ? "LONG" : "SHORT")));
                ps.setDouble(8, t.entryRef);
                ps.setDouble(9, t.nextOpen);
                ps.setDouble(10, t.dif);
                ps.setDouble(11, t.dea);
                ps.setDouble(12, t.prevDif);
                ps.setDouble(13, t.prevDea);
                ps.setInt(14, t.diffInt);
                ps.setString(15, t.t1h);
                ps.setString(16, t.note);
                ps.executeUpdate();
                long id = -1;
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) id = rs.getLong(1);
                }
                // retention：只保留最近 RT_TRIGGER_KEEP 条触发记录（防止长期运行无限增长）
                if (id > 0 && RT_TRIGGER_KEEP > 0) {
                    try (PreparedStatement ps2 = c.prepareStatement(
                            "DELETE FROM rt_trigger WHERE id IN (" +
                                    "SELECT id FROM rt_trigger ORDER BY id DESC LIMIT -1 OFFSET ?" +
                                    ");")) {
                        ps2.setInt(1, Math.max(1, RT_TRIGGER_KEEP));
                        ps2.executeUpdate();
                    }
                }
                return id;
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] insertRtTrigger failed: " + e.getMessage());
            }
            return -1;
        }

        static RtTrigger readLastRtTrigger() {
            if (!AUDIT_DB_ENABLED) return null;
            init();
            String sql = "SELECT id,savedAtMs,buildTag,instId,tf,signalTs,entryTs,side,entryRef,nextOpen,dif,dea,prevDif,prevDea,diffInt,t1h,note " +
                    "FROM rt_trigger ORDER BY id DESC LIMIT 1";
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                RtTrigger t = new RtTrigger();
                t.id = rs.getLong(1);
                t.savedAtMs = rs.getLong(2);
                t.buildTag = rs.getString(3);
                t.instId = rs.getString(4);
                t.tf = rs.getString(5);
                t.signalTs = rs.getLong(6);
                t.entryTs = rs.getLong(7);
                String s = rs.getString(8);
                t.side = ("LONG".equalsIgnoreCase(s) ? Side.LONG : ("SHORT".equalsIgnoreCase(s) ? Side.SHORT : null));
                t.entryRef = rs.getDouble(9);
                t.nextOpen = rs.getDouble(10);
                t.dif = rs.getDouble(11);
                t.dea = rs.getDouble(12);
                t.prevDif = rs.getDouble(13);
                t.prevDea = rs.getDouble(14);
                t.diffInt = rs.getInt(15);
                t.t1h = rs.getString(16);
                t.note = rs.getString(17);
                return t;
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] readLastRtTrigger failed: " + e.getMessage());
                return null;
            }
        }

        static List<RtTrigger> readRecentRtTriggers(int limit) {
            List<RtTrigger> out = new ArrayList<>();
            if (!AUDIT_DB_ENABLED) return out;
            init();
            String sql = "SELECT id,savedAtMs,buildTag,instId,tf,signalTs,entryTs,side,entryRef,nextOpen,dif,dea,prevDif,prevDea,diffInt,t1h,note " +
                    "FROM rt_trigger ORDER BY id DESC LIMIT ?";
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, Math.max(1, Math.min(100, limit)));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        RtTrigger t = new RtTrigger();
                        t.id = rs.getLong(1);
                        t.savedAtMs = rs.getLong(2);
                        t.buildTag = rs.getString(3);
                        t.instId = rs.getString(4);
                        t.tf = rs.getString(5);
                        t.signalTs = rs.getLong(6);
                        t.entryTs = rs.getLong(7);
                        String s = rs.getString(8);
                        t.side = ("LONG".equalsIgnoreCase(s) ? Side.LONG : ("SHORT".equalsIgnoreCase(s) ? Side.SHORT : null));
                        t.entryRef = rs.getDouble(9);
                        t.nextOpen = rs.getDouble(10);
                        t.dif = rs.getDouble(11);
                        t.dea = rs.getDouble(12);
                        t.prevDif = rs.getDouble(13);
                        t.prevDea = rs.getDouble(14);
                        t.diffInt = rs.getInt(15);
                        t.t1h = rs.getString(16);
                        t.note = rs.getString(17);
                        out.add(t);
                    }
                }
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] readRecentRtTriggers failed: " + e.getMessage());
            }
            return out;
        }


        static long insertRtTrade(CompletedTrade ct, long nowMs, String note) {
            if (!AUDIT_DB_ENABLED || ct == null) return -1;
            init();
            String side = (ct.side == null ? null : (ct.side == Side.LONG ? "LONG" : "SHORT"));
            String json = tradeToJsonString(ct, note);
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO rt_trade(savedAtMs,buildTag,instId,tf,enterTs,exitTs,signalTs,side,json) VALUES(?,?,?,?,?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, nowMs);
                ps.setString(2, BUILD_TAG);
                ps.setString(3, INST_ID);
                ps.setString(4, "30m");
                ps.setLong(5, ct.enterTs);
                ps.setLong(6, ct.exitTs);
                ps.setLong(7, ct.signalTs);
                ps.setString(8, side);
                ps.setString(9, json);
                ps.executeUpdate();
                long id = -1;
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) id = rs.getLong(1);
                }
                // retention
                if (id > 0 && RT_TRADE_KEEP > 0) {
                    try (PreparedStatement ps2 = c.prepareStatement(
                            "DELETE FROM rt_trade WHERE id IN (" +
                                    "SELECT id FROM rt_trade ORDER BY id DESC LIMIT -1 OFFSET ?" +
                                    ");")) {
                        ps2.setInt(1, Math.max(1, RT_TRADE_KEEP));
                        ps2.executeUpdate();
                    }
                }
                return id;
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] insertRtTrade failed: " + e.getMessage());
                return -1;
            }
        }

        static List<CompletedTrade> readRecentRtTradesAsCompletedTrades(int limit) {
            List<CompletedTrade> out = new ArrayList<>();
            if (!AUDIT_DB_ENABLED) return out;
            init();
            String sql = "SELECT json FROM rt_trade ORDER BY id DESC LIMIT ?";
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, Math.max(1, Math.min(200, limit)));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String js = rs.getString(1);
                        CompletedTrade ct = tradeFromJsonString(js);
                        if (ct != null) out.add(ct);
                    }
                }
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] readRecentRtTrades failed: " + e.getMessage());
            }
            return out;
        }

        static long insertEngineState(long nowMs, String reason, String json) {
            if (!AUDIT_DB_ENABLED) return -1;
            init();
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO engine_state(savedAtMs,buildTag,instId,reason,json) VALUES(?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, nowMs);
                ps.setString(2, BUILD_TAG);
                ps.setString(3, INST_ID);
                ps.setString(4, reason);
                ps.setString(5, json);
                ps.executeUpdate();
                long id = -1;
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) id = rs.getLong(1);
                }
                if (id > 0 && RT_STATE_KEEP > 0) {
                    try (PreparedStatement ps2 = c.prepareStatement(
                            "DELETE FROM engine_state WHERE id IN (" +
                                    "SELECT id FROM engine_state ORDER BY id DESC LIMIT -1 OFFSET ?" +
                                    ");")) {
                        ps2.setInt(1, Math.max(1, RT_STATE_KEEP));
                        ps2.executeUpdate();
                    }
                }
                return id;
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] insertEngineState failed: " + e.getMessage());
                return -1;
            }
        }

        static RtStateRow readLastEngineState() {
            if (!AUDIT_DB_ENABLED) return null;
            init();
            String sql = "SELECT id,savedAtMs,buildTag,instId,reason,json FROM engine_state ORDER BY id DESC LIMIT 1";
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                RtStateRow r = new RtStateRow();
                r.id = rs.getLong(1);
                r.savedAtMs = rs.getLong(2);
                r.buildTag = rs.getString(3);
                r.instId = rs.getString(4);
                r.reason = rs.getString(5);
                r.json = rs.getString(6);
                return r;
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] readLastEngineState failed: " + e.getMessage());
                return null;
            }
        }

        static long insertSnapshotBatch(String tf, long snapAtTs, long nowMs, List<Candle> candles, int tailN, String note) {
            if (!AUDIT_DB_ENABLED || !SNAPSHOT_ENABLED) return -1;
            init();
            if (candles == null || candles.isEmpty()) return -1;

            int n = Math.min(Math.max(10, tailN), candles.size());
            int from = Math.max(0, candles.size() - n);
            List<Candle> tail = candles.subList(from, candles.size());

            try (Connection c = conn()) {
                c.setAutoCommit(false);

                long batchId;
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO snapshot_batch(savedAtMs,buildTag,instId,tf,snapAtTs,n,note) VALUES(?,?,?,?,?,?,?)",
                        Statement.RETURN_GENERATED_KEYS)) {
                    ps.setLong(1, nowMs);
                    ps.setString(2, BUILD_TAG);
                    ps.setString(3, INST_ID);
                    ps.setString(4, tf);
                    ps.setLong(5, snapAtTs);
                    ps.setInt(6, n);
                    ps.setString(7, note);
                    ps.executeUpdate();
                    try (ResultSet rs = ps.getGeneratedKeys()) {
                        batchId = (rs.next() ? rs.getLong(1) : -1L);
                    }
                }

                if (batchId <= 0) {
                    c.rollback();
                    return -1;
                }

                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT OR REPLACE INTO candle_snapshot(batchId,idx,ts,o,h,l,c,vol,confirm) VALUES(?,?,?,?,?,?,?,?,?)")) {
                    int idx = 0;
                    for (Candle k : tail) {
                        ps.setLong(1, batchId);
                        ps.setInt(2, idx++);
                        ps.setLong(3, k.ts);
                        ps.setDouble(4, k.o);
                        ps.setDouble(5, k.h);
                        ps.setDouble(6, k.l);
                        ps.setDouble(7, k.c);
                        ps.setDouble(8, k.vol);
                        ps.setInt(9, k.confirm);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }

                // retention：每个 tf 只保留最近 SNAP_KEEP_BATCHES 个 batch
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM candle_snapshot WHERE batchId IN (" +
                                "SELECT batchId FROM snapshot_batch WHERE tf=? ORDER BY batchId DESC LIMIT -1 OFFSET ?" +
                                ");")) {
                    ps.setString(1, tf);
                    ps.setInt(2, Math.max(1, SNAP_KEEP_BATCHES));
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM snapshot_batch WHERE tf=? AND batchId IN (" +
                                "SELECT batchId FROM snapshot_batch WHERE tf=? ORDER BY batchId DESC LIMIT -1 OFFSET ?" +
                                ");")) {
                    ps.setString(1, tf);
                    ps.setString(2, tf);
                    ps.setInt(3, Math.max(1, SNAP_KEEP_BATCHES));
                    ps.executeUpdate();
                }

                c.commit();
                return batchId;
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] insertSnapshotBatch failed: " + e.getMessage());
                return -1;
            }
        }

        static List<Candle> loadSnapshotBatch(long batchId) {
            if (!AUDIT_DB_ENABLED || batchId <= 0) return Collections.emptyList();
            init();
            List<Candle> out = new ArrayList<>();
            try (Connection c = conn();
                 PreparedStatement ps = c.prepareStatement(
                         "SELECT ts,o,h,l,c,vol,confirm FROM candle_snapshot WHERE batchId=? ORDER BY idx ASC")) {
                ps.setLong(1, batchId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // Candle has no no-arg ctor in this project; use the full-arg ctor that matches (ts,o,h,l,c,vol,confirm)
                        Candle k = new Candle(
                                rs.getLong(1),
                                rs.getDouble(2),
                                rs.getDouble(3),
                                rs.getDouble(4),
                                rs.getDouble(5),
                                rs.getDouble(6),
                                rs.getInt(7)
                        );
                        out.add(k);
                    }
                }
            } catch (Exception e) {
                System.out.println("[AUDIT-DB] loadSnapshotBatch failed: " + e.getMessage());
            }
            return out;
        }

        // ===== JSON helpers for rt_trade (store full CompletedTrade) =====
        static String tradeToJsonString(CompletedTrade ct, String note) {
            if (ct == null) return null;
            JSONObject j = new JSONObject();
            j.put("enterTs", ct.enterTs);
            j.put("exitTs", ct.exitTs);
            j.put("signalTs", ct.signalTs);
            j.put("side", ct.side == null ? JSONObject.NULL : (ct.side == Side.LONG ? "LONG" : "SHORT"));
            j.put("entry", ct.entry);
            j.put("exit", ct.exit);
            j.put("pnlPoints", ct.pnlPoints);
            j.put("grossPnlPoints", ct.grossPnlPoints);
            j.put("feePoints", ct.feePoints);
            j.put("isStop", ct.isStop);
            j.put("reason", ct.reason == null ? "" : ct.reason);

            j.put("entryEquity", ct.entryEquity);
            j.put("exitEquity", ct.exitEquity);
            j.put("pnlUSDT", ct.pnlUSDT);
            j.put("rMultiple", ct.rMultiple);
            j.put("mult", ct.mult);
            j.put("slPointsUsed", ct.slPointsUsed);
            j.put("regime", ct.regime == null ? "" : ct.regime);
            j.put("trendPtsAfter", ct.trendPtsAfter);
            j.put("bayesPosterior", ct.bayesPosterior);
            j.put("mfe", ct.mfe);
            j.put("mae", ct.mae);

            j.put("note", note == null ? "" : note);
            return j.toString();
        }

        static CompletedTrade tradeFromJsonString(String js) {
            if (js == null || js.isBlank()) return null;
            try {
                JSONObject j = new JSONObject(js);
                CompletedTrade ct = new CompletedTrade();
                ct.enterTs = j.optLong("enterTs", 0L);
                ct.exitTs = j.optLong("exitTs", 0L);
                ct.signalTs = j.optLong("signalTs", 0L);
                String s = j.optString("side", "");
                if ("LONG".equalsIgnoreCase(s)) ct.side = Side.LONG;
                else if ("SHORT".equalsIgnoreCase(s)) ct.side = Side.SHORT;

                ct.entry = j.optDouble("entry", 0.0);
                ct.exit = j.optDouble("exit", 0.0);
                ct.pnlPoints = j.optDouble("pnlPoints", 0.0);
                ct.grossPnlPoints = j.optDouble("grossPnlPoints", 0.0);
                ct.feePoints = j.optDouble("feePoints", 0.0);
                ct.isStop = j.optBoolean("isStop", false);
                ct.reason = j.optString("reason", "");

                ct.entryEquity = j.optDouble("entryEquity", 0.0);
                ct.exitEquity = j.optDouble("exitEquity", 0.0);
                ct.pnlUSDT = j.optDouble("pnlUSDT", 0.0);
                ct.rMultiple = j.optDouble("rMultiple", 0.0);
                ct.mult = j.optDouble("mult", 0.0);
                ct.slPointsUsed = j.optDouble("slPointsUsed", 0.0);
                ct.regime = j.optString("regime", "");
                ct.trendPtsAfter = j.optDouble("trendPtsAfter", 0.0);
                ct.bayesPosterior = j.optDouble("bayesPosterior", 0.5);
                ct.mfe = j.optDouble("mfe", 0.0);
                ct.mae = j.optDouble("mae", 0.0);
                return ct;
            } catch (Exception e) {
                return null;
            }
        }

    }


    // ===================== SSOT：K线 SQLite（闹钟/回测只读；拉取器只增量写） =====================
    static final class CandleDb {
        static volatile boolean inited = false;

        static void init() {
            if (!USE_DB_CACHE) return;
            if (inited) return;
            synchronized (CandleDb.class) {
                if (inited) return;
                try { Class.forName("org.sqlite.JDBC"); } catch (Throwable ignore) {}
                try (Connection c = conn(); Statement st = c.createStatement()) {
                    st.executeUpdate("PRAGMA journal_mode=WAL;");
                    st.executeUpdate("PRAGMA synchronous=NORMAL;");
                    st.executeUpdate("PRAGMA busy_timeout=5000;");
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS candles (" +
                            "instId TEXT NOT NULL," +
                            "bar TEXT NOT NULL," +
                            "ts INTEGER NOT NULL," +
                            "o REAL NOT NULL," +
                            "h REAL NOT NULL," +
                            "l REAL NOT NULL," +
                            "c REAL NOT NULL," +
                            "vol REAL NOT NULL," +
                            "confirm INTEGER NOT NULL," +
                            "updatedAtMs INTEGER NOT NULL," +
                            "PRIMARY KEY(instId, bar, ts)" +
                            ");");
                    st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_candles_inst_bar_ts ON candles(instId, bar, ts);");
                } catch (SQLException e) {
                    throw new RuntimeException("CandleDb init failed: " + e.getMessage(), e);
                }
                inited = true;
            }
        }

        static Connection conn() throws SQLException {
            return DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);
        }

        static long barMs(String bar) {
            if (bar == null) return BAR30_MS;
            String b = bar.trim();
            if (b.equalsIgnoreCase("30m")) return BAR30_MS;
            if (b.equalsIgnoreCase("1H") || b.equalsIgnoreCase("1h")) return BAR1H_MS;
            // 保守：默认 30m
            return BAR30_MS;
        }

        static int confirmByTime(long ts, long barMs, long nowMs) {
            long end = ts + barMs;
            long cut = nowMs - SAFE_CLOSE_MS;
            return (end <= cut) ? 1 : 0;
        }

        static void upsertNormalized(String instId, String bar, List<Candle> candles, long nowMs) throws SQLException {
            if (candles == null || candles.isEmpty()) return;
            init();
            long bms = barMs(bar);
            long updatedAt = nowMs;

            String sql = "INSERT INTO candles(instId, bar, ts, o, h, l, c, vol, confirm, updatedAtMs) " +
                    "VALUES(?,?,?,?,?,?,?,?,?,?) " +
                    "ON CONFLICT(instId, bar, ts) DO UPDATE SET " +
                    "o=excluded.o, h=excluded.h, l=excluded.l, c=excluded.c, vol=excluded.vol, " +
                    "confirm=CASE WHEN candles.confirm=1 THEN 1 ELSE excluded.confirm END, " +
                    "updatedAtMs=excluded.updatedAtMs";

            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                c.setAutoCommit(false);
                for (Candle k : candles) {
                    if (k == null) continue;
                    int conf = confirmByTime(k.ts, bms, nowMs);
                    ps.setString(1, instId);
                    ps.setString(2, bar);
                    ps.setLong(3, k.ts);
                    ps.setDouble(4, k.o);
                    ps.setDouble(5, k.h);
                    ps.setDouble(6, k.l);
                    ps.setDouble(7, k.c);
                    ps.setDouble(8, k.vol);
                    ps.setInt(9, conf);
                    ps.setLong(10, updatedAt);
                    ps.addBatch();
                }
                ps.executeBatch();
                c.commit();
            }
        }

        static List<Candle> loadRange(String instId, String bar, long startMs, long endMs, boolean includeUnconfirmed) throws SQLException {
            init();
            String sql = "SELECT ts,o,h,l,c,vol,confirm FROM candles " +
                    "WHERE instId=? AND bar=? AND ts>=? AND ts<=? " +
                    (includeUnconfirmed ? "" : "AND confirm=1 ") +
                    "ORDER BY ts ASC";
            ArrayList<Candle> out = new ArrayList<>();
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, instId);
                ps.setString(2, bar);
                ps.setLong(3, startMs);
                ps.setLong(4, endMs);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long ts = rs.getLong(1);
                        double o = rs.getDouble(2);
                        double h = rs.getDouble(3);
                        double l = rs.getDouble(4);
                        double cl= rs.getDouble(5);
                        double vol= rs.getDouble(6);
                        int confirm = rs.getInt(7);
                        out.add(new Candle(ts, o, h, l, cl, vol, confirm));
                    }
                }
            }
            return out;
        }

        static List<Candle> loadLatest(String instId, String bar, int limit, boolean includeUnconfirmed) throws SQLException {
            init();
            String sql = "SELECT ts,o,h,l,c,vol,confirm FROM candles " +
                    "WHERE instId=? AND bar=? " +
                    (includeUnconfirmed ? "" : "AND confirm=1 ") +
                    "ORDER BY ts DESC LIMIT ?";
            ArrayList<Candle> tmp = new ArrayList<>();
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, instId);
                ps.setString(2, bar);
                ps.setInt(3, Math.max(1, limit));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long ts = rs.getLong(1);
                        double o = rs.getDouble(2);
                        double h = rs.getDouble(3);
                        double l = rs.getDouble(4);
                        double cl= rs.getDouble(5);
                        double vol= rs.getDouble(6);
                        int confirm = rs.getInt(7);
                        tmp.add(new Candle(ts, o, h, l, cl, vol, confirm));
                    }
                }
            }
            Collections.reverse(tmp);
            return tmp;
        }

        static long countBars(String instId, String bar) throws SQLException {
            init();
            String sql = "SELECT COUNT(1) FROM candles WHERE instId=? AND bar=?";
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, instId);
                ps.setString(2, bar);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong(1) : 0L;
                }
            }
        }

        static void syncLastYearsIfNeeded(String instId, String bar, int years, long nowMs) throws Exception {
            init();
            long n = 0;
            try { n = countBars(instId, bar); } catch (Throwable ignore) {}
            if (n >= 1000) return; // 已有数据，跳过全量
            long start = Instant.ofEpochMilli(nowMs).minus(Duration.ofDays(365L * Math.max(1, years) + 7)).toEpochMilli();
            System.out.printf(Locale.US, "[CANDLE-DB] bootstrap sync %s %s: %d years...%n", instId, bar, years);
            List<Candle> net = fetchRangeCandlesNet(instId, bar, start, nowMs);
            upsertNormalized(instId, bar, net, nowMs);
            validateInvariants(instId, bar, barMs(bar), nowMs);
        }

        static void ingestRecent(String instId, String bar, int bars, long nowMs) throws Exception {
            init();
            // 用 history-candles 分页拿最近 bars 根（更稳定），写入 DB
            List<Candle> net = fetchRecentByCountNet(bar, Math.max(50, bars));
            upsertNormalized(instId, bar, net, nowMs);
            validateInvariants(instId, bar, barMs(bar), nowMs);
        }

        static void ingestLatest(String instId, String bar, int limit, long nowMs) throws Exception {
            init();
            List<Candle> net = fetchLatestCandlesNet(bar, Math.max(10, limit));
            upsertNormalized(instId, bar, net, nowMs);
            // latest 里可能包含未收盘K；不影响 invariant（步长/重复仍可校验）
            validateInvariants(instId, bar, barMs(bar), nowMs);
        }

        static void validateInvariants(String instId, String bar, long barMs, long nowMs) throws Exception {
            init();
            int maxBars = Math.max(200, CANDLE_INVARIANT_MAX_BARS);
            // 只校验最近 maxBars 根（性能）
            List<Long> tsList = new ArrayList<>();
            List<Integer> confList = new ArrayList<>();

            String sql = "SELECT ts,confirm FROM candles WHERE instId=? AND bar=? ORDER BY ts DESC LIMIT ?";
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, instId);
                ps.setString(2, bar);
                ps.setInt(3, maxBars);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tsList.add(rs.getLong(1));
                        confList.add(rs.getInt(2));
                    }
                }
            }
            if (tsList.isEmpty()) return;
            Collections.reverse(tsList);
            Collections.reverse(confList);

            // 1) ts 单调递增（严格）
            for (int i = 1; i < tsList.size(); i++) {
                if (tsList.get(i) <= tsList.get(i - 1)) {
                    String msg = String.format(Locale.US,
                            "[INV][%s %s] ts not strictly increasing: %d <= %d (idx=%d/%d)",
                            instId, bar, tsList.get(i), tsList.get(i - 1), i, tsList.size());
                    if (CANDLE_INVARIANT_HARD_FAIL) throw new IllegalStateException(msg);
                    System.out.println(msg);
                    break;
                }
            }

            // 2) 步长恒定（允许最后一根未收盘，但 ts 仍应按步长递增）
            boolean stepOk = true;
            int badI = -1;
            long badDt = 0;
            for (int i = 1; i < tsList.size(); i++) {
                long dt = tsList.get(i) - tsList.get(i - 1);
                if (dt != barMs) {
                    stepOk = false;
                    badI = i;
                    badDt = dt;
                    break;
                }
            }
            if (!stepOk) {
                String msg = String.format(Locale.US,
                        "[INV][%s %s] step not constant: dt=%d (expect=%d) at i=%d (ts=%d -> %d)",
                        instId, bar, badDt, barMs, badI, tsList.get(badI - 1), tsList.get(badI));
                System.out.println(msg);

                if (CANDLE_AUTO_REPAIR_GAPS) {
                    // 自动补洞：把缺口区间用网络拉取再写回
                    long from = tsList.get(Math.max(0, badI - 2));
                    long to = tsList.get(Math.min(tsList.size() - 1, badI + 2)) + barMs;
                    System.out.printf(Locale.US, "[INV-REPAIR] fetchRange %s %s: [%d, %d)%n", instId, bar, from, to);
                    List<Candle> net = fetchRangeCandlesNet(instId, bar, from, to);
                    upsertNormalized(instId, bar, net, nowMs);
                    // 再校验一次（防止无限递归：最多 3 次）
                    int d = INV_DEPTH.get();
                    if (d >= 2) {
                        String msg2 = "[INV-REPAIR] too many retries, stop.";
                        if (CANDLE_INVARIANT_HARD_FAIL) throw new IllegalStateException(msg2);
                        System.out.println(msg2);
                        return;
                    }
                    INV_DEPTH.set(d + 1);
                    try { validateInvariants(instId, bar, barMs, nowMs); } finally { INV_DEPTH.set(d); }
                    return;
                }

                if (CANDLE_INVARIANT_HARD_FAIL) throw new IllegalStateException(msg);
            }

            // 3) “可用于信号计算区”必须只用 confirm=1（我们写入时已用时间规则归一化）
            //    这里做个轻量检查：找到最后一个 confirm=1，确保它确实已到收盘时间
            int lastConfIdx = -1;
            for (int i = confList.size() - 1; i >= 0; i--) {
                if (confList.get(i) == 1) { lastConfIdx = i; break; }
            }
            if (lastConfIdx >= 0) {
                long ts = tsList.get(lastConfIdx);
                long end = ts + barMs;
                long cut = nowMs - SAFE_CLOSE_MS;
                if (end > cut) {
                    String msg = String.format(Locale.US,
                            "[INV][%s %s] last confirm=1 is not closed by time: ts=%d end=%d now=%d cut=%d",
                            instId, bar, ts, end, nowMs, cut);
                    if (CANDLE_INVARIANT_HARD_FAIL) throw new IllegalStateException(msg);
                    System.out.println(msg);
                }
            }
        }
    }

    static void initAuditDbAndState() {
        try {
            AuditDb.init();
            System.out.println("[AUDIT-DB] enabled=" + AUDIT_DB_ENABLED + " file=" + AUDIT_DB_FILE + " viewUseReplay=" + VIEW_USE_REPLAY);
        } catch (Throwable t) {
            System.out.println("[AUDIT-DB] init exception: " + t.getMessage());
        }
        if (RESTORE_LIVE_STATE) {
            restoreLiveState();
        }
    }

    static void persistSnapshotsIfEnabled(long nowMs, long snapAtTs, List<Candle> m30Closed, List<Candle> h1Closed) {
        if (!SNAPSHOT_ENABLED) return;
        try {
            long b30 = AuditDb.insertSnapshotBatch("30m", snapAtTs, nowMs, m30Closed, SNAP_30M_N, "runOnce confirmed");
            if (b30 > 0) LAST_SNAP_BATCH_30M = b30;
        } catch (Throwable t) {
            System.out.println("[SNAPSHOT] save 30m failed: " + t.getMessage());
        }
        try {
            long b1 = AuditDb.insertSnapshotBatch("1H", snapAtTs, nowMs, h1Closed, SNAP_1H_N, "runOnce confirmed");
            if (b1 > 0) LAST_SNAP_BATCH_1H = b1;
        } catch (Throwable t) {
            System.out.println("[SNAPSHOT] save 1H failed: " + t.getMessage());
        }
        persistLiveState("snapshot");
    }

    static void persistRtTriggerIfEnabled(long nowMs, Candle sigC0, long entryTs, Side side,
                                          double entryRefK0Close, double nextOpenPx,
                                          MacdPoint m0, MacdPoint m1, int diffInt,
                                          String t1h, String note) {
        try {
            RtTrigger t = new RtTrigger();
            t.savedAtMs = nowMs;
            t.buildTag = BUILD_TAG;
            t.instId = INST_ID;
            t.tf = "30m";
            // signalTs = 信号K0开盘；若 sigC0 缺失，则按 entryTs 推回上一根30m（K0 close = K1 open）
            t.signalTs = (sigC0 != null ? sigC0.ts : (entryTs - BAR30_MS));
            t.entryTs = entryTs;
            t.side = side;
            t.entryRef = entryRefK0Close;
            t.nextOpen = nextOpenPx;
            t.dif = (m0 != null ? m0.dif : 0);
            t.dea = (m0 != null ? m0.dea : 0);
            t.prevDif = (m1 != null ? m1.dif : 0);
            t.prevDea = (m1 != null ? m1.dea : 0);
            t.diffInt = diffInt;
            t.t1h = t1h;
            t.note = note;
            t.id = AuditDb.insertRtTrigger(t);
            if (t.id > 0) {
                System.out.println(String.format(Locale.US,
                        "[RT-TRIGGER][DB-OK] id=%d tf=%s side=%s signalTs=%s entryTs=%s entryRef=%.2f nextOpen=%.2f dif=%.6f dea=%.6f note=%s",
                        t.id, t.tf,
                        (t.side==null?"NA":(t.side==Side.LONG?"LONG":"SHORT")),
                        fmtOpen(t.signalTs), fmtOpen(t.entryTs),
                        t.entryRef, t.nextOpen, t.dif, t.dea,
                        (t.note==null?"":t.note)));
            } else {
                System.out.println("[RT-TRIGGER][DB-WARN] insert failed (id=" + t.id + ")");
            }
        } catch (Throwable ex) {
            System.out.println("[AUDIT-RT] persist trigger failed: " + ex.getMessage());
        }
    }


    static JSONObject alignToJson(AlignAudit a) {
        JSONObject o = new JSONObject();
        o.put("src", a.src);
        o.put("nowMs", a.nowMs);
        o.put("signalTs", a.signalTs);
        o.put("sigCloseTs", a.sigCloseTs);
        o.put("sigConfirm", a.sigConfirm);
        o.put("sigO", a.sigO);
        o.put("sigC", a.sigC);
        o.put("entryTs", a.entryTs);
        o.put("entryRefPx", a.entryRefPx);
        o.put("k1OpenPx", a.k1OpenPx);
        o.put("note", a.note == null ? "" : a.note);
        return o;
    }

    static AlignAudit alignFromJson(JSONObject o) {
        if (o == null) return null;
        AlignAudit a = new AlignAudit();
        a.src = o.optString("src", "LOAD");
        a.nowMs = o.optLong("nowMs", System.currentTimeMillis());
        a.signalTs = o.optLong("signalTs", 0L);
        a.sigCloseTs = o.optLong("sigCloseTs", a.signalTs + BAR30_MS);
        a.sigConfirm = o.optInt("sigConfirm", -2);
        a.sigO = o.has("sigO") ? o.optDouble("sigO", Double.NaN) : Double.NaN;
        a.sigC = o.has("sigC") ? o.optDouble("sigC", Double.NaN) : Double.NaN;
        a.entryTs = o.optLong("entryTs", 0L);
        a.entryRefPx = o.has("entryRefPx") ? o.optDouble("entryRefPx", Double.NaN) : (o.has("entryRef") ? o.optDouble("entryRef", Double.NaN) : Double.NaN);
        a.k1OpenPx = o.has("k1OpenPx") ? o.optDouble("k1OpenPx", Double.NaN) : (o.has("k1Open") ? o.optDouble("k1Open", Double.NaN) : Double.NaN);
        a.note = o.optString("note", "");
        return a;
    }

    static JSONObject posToJson(Position p) {
        JSONObject o = new JSONObject();
        o.put("isVirtual", p.isVirtual);
        o.put("side", p.side == null ? JSONObject.NULL : (p.side == Side.LONG ? "LONG" : "SHORT"));
        o.put("entry", p.entry);
        o.put("signalTs", p.signalTs);
        o.put("entryTs", p.entryTs);
        o.put("slPoints", p.slPoints);
        o.put("slPointsAtEntry", p.slPointsAtEntry);
        o.put("entryTierMult", p.entryTierMult);
        o.put("entryMult", p.entryMult);
        o.put("entryVoteScale", p.entryVoteScale);
        o.put("entryVoteScore", p.entryVoteScore);
        o.put("entryVoteDetail", p.entryVoteDetail == null ? "" : p.entryVoteDetail);
        o.put("regimeAtEntry", p.regimeAtEntry == null ? "" : p.regimeAtEntry);
        o.put("trendPtsAtEntry", p.trendPtsAtEntry);
        o.put("entryNotional", p.entryNotional);
        o.put("entryEquity", p.entryEquity);
        o.put("okxResistanceAtEntry", p.okxResistanceAtEntry);
        o.put("okxSupportAtEntry", p.okxSupportAtEntry);
        o.put("okxGapAtEntry", p.okxGapAtEntry);
        o.put("mfe", p.mfe);
        o.put("mae", p.mae);
        o.put("bayesPosterior", p.bayesPosterior);
        o.put("bayesScale", p.bayesScale);
        return o;
    }

    static Position jsonToPos(JSONObject o) {
        Position p = new Position();
        p.isVirtual = o.optBoolean("isVirtual", true);
        String s = o.optString("side", "");
        p.side = ("LONG".equalsIgnoreCase(s) ? Side.LONG : ("SHORT".equalsIgnoreCase(s) ? Side.SHORT : null));
        p.entry = o.optDouble("entry", 0);
        p.signalTs = o.optLong("signalTs", 0);
        p.entryTs = o.optLong("entryTs", 0);
        p.slPoints = o.optDouble("slPoints", 0);
        p.slPointsAtEntry = o.optDouble("slPointsAtEntry", p.slPoints);
        p.entryTierMult = o.optDouble("entryTierMult", 0);
        p.entryMult = o.optDouble("entryMult", 0);
        p.entryVoteScale = o.optDouble("entryVoteScale", 1.0);
        p.entryVoteScore = o.optInt("entryVoteScore", 0);
        p.entryVoteDetail = o.optString("entryVoteDetail", "");
        p.regimeAtEntry = o.optString("regimeAtEntry", "");
        p.trendPtsAtEntry = o.optDouble("trendPtsAtEntry", 0);
        p.entryNotional = o.optDouble("entryNotional", 0);
        p.entryEquity = o.optDouble("entryEquity", 0);
        p.okxResistanceAtEntry = o.optDouble("okxResistanceAtEntry", 0);
        p.okxSupportAtEntry = o.optDouble("okxSupportAtEntry", 0);
        p.okxGapAtEntry = o.optDouble("okxGapAtEntry", 0);
        p.mfe = o.optDouble("mfe", 0);
        p.mae = o.optDouble("mae", 0);
        p.bayesPosterior = o.optDouble("bayesPosterior", 0.5);
        p.bayesScale = o.optDouble("bayesScale", 1.0);
        return p;
    }

    static void persistLiveState(String reason) {
        try {
            JSONObject root = new JSONObject();
            root.put("buildTag", BUILD_TAG);
            root.put("savedAtMs", System.currentTimeMillis());
            root.put("reason", reason == null ? "" : reason);

            root.put("autoEntryPending", autoEntryPending);
            root.put("autoEntryExpireAtMs", autoEntryExpireAtMs);
            root.put("autoEntryLockedEntryTs", autoEntryLockedEntryTs);
            root.put("autoClosePending", autoClosePending);

            root.put("lastRtEntryAlertTs", LAST_RT_ENTRY_ALERT_TS);
            root.put("lastRtSignalTsAtAlert", LAST_RT_SIGNAL_TS_AT_ALERT);
            root.put("pendingVerifyEntryTs", PENDING_VERIFY_ENTRY_TS);
            root.put("pendingVerifySetAtMs", PENDING_VERIFY_SET_AT_MS);

            root.put("lastSnapBatch30m", LAST_SNAP_BATCH_30M);
            root.put("lastSnapBatch1h", LAST_SNAP_BATCH_1H);

            // ===== EngineState（单一真源） =====
            root.put("pos", (LIVE_ENGINE.pos == null ? JSONObject.NULL : posToJson(LIVE_ENGINE.pos)));
            root.put("lastStopSigTs", LIVE_ENGINE.lastStopSigTs);
            root.put("lastTpSigTs", LIVE_ENGINE.lastTpSigTs);
            root.put("lastR1VetoLongTs", LIVE_ENGINE.lastR1VetoLongTs);
            root.put("lastR1VetoShortTs", LIVE_ENGINE.lastR1VetoShortTs);
            root.put("stopCooldownUntilEntryTs", LIVE_ENGINE.stopCooldownUntilEntryTs);
            root.put("tpCooldownUntilEntryTs", LIVE_ENGINE.tpCooldownUntilEntryTs);
            root.put("lastProcessedSigTs", LIVE_ENGINE.lastProcessedSigTs);
            root.put("equity", LIVE_ENGINE.equity);
            root.put("srmf", (LIVE_ENGINE.srmf == null ? JSONObject.NULL : SRMF.toJson(LIVE_ENGINE.srmf)));


            root.put("bayesState", (LIVE_ENGINE.bayesState == null ? JSONObject.NULL : MultiBinBayesFilter.stateToJson(LIVE_ENGINE.bayesState, BAYES_LOOKBACK_TRADES)));
            root.put("currentBayesSnap", (LIVE_ENGINE.currentBayesSnap == null ? JSONObject.NULL : MultiBinBayesFilter.snapToJson(LIVE_ENGINE.currentBayesSnap)));

            // 把 rt/hist audit_last 的内容也带一份（便于单文件排查 & 重启后仍可 [ALIGN-CHECK]）
            root.put("lastRtAudit", LAST_RT_AUDIT == null ? JSONObject.NULL : alignToJson(LAST_RT_AUDIT));
            root.put("lastHistAudit", LAST_HIST_AUDIT == null ? JSONObject.NULL : alignToJson(LAST_HIST_AUDIT));

            Path p = Paths.get(LIVE_STATE_FILE);
            Path tmp = Paths.get(LIVE_STATE_FILE + ".tmp");
            Files.writeString(tmp, root.toString(2), StandardCharsets.UTF_8);
            try {
                Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception moveEx) {
                Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING);
            }

            // 同步写入 audit DB（跨重启一致：EngineState 也落库）
            if (AUDIT_DB_ENABLED) {
                long nowMs = root.optLong("savedAtMs", System.currentTimeMillis());
                AuditDb.insertEngineState(nowMs, "persist:" + (reason == null ? "" : reason), root.toString());
            }
        } catch (Exception e) {
            System.out.println("[STATE] persistLiveState failed: " + e.getMessage());
        }
    }

    static void restoreLiveState() {
        try {
            JSONObject root = null;
            long fileSavedAt = -1L;

            // 1) 先尝试从文件恢复
            try {
                Path p = Paths.get(LIVE_STATE_FILE);
                if (Files.exists(p)) {
                    String s = Files.readString(p, StandardCharsets.UTF_8);
                    if (s != null && !s.trim().isEmpty()) {
                        JSONObject tmp = new JSONObject(s);
                        root = tmp;
                        fileSavedAt = tmp.optLong("savedAtMs", -1L);
                    }
                }
            } catch (Exception ignore) {
            }

            // 2) 再尝试从 audit DB 恢复（更稳：跨重启/跨目录也一致）
            if (AUDIT_DB_ENABLED) {
                RtStateRow dbRow = AuditDb.readLastEngineState();
                if (dbRow != null && dbRow.json != null && !dbRow.json.isBlank()) {
                    try {
                        JSONObject tmp = new JSONObject(dbRow.json);
                        long dbSavedAt = tmp.optLong("savedAtMs", dbRow.savedAtMs);
                        if (root == null || dbSavedAt > fileSavedAt) {
                            root = tmp;
                            System.out.println("[STATE] restoreLiveState from auditDB: savedAtMs=" + dbSavedAt + " reason=" + (dbRow.reason == null ? "" : dbRow.reason));
                        }
                    } catch (Exception ignore) {
                    }
                }
            }

            if (root == null) return;

            autoEntryPending = root.optBoolean("autoEntryPending", autoEntryPending);
            autoEntryExpireAtMs = root.optLong("autoEntryExpireAtMs", autoEntryExpireAtMs);
            autoEntryLockedEntryTs = root.optLong("autoEntryLockedEntryTs", autoEntryLockedEntryTs);
            autoClosePending = root.optBoolean("autoClosePending", autoClosePending);

            LAST_RT_ENTRY_ALERT_TS = root.optLong("lastRtEntryAlertTs", LAST_RT_ENTRY_ALERT_TS);
            LAST_RT_SIGNAL_TS_AT_ALERT = root.optLong("lastRtSignalTsAtAlert", LAST_RT_SIGNAL_TS_AT_ALERT);
            PENDING_VERIFY_ENTRY_TS = root.optLong("pendingVerifyEntryTs", PENDING_VERIFY_ENTRY_TS);
            PENDING_VERIFY_SET_AT_MS = root.optLong("pendingVerifySetAtMs", PENDING_VERIFY_SET_AT_MS);

            // 允许从 live_state 中恢复最后一次对齐审计（避免刚重启就一直 [ALIGN-CHECK][SKIP]）
            if (DEBUG_ALIGN) {
                try {
                    if (LAST_RT_AUDIT == null && root.has("lastRtAudit") && !root.isNull("lastRtAudit")) {
                        LAST_RT_AUDIT = alignFromJson(root.getJSONObject("lastRtAudit"));
                    }
                    if (LAST_HIST_AUDIT == null && root.has("lastHistAudit") && !root.isNull("lastHistAudit")) {
                        LAST_HIST_AUDIT = alignFromJson(root.getJSONObject("lastHistAudit"));
                    }
                } catch (Exception ignore) {
                }
            }

            LAST_SNAP_BATCH_30M = root.optLong("lastSnapBatch30m", LAST_SNAP_BATCH_30M);
            LAST_SNAP_BATCH_1H  = root.optLong("lastSnapBatch1h",  LAST_SNAP_BATCH_1H);

            if (root.has("pos") && !root.isNull("pos")) {
                LIVE_ENGINE.pos = jsonToPos(root.getJSONObject("pos"));
            }

            LIVE_ENGINE.lastStopSigTs = root.optLong("lastStopSigTs", LIVE_ENGINE.lastStopSigTs);
            LIVE_ENGINE.lastTpSigTs = root.optLong("lastTpSigTs", LIVE_ENGINE.lastTpSigTs);
            LIVE_ENGINE.lastR1VetoLongTs = root.optLong("lastR1VetoLongTs", LIVE_ENGINE.lastR1VetoLongTs);
            LIVE_ENGINE.lastR1VetoShortTs = root.optLong("lastR1VetoShortTs", LIVE_ENGINE.lastR1VetoShortTs);
            LIVE_ENGINE.stopCooldownUntilEntryTs = root.optLong("stopCooldownUntilEntryTs", LIVE_ENGINE.stopCooldownUntilEntryTs);
            LIVE_ENGINE.tpCooldownUntilEntryTs = root.optLong("tpCooldownUntilEntryTs", LIVE_ENGINE.tpCooldownUntilEntryTs);
            LIVE_ENGINE.lastProcessedSigTs = root.optLong("lastProcessedSigTs", LIVE_ENGINE.lastProcessedSigTs);
            LIVE_ENGINE.equity = root.optDouble("equity", LIVE_ENGINE.equity);
            if (root.has("srmf") && !root.isNull("srmf")) {
                try { LIVE_ENGINE.srmf = SRMF.fromJson(root.getJSONObject("srmf")); } catch (Exception ignore) {}
                // ===== BAYES：恢复滚动学习状态 + 当前持仓快照 =====
                try {
                    if (root.has("bayesState") && !root.isNull("bayesState")) {
                        LIVE_ENGINE.bayesState = MultiBinBayesFilter.stateFromJson(root.getJSONObject("bayesState"));
                    }
                } catch (Exception ignore) {}
                try {
                    if (root.has("currentBayesSnap") && !root.isNull("currentBayesSnap")) {
                        LIVE_ENGINE.currentBayesSnap = MultiBinBayesFilter.snapFromJson(root.getJSONObject("currentBayesSnap"));
                    }
                } catch (Exception ignore) {}

            }

            Position ppos = LIVE_ENGINE.pos;
            System.out.println("[STATE] restored from " + LIVE_STATE_FILE +
                    " | autoEntryPending=" + autoEntryPending +
                    " pos=" + (ppos == null ? "null" : (ppos.isVirtual ? "VIRTUAL" : "REAL") + "/" + (ppos.side == Side.LONG ? "LONG" : "SHORT") + "@" + String.format(Locale.US, "%.2f", ppos.entry) + " entryTs=" + fmtOpen(ppos.entryTs)) +
                    " | lastRtEntryAlertTs=" + (LAST_RT_ENTRY_ALERT_TS > 0 ? fmtOpen(LAST_RT_ENTRY_ALERT_TS) : "null") +
                    " | lastSnap30m=" + LAST_SNAP_BATCH_30M +
                    " | lastSnap1h=" + LAST_SNAP_BATCH_1H);
        } catch (Exception e) {
            System.out.println("[STATE] restoreLiveState failed: " + e.getMessage());
        }
    }

    // 提示：OKX 下单需要 API Key。请在启动参数中传入：
    // -Dokx.apiKey=... -Dokx.secretKey=... -Dokx.passphrase=...
    static final OkxAutoTradeBridge AUTO = OkxAutoTradeBridge.get();

    // 自动下单任务不能阻塞 30m 定时线程，因此使用单线程执行器串行处理（避免重复下单/重复平仓）
    static final ExecutorService AUTO_ES = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "okx-auto-trade");
            t.setDaemon(true);
            return t;
        }
    });

    // 入场/出场任务状态
    static volatile boolean autoEntryPending = false;
    static volatile boolean autoClosePending = false;
    // 平仓幂等锁：用于 BT-SYNC/ORPHAN 平仓补偿，避免每轮重复提交
    static volatile long autoCloseLockedKey = Long.MIN_VALUE; // 通常为 exitTs；ORPHAN 使用固定 key
    static volatile long autoCloseExpireAtMs = 0L;
    static volatile long autoEntryExpireAtMs = 0L;
    static volatile long autoEntryLockedEntryTs = -1L; // entryTs 幂等锁：同一 entryTs 在有效期内只允许触发一次
    static volatile String autoEntryOrdId = null;
    static volatile Future<?> autoEntryFuture = null;
    static volatile Future<?> autoCloseFuture = null;

    // 实盘持仓状态（由自动下单任务在成交/平仓后更新）
    // LIVE_ENGINE.pos 仍使用原文件的字段；这里仅声明：AUTO 线程会更新 LIVE_ENGINE.pos，因此建议将原 LIVE_ENGINE.pos 改为 volatile。

    static void initAutoTradeIfEnabled() {
        if (!AUTO_TRADE_ENABLED) {
            System.out.println("【OKX-AUTO】autoTrade=OFF（仅提醒，不下单）");
            return;
        }

        // 初始化 simulated / WS 等（从 -Dokx.simulated 读取，key=okx.simulated）
        try {
            AUTO.initFromSystemProps();
        } catch (Exception e) {
            System.out.println("【OKX-AUTO】init 失败：" + e.getMessage());
            e.printStackTrace();
        }

        // 注入 API Key（如未提供，会导致下单失败，但不影响提醒/回测）
        String apiKey = sysStr("okx.apiKey", "");
        String secretKey = sysStr("okx.secretKey", "");
        String passphrase = sysStr("okx.passphrase", "");
        if (!apiKey.isBlank() && !secretKey.isBlank() && !passphrase.isBlank()) {
            AUTO.setAuth(apiKey, secretKey, passphrase);
            System.out.println("【OKX-AUTO】auth 已注入（apiKey/secretKey/passphrase）");
        } else {
            System.out.println("【OKX-AUTO】未提供 okx.apiKey/okx.secretKey/okx.passphrase：将无法真实下单（但仍会打印计划与信号）");
        }
    }

    static class NotionalDecision {
        double baseNotional;
        double notionalUsdt;
        boolean fromFixed;
        String mode;
    }


    // ===== 用于核对“入场名义/最终mult/自动下单传参”的最近一次入场快照（仅打印用途） =====
    static class LastNotionalDebug {
        long entryTs;                 // 入场K1开盘（entryTs=K0收盘=K1开盘）
        long sigTs;                   // 信号K0开盘
        String src;                   // "实时" / "回测" / "预览"
        Side side;

        // mult 分解
        double tierMult;              // trendPts 档位倍率（不含投票）
        int voteScore;                // 票数（例如 4/5/6）
        double voteScale;             // 票数倍率（例如 0.9/1.7）
        int riskVote;                 // 风控票（-2/-1/0/+1）
        double riskCut;               // 风控降档系数（例如 0.7/0.5；不降档=1.0）
        double entryMultFinal;        // 最终 mult（=tierMult*voteScale*riskCut 或 cap 后结果）

        // 名义计算
        String ndMode;                // FIXED / SRMF
        double baseNotional;          // FIXED 模式下的 baseNotional
        double equityUsed;            // SRMF 模式下使用的权益
        double baseRisk;              // baseRisk=equity*RISK_PCT
        double entryPx;               // entryRef（K0收盘价）
        double slPts;                 // 止损点数
        double computedNotional;      // calcEntryNotional 计算出的名义
        double sentToAutoNotional;    // 实际传给自动下单的名义（提交那一刻）
        boolean autoTradeEnabled;     // okx.autoTrade
        boolean autoCanSubmitEntry;   // 本轮是否允许提交入场
        String detail;                // 风控票明细等
    }

    static final LastNotionalDebug LAST_NOTIONAL_DEBUG = new LastNotionalDebug();

    /**
     * 计算入场名义：
     * - 若 JVM 参数 -Dokx.testNotional>0：走旧逻辑（固定 baseNotional * entryMult）。
     * - 否则：按 SRMF 风险反推名义（riskAmt=equity*RISK_PCT*entryMult; notional=riskAmt*entryPx/slPts）。
     */
    static NotionalDecision calcEntryNotional(double entryMult, double entryPx, double slPts) {
        NotionalDecision nd = new NotionalDecision();
        double baseNotional = sysDouble("okx.testNotional", 0.0);
        if (baseNotional > 0.0) {
            nd.fromFixed = true;
            nd.mode = "FIXED";
            nd.baseNotional = baseNotional;
            nd.notionalUsdt = Math.max(0.0, baseNotional * entryMult);
            return nd;
        }
        // SRMF 风险反推名义（默认模式）
        double eq = (LIVE_ENGINE != null && LIVE_ENGINE.equity > 0.0) ? LIVE_ENGINE.equity : capital;
        double baseRisk = eq * RISK_PCT;
        double riskAmt = baseRisk * entryMult;
        double denom = Math.max(1e-9, slPts);
        nd.fromFixed = false;
        nd.mode = "SRMF";
        nd.baseNotional = 0.0; // SRMF 模式下 baseNotional 不使用
        nd.notionalUsdt = Math.max(0.0, riskAmt * entryPx / denom);
        return nd;
    }

    static void printAutoEntryPlan(long sigTs, Side side, double openPx, double rawLimitPx,
                                   double notionalUsdt, double slPts, double slTriggerPx,
                                   double entryMult, double tierMult, double voteScale,
                                   long expireAtMs, NotionalDecision nd) {
        System.out.printf(Locale.US,
                "【AUTO-ENTRY计划】sig=%s | inst=%s%n" +
                        "  1) 当前K线开盘价(open)=%.2f%n" +
                        "  2) 方向=%s%n" +
                        "  3) 挂单价(rawLimit)=%.2f%n" +
                        "  4) 开仓名义(notional)=%.2f USDT (base=%s%s * mult=%.4f | tier=%.4f voteScale=%.4f)%n" +
                        "  5) 动态止损(slPts)=%.2f 点 | 触发价≈%.2f%n" +
                        "  订单=post-only 单挂 | TTL=15min(到 %s)%n",
                FMT_JST.format(Instant.ofEpochMilli(sigTs)),
                INST_ID,
                openPx,
                (side == Side.LONG ? "做多" : "做空"),
                rawLimitPx,
                notionalUsdt,
                (nd != null ? nd.mode : "?"),
                (nd != null && nd.fromFixed ? String.format(Locale.US, "=%.2f", nd.baseNotional) : ""),
                entryMult, tierMult, voteScale,
                slPts, slTriggerPx,
                FMT_JST.format(Instant.ofEpochMilli(expireAtMs))
        );
    }

    /**
     * 名义核对（单行）：用于在【AUTO-ENTRY计划】后、以及提交自动下单前，快速核对
     * tierMult × voteScale × riskCut => entryMult，以及 计算名义 / 传给AUTO名义。
     */
    static void printNotionalCheckLine(String tag) {
        try {
            if (LAST_NOTIONAL_DEBUG == null || LAST_NOTIONAL_DEBUG.entryTs <= 0) return;
            LastNotionalDebug d = LAST_NOTIONAL_DEBUG;
            double multParts = d.tierMult * d.voteScale * d.riskCut;
            System.out.printf(Locale.US,
                    "【名义核对-%s】tier=%.2f × vote=%.2f(票=%d) × cut=%.2f(RV=%d) => parts=%.4f | entryMult=%.4f | 计算名义=%.0f | 传给AUTO=%.0f | mode=%s%n",
                    (tag == null ? "" : tag),
                    d.tierMult, d.voteScale, d.voteScore, d.riskCut, d.riskVote,
                    multParts, d.entryMultFinal,
                    d.computedNotional, d.sentToAutoNotional,
                    (d.ndMode == null ? "?" : d.ndMode)
            );
        } catch (Exception ignore) {
        }
    }


    // ===================== 基础参数 =====================
    static String INST_ID = sysStr("okx.instId", "ETH-USDT-SWAP");

    // =====================  SQLite K线数据库缓存（4年） =====================
    // 目标：启动/回测/定时不再频繁全量拉K线；优先从DB读，不够再分页补齐，并且只入库已收盘(confirm=1)。
    static final boolean USE_DB_CACHE = sysBool("okx.useDbCache", true);
    static final boolean STRICT_SSOT = sysBool("okx.strictSsot", isEthDefaultInst()); // ETH默认true(只读SSOT,配独立puller)；其它币默认false(单进程自缓存,边拉边写库)
    static final boolean DEBUG_K_USED = sysBool("okx.debugKUsed", false);

    // 单一真源（SSOT）K线库：闹钟/回测都只读这里；拉取器只负责增量写入
    // 重要：SSOT 场景下必须避免相对路径导致“同名 DB 其实不是同一份”。
    // 统一用 -Dokx.candlesDb=绝对路径（Puller/Engine 两个进程必须完全一致）。
    static final String DB_FILE_RAW = sysStr("okx.candlesDb", "./okx_candles" + instFileTag() + ".db");
    static final String DB_FILE = canonicalPath(DB_FILE_RAW);
    static final int DB_KEEP_YEARS = (int) sysLong("okx.candlesKeepYears", 4);
    // 不变量：失败是否硬停（建议实盘 true）
    static final boolean CANDLE_INVARIANT_HARD_FAIL = sysBool("okx.candleInvariantHardFail", true);
    // 不变量：每次写入/启动最多校验多少根（越大越慢，建议 5000-20000）
    static final int CANDLE_INVARIANT_MAX_BARS = (int) sysLong("okx.candleInvariantMaxBars", 8000);
    // 写入后自动补洞（gap repair）开关
    static final boolean CANDLE_AUTO_REPAIR_GAPS = sysBool("okx.candleAutoRepairGaps", true);


    // =====================  OKX 合约手续费（双边） =====================
    // 说明：以“点数”方式扣减手续费：feePts = feeRatePerSide * (|entry| + |exit|)
    // 你只需要改 OKX_FEE_RATE_PER_SIDE 即可（例如 0.0002=0.02% 单边；双边=0.04%）
    static final boolean APPLY_OKX_FEE = true;
    static final double OKX_FEE_RATE_PER_SIDE = 0.00075; // 单边费率（按你的实际等级/费率修改）

    static double okxFeePoints(double entryPx, double exitPx) {
        if (!APPLY_OKX_FEE) return 0.0;
        return OKX_FEE_RATE_PER_SIDE * (Math.abs(entryPx) + Math.abs(exitPx));
    }

    // =====================  手续费点数（护栏专用：不受 APPLY_OKX_FEE 开关影响） =====================
    // 说明：
    // - 这里用于‘入场护栏/出场护栏’的计算，即使你临时关闭 APPLY_OKX_FEE 也仍然按费率评估空间是否足够
    static double feePtsRoundTripAtPriceForGuard(double price) {
        return OKX_FEE_RATE_PER_SIDE * 2.0 * Math.abs(price);
    }

    static double feePtsForExitNowForGuard(double entryPx, double exitPx) {
        return OKX_FEE_RATE_PER_SIDE * (Math.abs(entryPx) + Math.abs(exitPx));
    }

    // =====================  解法一：手续费 / 波动门槛（进场过滤） =====================
    // 目标：过滤掉波动不足以覆盖往返手续费的低质量信号（最不易过拟合）
    static boolean USE_FEE_VOL_GUARD = false;
    static int FEE_VOL_ATR_N = 20;
    static double FEE_VOL_K = 4.95; // 波动至少覆盖 K 倍往返手续费（建议 4~6）

    static boolean passFeeVolGuard(double atrPts, double entryPx) {
        if (!USE_FEE_VOL_GUARD) return true;
        if (!Double.isFinite(atrPts) || atrPts <= 0) return false;
        double feePts = feePtsRoundTripAtPriceForGuard(entryPx);
        return atrPts >= FEE_VOL_K * feePts;
    }

    static String feeVolGuardReason(double atrPts, double entryPx) {
        double feePts = feePtsRoundTripAtPriceForGuard(entryPx);
        return String.format(Locale.US, "ATR=%.2f < %.2f (=K%.2f * feePts%.2f)", atrPts, FEE_VOL_K * feePts, FEE_VOL_K, feePts);
    }
    // =====================  解法一.5：动态止损百分比（slPts/entryPrice）分位数门槛 =====================
    // 目的：替代固定阈值(例如 slPts > 32)——避免“价格长期抬升/波动结构变化”导致阈值失效。
    // 核心思想：用你当前的 slPts 计算公式（ATR_N/SL_ATR_K/true-range），把止损点数转换为百分比 slPct=slPts/entryPrice，
    // 然后用过去 lookbackBars 的 slPct 分位数作为动态门槛（只用过去数据，不引入未来函数）。
    static boolean USE_SL_PCT_Q_GATE = false;
    static int SL_PCT_Q_LOOKBACK_BARS = 1000; // 只用过去 N 根30m 的 slPct（每根用“下一根开盘价”作为当时的入场价）
    static double SL_PCT_Q = 0.75;            // 0.90=过滤最“贵”的10%止损环境
    static int SL_PCT_Q_MIN_SAMPLES = 200;    // 少于该样本数则不启用（避免冷启动误杀）

    static class SlPctGateResult {
        boolean pass = true;
        double slPts = Double.NaN;
        double slPct = Double.NaN;
        double qVal  = Double.NaN;
        int sampleN  = 0;
    }

    static SlPctGateResult evalSlPctQuantileGate(List<Candle> m30, int sigIdx, double entryPx) {
        SlPctGateResult r = new SlPctGateResult();
        if (!USE_SL_PCT_Q_GATE) return r;
        if (m30 == null || m30.size() < 3) return r;
        if (sigIdx < 1 || sigIdx >= m30.size() - 1) return r; // 需要 sigIdx-1 至少一根历史，且 j+1 不越界
        if (!Double.isFinite(entryPx) || entryPx <= 0) return r;

        // 当前这次入场对应的 slPts / slPct（slPts 用你现有的动态止损公式）
        r.slPts = calcSlPointsForIndex(m30, sigIdx); // 只用信号K0及之前数据
        r.slPct = r.slPts / entryPx;

        // 只用过去数据：j ∈ [sigIdx-lookback .. sigIdx-1]
        // 每个 j 的 entryPx 取 m30[j+1].o（当时已发生的“下一根开盘价”）
        int endJ = Math.min(sigIdx - 1, m30.size() - 2); // 保证 j+1 不越界
        int startJ = Math.max(0, endJ - SL_PCT_Q_LOOKBACK_BARS + 1);
        int n = endJ - startJ + 1;
        if (n < SL_PCT_Q_MIN_SAMPLES) return r;

        double[] arr = new double[n];
        int k = 0;
        for (int j = startJ; j <= endJ; j++) {
            double slPtsJ = calcSlPointsForIndex(m30, j);
            double pxJ = m30.get(j + 1).o;
            double slPctJ = (!Double.isFinite(pxJ) || pxJ <= 0) ? 0.0 : (slPtsJ / pxJ);
            arr[k++] = slPctJ;
        }
        r.sampleN = n;
        r.qVal = quantile(arr, SL_PCT_Q);

        if (Double.isFinite(r.qVal) && Double.isFinite(r.slPct) && r.slPct > r.qVal) r.pass = false;
        return r;
    }

    // 线性插值分位数（q ∈ [0,1]）
    static double quantile(double[] a, double q) {
        if (a == null || a.length == 0) return Double.NaN;
        q = Math.max(0.0, Math.min(1.0, q));
        Arrays.sort(a);
        if (a.length == 1) return a[0];
        double pos = q * (a.length - 1);
        int idx = (int) Math.floor(pos);
        double frac = pos - idx;
        double lo = a[idx];
        double hi = a[Math.min(idx + 1, a.length - 1)];
        return lo + frac * (hi - lo);
    }


    // =====================  解法三：市场状态机（Trend / Range） =====================
    // 目标：把“弱势月份/低波动震荡期”从趋势系统中剥离（不改信号细节，最稳）
    static boolean USE_MARKET_REGIME_FILTER = true;
    static int REGIME_LOOKBACK_BARS_1H = 200;   // 用近 N 根1H的真实波动分布做分位数门槛
    static double REGIME_TREND_RANGE_Q = 0.85; // 当前1H波动 >= 55%分位 → 趋势态，否则震荡态

    enum MarketRegime {TREND, RANGE}

    // 取“在 tsMs 时刻已收盘”的 K线 idx：要求 candle.ts + barMs <= tsMs 且 confirm=1
    // 目的：避免在整点/半点用到“当前未收盘的1H”造成未来函数（例如 entryTs=00:30 不能用 00:00-01:00 这根的高低点）
    static int lastClosedCandleIdx(List<Candle> ks, long tsMs, long barMs) {
        if (ks == null || ks.isEmpty()) return -1;

        // open <= (tsMs - barMs)  ⇒  close=open+barMs <= tsMs
        long tsNeed = tsMs - barMs;

        int idx = upperBoundCandle(ks, tsNeed) - 1;

        // ✅一致性修复：允许 confirm=0 的“已足够久的已收盘K”参与（与 scanHistory 的 filterClosedByTime 口径对齐）
        // - 对“非常新”的刚收盘K：若 confirm!=1 且 closeTs 仍在 SAFE_CLOSE_MS 安全窗内，则跳过，使用上一根更稳的K（避免 OKX 延迟/修正导致抖动）
        long nowMs = System.currentTimeMillis();
        while (idx >= 0) {
            Candle c = ks.get(idx);
            long closeTs = c.ts + barMs;

            // 额外保护：若数据乱序/异常，确保 close<=tsMs
            if (closeTs > tsMs) { idx--; continue; }

            if (c.confirm == 1) break;

            // confirm=0：只有当它“距离现在”已过安全窗才允许使用
            if (closeTs <= (nowMs - SAFE_CLOSE_MS)) break;

            idx--;
        }
        return idx;
    }


    // Regime 专用：防未来函数保护
    // 如果选中的 1H K 仍未收盘（c.ts + 1H > entryTs），则打印 [REGIME][FUTURE-BUG] 并强制降级到上一根已收盘K。
    static boolean REGIME_FUTURE_BUG_GUARD = sysBool("okx.regimeFutureBugGuard", true);
    static long REGIME_FUTURE_BUG_LAST_PRINT_TS = Long.MIN_VALUE;

    static int regimeSafeH1Idx(List<Candle> h1, long entryTs) {
        int idx = lastClosedCandleIdx(h1, entryTs, BAR1H_MS);
        if (!REGIME_FUTURE_BUG_GUARD) return idx;
        if (h1 == null || h1.isEmpty() || idx < 0) return idx;

        // 理论上 lastClosedCandleIdx 已保证 close<=entryTs；这里做强校验兜底，防止调用处传错 tsMs 或后续改动引入未来函数。
        if (h1.get(idx).ts + BAR1H_MS > entryTs) {
            long bugKey = entryTs; // 按 entryTs 去重
            if (REGIME_FUTURE_BUG_LAST_PRINT_TS != bugKey) {
                REGIME_FUTURE_BUG_LAST_PRINT_TS = bugKey;
                System.out.printf(Locale.US,
                        "[REGIME][FUTURE-BUG] entryTs=%s picked1h=%s close=%s (NOT closed yet) -> downgrade%n",
                        FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                        FMT_JST.format(Instant.ofEpochMilli(h1.get(idx).ts)),
                        FMT_JST.format(Instant.ofEpochMilli(h1.get(idx).ts + BAR1H_MS))
                );
            }
            idx--;
            while (idx >= 0 && (h1.get(idx).confirm != 1 || h1.get(idx).ts + BAR1H_MS > entryTs)) idx--;
        }
        return idx;
    }

    // Regime 专用：如果调用方已经给了一个 idx（例如来自 1H 过滤器），这里再做一次“必须在 entryTs 前已收盘”的强校验。
    // - 若 idx 指向的 1H 仍未收盘（未来函数）或处于 SAFE_CLOSE_MS 安全窗内的 confirm=0，则强制改用 regimeSafeH1Idx。
    // - 同时打印硬检查日志，方便定位“哪条链路传入了不安全的 idx”。
    static int enforceRegimeAnchorH1Idx(List<Candle> h1, long entryTs, int proposedIdx, String tag) {
        int safeIdx = regimeSafeH1Idx(h1, entryTs);
        if (h1 == null || h1.isEmpty()) return -1;
        if (proposedIdx < 0 || proposedIdx >= h1.size()) return safeIdx;
        if (safeIdx < 0 || safeIdx >= h1.size()) return proposedIdx;

        Candle p = h1.get(proposedIdx);
        long pClose = p.ts + BAR1H_MS;
        long nowMs = System.currentTimeMillis();
        boolean pNotClosed = (pClose > entryTs);
        boolean pUnstableConfirm0 = (p.confirm != 1) && (pClose > (nowMs - SAFE_CLOSE_MS));

        if (pNotClosed || pUnstableConfirm0) {
            Candle s = h1.get(safeIdx);
            System.out.printf(Locale.US,
                    "[REGIME][ANCHOR-HARD-CHECK] tag=%s | entryTs=%s | proposedIdx=%d open=%s close=%s confirm=%d -> useSafeIdx=%d open=%s close=%s confirm=%d%n",
                    tag,
                    FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                    proposedIdx,
                    FMT_JST.format(Instant.ofEpochMilli(p.ts)),
                    FMT_JST.format(Instant.ofEpochMilli(pClose)),
                    p.confirm,
                    safeIdx,
                    FMT_JST.format(Instant.ofEpochMilli(s.ts)),
                    FMT_JST.format(Instant.ofEpochMilli(s.ts + BAR1H_MS)),
                    s.confirm
            );
            return safeIdx;
        }
        return proposedIdx;
    }

    // 调试：打印 Regime 选中的 1H K（用于确认不会用到未来 1H）
    static boolean DEBUG_REGIME_PICK_1H = sysBool("okx.debugRegimePick1H", false);
    static long DEBUG_REGIME_PICK_LAST_PRINT_KEY = Long.MIN_VALUE;

    static void debugPrintRegimePick1H(String tag, long entryTs, List<Candle> h1, int h1Idx) {
        if (!DEBUG_REGIME_PICK_1H) return;
        if (h1 == null || h1.isEmpty() || h1Idx < 0 || h1Idx >= h1.size()) return;

        // 去重：同一 entryTs + tag 只打印一次，避免刷屏
        long key = (entryTs ^ ((long) tag.hashCode() << 1));
        if (DEBUG_REGIME_PICK_LAST_PRINT_KEY == key) return;
        DEBUG_REGIME_PICK_LAST_PRINT_KEY = key;

        Candle c = h1.get(h1Idx);
        long openTs = c.ts;
        long closeTs = c.ts + BAR1H_MS;
        System.out.printf(Locale.US,
                "[REGIME][PICK-1H] tag=%s | entryTs=%s | picked1h.open=%s | picked1h.close=%s | idx=%d confirm=%d%n",
                tag,
                FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                FMT_JST.format(Instant.ofEpochMilli(openTs)),
                FMT_JST.format(Instant.ofEpochMilli(closeTs)),
                h1Idx,
                c.confirm
        );
    }


    static MarketRegime detectMarketRegimeBy1HRangeQuantile(List<Candle> h1, int endIdx) {
        if (!USE_MARKET_REGIME_FILTER) return MarketRegime.TREND;
        if (h1 == null || h1.isEmpty() || endIdx < 0 || endIdx >= h1.size()) return MarketRegime.RANGE;
        double cur = h1.get(endIdx).h - h1.get(endIdx).l;
        if (!Double.isFinite(cur) || cur <= 0) return MarketRegime.RANGE;
        int start = Math.max(0, endIdx - REGIME_LOOKBACK_BARS_1H + 1);
        ArrayList<Double> ranges = new ArrayList<>();
        for (int i = start; i <= endIdx; i++) {
            double r = h1.get(i).h - h1.get(i).l;
            if (Double.isFinite(r) && r > 0) ranges.add(r);
        }
        if (ranges.size() < Math.max(50, REGIME_LOOKBACK_BARS_1H / 4)) return MarketRegime.RANGE;
        Collections.sort(ranges);
        int qIdx = (int) Math.floor((ranges.size() - 1) * REGIME_TREND_RANGE_Q);
        qIdx = Math.max(0, Math.min(qIdx, ranges.size() - 1));
        double thr = ranges.get(qIdx);
        return (cur >= thr) ? MarketRegime.TREND : MarketRegime.RANGE;
    }

    static String formatRegimeInfo(List<Candle> h1, int endIdx) {
        if (h1 == null || h1.isEmpty() || endIdx < 0 || endIdx >= h1.size()) return "h1=EMPTY";
        double cur = h1.get(endIdx).h - h1.get(endIdx).l;
        int start = Math.max(0, endIdx - REGIME_LOOKBACK_BARS_1H + 1);
        ArrayList<Double> ranges = new ArrayList<>();
        for (int i = start; i <= endIdx; i++) {
            double r = h1.get(i).h - h1.get(i).l;
            if (Double.isFinite(r) && r > 0) ranges.add(r);
        }
        Collections.sort(ranges);
        double thr = Double.NaN;
        if (!ranges.isEmpty()) {
            int qIdx = (int) Math.floor((ranges.size() - 1) * REGIME_TREND_RANGE_Q);
            qIdx = Math.max(0, Math.min(qIdx, ranges.size() - 1));
            thr = ranges.get(qIdx);
        }
        return String.format(Locale.US, "curRange=%.2f thr(q=%.2f)=%.2f lookback=%d used=%d",
                cur, REGIME_TREND_RANGE_Q, thr, REGIME_LOOKBACK_BARS_1H, ranges.size());
    }

    // =====================  对比回测：一键跑 4 个场景 =====================
    static boolean RUN_COMPARE_ON_BOOT = false;
    static boolean COMPARE_EXIT_AFTER = false;

    static void runCompareBacktestOnBoot() throws Exception {
        boolean oldFee = USE_FEE_VOL_GUARD;
        boolean oldReg = USE_MARKET_REGIME_FILTER;
        System.out.println();
        System.out.println("====================  对比回测（4个场景） ====================");
        runScenario("BASE", false, false);
        runScenario("FEE_VOL", true, false);
        runScenario("REGIME", false, true);
        runScenario("BOTH", true, true);
        USE_FEE_VOL_GUARD = oldFee;
        USE_MARKET_REGIME_FILTER = oldReg;
        System.out.println("====================  对比回测结束 ====================");
    }


    // ===================== 回测（SSOT：只读 CandleDb / DB） =====================
    // 注意：这个方法名与旧版 static import 同名；类内定义会覆盖 imported symbol，确保回测也走 DB。
    // ===================== 回测（SSOT：只读 CandleDb / DB） =====================
// 注意：方法名保留为 runBacktestOneYearOnBoot（兼容旧入口），但窗口由 BOOT_BACKTEST_YEARS 控制。
    static void runBacktestOneYearOnBoot() {
        resetEngineForReDerive(VIEW_ENGINE);
        resetEngineForReDerive(RESEARCH_ENGINE);

        try {
            long nowMs = System.currentTimeMillis();
            Instant end = Instant.ofEpochMilli(nowMs);

            int years = Math.max(1, BOOT_BACKTEST_YEARS);
            int warmupDays = Math.max(0, BOOT_BACKTEST_WARMUP_DAYS);

            // start = years + warmup（指标预热）
            long startMs = end.minus(Duration.ofDays((long) years * 365L + warmupDays)).toEpochMilli();

            // ===== 回测区间开关覆盖（用于样本外/时间切分验证）=====
            // endMs：数据加载与已收盘过滤的右边界。默认=nowMs；若指定 BT_END_DATE 则用之。
            long endMs = nowMs;
            long statStartMs = 0L; // 成交统计起算点（warmup 段不计），0=不额外限制
            try {
                DateTimeFormatter dfmt = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZONE);
                if (BT_END_DATE != null && !BT_END_DATE.isBlank()) {
                    endMs = LocalDate.parse(BT_END_DATE.trim()).atStartOfDay(ZONE).toInstant().toEpochMilli();
                }
                if (BT_START_DATE != null && !BT_START_DATE.isBlank()) {
                    long bs = LocalDate.parse(BT_START_DATE.trim()).atStartOfDay(ZONE).toInstant().toEpochMilli();
                    statStartMs = bs;
                    // 加载起点 = 指定起点再往前留 warmup 天做指标预热
                    startMs = bs - Duration.ofDays(warmupDays).toMillis();
                }
                if (BT_START_DATE != null && !BT_START_DATE.isBlank()
                        || BT_END_DATE != null && !BT_END_DATE.isBlank()) {
                    System.out.println("[BOOT-BT][区间开关] start=" + (BT_START_DATE.isBlank() ? "(默认)" : BT_START_DATE)
                            + " end=" + (BT_END_DATE.isBlank() ? "(最新)" : BT_END_DATE)
                            + " | 加载startMs=" + fmtTs(startMs) + " endMs=" + fmtTs(endMs)
                            + " | 统计起算=" + (statStartMs > 0 ? fmtTs(statStartMs) : "(不限)"));
                }
            } catch (Throwable t) {
                System.out.println("[BOOT-BT][区间开关] 日期解析失败，回退默认区间：" + t.getMessage());
                endMs = nowMs; statStartMs = 0L;
            }

            List<Candle> m30;
            List<Candle> h1;

            if (USE_DB_CACHE) {
                CandleDb.init();
                // 启动时确保 DB 至少有一段最新数据（增量写，避免“最新几根缺失”）
                CandleDb.ingestRecent(INST_ID, "30m", 2200, nowMs);

                // v7: 校验 DB 尾部是否真的被更新到了“当前应已收盘区”，否则强制联网补齐（避免回测窗口停在 2026-01-30）
                try {
                    List<Candle> _chk = CandleDb.loadLatest(INST_ID, "30m", 5, true);
                    long dbClose = (!_chk.isEmpty() ? (_chk.get(_chk.size()-1).ts + BAR30_MS) : -1L);
                    long expectedClose = alignLastClosed(nowMs, BAR30_MS, SAFE_CLOSE_MS);
                    if (dbClose > 0 && expectedClose > 0 && dbClose + 2*BAR30_MS <= expectedClose) {
                        System.out.println("[BOOT-BT][TAIL-STALE][30m] dbTailClose=" + fmtTs(dbClose)
                                + " expectedClose=" + fmtTs(expectedClose) + " (lagBars=" + ((expectedClose-dbClose)/BAR30_MS) + ")"
                                + " | DB_FILE=" + DB_FILE + " -> force net refill");
                        long refillStart = Math.max(0, expectedClose - BAR30_MS * 6000L);
                        List<Candle> net = fetchRangeCandlesNet(INST_ID, "30m", refillStart, expectedClose);
                        if (!net.isEmpty()) {
                            CandleDb.upsertNormalized(INST_ID, "30m", net, nowMs);
                            CandleDb.validateInvariants(INST_ID, "30m", BAR30_MS, nowMs);
                        }
                    }
                } catch (Throwable t) {
                    System.out.println("[BOOT-BT][TAIL-STALE] force net refill error: " + t);
                }
                CandleDb.ingestRecent(INST_ID, "1H",  1200, nowMs);

                m30 = CandleDb.loadRange(INST_ID, "30m", startMs, nowMs, false);
                h1  = CandleDb.loadRange(INST_ID, "1H",  startMs, nowMs, false);
            } else {
                m30 = fetchRangeCandlesNet(INST_ID, "30m", startMs, nowMs);
                h1  = fetchRangeCandlesNet(INST_ID, "1H",  startMs, nowMs);
            }

            // 再按时间口径做一次“已收盘过滤”，确保不会把未闭合K喂给回测
            m30 = filterClosedByTime(m30, BAR30_MS, nowMs, SAFE_CLOSE_MS);
            h1  = filterClosedByTime(h1,  BAR1H_MS, nowMs, SAFE_CLOSE_MS);

            if (m30 == null || m30.size() < 500) {
                System.out.println("[BOOT-BT] m30 candles too few, skip backtest on boot.");
                return;
            }

            List<MacdPoint> macd30 = calcMacd(m30);
            List<MacdPoint> macd1h = (h1 == null || h1.isEmpty() ? Collections.emptyList() : calcMacd(h1));

            // 用同一套逻辑跑（不改变策略逻辑；仅用于统计/展示）
            EngineState btEng = new EngineState();
            IN_BOOT_BACKTEST = true;
            SCAN_TAIL_CLOSE_TS = calcTailCloseTs(m30);
            List<CompletedTrade> trades;
            try {
                trades = runBacktestToState(btEng, m30, macd30, h1, macd1h);
            } finally {
                IN_BOOT_BACKTEST = false;
            }

            System.out.printf(Locale.US, "[BOOT-BT] years=%d(+warmup=%dd) | bars30=%d bars1h=%d trades=%d | from=%s to=%s%n",
                    years,
                    warmupDays,
                    (m30 == null ? 0 : m30.size()),
                    (h1 == null ? 0 : h1.size()),
                    (trades == null ? 0 : trades.size()),
                    FMT_JST.format(Instant.ofEpochMilli(m30.get(0).ts)),
                    FMT_JST.format(Instant.ofEpochMilli(m30.get(m30.size() - 1).ts + BAR30_MS))
            );

            // ✅ 输出“胜率/回撤/月度胜率/EQUITY”等（按你提供的 da_1770564188144.txt 格式）
            if (BOOT_BACKTEST_PRINT_REPORT && trades != null) {
                printBacktestReport(trades, btEng, "BOOT-BT-" + years + "Y");
            }

            // Sync boot-backtest SRMF trendPts into live/view engines (avoid 45 vs 84 split).
            syncSrmfTrendPtsFromBoot(btEng.srmf);

            // 继承 boot 满样本贝叶斯状态到实时/展示引擎(修小窗口样本不足→校准退化→误杀进场)
            syncBayesStateFromBoot(btEng.bayesState);
        } catch (Throwable t) {
            System.out.println("[BOOT-BT] exception: " + t.getMessage());
        }
    }


    // ===================== SRMF boot-backtest sync helpers =====================
    // Goal: after boot backtest derives SRMF state (esp. trendPts), make realtime sizing panel/order params
    // reflect the same trendPts instead of the default (e.g., 45.00).
    static void syncSrmfTrendPtsFromBoot(SRMFState bootSrmf) {
        if (bootSrmf == null) return;
        double tp = bootSrmf.trendPts;
        if (!Double.isFinite(tp)) return;

        // Only sync the trendPts as requested (do NOT overwrite other SRMF state like dd/streak/regime).
        try {
            if (LIVE_ENGINE != null && LIVE_ENGINE.srmf != null) LIVE_ENGINE.srmf.trendPts = tp;
            if (VIEW_ENGINE != null && VIEW_ENGINE.srmf != null) VIEW_ENGINE.srmf.trendPts = tp;
            if (RESEARCH_ENGINE != null && RESEARCH_ENGINE.srmf != null) RESEARCH_ENGINE.srmf.trendPts = tp;
        } catch (Throwable ignore) {
        }
    }

    /**
     * 把 boot 回测(完整4年历史)训练出的满样本贝叶斯状态，交给实时/展示引擎。
     * 解决:实时引擎用小窗口(~4000根)只能攒~44笔→等渗校准退化→posterior被压到0→误杀进场。
     * boot 回测用完整历史攒满200笔→校准正常,把它继承给 LIVE_ENGINE 即可。
     *
     * 无未来函数:boot 回测的 history 全是 entryTs ≤ 启动时刻的【已完成历史交易】,
     * 实时引擎之后评估的是更晚的新K,用过去已完成交易训练评判现在=贝叶斯正常用法。
     * 时序安全:本方法在 restoreLiveState()(磁盘加载)之后、boot 回测末尾调用,
     * 覆盖磁盘里可能存在的小样本旧状态,且其后无任何逻辑再覆盖 bayesState。
     * 三引擎各持独立副本(LIVE 接管 boot 的,VIEW/RESEARCH 各深拷贝),避免共享引用串扰。
     */
    static void syncBayesStateFromBoot(MultiBinBayesFilter.BayesState bootState) {
        if (bootState == null) return;
        try {
            int hist = BAYES_LOOKBACK_TRADES;
            // 序列化一次作为深拷贝模板(复用已验证的存/取路径)
            org.json.JSONObject snapshot = MultiBinBayesFilter.stateToJson(bootState, hist);
            if (snapshot == null) return;
            // LIVE:直接接管 boot 的状态对象(btEng 随方法结束销毁,所有权转移,安全)
            LIVE_ENGINE.bayesState = bootState;
            // VIEW / RESEARCH:各自从快照深拷贝独立副本,避免与 LIVE 共享同一引用
            if (VIEW_ENGINE != null) {
                MultiBinBayesFilter.BayesState v = MultiBinBayesFilter.stateFromJson(snapshot);
                if (v != null) VIEW_ENGINE.bayesState = v;
            }
            if (RESEARCH_ENGINE != null) {
                MultiBinBayesFilter.BayesState r = MultiBinBayesFilter.stateFromJson(snapshot);
                if (r != null) RESEARCH_ENGINE.bayesState = r;
            }
            int n = bootState.history == null ? 0 : bootState.history.size();
            System.out.printf(Locale.US, "[BOOT-BAYES] 已继承 boot 满样本贝叶斯状态到实时引擎: history=%d 笔%n", n);
        } catch (Throwable t) {
            System.out.println("[BOOT-BAYES] sync failed(忽略,实时引擎将沿用自身样本): " + t.getMessage());
        }
    }

    static void runScenario(String name, boolean feeVol, boolean regime) throws Exception {
        USE_FEE_VOL_GUARD = feeVol;
        USE_MARKET_REGIME_FILTER = regime;
        System.out.println();
        System.out.println("############################################################");
        System.out.printf(Locale.US, "### SCENARIO: %s | USE_FEE_VOL_GUARD=%s | USE_MARKET_REGIME_FILTER=%s ###%n", name, feeVol, regime);
        System.out.println("############################################################");
        runBacktestOneYearOnBoot();
    }

    static int barsHeldSinceEntry(List<Candle> m30, int curIdx, long entryTs) {
        if (m30 == null || m30.isEmpty() || curIdx < 0 || curIdx >= m30.size()) return 0;
        int entryIdx = upperBoundCandle(m30, entryTs) - 1;
        if (entryIdx < 0) return 0;
        if (m30.get(entryIdx).ts != entryTs) return 0; // 找不到严格对齐的入场K，保守处理
        return Math.max(0, curIdx - entryIdx + 1);
    }

    // MACD 出场护栏：两根确认 + 最小持仓 + 盈利不足不因MACD出场
    static boolean shouldExitByMacdWithGuards(List<Candle> m30, List<MacdPoint> macd30, int i, Position pos,
                                              double exitCandidatePx, String phase, long tsForPrint) {
        if (!USE_MACD_EXIT || pos == null) return false;

        // 先满足一次‘颜色反转’
        boolean rev1 = isMacdColorReversal(macd30, i, pos.side);
        if (!rev1) return false;

        // 1) 两根确认：连续 N 根都满足‘颜色反转’
        if (MACD_EXIT_2BAR_CONFIRM) {
            int n = Math.max(2, MACD_EXIT_CONFIRM_BARS);
            if (i < n) {
                if (PRINT_MACD_EXIT_BLOCK) {
                    System.out.printf(Locale.US,
                            "【MACD护栏-%s】时间=%s | 两根确认数据不足(i=%d,n=%d) → 不因MACD出场%n",
                            phase, FMT_JST.format(Instant.ofEpochMilli(tsForPrint)), i, n
                    );
                }
                return false;
            }
            for (int k = 0; k < n; k++) {
                if (!isMacdColorReversal(macd30, i - k, pos.side)) {
                    if (PRINT_MACD_EXIT_BLOCK) {
                        System.out.printf(Locale.US,
                                "【MACD护栏-%s】时间=%s | 两根确认未满足(k=%d) → 不因MACD出场%n",
                                phase, FMT_JST.format(Instant.ofEpochMilli(tsForPrint)), k
                        );
                    }
                    return false;
                }
            }
        }

        // 2) 最小持仓时间（只限制 MACD 出场，不影响 SL/TP）
        if (MACD_EXIT_MIN_HOLD_ENABLE) {
            int barsHeld = barsHeldSinceEntry(m30, i, pos.entryTs);
            if (barsHeld < MACD_EXIT_MIN_HOLD_BARS) {
                if (PRINT_MACD_EXIT_BLOCK) {
                    System.out.printf(Locale.US,
                            "【MACD护栏-%s】时间=%s | barsHeld=%d < minHold=%d → 不因MACD出场%n",
                            phase, FMT_JST.format(Instant.ofEpochMilli(tsForPrint)), barsHeld, MACD_EXIT_MIN_HOLD_BARS
                    );
                }
                return false;
            }
        }

        // 3) 盈利不足：不因 MACD 出场（避免小利润/小亏损被手续费吃掉）
        if (MACD_EXIT_MIN_PROFIT_ENABLE) {
            double gross = (pos.side == Side.LONG) ? (exitCandidatePx - pos.entry) : (pos.entry - exitCandidatePx);
            double feeNow = feePtsForExitNowForGuard(pos.entry, exitCandidatePx);
            double needProfit = MACD_EXIT_MIN_PROFIT_FEE_K * feeNow;
            if (gross < needProfit) {
                if (PRINT_MACD_EXIT_BLOCK) {
                    System.out.printf(Locale.US,
                            "【MACD护栏-%s】时间=%s | gross=%.2f < needProfit=%.2f (=%.2f*feeNow) | feeNow=%.2f → 不因MACD出场%n",
                            phase, FMT_JST.format(Instant.ofEpochMilli(tsForPrint)), gross, needProfit, MACD_EXIT_MIN_PROFIT_FEE_K, feeNow
                    );
                }
                return false;
            }
        }

        return true;
    }

    // 固定止损点数（你已设好28）
    static final double SL_POINTS = 0.0;

    // =====================  动态止损（在固定28点底线上自适应波动） =====================
    // 说明：
    //  - 不改你的风险框架：SL_POINTS 仍作为“固定底线（最小止损点数）”
    //  - 动态止损只会在波动变大时放宽（避免被洗），波动变小时不会无限缩小（防噪声）
    //  - 最终生效止损点数：max(SL_POINTS, clamp(k*ATR, SL_MIN, SL_MAX))
    static final boolean USE_DYNAMIC_SL = true;     // 是否启用动态止损
    static final int ATR_N = 24;                    // ATR 周期（30m）
    static final double SL_ATR_K = 1.5;             // k * ATR
    static final double SL_MIN = sysDouble("okx.slMin", 8.0);   // 动态止损最小点数（绝对模式clamp下限）
    static final double SL_MAX = sysDouble("okx.slMax", 45.0);  // 动态止损最大点数（绝对模式clamp上限）
    // 换币种通用：SL_CLAMP_PCT=true 时，止损上下限改用“价格百分比”，自动适配任何币价(BTC/XAUT…)；默认false=ETH绝对点数不变
    static final boolean SL_CLAMP_PCT = sysBool("okx.slClampPct", true);
    static final double  SL_MIN_PCT   = sysDouble("okx.slMinPct", 0.006); // 下限=价格的0.8%
    static final double  SL_MAX_PCT   = sysDouble("okx.slMaxPct", 0.025); // 上限=价格的2.0%
    // 统一止损 clamp：按价格百分比(换币通用) 或 绝对点数(ETH原行为)
    static double clampSl(double value, double price) {
        if (SL_CLAMP_PCT && price > 0) return clamp(value, SL_MIN_PCT * price, SL_MAX_PCT * price);
        return clamp(value, SL_MIN, SL_MAX);
    }
    //双动态止损 ->根据最新价格设定动态止损的最大/最小值目前区间 1000(8)->4800(35)

    //  TP2：达到 TP2_TRIGGER 点后，全平（剩余 size 全部平掉）
    // 注意：当前值 5555 远超 ETH 日均波动（约100-200点），实际上永远不会触发 TP2 全平。
    // 若需要启用 TP2 止盈，请将此值改为合理范围（例如 60~120 点）。
    static final double TP2_TRIGGER = 5555.0; // 例：60（当前设为5555=禁用TP2）
    // =====================  OKX 撑压线（Support/Resistance）过滤 & 止盈 =====================
    // 说明：OKX APP 图上的 "Resistance / Support" 你无法从行情接口直接拿到（除非OKX单独提供指标接口），
    // 所以这里用“最近N根已收盘K”的最高/最低（类似 Donchian 通道）来生成一组撑压线，效果最接近也最稳定。
    static final boolean USE_OKX_SR_ENTRY_FILTER = true;

    // =====================  TSRND 外层投票 + 仓位加成（最外层） =====================
    // 规则：方向票通过=2分即可进场（名义 *1.0），方向票+总体票通过=3分更强（名义 *1.2）
    static final boolean USE_TSRND_OUTER_VOTE = true;
    static final int TSRND_LOOKBACK_BARS = 12;       // 投票强度累积窗口（避免单根噪声）
    static final int TSRND_SCORE_THRESHOLD = 5;      // 至少 2 分（方向票）才允许进场
    // 外层投票倍率档位（用于 notional）：
    // - 你当前设定是“两档”：低分=0.9，高分(>=5)=1.7
    // - 这样三处输出（AUTO-ENTRY计划 / SRMF 入场锁定 / 进场摘要）都会严格一致
    static final double TSRND_SCALE_SCORE2 = 0.9;    // score<3：名义*0.9
    static final double TSRND_SCALE_SCORE3 = 1.3;    // score>=3：仍按低档处理（保持两档）
    static final double TSRND_SCALE_SCORE5 = 1.5;    // score>=5：名义*1.7
    //  新增总体 FailMonth 票（COMBO7）：EMA60+EMA100+EMA200+STOCH14_3_band+ROC12_thr0.01+TSI25_13+BBWidth20_thr0.03
    static final boolean TSRND_FAILM_VOTER_ENABLE = true;
    static final double TSRND_FAILM_BBWIDTH_THR = 0.2;
    static final double TSRND_FAILM_ROC_THR = 0.02;

    // TSRND 总体票模式：
    // 0=旧总体Top1: EMA20+EMA60+EMA100+EMA200+ADX14_DI+CCI20_thr100+TSI25_13
    // 1=新总体Top1: EMA60+EMA100+EMA200+ADX14_DI+ROC12_thr0.01+TSI25_13+SQUEEZE20_20  （你最新结果）
    // 2=两套总体都算（总体得分可能 +2；scale 仍按 score>=3）
    static final int TSRND_OVERALL_MODE = 2;

    // 若为 true：总体票必须通过，否则即使方向票=2 也拦截（更“硬”的外层过滤）
    static final boolean TSRND_OVERALL_STRICT_GATE = true;

    // 入场限制：离目标撑/压太近则不进
    static final boolean USE_OKX_SR_TP = false;           // 止盈：触达撑/压线即止盈


    // ===================== 放量滚动防守出场（推荐A） =====================
    // N=10, Vmult=2.5, steps=1, lossGate=是
    static final boolean USE_VOLROLL_EXIT = false;
    static final int     VOLROLL_N = 10;
    static final double  VOLROLL_VMULT = 2.5;
    static final int     VOLROLL_STEPS = 1;      // 当前落地只实现 steps=1（无状态）
    static final int     VOLROLL_PRE_BARS = 3;   // preLow/preHigh：基于“放量K(prev)”之前的 3 根
    static final boolean VOLROLL_LOSS_GATE = true;  // 仅浮亏时启用（防守，不砍利润）
    static final boolean PRINT_VOLROLL_EXIT = false;
    // ★ [FIX] 放量出场最小持仓根数：入场后至少持有 N 根才允许触发 VolRoll 出场
    // 防止"入场根和出场根是同一根K"导致 entryNotional=0 时 lossGate 误判
    static final int     VOLROLL_MIN_HOLD_BARS = sysInt("okx.volrollMinHoldBars", 1);
    static final int OKX_SR_N = 888;                      // 30m*96=48小时窗口（你可调：48/96/144/200）
    static final double OKX_SR_ENTRY_MIN_GAP_PTS = 0.0; // 原固定阈值（动态模式下作为fallback；或关闭动态后直接使用）
    static final boolean OKX_SR_ENTRY_GAP_USE_DYN_SL_MULT = true; // true=阈值=动态止损点数*倍数；false=用固定 OKX_SR_ENTRY_MIN_GAP_PTS
    static final double OKX_SR_ENTRY_GAP_DYN_SL_MULT = 0.0; // 入场限制阈值 = 动态止损点数 * 倍数（建议 1.5~3 之间）

    static double okxSrEntryMinGapPts(double dynSlPts) {
        if (!OKX_SR_ENTRY_GAP_USE_DYN_SL_MULT) return OKX_SR_ENTRY_MIN_GAP_PTS;
        if (!(dynSlPts > 0)) return OKX_SR_ENTRY_MIN_GAP_PTS; // fallback
        return dynSlPts * OKX_SR_ENTRY_GAP_DYN_SL_MULT;
    }

    // =====================  MACD 出场护栏（两根确认/最小持仓/盈利不足不因MACD出场） =====================
    static final boolean USE_MACD_EXIT = false;
    static final boolean MACD_EXIT_2BAR_CONFIRM = true;
    static final int MACD_EXIT_CONFIRM_BARS = 1;
    static final boolean MACD_EXIT_MIN_HOLD_ENABLE = true;
    static final int MACD_EXIT_MIN_HOLD_BARS = 2;
    static final boolean MACD_EXIT_MIN_PROFIT_ENABLE = false;
    static final double MACD_EXIT_MIN_PROFIT_FEE_K = 10.0;
    static final boolean PRINT_MACD_EXIT_BLOCK = false;

    //  是否启用1H方向硬过滤（你已设好）
    static final boolean USE_1H_FILTER = false;
    // 30分 0.88=n
    private static final int AVG_N = 20;
    //  1H 新过滤：阈值用最近 N 根 1H 的 DIF/DEA 绝对值平均（替代固定10），并仅使用已收盘K线（无未来函数）
    static final int FILTER_1H_AVG_N = 20;                 // 最近 N 根用于计算 DIF/DEA 平均阈值
    static final boolean FILTER_1H_AVG_INCLUDE_SELF = true; // true=包含当前1H信号K0；false=只看之前N根

    // ===================== 回撤入场（优先级#1：三连K确认不变，只改“入场价”回撤；避免未来函数） =====================
    // 说明：
    // - 回测：信号K0收盘确认后，在“下一根K开盘价(entryRef)”挂回撤限价（LONG: entryRef - x；SHORT: entryRef + x）
    //       只在【该入场K】触达 limit 才成交（不触达则本次信号不成交），从而避免“用同一根K的低点/高点当入场”造成未来函数。
    // - 实盘：默认不自动用回撤入场（只打印参考），你可以后续接交易执行或手动挂单。
    static final boolean USE_3K_ENTRY_CONFIRM = true;            //  开关：三连K入场确认（true=3根同号；false=2根同号）
    static final boolean ENABLE_PULLBACK_ENTRY_BACKTEST = false;  //  开关：回测是否启用回撤限价入场
    static final boolean ENABLE_PULLBACK_ENTRY_LIVE_PRINT = false; //  开关：实盘打印“回撤入场参考”（不改变原入场提醒逻辑）
    static final PullbackMode PULLBACK_MODE = PullbackMode.ATR;   // ATR 或 FIXED
    static final double PULLBACK_ATR_MULT = 0.5;                 // x = ATR * mult原0.25
    static final double PULLBACK_FIXED_POINTS = 15;              // x = 固定点数（当模式=FIXED时生效）原8
    static final int PULLBACK_VALID_BARS = 1;                      // 订单有效K数：1=只看下一根K；>1 可后续扩展
    //  开关：回撤未成交时的口径（与实盘“响铃即持仓”MANUAL_VIRTUAL_POS_ON_ALERT 解耦）
    //  - true ：未触达回撤限价 = 本次不成交，直接跳过；下一根K若信号仍成立会自然重新挂(fresh 重报价)，无需跨K挂单状态
    //  - false：维持旧口径（受 MANUAL_VIRTUAL_POS_ON_ALERT 控制：开则按“虚拟持仓”以 entryRef 进场）
    static final boolean PULLBACK_SKIP_ON_NOFILL = sysBool("okx.pullbackSkipOnNoFill", true);

    //srmf系统参数
    static volatile double lastPriceCache = 0.0;
    static volatile double lastSlPointsCache = SL_POINTS; // 收盘确认时的动态止损点数缓存
    static volatile double lastAtrCache = 0.0;          // ATR(30m, ATR_N)
    static volatile double lastDynSlRawCache = 0.0;       // k*ATR
    static volatile double lastDynSlClampedCache = 0.0;   // clamp 后的动态止损点数

    //  用于打印/定时刷新：保存 runOnce 里“按时间过滤后的已收盘30m列表”，保证动态止损不会打印为0
    static volatile List<Candle> lastM30ClosedForPrint = null;
    static volatile int lastM30ClosedEndIdxForPrint = -1;

    //  用于打印：OKX 撑压线缓存（Resistance/Support）
    static volatile boolean lastOkxSrValidForPrint = true;
    static volatile double lastOkxResistanceForPrint = Double.NaN;
    static volatile double lastOkxSupportForPrint = Double.NaN;
    static volatile int lastOkxSrUsedBarsForPrint = 0;

    static final double RISK_PCT = 0.02;      // 1.5%
    //本金！！！！PlanB:平均仓位*1.15+最高点回撤≥25%降到*0.8 25回撤0.8缓冲区回撤回到20回撤变回1.15
    static double capital = 3080;      // 你已设好 峰值6000 ->（6000-4500）/6000=25% (25回撤0.9缓冲区回撤回到20回撤变回1.0，35回撤0.8回到30回撤变为0.9，45回撤0.7回到40回撤变为0.8)
    // 兼容：OkxPagingAlarm 旧版会静态导入 equitySim
    public static volatile double equitySim = capital;
    // ===================== SRMF（Functional / SSOT） =====================
    enum SRMFRegime { STABLE, ATTACK }

    static class SRMFState {
        SRMFRegime regime = SRMFRegime.STABLE;
        double trendPts = 45.0;   // 与 stable4MultByTrendPts 的阈值对齐
        int streak = 0;           // 连胜为正，连败为负
        int tradeN = 0;

        double equityPeak = 0.0;
        double ddPct = 0.0;

        static SRMFState init(double equity0) {
            SRMFState s = new SRMFState();
            s.equityPeak = equity0;
            s.ddPct = 0.0;
            return s;
        }
    }

    static final class SRMF {
        private SRMF() {}

        // ===== 兼容（给 OkxPagingAlarm 旧接口）=====
        // 旧版外部类会直接调用 SRMF.getRegime()/getTrendPts()/getCurrentMult()（无参）
        // 现在统一从 VIEW_ENGINE.srmf 读取（回看/回测口径），保证不污染 LIVE_ENGINE。
        static SRMFRegime getRegime() {
            SRMFState s = (VIEW_ENGINE == null ? null : VIEW_ENGINE.srmf);
            return (s == null || s.regime == null) ? SRMFRegime.STABLE : s.regime;
        }
        static double getTrendPts() {
            return getTrendPts(VIEW_ENGINE == null ? null : VIEW_ENGINE.srmf);
        }
        static double getCurrentMult() {
            return getCurrentMult(VIEW_ENGINE == null ? null : VIEW_ENGINE.srmf);
        }


        static double getTrendPts(SRMFState s) {
            return (s == null ? 45.0 : s.trendPts);
        }

        static String getRegimeName(SRMFState s) {
            return (s == null ? SRMFRegime.STABLE.name() : s.regime.name());
        }

        static String regimeCn(String regimeName) {
            if (regimeName == null) return "稳健";
            String n = regimeName.toUpperCase(Locale.ROOT);
            if (n.contains("ATTACK")) return "进攻";
            return "稳健";
        }

        static double getCurrentMult(SRMFState s) {
            String r = getRegimeName(s);
            double tp = getTrendPts(s);
            if (r.contains("ATTACK")) return attack5MultByTrendPts(tp);
            return stable4MultByTrendPts(tp);
        }

        static JSONObject toJson(SRMFState s) {
            if (s == null) return null;
            JSONObject o = new JSONObject();
            o.put("regime", getRegimeName(s));
            o.put("trendPts", s.trendPts);
            o.put("streak", s.streak);
            o.put("tradeN", s.tradeN);
            o.put("equityPeak", s.equityPeak);
            o.put("ddPct", s.ddPct);
            return o;
        }

        static SRMFState fromJson(JSONObject o) {
            if (o == null) return SRMFState.init(capital);
            SRMFState s = new SRMFState();
            String rg = o.optString("regime", SRMFRegime.STABLE.name());
            try { s.regime = SRMFRegime.valueOf(rg.toUpperCase(Locale.ROOT)); }
            catch (Exception ignore) { s.regime = SRMFRegime.STABLE; }
            s.trendPts = o.optDouble("trendPts", 45.0);
            s.streak = o.optInt("streak", 0);
            s.tradeN = o.optInt("tradeN", 0);
            s.equityPeak = o.optDouble("equityPeak", capital);
            s.ddPct = o.optDouble("ddPct", 0.0);
            return s;
        }

        // 纯函数：prev + trade + newEquity -> next
        static SRMFState reduce(SRMFState prev, CompletedTrade trade, double newEquity) {
            SRMFState s = (prev == null ? SRMFState.init(capital) : prev);

            // 复制（避免意外共享）
            SRMFState n = new SRMFState();
            n.regime = s.regime;
            n.trendPts = s.trendPts;
            n.streak = s.streak;
            n.tradeN = s.tradeN;
            n.equityPeak = s.equityPeak;
            n.ddPct = s.ddPct;

            n.tradeN++;

            // equity peak / drawdown
            if (n.equityPeak <= 0) n.equityPeak = newEquity;
            if (newEquity > n.equityPeak) n.equityPeak = newEquity;
            if (n.equityPeak > 0) n.ddPct = Math.max(0.0, 1.0 - (newEquity / n.equityPeak));

            // streak
            boolean win = (trade != null && trade.pnlUSDT > 0.0);
            if (win) n.streak = (n.streak >= 0 ? n.streak + 1 : 1);
            else     n.streak = (n.streak <= 0 ? n.streak - 1 : -1);

            // trendPts：用 rMultiple 推进（轻量版，避免过拟合）
            double r = (trade == null ? 0.0 : trade.rMultiple);
            n.trendPts = n.trendPts + 8.0 * r;
            if (n.trendPts < 0) n.trendPts = 0;
            if (n.trendPts > 120) n.trendPts = 120;


            // 关键修复：任何一次“亏损”都必须立刻把 SRMF 档位打回低档，避免倍数继续保持高位
            // 你的档位映射里，<=30/<=45 是两个主要档位（1.2/1.5），所以这里在亏损时强制把 trendPts 压回阈值以下，避免继续处于更高档位。
            if (!win) {
                n.regime = SRMFRegime.STABLE;      // 输了先回稳健
                n.trendPts = Math.min(n.trendPts, 44.0); // 强制降档（<45）
            }

            // regime：DD 或连续亏损 -> 稳健；状态好且趋势分数高 -> 进攻
            if (n.ddPct >= 0.18 || n.streak <= -3) {
                n.regime = SRMFRegime.STABLE;
            } else if (n.ddPct <= 0.12 && n.streak >= 2 && n.trendPts >= 55) {
                n.regime = SRMFRegime.ATTACK;
            } // else 保持

            return n;
        }
    }

    static class EngineState {
        // ===== 交易引擎状态（可持久化） =====
        volatile Position pos;

        // 禁止同一根信号K重复入场（用于实盘：止损/止盈后的“同K禁入”）
        volatile long lastStopSigTs = -1L;
        volatile long lastTpSigTs   = -1L;

        // R1 否决冷却：记录“最后一次 R1 否决”的信号收盘时间（K0 close 对应 ts）
        // 仅使用 SSOT 已收盘K（confirm=1）推导的时间戳，避免未来函数
        volatile long lastR1VetoLongTs  = -1L;
        volatile long lastR1VetoShortTs = -1L;

        // 冷却：candidateEntryTs < cooldownUntilEntryTs -> 禁止入场
        volatile long stopCooldownUntilEntryTs = -1L;
        volatile long tpCooldownUntilEntryTs   = -1L;

        // 逐K推进（确认K）去重：避免同一根 confirm=1 重复触发
        volatile long lastProcessedSigTs = -1L;

        // SRMF（功能式状态）
        volatile SRMFState srmf;

        // 资金曲线（用于 SRMF / 报告）
        volatile double equity = capital;

        // ===== BAYES（滚动学习状态 + 当前持仓快照，需与持仓生命周期一致）=====
        volatile MultiBinBayesFilter.BayesState bayesState = new MultiBinBayesFilter.BayesState();
        volatile MultiBinBayesFilter.BayesEntrySnapshot currentBayesSnap = null;


        // ===== “展示层”也尽量放进状态（避免 scanHistory 覆盖/消失） =====
        EnterOpportunity lastEnterOpp = null;
        ExitOpportunity  lastExitOpp  = null;
        final Deque<CompletedTrade> lastTrades = new ArrayDeque<>();

        EngineState() {
            this.srmf = SRMFState.init(capital);
            this.equity = capital;
        }
    }

    // ===================== Signal 输出层（唯一出口：打印/闹钟） =====================
    enum SignalKind { ENTER, EXIT }

    static final class EngineEvent {
        final SignalKind kind;
        final long alarmKeyTs;      // 用于 alarmOnceInTick 去重
        final boolean shouldAlarm;  // 一般 ENTER/EXIT 为 true
        final String text;          // 需要打印的文本（已包含换行）

        EngineEvent(SignalKind kind, long alarmKeyTs, boolean shouldAlarm, String text) {
            this.kind = kind;
            this.alarmKeyTs = alarmKeyTs;
            this.shouldAlarm = shouldAlarm;
            this.text = (text == null ? "" : text);
        }

        static EngineEvent enter(long alarmKeyTs, String text) {
            return new EngineEvent(SignalKind.ENTER, alarmKeyTs, true, text);
        }
        static EngineEvent exit(long alarmKeyTs, String text) {
            return new EngineEvent(SignalKind.EXIT, alarmKeyTs, true, text);
        }
    }

    static void dispatchSignalEvent(EngineEvent ev) {
        if (ev == null) return;
        if (ev.shouldAlarm) alarmOnceInTick(ev.alarmKeyTs);
        if (ev.text != null && !ev.text.isEmpty()) System.out.print(ev.text);
    }



    // 单一真源：实时引擎状态（会被 persist/restore）

    // ===================== scanHistory 推演复现修复：每次刷新都重置推演引擎 =====================
    static void resetEngineForReDerive(EngineState e) {
        if (e == null) return;
        e.pos = null;
        e.lastStopSigTs = -1L;
        e.lastTpSigTs = -1L;
        e.lastR1VetoLongTs = -1L;
        e.lastR1VetoShortTs = -1L;
        e.stopCooldownUntilEntryTs = -1L;
        e.tpCooldownUntilEntryTs = -1L;
        e.lastProcessedSigTs = -1L;
        e.srmf = SRMFState.init(capital);
        e.equity = capital;
        e.bayesState = new MultiBinBayesFilter.BayesState();
        e.currentBayesSnap = null;
        e.lastEnterOpp = null;
        e.lastExitOpp = null;
        e.lastTrades.clear();
    }

    static final EngineState LIVE_ENGINE = new EngineState();
    static final EngineState VIEW_ENGINE = new EngineState();
    static final EngineState RESEARCH_ENGINE = new EngineState();
    // MACD 参数（12/26/9）
    static final int FAST = 12;
    static final int SLOW = 26;
    static final int SIGNAL = 9;

    // 最近交易展示条数
    static final int LAST_N = 10;

    // 定时（每半小时收盘前约 :00:10 / :30:10）
    static final int CHECK_SECOND = (int) sysLong("okx.engineSecond", 6);

    //  K线收盘判定（避免 confirm 延迟导致 K 线滞后一根）
    static final long BAR30_MS = 30L * 60 * 1000;

    static final long BAR1H_MS = 1L * 60 * 60 * 1000;

    static final long BAR4H_MS = 4L * 60 * 60 * 1000;

    static boolean DEBUG_REALTIME = true;
    // 收盘后安全窗口：Puller 与 Engine 必须一致，否则会出现“读不到/早读/晚读一根”。
    // 统一使用同一个 VM 参数：-Dokx.safeCloseMs=3000
    static final long SAFE_CLOSE_MS = sysLong("okx.safeCloseMs", 3_000L);
    static final int WAIT_OKX_REFRESH_TRIES = (int) sysLong("okx.engineRetry", 10);  //  若 OKX 收盘后延迟更新，最多等待次数
    static final long WAIT_OKX_REFRESH_MS = sysLong("okx.engineRetryMs", 2000L); //  每次等待间隔(ms)，配合 :00:07/:30:07

    // 实时引擎读库窗口（小窗口：实时轻量）
    static final int ENGINE_BARS_30M = (int) sysLong("okx.engineBars30m", 4000);
    static final int ENGINE_BARS_1H  = (int) sysLong("okx.engineBars1h",  2000);

    // 回看/推演（scanHistory）读库窗口（大窗口：保证 lastTrade/opp 不断档）
    static final int SCAN_BARS_30M = (int) sysLong("okx.scanBars30m", 4000);
    static final int SCAN_BARS_1H  = (int) sysLong("okx.scanBars1h",  2000);
    static final boolean SCAN_HISTORY_USE_LARGE_WINDOW = sysBool("okx.scanUseLargeWindow", true);

    // ✅ 关键一致性修复：Regime(1H分位数波动阈值) 依赖 h2 的历史窗口。
    // 实盘与 scanHistory 若读到的 1H 窗口大小不同，会导致同一时刻的分位数阈值(thr)不同，从而出现 TREND/RANGE 分叉。
    // 因此：两条链路统一使用同一个 1H 读库窗口（默认取 engine/scan 的较大值，可用 -Dokx.h2LoadBars1h 覆盖）。
    static final int H2_LOAD_BARS_1H = (int) sysLong("okx.h2LoadBars1h", SCAN_BARS_1H);
    // 避免“等待 DB 更新”时反复清 cache 造成抖动（默认不清）
    static final boolean CLEAR_SERIES_CACHE_EACH_RUN = sysBool("okx.clearSeriesCacheEachRun", false);

    //  若本轮因“未刷新到期望已收K”而 skip，则短间隔重试（更接近零误差）
    static final boolean ENABLE_SKIP_RETRY = sysBool("okx.enableSkipRetry", true);
    static final long SKIP_RETRY_DELAY_MS = sysLong("okx.skipRetryDelayMs", 8_000L);
    static final int SKIP_RETRY_MAX = (int) sysLong("okx.skipRetryMax", 5L);

    static final Object RUNONCE_LOCK = new Object();
    static volatile boolean RUNONCE_RUNNING = false;

    static volatile long SKIP_RETRY_EXPECTED_OPEN = -1L;
    static volatile int SKIP_RETRY_COUNT = 0;

    static final ScheduledExecutorService RETRY_ES = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "okx-runOnce-retry");
            t.setDaemon(true);
            return t;
        }
    });

    static boolean tryEnterRunOnce(String src) {
        synchronized (RUNONCE_LOCK) {
            if (RUNONCE_RUNNING) {
                System.out.println("[RUN-ONCE] skip: already running, src=" + src);
                return false;
            }
            RUNONCE_RUNNING = true;
            return true;
        }
    }

    static void leaveRunOnce() {
        synchronized (RUNONCE_LOCK) {
            RUNONCE_RUNNING = false;
        }
    }

    static void requestSkipRetry(long expectedOpen, Candle last30, String reason) {
        if (!ENABLE_SKIP_RETRY) return;
        synchronized (RUNONCE_LOCK) {
            if (expectedOpen != SKIP_RETRY_EXPECTED_OPEN) {
                SKIP_RETRY_EXPECTED_OPEN = expectedOpen;
                SKIP_RETRY_COUNT = 0;
            }
            if (SKIP_RETRY_COUNT >= SKIP_RETRY_MAX) {
                System.out.printf(Locale.US,
                        "[RETRY] reached max (%d). expectedOpen=%s last=%s reason=%s%n",
                        SKIP_RETRY_MAX, fmtOpen(expectedOpen), (last30==null?"(null)":fmtOpen(last30.ts)), reason);
                return;
            }
            SKIP_RETRY_COUNT++;
        }

        long delay = Math.max(1_000L, SKIP_RETRY_DELAY_MS);
        int thisTry = SKIP_RETRY_COUNT;

        System.out.printf(Locale.US,
                "[RETRY] schedule runOnce after %.1fs (try %d/%d) expectedOpen=%s last=%s reason=%s%n",
                delay/1000.0, thisTry, SKIP_RETRY_MAX,
                fmtOpen(expectedOpen),
                (last30==null?"(null)":fmtOpen(last30.ts)),
                reason);

        RETRY_ES.schedule(() -> {
            try {
                runOnce();
            } catch (Throwable t) {
                System.out.println("[RETRY] runOnce exception: " + t.getMessage());
                t.printStackTrace();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }


    //  收盘后确认（:00:10 / :30:10）
    static final int POST_CLOSE_SECOND = CHECK_SECOND;

    // 分页参数（OKX history-candles 最大100）
    static final int LIMIT = 100;
    static final int SLEEP_MS = 420;
    static final int GUARD_MAX = 12000;

    // 时区：日本
    static final ZoneId ZONE = ZoneId.of("Asia/Tokyo");
    static final DateTimeFormatter FMT_JST = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZONE);
    static final DateTimeFormatter FMT_JST_SHORT = DateTimeFormatter.ofPattern("MM-dd HH:mm").withZone(ZONE);
    static String fmtOpen(long openTs) {
        return FMT_JST_SHORT.format(Instant.ofEpochMilli(openTs));
    }

    static String fmtClose(long openTs, long barMs) {
        return FMT_JST_SHORT.format(Instant.ofEpochMilli(openTs + barMs));
    }

    static String fmtJst(long ts) {
        return FMT_JST_SHORT.format(Instant.ofEpochMilli(ts));
    }
    static String fmtTs(long ts) { return fmtJst(ts); }




    // v7: 计算“当前应已收盘的最后一根K”的 closeTs（用于判定 DB 尾部是否落后）
// closeTs = floor((now - safeClose) / barMs) * barMs
    static long alignLastClosed(long nowMs, long barMs, long safeCloseMs) {
        long t = nowMs - Math.max(0, safeCloseMs);
        if (t < 0) return -1L;
        return (t / barMs) * barMs;
    }




    // ===================== K-USED（定位回测/实时用到哪根K） =====================
    static void printKUsedEntry(String who, String stage,
                                long boundaryCloseTs,
                                List<Candle> m30, int sigIdx,
                                Candle k1CandleOrNull,
                                double entryPx,
                                boolean usedFallback,
                                List<MacdPoint> macd30OrNull) {
        if (!DEBUG_K_USED) return;
        try {
            Candle k0 = (m30 != null && sigIdx >= 0 && sigIdx < m30.size()) ? m30.get(sigIdx) : null;
            Candle p0 = (m30 != null && sigIdx - 1 >= 0 && sigIdx - 1 < m30.size()) ? m30.get(sigIdx - 1) : null;

            System.out.printf(Locale.US,
                    "[K-USED][ENTRY][%s][%s] boundaryClose=%s | sigIdx=%d | entryPx=%.2f | usedFallback=%s%n",
                    who, stage, FMT_JST.format(Instant.ofEpochMilli(boundaryCloseTs)), sigIdx, entryPx, usedFallback);

            // K0 / P0 / K1
            if (k0 != null) {
                System.out.printf(Locale.US,
                        "  K0 idx=%d ts=%s (open)/%s (close) confirm=%d o=%.2f h=%.2f l=%.2f c=%.2f%n",
                        sigIdx,
                        FMT_JST.format(Instant.ofEpochMilli(k0.ts)),
                        FMT_JST.format(Instant.ofEpochMilli(k0.ts + BAR30_MS)),
                        k0.confirm, k0.o, k0.h, k0.l, k0.c);
            } else {
                System.out.println("  K0=MISSING");
            }

            if (p0 != null) {
                System.out.printf(Locale.US,
                        "  P0 idx=%d ts=%s (open)/%s (close) confirm=%d o=%.2f h=%.2f l=%.2f c=%.2f%n",
                        sigIdx - 1,
                        FMT_JST.format(Instant.ofEpochMilli(p0.ts)),
                        FMT_JST.format(Instant.ofEpochMilli(p0.ts + BAR30_MS)),
                        p0.confirm, p0.o, p0.h, p0.l, p0.c);
            } else {
                System.out.println("  P0=MISSING");
            }

            if (k1CandleOrNull != null) {
                // 注意：K1 是“下一根开盘进场”的那根K（可能 confirm=0）
                System.out.printf(Locale.US,
                        "  K1 ts=%s (open)/%s (close) confirm=%d o=%.2f h=%.2f l=%.2f c=%.2f%n",
                        FMT_JST.format(Instant.ofEpochMilli(k1CandleOrNull.ts)),
                        FMT_JST.format(Instant.ofEpochMilli(k1CandleOrNull.ts + BAR30_MS)),
                        k1CandleOrNull.confirm, k1CandleOrNull.o, k1CandleOrNull.h, k1CandleOrNull.l, k1CandleOrNull.c);
            } else {
                System.out.println("  K1=MISSING (entryPx fallback to K0.close)");
            }

            // 窗口：m30
            if (m30 != null && !m30.isEmpty()) {
                long from = m30.get(0).ts;
                long to = boundaryCloseTs;
                boolean tailHasUnclosed = (m30.get(m30.size() - 1) != null && m30.get(m30.size() - 1).confirm == 0);
                System.out.printf(Locale.US,
                        "  [WIN] m30 bars=%d from=%s to=%s tailUnclosed=%s%n",
                        m30.size(),
                        FMT_JST.format(Instant.ofEpochMilli(from)),
                        FMT_JST.format(Instant.ofEpochMilli(to)),
                        tailHasUnclosed);
            }

            // 窗口：macd30（只打印长度和尾部对齐信息）
            if (macd30OrNull != null && !macd30OrNull.isEmpty()) {
                // macd30 与 m30 可能长度对齐（placeholder/extend），这里打印长度便于排查“回测吃了未收K/placeholder”
                System.out.printf(Locale.US,
                        "  [WIN] macd30 len=%d | m30 len=%s | note=macd30应仅由已收K计算，placeholder仅允许对齐长度%n",
                        macd30OrNull.size(),
                        (m30 == null ? "NA" : String.valueOf(m30.size())));
            }
        } catch (Exception e) {
            System.out.println("[K-USED] print failed: " + e.getMessage());
        }
    }

    // ===================== 一致性校验输出（RT vs HIST） =====================
    static final boolean DEBUG_ALIGN = sysBool("DEBUG_ALIGN", true);
    static final boolean DEBUG_ALIGN_TAIL = sysBool("DEBUG_ALIGN_TAIL", true);

    // ===================== 回测/回看（scanHistory）尾部进场响铃（仅副作用） =====================
    // 需求：
    // 1) 回测打印【回测-进场】时响铃
    // 2) 只副作用：不影响策略/回测/下单逻辑
    // 3) 只尾部响：仅对最新的回测-进场机会更新响一次（去重）
    static final boolean BT_ALARM_ON_SCAN_ENTRY = sysBool("okx.btAlarmOnScanEntry", true);
    static final int BT_ALARM_TAIL_BARS = (int) sysLong("okx.btAlarmTailBars", 1); // 1=仅最后一根信号
    static volatile long LAST_BT_ALARM_ENTER_TS = Long.MIN_VALUE;

    // ===================== 回测进场展示锁（防止“最新一次机会”滚动覆盖） =====================
    // 需求：第一次出现【回测-进场】（且被打印/响铃的那一次）后锁定 entryTs，
    // 后续刷新即使出现新的【回测-进场】也不再打印/不再响铃，直到清锁。
    static final boolean BT_ENTER_LOCK = sysBool("okx.btEnterLock", true);
    static final boolean BT_ENTER_LOCK_RESET_ON_EXIT = sysBool("okx.btEnterLockResetOnExit", true);
    static volatile long LOCKED_BT_ENTER_TS = Long.MIN_VALUE;
    static volatile int  LOCKED_BT_ENTER_SIGIDX = Integer.MIN_VALUE;


    // ===================== scanHistory 推演：刷新时允许重复推演/重复输出（不受 BT_ENTER_LOCK 影响） =====================
    static final boolean BT_SCAN_ONLY_TAIL_PRINT = sysBool("okx.btScanOnlyTailPrint", true);
    static final int BT_SCAN_PRINT_LOOKBACK_HOURS = sysInt("okx.btScanPrintLookbackHours", 36);
    static volatile long SCAN_TAIL_CLOSE_TS = Long.MIN_VALUE;

    static volatile boolean IN_SCAN_HISTORY = false;
    static volatile boolean IN_BOOT_BACKTEST = false;
    static boolean btEnterLocked() { return BT_ENTER_LOCK && LOCKED_BT_ENTER_TS != Long.MIN_VALUE; }

    static void lockBtEnter(long entryTs, int sigIdx) {
        if (!BT_ENTER_LOCK) return;
        if (LOCKED_BT_ENTER_TS == Long.MIN_VALUE) {
            LOCKED_BT_ENTER_TS = entryTs;
            LOCKED_BT_ENTER_SIGIDX = sigIdx;
            System.out.println("[BT-LOCK][ENTER] lockedEntryTs=" + fmtTs(entryTs) + " sigIdx=" + sigIdx);
        }
    }

    static void unlockBtEnterIfNeeded(String reason) {
        if (!BT_ENTER_LOCK) return;
        if (!BT_ENTER_LOCK_RESET_ON_EXIT) return;
        if (LOCKED_BT_ENTER_TS != Long.MIN_VALUE) {
            System.out.println("[BT-LOCK][RESET] reason=" + reason + " lockedEntryTs=" + fmtTs(LOCKED_BT_ENTER_TS)
                    + " sigIdx=" + LOCKED_BT_ENTER_SIGIDX);
            LOCKED_BT_ENTER_TS = Long.MIN_VALUE;
            LOCKED_BT_ENTER_SIGIDX = Integer.MIN_VALUE;
        }
    }

    static boolean isTailSigIdx(int sigIdx, int size) {
        if (size <= 0) return false;
        int tail = Math.max(1, BT_ALARM_TAIL_BARS);
        return sigIdx >= (size - tail);
    }

    // scanHistory/BOOT-BT: 计算本次推演窗口的 tailClose（最后一根闭合K的 closeTs）
    static long calcTailCloseTs(List<Candle> m30Closed) {
        try {
            if (m30Closed == null || m30Closed.isEmpty()) return Long.MIN_VALUE;
            Candle last = m30Closed.get(m30Closed.size() - 1);
            if (last == null) return Long.MIN_VALUE;
            // OKX ts 是开盘时间；closeTs = ts + BAR30_MS
            long closeTs = last.ts + BAR30_MS;
            return closeTs;
        } catch (Throwable t) {
            return Long.MIN_VALUE;
        }
    }


    static void btAlarmOnceOnEnter(long entryTs, int sigIdx, String tag) {
        if (!BT_ALARM_ON_SCAN_ENTRY) return;
        if (entryTs <= 0) return;
        if (entryTs == LAST_BT_ALARM_ENTER_TS) return; // 去重：同一次机会只响一次
        LAST_BT_ALARM_ENTER_TS = entryTs;

        // 复用你实时链路的闹铃：OkxPagingAlarm.alarmOnceInTick(tickKey)
        // 这里只做“响铃”副作用：不触发下单、不改仓位、不改回测状态
        try {
            alarmOnceInTick(entryTs);
        } catch (Exception e) {
            try { Toolkit.getDefaultToolkit().beep(); } catch (Exception ignore) {}
            System.out.println("[BT-ALARM][FALLBACK] entryTs=" + fmtTs(entryTs) + " sigIdx=" + sigIdx
                    + " tag=" + tag + " err=" + e.getMessage());
        }
        System.out.println("[BT-ALARM][ENTER] entryTs=" + fmtTs(entryTs) + " sigIdx=" + sigIdx + " tag=" + tag);
    }


    static class AlignAudit {
        String src;        // RT / HIST
        long nowMs;
        long signalTs;     // 信号K0开盘
        long sigCloseTs;   // 信号K0收盘 (= signalTs + 30m)
        int sigConfirm;
        double sigO, sigC;
        long entryTs;      // 入场触发时间（默认=K1开盘=K0收盘）
        double entryRefPx; // 入场参考价（默认=K0.c）
        double k1OpenPx;   // K1.o（若可获得）
        String note;
    }

    static volatile AlignAudit LAST_RT_AUDIT = null;
    static volatile AlignAudit LAST_HIST_AUDIT = null;


    // —— 可选：持久化最后一次 RT 信号（用于“重启后还能对齐检查”）——
    static final String RT_AUDIT_FILE = sysStr("okx.rtAuditFile", "rt_audit_last.json");
    static final String HIST_AUDIT_FILE = sysStr("okx.histAuditFile", "hist_audit_last.json");

    static volatile long ALIGN_LAST_SKIP_PRINT_MS = 0L;

    static void saveRtAudit(AlignAudit a) {
        if (!DEBUG_ALIGN || a == null) return;
        try {
            JSONObject o = new JSONObject();
            o.put("signalTs", a.signalTs);
            o.put("entryTs", a.entryTs);
            o.put("entryRef", a.entryRefPx);
            o.put("savedAtMs", System.currentTimeMillis());
            Files.writeString(Paths.get(RT_AUDIT_FILE), o.toString(), StandardCharsets.UTF_8);
        } catch (Exception ignore) {}
    }

    static void loadRtAuditIfAny() {
        if (!DEBUG_ALIGN) return;
        try {
            Path p = Paths.get(RT_AUDIT_FILE);
            if (!Files.exists(p)) return;
            String s = Files.readString(p, StandardCharsets.UTF_8);
            if (s == null || s.isBlank()) return;
            JSONObject o = new JSONObject(s);
            long sigTs = o.optLong("signalTs", -1L);
            long entryTs = o.optLong("entryTs", -1L);
            double entryRef = o.has("entryRef") ? o.optDouble("entryRef", Double.NaN) : Double.NaN;
            if (sigTs <= 0 || entryTs <= 0) return;

            AlignAudit a = new AlignAudit();
            a.src = "RT-LOAD";
            a.nowMs = System.currentTimeMillis();
            a.signalTs = sigTs;
            a.sigCloseTs = sigTs + BAR30_MS;
            a.sigConfirm = -2; // loaded marker
            a.sigO = Double.NaN;
            a.sigC = Double.NaN;
            a.entryTs = entryTs;
            a.entryRefPx = entryRef;
            a.k1OpenPx = Double.NaN;
            a.note = "loaded from " + RT_AUDIT_FILE;

            LAST_RT_AUDIT = a;
            System.out.println("[ALIGN-LOAD] loaded last RT audit from " + RT_AUDIT_FILE
                    + " | sig=" + fmtOpen(sigTs) + " entry=" + fmtOpen(entryTs));
            alignLog(a);
        } catch (Exception e) {
            System.out.println("[ALIGN-LOAD][WARN] failed to load " + RT_AUDIT_FILE + " : " + e.getMessage());
        }
    }
    static void saveHistAudit(AlignAudit a) {
        if (!DEBUG_ALIGN || a == null) return;
        try {
            JSONObject o = new JSONObject();
            o.put("signalTs", a.signalTs);
            o.put("entryTs", a.entryTs);
            o.put("entryRef", a.entryRefPx);
            o.put("k1Open", a.k1OpenPx);
            o.put("src", a.src);
            o.put("note", a.note == null ? "" : a.note);
            o.put("savedAtMs", System.currentTimeMillis());
            Files.writeString(Paths.get(HIST_AUDIT_FILE), o.toString(), StandardCharsets.UTF_8);
        } catch (Exception ignore) {}
    }

    static void loadHistAuditIfAny() {
        if (!DEBUG_ALIGN) return;
        try {
            Path p = Paths.get(HIST_AUDIT_FILE);
            if (!Files.exists(p)) return;
            String s = Files.readString(p, StandardCharsets.UTF_8);
            if (s == null || s.isBlank()) return;
            JSONObject o = new JSONObject(s);
            AlignAudit a = new AlignAudit();
            a.src = o.optString("src", "HIST/DISK");
            a.signalTs = o.optLong("signalTs", 0L);
            a.entryTs = o.optLong("entryTs", 0L);
            a.entryRefPx = o.optDouble("entryRef", 0.0);
            a.k1OpenPx = o.optDouble("k1Open", 0.0);
            a.note = o.optString("note", "");
            a.nowMs = System.currentTimeMillis();
            LAST_HIST_AUDIT = a;
        } catch (Exception ignore) {}
    }


    static Candle findByTs(List<Candle> list, long ts) {
        if (list == null || list.isEmpty()) return null;
        for (int i = list.size() - 1; i >= 0; i--) {
            Candle c = list.get(i);
            if (c != null && c.ts == ts) return c;
        }
        return null;
    }

    static void alignPrintTail(String tag, long nowMs, List<Candle> list, long barMs) {
        if (!DEBUG_ALIGN || !DEBUG_ALIGN_TAIL) return;
        if (list == null || list.isEmpty()) {
            System.out.println("[ALIGN-" + tag + "-TAIL] empty");
            return;
        }
        int n = list.size();
        int from = Math.max(0, n - 3);
        System.out.println("[ALIGN-" + tag + "-TAIL] now=" + fmtOpen(nowMs)
                + " size=" + n + " barMs=" + barMs + " safeCloseMs=" + SAFE_CLOSE_MS);
        for (int i = from; i < n; i++) {
            Candle c = list.get(i);
            if (c == null) continue;
            long closeTs = c.ts + barMs;
            long lagMs = nowMs - closeTs;
            System.out.println("  idx=" + i
                    + " ts=" + fmtOpen(c.ts) + "(开)/" + fmtClose(c.ts, barMs) + "(收)"
                    + " confirm=" + c.confirm
                    + " lagMs=" + lagMs
                    + " o=" + String.format(Locale.US, "%.2f", c.o)
                    + " c=" + String.format(Locale.US, "%.2f", c.c));
        }
        cacheMetaFromList(tag, nowMs, list, barMs);

    }

    static AlignAudit alignMake(String src, long nowMs, Candle sigC0, Candle entryC, double entryRefPx, long signalTs, String note) {
        AlignAudit a = new AlignAudit();
        a.src = src;
        a.nowMs = nowMs;
        a.signalTs = signalTs;
        a.sigCloseTs = signalTs + BAR30_MS;
        if (sigC0 != null) {
            a.sigConfirm = sigC0.confirm;
            a.sigO = sigC0.o;
            a.sigC = sigC0.c;
        } else {
            a.sigConfirm = -1;
            a.sigO = Double.NaN;
            a.sigC = Double.NaN;
        }
        a.entryTs = (entryC != null ? entryC.ts : (signalTs + BAR30_MS));
        a.entryRefPx = entryRefPx;
        a.k1OpenPx = (entryC != null ? entryC.o : Double.NaN);
        a.note = note;
        return a;
    }

    static void alignLog(AlignAudit a) {
        if (!DEBUG_ALIGN || a == null) return;
        long dt = a.entryTs - a.sigCloseTs;
        System.out.println("[ALIGN-" + a.src + "] now=" + fmtOpen(a.nowMs)
                + " | sigK0=" + fmtOpen(a.signalTs) + "(开)/" + fmtClose(a.signalTs, BAR30_MS) + "(收)"
                + " confirm=" + a.sigConfirm
                + " o=" + (Double.isFinite(a.sigO) ? String.format(Locale.US, "%.2f", a.sigO) : "NA")
                + " c=" + (Double.isFinite(a.sigC) ? String.format(Locale.US, "%.2f", a.sigC) : "NA")
                + " | entryTs=" + fmtOpen(a.entryTs)
                + " dtToSigClose=" + String.format(Locale.US, "%.2f", dt / 60000.0) + "m"
                + " | entryRef(K0.c)=" + String.format(Locale.US, "%.2f", a.entryRefPx)
                + " | k1.o=" + (Double.isFinite(a.k1OpenPx) ? String.format(Locale.US, "%.2f", a.k1OpenPx) : "NA")
                + (a.note != null && !a.note.isBlank() ? " | " + a.note : ""));
    }

    static void alignCompare() {
        if (!DEBUG_ALIGN) return;

        if (LAST_RT_AUDIT == null || LAST_HIST_AUDIT == null) {
            long now = System.currentTimeMillis();
            // 防刷屏：同类 SKIP 60s 打印一次
            if (now - ALIGN_LAST_SKIP_PRINT_MS > 60_000L) {
                ALIGN_LAST_SKIP_PRINT_MS = now;
                System.out.println("[ALIGN-CHECK][SKIP] 无法对比："
                        + (LAST_RT_AUDIT == null ? "RT_AUDIT 缺失(本次运行尚未触发过入场提醒；若你刚重启，可从磁盘加载)" : "")
                        + (LAST_RT_AUDIT == null && LAST_HIST_AUDIT == null ? " & " : "")
                        + (LAST_HIST_AUDIT == null ? "HIST_AUDIT 缺失(本轮 scanHistory 未产出 lastTrade/opp)" : "")
                        + " | 提示：等待下一次入场提醒后再看 [ALIGN-CHECK][OK/WARN]");
            }
            return;
        }

        long dSig = LAST_HIST_AUDIT.signalTs - LAST_RT_AUDIT.signalTs;
        long dEntry = LAST_HIST_AUDIT.entryTs - LAST_RT_AUDIT.entryTs;

        boolean sigWarn = Math.abs(dSig) >= (BAR30_MS / 2);
        boolean entryWarn = Math.abs(dEntry) >= (BAR30_MS / 2);

        if (sigWarn || entryWarn) {

            // 如果刚刚触发了 RT 入场提醒，本轮 scanHistory 在 checkRealtime 之前执行，
            // 那么“RT 新于 HIST”是预期现象；让下一轮 scanHistory 去复现，再决定是否 WARN。
            if (PENDING_VERIFY_ENTRY_TS > 0
                    && PENDING_VERIFY_ENTRY_TS == LAST_RT_AUDIT.entryTs
                    && LAST_HIST_AUDIT.entryTs < LAST_RT_AUDIT.entryTs
                    && PENDING_VERIFY_SET_AT_MS > 0
                    && Math.abs(LAST_RT_AUDIT.nowMs - PENDING_VERIFY_SET_AT_MS) < 60_000L) {
                System.out.println("[ALIGN-CHECK][INFO] RT 新于 HIST（本轮 scanHistory 在前），等待下一轮 scanHistory 复现。"
                        + " | pendingEntry=" + fmtOpen(PENDING_VERIFY_ENTRY_TS)
                        + " | HIST(entry=" + fmtOpen(LAST_HIST_AUDIT.entryTs) + ")");
                return;
            }

            System.out.println("[ALIGN-CHECK][WARN] HIST vs RT mismatch"
                    + " dSignal=" + String.format(Locale.US, "%.2f", dSig / 60000.0) + "m"
                    + " dEntry=" + String.format(Locale.US, "%.2f", dEntry / 60000.0) + "m"
                    + " | RT(sig=" + fmtOpen(LAST_RT_AUDIT.signalTs) + " entry=" + fmtOpen(LAST_RT_AUDIT.entryTs) + ")"
                    + " | HIST(sig=" + fmtOpen(LAST_HIST_AUDIT.signalTs) + " entry=" + fmtOpen(LAST_HIST_AUDIT.entryTs) + ")");
            System.out.println("  -> 常见原因：① scanHistory 最新K未进来/被 SAFE_CLOSE_MS 挡住 ② 历史逻辑 idx 偏移 ③ 运行旧 class（看 BUILD 行）");

// v28 增强：打印关键状态，直接定位“不一致来自哪里”
            System.out.println("  [STATE] autoEntryPending=" + autoEntryPending
                    + " expire=" + (autoEntryExpireAtMs > 0 ? fmtOpen(autoEntryExpireAtMs) : "NA")
                    + " | LIVE_ENGINE.pos=" + (LIVE_ENGINE.pos == null ? "NULL"
                    : ((LIVE_ENGINE.pos.isVirtual ? "VIRTUAL" : "REAL")
                    + " " + (LIVE_ENGINE.pos.side == Side.LONG ? "LONG" : "SHORT")
                    + " entryTs=" + fmtOpen(LIVE_ENGINE.pos.entryTs)
                    + " entry=" + String.format(Locale.US, "%.2f", LIVE_ENGINE.pos.entry))));

            ListMeta rt30 = META_RT_30M, hi30 = META_HIST_30M;
            if (rt30 != null) {
                System.out.println("  [META-RT-30m] size=" + rt30.size + " range=" + rt30.rangeStr()
                        + " | hasSig=" + (rt30.inRange(LAST_RT_AUDIT.signalTs) ? "Y" : "N")
                        + " hasEntry=" + (rt30.inRange(LAST_RT_AUDIT.entryTs) ? "Y" : "N")
                        + " tailHasSig=" + (rt30.inTail(LAST_RT_AUDIT.signalTs) ? "Y" : "N")
                        + " tailHasEntry=" + (rt30.inTail(LAST_RT_AUDIT.entryTs) ? "Y" : "N"));
            } else {
                System.out.println("  [META-RT-30m] NULL");
            }
            if (hi30 != null) {
                System.out.println("  [META-HIST-30m] size=" + hi30.size + " range=" + hi30.rangeStr()
                        + " | hasSig=" + (hi30.inRange(LAST_HIST_AUDIT.signalTs) ? "Y" : "N")
                        + " hasEntry=" + (hi30.inRange(LAST_HIST_AUDIT.entryTs) ? "Y" : "N")
                        + " tailHasSig=" + (hi30.inTail(LAST_HIST_AUDIT.signalTs) ? "Y" : "N")
                        + " tailHasEntry=" + (hi30.inTail(LAST_HIST_AUDIT.entryTs) ? "Y" : "N"));
            } else {
                System.out.println("  [META-HIST-30m] NULL");
            }

        } else {
            System.out.println("[ALIGN-CHECK][OK] RT 与 HIST 对齐"
                    + " dSignal=" + String.format(Locale.US, "%.2f", dSig / 60000.0) + "m"
                    + " dEntry=" + String.format(Locale.US, "%.2f", dEntry / 60000.0) + "m");
        }
    }

    // （可选）播放 mp3：能播就播，不能播就只 beep
    static final boolean USE_MP3 = true;
    static final boolean VERBOSE_BACKTEST = true; // 回放/回测时是否打印每笔SRMF进出场明细

    //  注意：路径仅示例。若本机不存在该文件，程序会自动 fallback 为 Toolkit.beep()
    static final String ALARM_MP3 = "C:\\\\Users\\\\baiyu\\\\Desktop\\\\室内系的TrackMaker(Melodic Mi - mm铃声.mp3";

    // ===================== HTTP 客户端（OKX接口请求） =====================
    static final HttpClient HTTP = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    // =====================  新增过滤参数（只影响进场） =====================

    // A)  亚洲盘前半（流动性低）不进场（默认 JST 04:00-08:00 && 13:00-20:00）
    //最新不开仓时间 东京周六 周天的早8 到晚9
    static final boolean FILTER_ASIA_LOW_LIQ = false;
    static final int NO_TRADE_1_START = 0;
    static final int NO_TRADE_1_END = 0;
    static final int NO_TRADE_2_START = 0;
    static final int NO_TRADE_2_END = 0;

    // B)  大级别中性期第一根不进场（1H趋势升级）
    static final boolean FILTER_1H_NEUTRAL_FIRST_BAR = false;
    static final boolean FILTER_1H_SECOND_TREND_BAR = false;  // 新增：中性→趋势后的「第二根趋势K」禁止进场

    // C)  12月禁止开仓（只影响新开仓，不影响已有持仓的止盈/止损/管理）
    //     月份判断：只用 30m 的 K1（sigIdx-1）的时间，避免未来函数/边界误判
    static final boolean DISABLE_DECEMBER_NEW_ENTRY = false;

    // ========== 回测开仓日期窗口（用于样本外/时间切分验证）==========
    // 作用：只允许在 [START, END] 区间内开新仓；窗口外的K照常参与数据遍历和指标预热(warmup不受影响)，
    //       但不开新仓。已持仓的止盈/止损/管理不受影响（让窗口内开的仓能正常平掉）。
    // 用法：格式 "yyyy-MM-dd"，空字符串 "" 表示不限制该端。
    //   - 前2年(训练段)：START=""           END="2024-01-01"
    //   - 后2年(测试段)：START="2024-01-01"  END=""
    //   - 不分段(全样本)：两个都留 ""（=当前行为，零变更）
    // 注意：这是按"开仓时间"过滤，窗口边界附近的持仓可能跨段平仓，属正常。
    static final String BT_ENTRY_WINDOW_START = "";   // 空=不限开始
    static final String BT_ENTRY_WINDOW_END   = "";   // 空=不限结束

    // 1H 中性阈值：|hist| < X 视为中性（你想用 3 就是 3）
    static final double NEUTRAL_ABS_HIST_X = 6.0;

    // hist 在 0 附近来回：回看最近 N 根 1H（已收盘）
    static final int NEUTRAL_OSC_N = 4;

    // 0 附近振荡的“幅度上限”（越大越容易被判成中性）
    static final double NEUTRAL_OSC_ABS_HIST_MAX = 6.0; // 建议先与 X 一样

    //新增信号质量过滤规则
    //规则A：实体占比过滤（避免上影下影乱扫）
    static final double MIN_BODY_RATIO = 0.0;   // 可调：0.30~0.45
    //规则B：最小波动过滤（避免太小波动进场）
    static final double MIN_RANGE_POINTS = 0.0; // 可调：15~35（40+ 会导致几乎无进场）

    //  动态最小波动阈值：用最近 N 根K线平均波动（high-low）替代固定 MIN_RANGE_POINTS（无未来函数）
    static final boolean USE_DYNAMIC_MIN_RANGE = false; // true=动态阈值；false=固定 MIN_RANGE_POINTS
    static final int MIN_RANGE_LOOKBACK_N = 20;        // 最近 N 根用于计算平均波动阈值
    static final boolean MIN_RANGE_INCLUDE_SELF = true; // true=包含当前信号K；false=只看之前N根
    static final boolean USE_QUALITY_FILTER = false; //  开关：信号质量过滤（实体比例/最小波动）

    // ===== MACD动量仓位动态调整（数据驱动：327笔回测验证，p=0.035显著）=====2026年3月20日硬编码 涨超过ETH币5000要修改！！要修改！！要修改更新1111
    // 规则：MACD_HIST>5时加仓1.5x（动量爆发）；diffInt<0时减仓0.7x（动量衰减）
    // 回测结果：最终资金+10,194,671U | 最大回撤34.4%→32.2% | Calmar 21.59→24.33
    // 关闭：-Dokx.useMacdPosSizing=false
    static final boolean USE_MACD_POS_SIZING  = sysBool("okx.useMacdPosSizing", false);
    static final double  MACD_BOOST_THRESHOLD = sysDouble("okx.macdBoostThreshold", 5.0);
    static final double  MACD_BOOST_SCALE     = sysDouble("okx.macdBoostScale", 1.3);
    static final double  MACD_REDUCE_SCALE    = sysDouble("okx.macdReduceScale", 0.7);

    // =====================  风控票（R1~R4：只用信号K0及历史已收盘数据，避免未来函数） =====================
    // 用法（示例）：
    // -Dokx.riskVote.enabled=true
    // -Dokx.riskVote.r1Veto=true
    // -Dokx.riskVote.r2StrongK=true
    // -Dokx.riskVote.r3VolNotLow=true
    // -Dokx.riskVote.r4FarEma400=true
    //
    // 说明：
    // A) R1：反向长影线出现 => 直接否决（不进场）
    // B) R2~R4：风控票（riskVote）只做“降仓/不加仓”，不反向追涨杀跌
    //    riskVote <= -1：mult *= 0.8
    //    riskVote <= -2：mult *= 0.6
    //    riskVote >= +1：默认不收缩（保持原 tierMult*voteScale 不变）
    //      - 如你确实想“禁止票数加仓”，可开启 okx.riskVote.capAtTierMult=true，把 mult 上限压到 tierMult
    static final boolean USE_RISK_VOTE = sysBool("okx.riskVote.enabled", false);
    static final boolean RV_PRINT_BLOCK = sysBool("okx.riskVote.printBlock", false); // 仅在拦截/降仓时打印

    static final boolean RV_R1_VETO = sysBool("okx.riskVote.r1Veto", true);
    static final boolean RV_R2_STRONGK = sysBool("okx.riskVote.r2StrongK", false);
    static final boolean RV_R3_VOLNOTLOW = sysBool("okx.riskVote.r3VolNotLow", false);
    static final boolean RV_R4_FAR_EMA400 = sysBool("okx.riskVote.r4FarEma400", false);

    static final double RV_CUT_1 = sysDouble("okx.riskVote.cut1", 0.7);
    static final double RV_CUT_2 = sysDouble("okx.riskVote.cut2", 0.5);

    // riskVote>=+1 时是否把 mult 上限压到 tierMult（默认 false：不收缩，保持原 tierMult*voteScale）
    static final boolean RV_CAP_AT_TIER_MULT = sysBool("okx.riskVote.capAtTierMult", false);

    // R1：反向长影线判定（更抗拟合：用“影线占比 + 相对实体”双条件）
    static final double RV_R1_WICK_RANGE_RATIO = sysDouble("okx.riskVote.r1WickRangeRatio", 0.6);
    static final double RV_R1_WICK_BODY_MULT = sysDouble("okx.riskVote.r1WickBodyMult", 0);
    static final double RV_R1_DOJI_BODY_RATIO = sysDouble("okx.riskVote.r1DojiBodyRatio", 0.12);

    // R2：强势K确认（满足 +1；不满足 0）
    static final double RV_R2_BODY_RANGE_MIN = sysDouble("okx.riskVote.r2BodyRangeMin", 0.50);
    static final double RV_R2_CLOSE_POS_MIN = sysDouble("okx.riskVote.r2ClosePosMin", 0.80);

    // R3：波动不低（不满足 -1；满足 0）。用 ATR 短/长相对 + 最小波动百分比（抗不同价格区间）
    static final int RV_R3_ATR_N = sysInt("okx.riskVote.r3AtrN", 48);
    static final int RV_R3_ATR_LONG_N = sysInt("okx.riskVote.r3AtrLongN", 200);
    static final double RV_R3_MIN_ATR_PCT = sysDouble("okx.riskVote.r3MinAtrPct", 0.003); // 0.3%
    static final double RV_R3_MIN_ATR_RATIO = sysDouble("okx.riskVote.r3MinAtrRatio", 0.80);

    // R4：远离 EMA400（不满足 -1；满足 0）
    static final double RV_R4_MIN_EMA400_DIST_PCT = sysDouble("okx.riskVote.r4MinEma400DistPct", 0.003); // 0.3%

    // =====================  EMA 多时间投票过滤（30m EMA400 / 1H EMA200 / 4H EMA50） =====================
    // 说明：
    // - 只用【已收盘K线】计算，避免未来函数
    // - 每周期票：基础 0.5；若 |diff01| >= 1.5 * avgDiff，则追加强度票 0.3
    // - diff/avgDiff 默认用“百分比归一”（可切换），更抗价格区间切换
    // - avgDiff 窗口不包含当前 diff01（用历史 k1-k2..kN-kN+1 的均值），避免“自己抬高阈值”
    static final boolean USE_EMA_VOTE_FILTER = sysBool("okx.emaVote.enabled", true);
    static final boolean EMA_VOTE_USE_PCT_NORM = sysBool("okx.emaVote.usePctNorm", true);      // diffPct=(ema0-ema1)/ema1
    static final boolean PRINT_EMA_VOTE_BLOCK = sysBool("okx.emaVote.printBlock", true);       // 仅在拦截时打印

    // Opt-A：斜率死区（默认关闭，保持原行为）
    static final boolean EMA_VOTE_DEADBAND_ENABLED = sysBool("okx.emaVote.deadband.enabled", true);
    static final double EMA_VOTE_DEADBAND_MULT = sysDouble("okx.emaVote.deadband.mult", 0.35); // dead=mult*avgDiff

    // Opt-A：要求“无反向票”才放行（默认关闭，保持原行为）这个不开没有价值
    static final boolean EMA_VOTE_REQUIRE_NO_OPPOSITE = sysBool("okx.emaVote.requireNoOpposite", false);

    // Opt-B：4H 只做强反向否决票（默认关闭，保持原行为：仍参与加权）
    static final boolean EMA_VOTE_4H_VETO_ONLY = sysBool("okx.emaVote.4hVetoOnly", false);

    static final double EMA_VOTE_BASE = sysDouble("okx.emaVote.base", 0.5);
    static final double EMA_VOTE_STRONG_EXTRA = sysDouble("okx.emaVote.strongExtra", 0.3);
    static final double EMA_VOTE_STRONG_MULT = sysDouble("okx.emaVote.strongMult", 1.5);

    // 总分=3周期各自最大(0.8)之和=2.4；阈值=35% => 0.84（若启用 4hVetoOnly，则按 2 周期计算）
    static final double EMA_VOTE_SCORE_PCT = sysDouble("okx.emaVote.scorePct", 0.35);

    // 周期参数（默认：30m EMA400 / 1H EMA200 / 4H EMA50）
    static final int EMA30_PERIOD = sysInt("okx.emaVote.ema30.period", 400);
    static final int EMA1H_PERIOD = sysInt("okx.emaVote.ema1h.period", 200);
    static final int EMA4H_PERIOD = sysInt("okx.emaVote.ema4h.period", 50);

    // 强度阈值窗口（默认：与 period 同值）
    static final int EMA30_AVG_N = sysInt("okx.emaVote.ema30.avgN", 400);
    static final int EMA1H_AVG_N = sysInt("okx.emaVote.ema1h.avgN", 200);
    static final int EMA4H_AVG_N = sysInt("okx.emaVote.ema4h.avgN", 50);

    // warmup（默认：你原先指定的 700/400/150；可用 -D 覆盖）
    static final int EMA30_MIN_BARS = sysInt("okx.emaVote.ema30.minBars", EMA30_PERIOD + 300); // 默认 700
    static final int EMA1H_MIN_BARS = sysInt("okx.emaVote.ema1h.minBars", EMA1H_PERIOD + 200); // 默认 400
    static final int EMA4H_MIN_BARS = sysInt("okx.emaVote.ema4h.minBars", EMA4H_PERIOD + 100); // 默认 150
    // 实盘/回测：投票上下文（预先算好 EMA 与 rolling avg，避免 O(n^2)）
    static volatile EmaVoteContext EMA_VOTE_CTX_REALTIME = null;
    static EmaVoteContext EMA_VOTE_CTX_BACKTEST = null;

    // =========================
    // 第二投票机（Pool-2）：把你指定的组合嵌入到趋势系。
    // 重要：默认不改变原逻辑，只做“并行计算 + 打印/闹钟”。
    // 如需把它变成硬过滤，只需把 USE_POOL2_ENTRY_FILTER 改为 true。
    // 组合：POOLM_SQUEEZE20_20_CVD_sign_EMA200_FISHER_P10x2_MACD12_26_9_StochRSI14_14_RSI14_rev30_70x2_PS_TSOUP_96_TH6_FLAT
    // =========================
    static final boolean USE_POOL2_VOTE_MACHINE = true;
    static final boolean PRINT_POOL2_ON_SIGNAL = true;   // 仅在“出现入场候选side”时打印

    // ========== 信号来源归因统计开关 ==========
    // 作用：回测结束后，按入场信号来源(MACD3K/RANGE_BK/DOUBLE_TB/OKX_COMBO)
    //       分组打印 笔数/占比/胜率/总盈亏/平均每笔/盈亏比，用于看“系统靠哪个源在赚钱”。
    // 资源说明：仅是回测结束后遍历一次已有成交列表做汇总打印，开销很小；
    //       但默认关闭(false)——平时不需要就不刷屏，想分析时改成 true 跑一次即可。
    // 注意：来源字段(entrySource)始终在回测中记录(零成本)，本开关只控制“是否打印统计”，
    //       所以即使平时关着，打开后下一次回测立刻就能出完整归因，无需补数据。
    static final boolean PRINT_SOURCE_STATS = true;   // ← 平时关闭；要看信号源成绩时改 true

    //  方案2：作为硬过滤器（投票必须一致才能进）。
    // - true：Pool-2 方向必须与原本 entrySideByDifDea 的方向一致，才允许进场。
    // - false：只打印/报警，不影响原本逻辑。
    static final boolean USE_POOL2_ENTRY_FILTER = true;

    // ========== Pool-2 诊断统计开关 ==========
    // 作用：回测结束后打印两张表 ——
    //   A) score 分布：每次 Pool-2 实际参与过滤(side!=null)时算出的 score 落在各值的频次，
    //      标出 ±POOL2_TH 这刀切在分布的什么位置(看过滤器松紧)。
    //   B) 成员投票行为：每个成员投 +1/-1/0 的次数 + 与最终 dir 的一致率
    //      (看谁在干活、谁在划水、谁在对抗；不记盈利归因，因为盈利无法干净拆给单个成员)。
    // 资源说明：仅在 side!=null 时累加几个计数器，回测结束打印一次，开销极小。
    //   默认关闭(false)，想分析时改 true 跑一次即可。
    static final boolean PRINT_POOL2_DIAG = false;   // ← 平时关闭；要看 Pool-2 内部分布时改 true

    static final String POOL2_NAME = "POOLM_SQUEEZE20_20_CVD_sign_EMA200_FISHER_P10x2_MACD12_26_9_StochRSI14_14_RSI14_rev30_70x2_PS_TSOUP_96_TH6_FLAT";

    static final int POOL2_TH = 4;
    static final int POOL2_W_FISHER = 2;
    static final int POOL2_W_RSI_REV = 2;

    // ========== Pool-2 成员权重（通用实验开关）==========
    // 每个成员一个权重：设 0 = 剔除该成员；设 1/2/3 = 调整话语权。
    // 默认值精确等于当前行为（Fisher/RSI_rev=2，其余=1），所以不改默认就是零行为变更。
    // 用法举例：
    //   - 测"删 StochRSI 会怎样"：把 POOL2_WT_STOCHRSI 改成 0 跑一次，对比 MACD3K 成绩。
    //   - 测 Fisher 的 ×2 是不是过拟合：POOL2_WT_FISHER 取 1/2/3 各跑一次看稳不稳。
    //   - 注意：改权重会改变 score 进而改变进场集合，属于行为变更，需用样本外/扰动验证，不能凭样本内一次结果定论。
    static final int POOL2_WT_SQUEEZE  = 1;
    static final int POOL2_WT_CVD      = 1;
    static final int POOL2_WT_EMA200   = 1;
    static final int POOL2_WT_FISHER   = POOL2_W_FISHER;   // 默认2（沿用原值）
    static final int POOL2_WT_MACD     = 1;
    static final int POOL2_WT_STOCHRSI = 1;
    static final int POOL2_WT_RSI_REV  = POOL2_W_RSI_REV;  // 默认2（沿用原值）
    static final int POOL2_WT_MA60     = 1;
    static final int POOL2_WT_BOLLBR   = 1;
    static final int POOL2_WT_ROC12    = 1;
    static final int POOL2_WT_CCI      = 1;

    static final Pool2VoteMachine POOL2 = new Pool2VoteMachine();

    static class EmaVoteSnapshot {
        double v30, v1h, v4h;
        double score;
        int posCnt, negCnt;
        boolean allowLong, allowShort;

        // debug
        VoteTF t30, t1h, t4h;
    }

    static class VoteTF {
        double vote;
        double diffUnit;      // diff01（pct 或 abs）带符号
        double avgUnit;       // avgDiff（pct 或 abs）
        boolean strong;
        int idx0;
        long k0ts;
        String note;
    }

    static class EmaVoteContext {
        final List<Candle> m30;
        final List<Candle> h1;
        final List<Candle> h4;

        final double[] ema30, ema1h, ema4h;
        final double[] absUnit30, absUnit1h, absUnit4h;
        final double[] ps30, ps1h, ps4h;

        private EmaVoteContext(List<Candle> m30, List<Candle> h1, List<Candle> h4,
                               double[] ema30, double[] ema1h, double[] ema4h,
                               double[] absUnit30, double[] absUnit1h, double[] absUnit4h,
                               double[] ps30, double[] ps1h, double[] ps4h) {
            this.m30 = m30;
            this.h1 = h1;
            this.h4 = h4;
            this.ema30 = ema30;
            this.ema1h = ema1h;
            this.ema4h = ema4h;
            this.absUnit30 = absUnit30;
            this.absUnit1h = absUnit1h;
            this.absUnit4h = absUnit4h;
            this.ps30 = ps30;
            this.ps1h = ps1h;
            this.ps4h = ps4h;
        }

        static EmaVoteContext build(List<Candle> m30Closed, List<Candle> h1Closed) {
            if (m30Closed == null || m30Closed.size() < 5) return null;
            if (h1Closed == null || h1Closed.size() < 5) return null;

            // 4H：用 30m 聚合（8根=1根4H），避免新增 API/DB
            List<Candle> h4 = aggregateFrom30m(m30Closed, BAR4H_MS, 8);

            double[] ema30 = calcEmaArray(m30Closed, EMA30_PERIOD);
            double[] ema1h = calcEmaArray(h1Closed, EMA1H_PERIOD);
            double[] ema4h = calcEmaArray(h4, EMA4H_PERIOD);

            double[] abs30 = buildAbsUnit(ema30, EMA_VOTE_USE_PCT_NORM);
            double[] abs1h = buildAbsUnit(ema1h, EMA_VOTE_USE_PCT_NORM);
            double[] abs4h = buildAbsUnit(ema4h, EMA_VOTE_USE_PCT_NORM);

            double[] ps30 = prefixSum(abs30);
            double[] ps1h = prefixSum(abs1h);
            double[] ps4h = prefixSum(abs4h);

            return new EmaVoteContext(m30Closed, h1Closed, h4, ema30, ema1h, ema4h, abs30, abs1h, abs4h, ps30, ps1h, ps4h);
        }

        EmaVoteSnapshot snapshotAt(long entryTs) {
            // 用 entryTs-1ms：确保“不用刚开盘未收的那根”
            long ts = entryTs - 1;
            int idx30 = upperBoundCandle(m30, ts) - 1;
            int idx1h = upperBoundCandle(h1, ts) - 1;
            int idx4h = upperBoundCandle(h4, ts) - 1;

            // ★ 修复未来函数：4H/1H K 必须已收盘才能使用
            // 根因：4H 由 30m 聚合，ts=21:00(JST) 的 4H K 包含 21:00~00:30 共8根30m数据
            //       在 entryTs=00:00 时，00:00 和 00:30 还未收盘 → 使用它就是未来函数
            //       导致：00:00 那轮 BT 用旧4H桶(17:00)→EMA 拦截，01:00 那轮 BT 用新4H桶(21:00)→EMA 放行
            //       表现：信号延迟半小时~1小时才提醒
            // 修复：回退到"收盘时间 <= entryTs"的最后一个桶
            while (idx4h >= 0 && h4.get(idx4h).ts + BAR4H_MS > entryTs) {
                idx4h--;
            }
            while (idx1h >= 0 && h1.get(idx1h).ts + BAR1H_MS > entryTs) {
                idx1h--;
            }

            EmaVoteSnapshot s = new EmaVoteSnapshot();

            s.t30 = voteOne("30m", ema30, absUnit30, ps30, idx30, EMA30_AVG_N, EMA30_MIN_BARS, m30);
            s.t1h = voteOne("1h", ema1h, absUnit1h, ps1h, idx1h, EMA1H_AVG_N, EMA1H_MIN_BARS, h1);
            s.t4h = voteOne("4h", ema4h, absUnit4h, ps4h, idx4h, EMA4H_AVG_N, EMA4H_MIN_BARS, h4);

            s.v30 = s.t30.vote;
            s.v1h = s.t1h.vote;
            s.v4h = s.t4h.vote;

            if (EMA_VOTE_4H_VETO_ONLY) {
                // 4H 不参与加权：只用 30m+1H 计算 score/count（更稳，避免 4H 聚合缺桶/回退带来误杀）
                s.score = s.v30 + s.v1h;
                s.posCnt = (s.v30 > 0 ? 1 : 0) + (s.v1h > 0 ? 1 : 0);
                s.negCnt = (s.v30 < 0 ? 1 : 0) + (s.v1h < 0 ? 1 : 0);
            } else {
                s.score = s.v30 + s.v1h + s.v4h;
                s.posCnt = (s.v30 > 0 ? 1 : 0) + (s.v1h > 0 ? 1 : 0) + (s.v4h > 0 ? 1 : 0);
                s.negCnt = (s.v30 < 0 ? 1 : 0) + (s.v1h < 0 ? 1 : 0) + (s.v4h < 0 ? 1 : 0);
            }

            int tfN = (EMA_VOTE_4H_VETO_ONLY ? 2 : 3);
            double maxScore = tfN * (EMA_VOTE_BASE + EMA_VOTE_STRONG_EXTRA);
            double thr = maxScore * EMA_VOTE_SCORE_PCT;

            if (EMA_VOTE_REQUIRE_NO_OPPOSITE) {
                // 更严格：不允许存在反向票（默认关闭，保持原行为）
                s.allowLong = (s.posCnt >= 2) && (s.negCnt == 0) && (s.score >= thr);
                s.allowShort = (s.negCnt >= 2) && (s.posCnt == 0) && (s.score <= -thr);
            } else {
                s.allowLong = (s.posCnt >= 2) && (s.score >= thr);
                s.allowShort = (s.negCnt >= 2) && (s.score <= -thr);
            }

            if (EMA_VOTE_4H_VETO_ONLY && s.t4h != null && s.t4h.strong) {
                // 4H 只做强反向否决：强反向时直接挡掉（不改变 score/count）
                if (s.t4h.vote < 0) s.allowLong = false;
                if (s.t4h.vote > 0) s.allowShort = false;
            }
            return s;
        }

        private static VoteTF voteOne(String name,
                                      double[] ema, double[] absUnit, double[] ps,
                                      int idx0, int avgN, int minBars,
                                      List<Candle> candles) {
            VoteTF t = new VoteTF();
            t.idx0 = idx0;
            t.vote = 0.0;
            t.strong = false;
            t.diffUnit = 0.0;
            t.avgUnit = 0.0;
            t.note = "";

            if (ema == null || ema.length < 2) {
                t.note = name + ":ema太短";
                return t;
            }
            if (idx0 < 1 || idx0 >= ema.length) {
                t.note = name + ":idx0不足";
                return t;
            }
            // warmup：必须保证 idx0 对应的历史长度够（避免刚启动 avg 太小导致“全是强度票”）
            if ((idx0 + 1) < minBars) {
                t.note = name + ":warmup不足";
                t.k0ts = (candles != null && idx0 < candles.size() ? candles.get(idx0).ts : 0L);
                return t;
            }

            double emaNow = ema[idx0];
            double emaPrev = ema[idx0 - 1];
            double diff = emaNow - emaPrev;

            // 单位：pct 或 abs
            double diffUnit;
            if (EMA_VOTE_USE_PCT_NORM) {
                if (Math.abs(emaPrev) < 1e-12) {
                    diffUnit = 0.0;
                } else {
                    diffUnit = diff / emaPrev;
                }
            } else {
                diffUnit = diff;
            }
            t.diffUnit = diffUnit;
            t.k0ts = (candles != null && idx0 < candles.size() ? candles.get(idx0).ts : 0L);

            if (diffUnit > 0) t.vote = +EMA_VOTE_BASE;
            else if (diffUnit < 0) t.vote = -EMA_VOTE_BASE;
            else {
                t.vote = 0.0;
                t.note = name + ":diff=0";
                return t;
            }

            // avgDiff：abs(ema[i]-ema[i-1]) 的均值；排除当前 diff01（用 idx0-avgN .. idx0-1）
            boolean hasFullWindow = (idx0 >= (avgN + 1));
            if (!hasFullWindow) {
                t.note = name + ":avg窗口不足";
                return t;
            }
            int start = idx0 - avgN;  // inclusive
            int end = idx0 - 1;       // inclusive
            // absUnit[0]=0，占位；start 必须 >=1
            if (start < 1) {
                t.note = name + ":avg窗口越界";
                return t;
            }
            double sum = ps[end] - ps[start - 1];
            double avg = sum / avgN;
            t.avgUnit = avg;

            if (avg > 1e-15) {
                double absCur = Math.abs(diffUnit);

                // Opt-A：斜率死区（默认关闭）。absCur < dead=mult*avgDiff 时视为“中性”，不给正/负票
                if (EMA_VOTE_DEADBAND_ENABLED) {
                    double dead = EMA_VOTE_DEADBAND_MULT * avg;
                    if (absCur < dead) {
                        t.vote = 0.0;
                        t.strong = false;
                        t.note = name + ":deadband";
                        return t;
                    }
                }

                if (absCur >= EMA_VOTE_STRONG_MULT * avg) {
                    t.strong = true;
                    if (t.vote > 0) t.vote = +(EMA_VOTE_BASE + EMA_VOTE_STRONG_EXTRA);
                    else t.vote = -(EMA_VOTE_BASE + EMA_VOTE_STRONG_EXTRA);
                }
            }
            return t;
        }
    }

    // ===================== EMA Vote helpers =====================

    // 30m -> 4H 聚合（8根30m=1根4H），只保留“完整桶”（避免不完整K污染）
    static List<Candle> aggregateFrom30m(List<Candle> m30Closed, long frameMs, int perBucket) {
        if (m30Closed == null || m30Closed.isEmpty()) return Collections.emptyList();
        ArrayList<Candle> base = new ArrayList<>(m30Closed);
        base.sort(Comparator.comparingLong(c -> c.ts));

        TreeMap<Long, ArrayList<Candle>> buckets = new TreeMap<>();
        for (Candle c : base) {
            long b = (c.ts / frameMs) * frameMs;
            buckets.computeIfAbsent(b, k -> new ArrayList<>()).add(c);
        }

        ArrayList<Candle> out = new ArrayList<>();
        for (Map.Entry<Long, ArrayList<Candle>> e : buckets.entrySet()) {
            ArrayList<Candle> list = e.getValue();
            if (list.size() != perBucket) continue; // 只收完整4H
            list.sort(Comparator.comparingLong(c -> c.ts));
            Candle first = list.get(0);
            Candle last = list.get(list.size() - 1);
            double o = first.o;
            double h = -Double.MAX_VALUE;
            double l = Double.MAX_VALUE;
            double v = 0.0;
            for (Candle x : list) {
                h = Math.max(h, x.h);
                l = Math.min(l, x.l);
                v += x.vol;
            }
            double cclose = last.c;
            out.add(new Candle(e.getKey(), o, h, l, cclose, v, 1));
        }
        return out;
    }

    static double[] calcEmaArray(List<Candle> cs, int period) {
        if (cs == null || cs.isEmpty()) return new double[0];
        int n = cs.size();
        double[] ema = new double[n];
        double k = 2.0 / (period + 1.0);
        ema[0] = cs.get(0).c;
        for (int i = 1; i < n; i++) {
            double price = cs.get(i).c;
            ema[i] = ema[i - 1] + k * (price - ema[i - 1]);
        }
        return ema;
    }

    // =====================  Pool2 EXTRA 指标计算（仅用闭合K历史序列；无未来函数） =====================

    static double[] calcSmaArray(List<Candle> cs, int period) {
        if (cs == null || cs.isEmpty()) return new double[0];
        int n = cs.size();
        double[] out = new double[n];
        if (period <= 1) {
            for (int i = 0; i < n; i++) out[i] = cs.get(i).c;
            return out;
        }
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            sum += cs.get(i).c;
            if (i >= period) sum -= cs.get(i - period).c;
            if (i >= period - 1) out[i] = sum / period;
            else out[i] = sum / (i + 1.0); // warmup
        }
        return out;
    }

    /**
     * Bollinger Bands (close-based)
     * @return double[4][n] => mid, upper, lower, bbWidth((upper-lower)/mid)
     */
    static double[][] calcBollingerBands(List<Candle> cs, int period, double k) {
        if (cs == null || cs.isEmpty()) return new double[][]{new double[0], new double[0], new double[0], new double[0]};
        int n = cs.size();
        double[] mid = new double[n];
        double[] up = new double[n];
        double[] lo = new double[n];
        double[] w = new double[n];

        double sum = 0.0;
        double sumSq = 0.0;
        for (int i = 0; i < n; i++) {
            double c = cs.get(i).c;
            sum += c;
            sumSq += c * c;

            if (i >= period) {
                double old = cs.get(i - period).c;
                sum -= old;
                sumSq -= old * old;
            }

            int win = Math.min(i + 1, period);
            double mean = sum / win;
            double var = sumSq / win - mean * mean;
            if (var < 0) var = 0;
            double sd = Math.sqrt(var);

            mid[i] = mean;
            up[i] = mean + k * sd;
            lo[i] = mean - k * sd;

            double denom = Math.abs(mean) > 1e-12 ? mean : 0.0;
            if (denom != 0.0) w[i] = (up[i] - lo[i]) / denom;
            else w[i] = 0.0;
        }

        return new double[][]{mid, up, lo, w};
    }

    /**
     * ROC (close-based): roc = close/close[period] - 1
     */
    static double[] calcRocArray(List<Candle> cs, int period) {
        if (cs == null || cs.isEmpty()) return new double[0];
        int n = cs.size();
        double[] out = new double[n];
        for (int i = 0; i < n; i++) {
            if (i >= period) {
                double prev = cs.get(i - period).c;
                out[i] = (prev == 0.0 ? 0.0 : (cs.get(i).c / prev - 1.0));
            } else {
                out[i] = 0.0;
            }
        }
        return out;
    }

    /**
     * CCI (typical price): CCI = (TP - SMA(TP)) / (0.015 * MeanDeviation)
     */
    static double[] calcCciArray(List<Candle> cs, int period) {
        if (cs == null || cs.isEmpty()) return new double[0];
        int n = cs.size();
        double[] tp = new double[n];
        for (int i = 0; i < n; i++) {
            Candle c = cs.get(i);
            tp[i] = (c.h + c.l + c.c) / 3.0;
        }

        double[] sma = new double[n];
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            sum += tp[i];
            if (i >= period) sum -= tp[i - period];
            int win = Math.min(i + 1, period);
            sma[i] = sum / win;
        }

        double[] out = new double[n];
        for (int i = 0; i < n; i++) {
            int start = Math.max(0, i - period + 1);
            int win = i - start + 1;
            double md = 0.0;
            for (int j = start; j <= i; j++) {
                md += Math.abs(tp[j] - sma[i]);
            }
            md /= win;
            double denom = 0.015 * md;
            if (denom <= 1e-12) out[i] = 0.0;
            else out[i] = (tp[i] - sma[i]) / denom;
        }
        return out;
    }



    static double[] buildAbsUnit(double[] ema, boolean pct) {
        int n = ema.length;
        double[] out = new double[n];
        out[0] = 0.0;
        for (int i = 1; i < n; i++) {
            double prev = ema[i - 1];
            double diff = ema[i] - prev;
            double unit;
            if (pct) {
                unit = (Math.abs(prev) < 1e-12) ? 0.0 : (diff / prev);
            } else {
                unit = diff;
            }
            out[i] = Math.abs(unit);
        }
        return out;
    }

    static double[] prefixSum(double[] a) {
        int n = a.length;
        double[] ps = new double[n];
        double s = 0.0;
        for (int i = 0; i < n; i++) {
            s += a[i];
            ps[i] = s;
        }
        return ps;
    }

    static Side applyEmaVoteFilter(Side side, long entryTs, EmaVoteContext ctx, boolean silent, String phase) {
        if (side == null || ctx == null) return side;

        EmaVoteSnapshot snap = ctx.snapshotAt(entryTs);

        boolean pass = (side == Side.LONG) ? snap.allowLong : snap.allowShort;
        if (pass) return side;

        if (!silent && PRINT_EMA_VOTE_BLOCK) {
            double maxScore = 3.0 * (EMA_VOTE_BASE + EMA_VOTE_STRONG_EXTRA);
            double thr = maxScore * EMA_VOTE_SCORE_PCT;

            System.out.printf(Locale.US,
                    "【EMA投票拦截-%s】ts=%s | side=%s | score=%.2f thr=%.2f | pos=%d neg=%d | v30=%+.2f v1h=%+.2f v4h=%+.2f%n" +
                            "    30m: diff=%.6f avg=%.6f strong=%s idx=%d%n" +
                            "    1h : diff=%.6f avg=%.6f strong=%s idx=%d%n" +
                            "    4h : diff=%.6f avg=%.6f strong=%s idx=%d%n",
                    phase,
                    FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                    (side == Side.LONG ? "做多" : "做空"),
                    snap.score, thr,
                    snap.posCnt, snap.negCnt,
                    snap.v30, snap.v1h, snap.v4h,
                    snap.t30.diffUnit, snap.t30.avgUnit, (snap.t30.strong ? "Y" : "N"), snap.t30.idx0,
                    snap.t1h.diffUnit, snap.t1h.avgUnit, (snap.t1h.strong ? "Y" : "N"), snap.t1h.idx0,
                    snap.t4h.diffUnit, snap.t4h.avgUnit, (snap.t4h.strong ? "Y" : "N"), snap.t4h.idx0
            );
        }
        return null;
    }

    // ===================== 运行状态（实时/回测同构） =====================
    // 所有“持仓/冷却/最近机会/最近交易/SRMF”等运行状态，统一放在 LIVE_ENGINE（EngineState）里。

    // =====================  止损冷却（Stop-loss Cooldown） =====================
    // 目的：在“震荡反复抽打”的阶段，止损后强制等待 N 根30m K线再允许重新开仓（可开关）。
    // 说明：
    // - 只影响【进场】；不影响持仓中的 SL/TP/形态/MACD 出场
    // - 与“禁止同一根信号K再进场(LIVE_ENGINE.lastStopSigTs / skipEntrySignalTsAfterStop)”互补
    // - scanHistory(runBacktestAlarmLogic) 与 checkRealtime 共用同一规则，避免回测/实时不一致
    static final boolean USE_STOPLOSS_COOLDOWN = true;
    static final int STOPLOSS_COOLDOWN_BARS = 2;            // 推荐：6根30m=3小时（可调 2/4/6/8/12）
    static final boolean PRINT_STOPLOSS_COOLDOWN_BLOCK = false;

    //  （stop/tp 冷却的“直到哪根 entryTs 才能重新进场”也存放在 LIVE_ENGINE.stopCooldownUntilEntryTs / tpCooldownUntilEntryTs）

    static long calcCooldownUntilEntryTsAfterStop(long stopExitTs) {
        if (!USE_STOPLOSS_COOLDOWN) return -1L;
        return stopExitTs + (long) STOPLOSS_COOLDOWN_BARS * BAR30_MS;
    }

    static boolean blockedByStoplossCooldown(long candidateEntryTs, long cooldownUntilEntryTs) {
        return USE_STOPLOSS_COOLDOWN && cooldownUntilEntryTs > 0 && candidateEntryTs < cooldownUntilEntryTs;
    }

    // =====================  止盈冷却（Take-profit Cooldown） =====================
    // 目的：止盈出场后，强制等待 N 根30m K线再允许重新开仓（可开关）。
    // 说明：
    // - 只影响【进场】；不影响持仓中的 SL/TP/形态/MACD 出场
    // - scanHistory(runBacktestAlarmLogic) 与 checkRealtime 共用同一规则，避免回测/实时不一致
    // - “止盈”口径：TP全平(tp2) 或 OKX撑压止盈(okxSrTp) 或 MACD反转止盈(macdRev)
    static final boolean USE_TAKEPROFIT_COOLDOWN = true;
    static final int TAKEPROFIT_COOLDOWN_BARS = 2;          //  你要的：1/2/3... 直接调根数
    static final boolean PRINT_TAKEPROFIT_COOLDOWN_BLOCK = false;

    //  实盘：止盈后，下一次允许进场的“entryTs”（entryTs=下一根K开盘）
    static long calcCooldownUntilEntryTsAfterTakeProfit(long tpExitTs) {
        if (!USE_TAKEPROFIT_COOLDOWN) return -1L;
        return tpExitTs + (long) TAKEPROFIT_COOLDOWN_BARS * BAR30_MS;
    }

    static boolean blockedByTakeprofitCooldown(long candidateEntryTs, long cooldownUntilEntryTs) {
        return USE_TAKEPROFIT_COOLDOWN && cooldownUntilEntryTs > 0 && candidateEntryTs < cooldownUntilEntryTs;
    }


    // =====================  R1 否决冷却（R1 Veto Cooldown） =====================
    // 目的：当出现“R1 反向长影线否决”时，强制等待 N 根 30m K 再允许重新开仓（只影响进场）。
    // 关键点：
    // 1) lastR1Veto*Ts 必须记录“信号K0收盘(K0 close) 对应的 ts”（= 下一根开盘 entryTs），确保无未来函数
    // 2) 必须放在最终入场确认层：回测链路 / 实盘链路共用同一 gate，避免分叉
    static final boolean USE_R1_VETO_COOLDOWN = true;
    static final int R1_VETO_COOLDOWN_LONG_BARS  = (int) sysLong("okx.riskVote.r1CooldownLongBars", 3); // 你要的：LONG 默认 3 根
    static final int R1_VETO_COOLDOWN_SHORT_BARS = (int) sysLong("okx.riskVote.r1CooldownShortBars", 3); // 你要的：SHORT 默认 0 根（保持原行为）

    static void markR1Veto(EngineState eng, long sigCloseTs, Side side) {
        if (eng == null || side == null) return;
        if (side == Side.LONG) eng.lastR1VetoLongTs = sigCloseTs;
        else eng.lastR1VetoShortTs = sigCloseTs;
    }

    static long getR1VetoTs(EngineState eng, Side side) {
        if (eng == null || side == null) return -1L;
        return (side == Side.LONG) ? eng.lastR1VetoLongTs : eng.lastR1VetoShortTs;
    }

    static long getR1CooldownUntilTs(EngineState eng, Side side) {
        if (!USE_R1_VETO_COOLDOWN) return -1L;
        if (eng == null || side == null) return -1L;
        long last = getR1VetoTs(eng, side);
        if (last <= 0) return -1L;
        int bars = (side == Side.LONG) ? R1_VETO_COOLDOWN_LONG_BARS : R1_VETO_COOLDOWN_SHORT_BARS;
        if (bars <= 0) return -1L;
        return last + (long) bars * BAR30_MS;
    }

    static boolean blockedByR1VetoCooldown(EngineState eng, long nowTs, Side side) {
        long until = getR1CooldownUntilTs(eng, side);
        return (until > 0 && nowTs < until);
    }


    static boolean printedBoot = false;

    // =====================  K线缓存（防止定时刷新窗口变化导致 MACD/EMA 漂移，回看分叉） =====================
    // 说明：
    //  - 启动时你会拉 1 年回测（全量已收K），这里把它作为“EMA/MACD 的稳定起算点”
    //  - 定时刷新只增量合并最新K线，不再用短窗口重算，避免“同一段时间的MACD”因为起点不同而漂移
    static final long CACHE_KEEP_MS = Duration.ofDays(370).toMillis();//原370
    static final Object CACHE_LOCK = new Object();
    static final TreeMap<Long, Candle> CACHE_M30 = new TreeMap<>();
    static final TreeMap<Long, Candle> CACHE_1H = new TreeMap<>();
    static volatile boolean CACHE_READY = false;

    static void initCacheFromBacktest(List<Candle> m30Closed, List<Candle> h2Closed, long nowMs) {
        if (m30Closed == null || m30Closed.isEmpty()) return;
        synchronized (CACHE_LOCK) {
            mergeIntoCacheLocked(CACHE_M30, m30Closed);
            if (h2Closed != null && !h2Closed.isEmpty()) mergeIntoCacheLocked(CACHE_1H, h2Closed);
            trimCacheLocked(CACHE_M30, nowMs);
            trimCacheLocked(CACHE_1H, nowMs);
            CACHE_READY = !CACHE_M30.isEmpty();
        }
    }

    // 定时刷新：确保缓存存在 + 增量合并最近K线
    static void syncCache(long nowMs) throws Exception {
        // 1) 若缓存还没初始化（极端：回测失败/被跳过），则用 370 天范围初始化一次
        if (!CACHE_READY) {
            List<Candle> m30;
            List<Candle> h2;
            if (USE_DB_CACHE) {
                // SSOT/DB：优先从 SQLite 初始化（可包含 confirm=0，但后续 merge 规则保证 confirm=1 不被覆盖）
                m30 = CandleDb.loadLatest(INST_ID, "30m", Math.max(4000, ENGINE_BARS_30M), true);
                h2  = CandleDb.loadLatest(INST_ID, "1H",  Math.max(2000, H2_LOAD_BARS_1H),  true);
            } else {
                Instant end = Instant.ofEpochMilli(nowMs);
                Instant start = end.minus(Duration.ofDays(370));
                m30 = fetchRangeCandles(INST_ID, "30m", start.toEpochMilli(), end.toEpochMilli());
                h2  = fetchRangeCandles(INST_ID, "1H",  start.toEpochMilli(), end.toEpochMilli());
            }

            // 初始化缓存时：30m 仍以 confirm=1 为闭合主干（避免用到刚收盘但尚未确认的异常值）
            m30 = m30.stream().filter(c -> c.confirm == 1).collect(Collectors.toList());
            // ✅关键一致性修复：1H 用“时间收盘”口径（与 scanHistory 一致），避免 confirm 历史维护不一致导致窗口长度/分位数阈值分叉
            h2  = filterClosedByTime(h2, BAR1H_MS, nowMs, SAFE_CLOSE_MS);
            initCacheFromBacktest(m30, h2, nowMs);
        }

        // 2) 增量：只拉最近一小段合并，避免每次短窗口起算导致EMA漂移
        //    SSOT=true 时：严格只读 DB（由独立 Puller 写库）。
        //    SSOT=false 时：允许网拉补齐。
        List<Candle> m30Recent;
        List<Candle> h2Recent;
        if (USE_DB_CACHE && STRICT_SSOT) {
            m30Recent = CandleDb.loadLatest(INST_ID, "30m", Math.max(4000, ENGINE_BARS_30M), true);
            h2Recent  = CandleDb.loadLatest(INST_ID, "1H",  Math.max(2000, H2_LOAD_BARS_1H),  true);
        } else {
            m30Recent = fetchRecentByCount("30m", Math.max(4000, ENGINE_BARS_30M));
            // ✅关键一致性修复：1H 增量合并也必须按与 scanHistory 相同的“更大窗口 + 允许未确认 + 时间收盘”口径
            // 说明：fetchRecentByCount() 默认只返回 confirm=1（用于等待机制），会导致 1H 历史维护不一致时窗口变短。
            if (USE_DB_CACHE) {
                if (!STRICT_SSOT) {
                    // 非 strict 模式下，本进程允许补写 DB（增量），确保 1H 尾部及时刷新
                    CandleDb.ingestRecent(INST_ID, "1H", Math.max(Math.max(2000, H2_LOAD_BARS_1H), 600), nowMs);
                }
                h2Recent = CandleDb.loadLatest(INST_ID, "1H", Math.max(2000, H2_LOAD_BARS_1H), true);
            } else {
                h2Recent = fetchRecentByCountNet("1H", Math.max(200, H2_LOAD_BARS_1H));
            }
        }

        synchronized (CACHE_LOCK) {
            mergeIntoCacheLocked(CACHE_M30, m30Recent);
            mergeIntoCacheLocked(CACHE_1H, h2Recent);
            trimCacheLocked(CACHE_M30, nowMs);
            trimCacheLocked(CACHE_1H, nowMs);
            CACHE_READY = !CACHE_M30.isEmpty();
        }
    }

    static List<Candle> snapshotCache(TreeMap<Long, Candle> cache) {
        synchronized (CACHE_LOCK) {
            return new ArrayList<>(cache.values());
        }
    }

    // ===== 合并规则（非常关键）=====
    // - 若已有 confirm=1，则不允许被 confirm=0 覆盖
    // - 若新数据 confirm=1，则覆盖旧数据（修正高/低/收盘等）
    static void mergeIntoCacheLocked(TreeMap<Long, Candle> cache, List<Candle> incoming) {
        if (incoming == null || incoming.isEmpty()) return;
        for (Candle c : incoming) {
            if (c == null) continue;
            Candle old = cache.get(c.ts);
            if (old == null) {
                cache.put(c.ts, c);
            } else {
                if (old.confirm == 1 && c.confirm != 1) {
                    // keep old
                } else if (c.confirm == 1 && old.confirm != 1) {
                    cache.put(c.ts, c);
                } else {
                    // 两者同 confirm（或都缺失字段）时，用“新”覆盖，避免旧数据残留
                    cache.put(c.ts, c);
                }
            }
        }
    }

    static void trimCacheLocked(TreeMap<Long, Candle> cache, long nowMs) {
        if (cache == null || cache.isEmpty()) return;
        long cut = nowMs - CACHE_KEEP_MS;
        while (!cache.isEmpty() && cache.firstKey() < cut) {
            cache.pollFirstEntry();
        }
    }

    /**
     * 计算最近 n 根(含 endIdx) 的 |dif| 平均值，返回 double
     */
    private static double avgAbsDif(List<MacdPoint> macd, int endIdx, int n) {
        if (macd == null || macd.isEmpty()) return 0.0;
        if (n <= 0) throw new IllegalArgumentException("n must be > 0");
        if (endIdx < 0 || endIdx >= macd.size()) throw new IllegalArgumentException("endIdx out of range");

        int start = Math.max(0, endIdx - n + 1);
        double sum = 0.0;
        int cnt = 0;

        for (int j = start; j <= endIdx; j++) {
            sum += Math.abs(macd.get(j).dif);
            cnt++;
        }
        return cnt == 0 ? 0.0 : (sum / cnt);
    }

    /**
     * 计算最近 n 根(含 endIdx) 的 |dea| 平均值，返回 double
     */
    private static double avgAbsDea(List<MacdPoint> macd, int endIdx, int n) {
        if (macd == null || macd.isEmpty()) return 0.0;
        if (n <= 0) throw new IllegalArgumentException("n must be > 0");
        if (endIdx < 0 || endIdx >= macd.size()) throw new IllegalArgumentException("endIdx out of range");

        int start = Math.max(0, endIdx - n + 1);
        double sum = 0.0;
        int cnt = 0;

        for (int j = start; j <= endIdx; j++) {
            sum += Math.abs(macd.get(j).dea);
            cnt++;
        }
        return cnt == 0 ? 0.0 : (sum / cnt);
    }

    //  统一记录：最近一次出场机会 + 最近N笔交易（回测/scanHistory/实盘复盘共用）
    static void recordClosedTradeForView(EngineState st, CompletedTrade ct) {
        if (ct == null) return;
        if (st == null) st = LIVE_ENGINE;

        // ===== BAYES：把入场 posterior 写入 trade，并在出场时回灌更新 BayesState（滚动窗口）=====
        try {
            if (ct != null && st != null) {
                // 若未显式赋值，则尽量从持仓里补上（回测/实盘都适用）
                if (st.pos != null) {
                    if (Double.isNaN(ct.bayesPosterior) || ct.bayesPosterior == 0.0 || ct.bayesPosterior == 0.5) {
                        ct.bayesPosterior = st.pos.bayesPosterior;
                    }
                }
                if (USE_BAYESIAN_FILTER && st.bayesState != null && st.currentBayesSnap != null) {
                    String lm = (BAYES_LABEL_MODE == null ? "PNL_SIGN" : BAYES_LABEL_MODE.trim().toUpperCase(Locale.ROOT));

                    // rMultiple：优先用 CompletedTrade 里已经算好的；若缺失则用 pnl/slPointsUsed 兜底（不会引入未来函数）
                    double r = ct.rMultiple;
                    if (!Double.isFinite(r)) {
                        double sl = ct.slPointsUsed;
                        if (sl > 1e-9) r = ct.pnlPoints / sl;
                    }

                    boolean train = true;
                    boolean win;

                    if ("R_SIGN".equals(lm)) {
                        // r>0 视为 win（若 r 不可用则回退 pnlSign）
                        win = (Double.isFinite(r) ? (r > 0) : (ct.pnlPoints > 0));
                    } else if ("R_QUALITY".equals(lm)) {
                        // 高质量赢/输：只把 |R| 足够大的交易用于训练（避免手续费附近的小噪声污染）
                        double rg = Math.abs(BAYES_R_GOOD);
                        double rb = Math.abs(BAYES_R_BAD);

                        if (Double.isFinite(r)) {
                            if (r >= rg) win = true;
                            else if (r <= -rb) win = false;
                            else {
                                if (BAYES_SKIP_NEUTRAL_TRAIN) {
                                    train = false; // neutral：跳过训练
                                    win = false;   // 占位（不会被使用）
                                } else {
                                    // 不跳过时，neutral 退化成按符号打标签（不推荐）
                                    win = (r >= 0);
                                }
                            }
                        } else {
                            // r 不可用：回退 pnlSign（保持可用性）
                            win = (ct.pnlPoints > 0);
                        }
                    } else {
                        // PNL_SIGN（旧行为）
                        win = (ct.pnlPoints > 0);
                    }

                    if (train) {
                        st.currentBayesSnap.isWin = win;
                        st.currentBayesSnap.pnlPoints = ct.pnlPoints;
                        st.currentBayesSnap.rMultiple = r;
                        MultiBinBayesFilter.updateOnTradeComplete(st.bayesState, st.currentBayesSnap, win, ct.pnlPoints, r, BAYES_LOOKBACK_TRADES);
                    } else if (BAYES_LABEL_DEBUG) {
                        System.out.printf(Locale.US, "[BAYES][LABEL] skip neutral: exitTs=%s r=%.3f pnl=%.2f%n",
                                fmtJst(ct.exitTs), r, ct.pnlPoints);
                    }

                    st.currentBayesSnap = null;
                }
            }
        } catch (Throwable __t) {
            System.out.println("[BAYES][WARN] updateOnTradeComplete failed: " + __t.getMessage());
        }

        // 最近一次出场机会
        st.lastExitOpp = new ExitOpportunity();
        st.lastExitOpp.ts = ct.exitTs;
        st.lastExitOpp.side = ct.side;
        st.lastExitOpp.price = ct.exit;
        st.lastExitOpp.reason = ct.reason;

        // 最近N笔
        while (st.lastTrades.size() >= LAST_N) st.lastTrades.removeFirst();
        st.lastTrades.addLast(ct);

        // Route A：仅记录“实盘链路”的成交（LIVE_ENGINE），用于回放完全一致
        if (st == LIVE_ENGINE && AUDIT_DB_ENABLED) {
            long id = AuditDb.insertRtTrade(ct, System.currentTimeMillis(), "rt-exit");
            if (id > 0) {
                System.out.println("[RT-TRADE] saved: id=" + id + " enterTs=" + fmtJst(ct.enterTs) + " exitTs=" + fmtJst(ct.exitTs) +
                        " side=" + (ct.side == null ? "?" : ct.side) + " pnlPts=" + String.format(Locale.ROOT, "%.2f", ct.pnlPoints));
            }
        }
    }

    static void recordClosedTradeForView(CompletedTrade ct) {
        recordClosedTradeForView(LIVE_ENGINE, ct);
    }


    // ===================== BAYES（入场评估辅助：特征离散化→后验→缩放） =====================
    static final class BayesEvalResult {
        Map<MultiBinBayesFilter.Feature, Integer> bins;
        EnumMap<MultiBinBayesFilter.Feature, Double> raw; // quantile 分箱需要的原始特征值（入场时锁定）
        double posterior;
        double scale;

        // 诊断字段：让控制台直接能看出“为什么被拦/为什么加仓”
        boolean gated;
        boolean boosted;
        int evidenceTrades;
        int usedFeatures;
        String mode;
    }

    static double safeBodyRatio(Candle k) {
        if (k == null) return 0.0;
        double rng = Math.max(1e-9, (k.h - k.l));
        return Math.abs(k.c - k.o) / rng;
    }

    static double safeVolumeRatio(List<Candle> m30, int sigIdx) {
        if (m30 == null || m30.isEmpty() || sigIdx < 0 || sigIdx >= m30.size()) return 1.0;
        int n = 20;
        int from = Math.max(0, sigIdx - (n - 1));
        int cnt = 0;
        double sum = 0.0;
        for (int i = from; i <= sigIdx; i++) {
            Candle k = m30.get(i);
            if (k == null) continue;
            double v = k.vol;
            if (v <= 0) continue;
            sum += v;
            cnt++;
        }
        if (cnt <= 0) return 1.0;
        double ma = sum / cnt;
        if (ma <= 1e-9) return 1.0;
        double cur = Math.max(0.0, m30.get(sigIdx).vol);
        return cur / ma;
    }

    static BayesEvalResult evalBayesForEntry(EngineState eng,
                                             List<Candle> m30, List<MacdPoint> macd30,
                                             List<MacdPoint> macd1h,
                                             Trend1HInfo t1hInfo,
                                             int sigIdx,
                                             Side side,
                                             Pool2VoteResult pool2,
                                             OuterVoteDecision ov,
                                             long entryTs,
                                             double emaVoteScore,
                                             double atrValue,
                                             double entryPx,
                                             double srGapPts,
                                             boolean silent,
                                             String tag) {
        if (eng == null || side == null || macd30 == null || sigIdx < 2 || sigIdx >= macd30.size()) return null;
        if (eng.bayesState == null) eng.bayesState = new MultiBinBayesFilter.BayesState();

        // 30m hist / dif accel（只用已收盘K0及之前）
        MacdPoint k0 = macd30.get(sigIdx);
        MacdPoint k1 = macd30.get(sigIdx - 1);
        MacdPoint k2 = macd30.get(sigIdx - 2);

        double macdHist30m = k0.hist;
        double difAccel = (k0.dif - 2.0 * k1.dif + k2.dif);

        // 1H hist：用 t1hInfo.idx 对齐到 entryTs（避免未来函数）
        double macdHist1h = 0.0;
        try {
            if (t1hInfo != null && macd1h != null && t1hInfo.idx >= 0 && t1hInfo.idx < macd1h.size()) {
                macdHist1h = macd1h.get(t1hInfo.idx).hist;
            }
        } catch (Throwable ignore) {}

        int pool2Score = (pool2 == null ? 0 : pool2.score);
        int pool2Dir   = (pool2 == null ? 0 : pool2.dir);
        int tsrndScore = (ov == null ? 0 : ov.score);
        int tsrndDirStr= (ov == null ? 0 : ov.dirStrength);

        Object t1hTrend = (t1hInfo == null ? Trend1H.中性 : t1hInfo.trend);

        // K0 的实体与量能
        Candle sigC0 = (m30 != null && sigIdx >= 0 && sigIdx < m30.size()) ? m30.get(sigIdx) : null;
        double bodyRatio = safeBodyRatio(sigC0);
        double volumeRatio = safeVolumeRatio(m30, sigIdx);


        // ===== 构建原始特征值（quantile 分箱用：滚动最近N笔拟合边界；入场时锁定）=====
        EnumMap<MultiBinBayesFilter.Feature, Double> raw = new EnumMap<>(MultiBinBayesFilter.Feature.class);
        // 只挑“低相关 + 可解释 + 不易拟合”的少量特征（其余缺失则自动跳过）
        raw.put(MultiBinBayesFilter.Feature.ATR_RELATIVE, (entryPx > 1e-9 ? (atrValue / entryPx) : 0.0));              // 波动率归一化
        raw.put(MultiBinBayesFilter.Feature.MACD_HIST, macdHist30m);                                                    // 30m 动量
        raw.put(MultiBinBayesFilter.Feature.TSRND_DIR_STRENGTH, (double) tsrndDirStr);                                 // 方向强度
        raw.put(MultiBinBayesFilter.Feature.SR_GAP_RATIO, (atrValue > 1e-9 ? (Math.abs(srGapPts) / atrValue) : 0.0));  // 撑压距离/ATR
        raw.put(MultiBinBayesFilter.Feature.BODY_RATIO, bodyRatio);                                                     // 实体/振幅
        raw.put(MultiBinBayesFilter.Feature.VOLUME_RATIO, volumeRatio);                                                 // 量能比
        raw.put(MultiBinBayesFilter.Feature.EMA_VOTE_SCORE, emaVoteScore);                                              // EMA 多TF票（连续值）

        boolean quantile = "quantile".equalsIgnoreCase(BAYES_SCHEME);

        Map<MultiBinBayesFilter.Feature, Integer> bins;
        double posterior;
        double rawPosterior = Double.NaN; // OPT-3: raw (pre-calibration) posterior
        double scale = 0;
        boolean gated = false;
        boolean boosted = false;

        // P3/P4：用于诊断打印（本次使用的阈值/是否触发极弱证据 veto）
        double gateThrUsed = BAYES_GATE_POSTERIOR;
        double boostThrUsed = BAYES_BOOST_POSTERIOR;
        String vetoReason = null;

        int evidenceTrades = 0;
        int usedFeatures = 0;

        String mode = (BAYES_MODE == null ? "BOOST" : BAYES_MODE.trim().toUpperCase(Locale.ROOT));

        if (quantile) {
            // quantile：滚动最近N笔 trade 的分位数分箱（无未来函数：只用已完成交易）
            MultiBinBayesFilter.ensureQuantileModel(eng.bayesState,
                    BAYES_LOOKBACK_TRADES,
                    BAYES_BIN_K,
                    BAYES_MIN_BIN_TRADES,
                    BAYES_WINSOR_PCT,
                    BAYES_REFIT_EVERY,
                    BAYES_REPORT_EVERY_REFIT,
                    BAYES_LAPLACE_ALPHA,
                    BAYES_MIN_BIN_EVIDENCE);

            bins = MultiBinBayesFilter.discretizeQuantile(raw, eng.bayesState);
            evidenceTrades = MultiBinBayesFilter.quantileEvidenceTrades(eng.bayesState);
            usedFeatures = MultiBinBayesFilter.countUsableFeaturesQuantile(bins, eng.bayesState, BAYES_MIN_BIN_EVIDENCE);
            posterior = MultiBinBayesFilter.computePosteriorQuantile(bins, eng.bayesState, BAYES_LAPLACE_ALPHA, BAYES_MIN_BIN_EVIDENCE);

            // OPT-3: Isotonic calibration (raw posterior -> calibrated probability)
            rawPosterior = posterior;
            posterior = MultiBinBayesFilter.isotonicCalibrate(eng.bayesState, rawPosterior);
            posterior = Math.max(0.0, Math.min(1.0, posterior)); // safety clamp

            boolean allowDecision = (evidenceTrades >= BAYES_MIN_TRADE_EVIDENCE);

            boolean wantGate = ("GATE".equals(mode) || "BOTH".equals(mode));
            boolean wantBoost = ("BOOST".equals(mode) || "BOTH".equals(mode));

            // 兼容：如果你还想用旧的 SCALE（0/0.7/1/1.3），这里保留
            if ("SCALE".equals(mode)) {
                // OPT-4: Kelly Criterion position sizing (replaces fixed 0/0.7/1/1.3 tiers)
                if (BAYES_USE_KELLY_SIZING) {
                    scale = MultiBinBayesFilter.kellyPositionScale(posterior,
                            eng.bayesState.kellyAvgWinR, eng.bayesState.kellyAvgLossR,
                            BAYES_KELLY_FRACTION, BAYES_BOOST_SCALE);
                } else {
                    scale = MultiBinBayesFilter.bayesPositionScale(posterior, BAYES_MIN_POSTERIOR, BAYES_MEDIUM_POSTERIOR, BAYES_STRONG_POSTERIOR);
                }
                gated = (scale <= 0.0);
                boosted = (scale > 1.0);
                // 冷启动保护：样本不足时不允许拦截
                if (!allowDecision && gated) {
                    gated = false;
                    scale = 1.0;
                }
            } else {
                // P3：posterior 阈值优先使用“滚动N笔 posterior 分位数阈值”，否则回退到固定阈值
                if (BAYES_USE_POSTERIOR_QUANTILE_THR && eng.bayesState != null
                        && !Double.isNaN(eng.bayesState.qGateThr) && !Double.isNaN(eng.bayesState.qBoostThr)) {
                    gateThrUsed = eng.bayesState.qGateThr;
                    boostThrUsed = eng.bayesState.qBoostThr;
                } else {
                    gateThrUsed = BAYES_GATE_POSTERIOR;
                    boostThrUsed = BAYES_BOOST_POSTERIOR;
                }

                // P4：极弱证据 veto（只在 Gate 模式启用；优先 BOOST，避免误杀）
                if (allowDecision && wantGate) {
                    // 先看 veto（如启用）
                    vetoReason = MultiBinBayesFilter.checkWeakVetoQuantile(bins, eng.bayesState, BAYES_LAPLACE_ALPHA, BAYES_MIN_BIN_EVIDENCE);
                    if (vetoReason != null) {
                        gated = true;
                        scale = 0.0;
                    } else {
                        // P5：Gate 依据：bucketAvgR（Expected R 口径）
                        // 关键：只要 calibReady，就【不再】用 posterior<gateThr 去 Gate（避免 nonMono 时误杀低 posterior 高期望桶）
                        boolean calibReady = MultiBinBayesFilter.isCalibReady(eng.bayesState);
                        if (calibReady) {
                            String calibGate = MultiBinBayesFilter.checkCalibAvgRGate(eng.bayesState, posterior);
                            if (calibGate != null) {
                                vetoReason = calibGate;
                                gated = true;
                                scale = 0.0;
                            }
                        } else if (posterior < gateThrUsed) {
                            // 校验样本不足时回退到 posterior gate
                            gated = true;
                            scale = 0.0;
                        }
                    }
                }


                if (!gated && allowDecision && wantBoost && posterior > boostThrUsed) {
                    boosted = true;
                    // OPT-4: Kelly-based boost scale (information-theoretically optimal)
                    if (BAYES_USE_KELLY_SIZING && eng.bayesState != null) {
                        double kellyScale = MultiBinBayesFilter.kellyPositionScale(posterior,
                                eng.bayesState.kellyAvgWinR, eng.bayesState.kellyAvgLossR,
                                BAYES_KELLY_FRACTION, BAYES_BOOST_SCALE);
                        scale = Math.max(1.0, kellyScale); // boost only
                    } else {
                        scale = BAYES_BOOST_SCALE;
                    }
                } else if (!gated) {
                    // OPT-4: Even normal trades use Kelly for fine-grained sizing
                    if (BAYES_USE_KELLY_SIZING && eng.bayesState != null && allowDecision) {
                        double kellyScale = MultiBinBayesFilter.kellyPositionScale(posterior,
                                eng.bayesState.kellyAvgWinR, eng.bayesState.kellyAvgLossR,
                                BAYES_KELLY_FRACTION, BAYES_BOOST_SCALE);
                        scale = Math.max(0.5, kellyScale); // don't go below 0.5 for non-gated
                    } else {
                        scale = 1.0;
                    }
                }
            }

        } else {
            // legacy：旧固定分箱（保留兼容）
            bins = MultiBinBayesFilter.discretize(
                    macdHist30m,
                    difAccel,
                    macdHist1h,
                    pool2Score,
                    pool2Dir,
                    tsrndScore,
                    tsrndDirStr,
                    t1hTrend,
                    side,
                    emaVoteScore,
                    atrValue,
                    entryPx,
                    srGapPts,
                    bodyRatio,
                    volumeRatio
            );

            evidenceTrades = (eng.bayesState == null ? 0 : (eng.bayesState.totalWins + eng.bayesState.totalLosses));
            usedFeatures = (bins == null ? 0 : bins.size());
            posterior = MultiBinBayesFilter.computePosterior(bins, eng.bayesState, BAYES_LAPLACE_ALPHA);

            if ("SCALE".equals(mode)) {
                scale = MultiBinBayesFilter.bayesPositionScale(posterior, BAYES_MIN_POSTERIOR, BAYES_MEDIUM_POSTERIOR, BAYES_STRONG_POSTERIOR);
                gated = (scale <= 0.0);
                boosted = (scale > 1.0);
            } else {
                boolean allowDecision = (evidenceTrades >= BAYES_MIN_TRADE_EVIDENCE);
                boolean wantGate = ("GATE".equals(mode) || "BOTH".equals(mode));
                boolean wantBoost = ("BOOST".equals(mode) || "BOTH".equals(mode));

                if (allowDecision && wantGate) {
                    boolean calibReady = MultiBinBayesFilter.isCalibReady(eng.bayesState);
                    if (calibReady) {
                        String calibGate = MultiBinBayesFilter.checkCalibAvgRGate(eng.bayesState, posterior);
                        if (calibGate != null) {
                            vetoReason = calibGate;
                            gated = true;
                            scale = 0.0;
                        }
                    } else if (posterior < BAYES_GATE_POSTERIOR) {
                        gated = true;
                        scale = 0.0;
                    }
                }

                if (!gated && allowDecision && wantBoost && posterior > BAYES_BOOST_POSTERIOR) {
                    boosted = true;
                    scale = BAYES_BOOST_SCALE;
                } else if (!gated) {
                    scale = 1.0;
                }
            }
        }

        if (BAYES_VERBOSE && !silent) {
            System.out.println("    【BAYES】" + (tag == null ? "" : ("[" + tag + "]"))
                    + " entryTs=" + fmtOpen(entryTs)
                    + " scheme=" + (quantile ? "quantile" : "legacy")
                    + " mode=" + mode
                    + " evidenceN=" + evidenceTrades
                    + " usedF=" + usedFeatures
                    + " posterior=" + String.format(Locale.ROOT, "%.4f", posterior)
                    + (BAYES_USE_ISOTONIC_CALIB && quantile ? " rawPost=" + String.format(Locale.ROOT, "%.4f", rawPosterior) : "")
                    + (vetoReason != null ? " ✅VETO" : (gated ? " ✅GATE" : (boosted ? " ✅BOOST" : "")))
                    + " scale=" + String.format(Locale.ROOT, "%.2f", scale)
                    + ("SCALE".equals(mode) ? "" :
                    (" gateThr=" + String.format(Locale.ROOT, "%.3f", gateThrUsed)
                            + " boostThr=" + String.format(Locale.ROOT, "%.3f", boostThrUsed)
                            + (quantile && BAYES_USE_POSTERIOR_QUANTILE_THR && eng.bayesState != null && !Double.isNaN(eng.bayesState.qGateQ)
                            ? (" qGate=" + String.format(Locale.ROOT, "%.2f", eng.bayesState.qGateQ)
                            + " qBoost=" + String.format(Locale.ROOT, "%.2f", eng.bayesState.qBoostQ))
                            : "")
                    ))
                    + (vetoReason != null ? (" veto=" + vetoReason) : "")
            );

            if (quantile) {
                System.out.println(MultiBinBayesFilter.diagStringQuantile(raw, bins, posterior, scale, eng.bayesState, BAYES_LAPLACE_ALPHA, BAYES_MIN_BIN_EVIDENCE));
            } else {
                System.out.println(MultiBinBayesFilter.diagString(bins, posterior, scale, eng.bayesState, BAYES_LAPLACE_ALPHA));
            }
        }

        BayesEvalResult out = new BayesEvalResult();
        out.bins = bins;
        out.raw = raw;
        out.posterior = posterior;
        out.scale = scale;
        out.gated = gated;
        out.boosted = boosted;
        out.evidenceTrades = evidenceTrades;
        out.usedFeatures = usedFeatures;
        out.mode = mode;
        return out;
    }




    /**
     * =====================================================================
     *  MultiBinBayesFilter —— 多档贝叶斯后验概率过滤器
     * =====================================================================
     *
     *  升级说明（相对二值版）：
     *  二值版把所有证据压成 true/false，丢弃了"有多强"的程度信息。
     *  例如 MACD hist=+0.5 和 hist=+20 在二值版里都是 "positive=true"，
     *  但它们对后续盈亏的预测力完全不同。
     *
     *  多 bin 版把每个连续指标切成 3~5 个区间（bin），
     *  分别统计每个 bin 在盈利/亏损交易中的出现频率，
     *  使贝叶斯能捕捉到"MACD hist 在 5~15 区间的盈利概率最高"这类非线性规律。
     *
     *  =====================================================================
     *  所有代码直接复制到 OkxSignalAlarmOnly.java 内部使用
     *  =====================================================================
     */

    // ===================== 配置常量（加到 OkxSignalAlarmOnly 静态常量区） =====================

    // static final boolean USE_BAYESIAN_FILTER   = sysBool("okx.useBayesian", true);
    // static final double  BAYES_MIN_POSTERIOR    = sysDouble("okx.bayesMinPosterior", 0.55);
    // static final double  BAYES_STRONG_POSTERIOR = sysDouble("okx.bayesStrongPosterior", 0.72);
    // static final double  BAYES_MEDIUM_POSTERIOR = sysDouble("okx.bayesMediumPosterior", 0.63);
    // static final boolean BAYES_VERBOSE          = sysBool("okx.bayesVerbose", true);
    // static final int     BAYES_LOOKBACK_TRADES  = sysInt("okx.bayesLookback", 200);
    // static final double  BAYES_LAPLACE_ALPHA    = sysDouble("okx.bayesAlpha", 1.0);
    // static final boolean BAYES_PRINT_LR_REPORT  = sysBool("okx.bayesPrintLrReport", true);

    static final class MultiBinBayesFilter {

        // =====================================================================
        //  第一部分：特征定义 —— 12 个维度，每个 3~5 个 bin
        // =====================================================================

        /**
         * 特征枚举 + 每个特征的 bin 数量
         *
         * bin 编号从 0 开始，每个特征的 bin 含义在 discretize() 中定义。
         * 设计原则：
         *  - bin 边界基于 ETH 30m K线的实际分布（4年回测观察值）
         *  - 尽量让每个 bin 在历史样本中都有足够计数（≥15笔）
         *  - 相邻 bin 之间的盈亏概率差异要有统计意义
         */
        enum Feature {
            // ---- 核心动量 ----
            MACD_HIST         (5),  // MACD 柱状图（30m）：反映短期动量强度
            MACD_DIF_ACCEL    (4),  // DIF 加速度 (k0.dif - k1.dif)：动量变化率
            MACD_1H_HIST      (4),  // 1H MACD hist：大级别动量方向

            // ---- 多指标投票强度 ----
            POOL2_SCORE       (4),  // Pool2 综合投票分数
            TSRND_SCORE       (4),  // TSRND 外层投票 score（0~7）
            TSRND_DIR_STRENGTH(4),  // TSRND 方向票强度（累积值）

            // ---- 市场结构 ----
            T1H_ALIGNMENT     (3),  // 1H 趋势与进场方向的对齐程度
            EMA_VOTE_SCORE    (4),  // EMA 多时间框架投票分数

            // ---- 波动率 / 空间 ----
            ATR_RELATIVE      (4),  // ATR / 入场价 的比率（波动率相对量）
            SR_GAP_RATIO      (3),  // 入场价到撑压线的距离 / ATR

            // ---- 价格结构 ----
            BODY_RATIO        (3),  // 信号K0 的实体比率（body/range）
            VOLUME_RATIO      (3);  // 当前K成交量 / 20K均量

            final int binCount;
            Feature(int binCount) { this.binCount = binCount; }
        }


        // =====================================================================
        //  第二部分：离散化函数 —— 连续值 → bin 编号
        // =====================================================================

        /**
         * 一次性计算所有特征的 bin 值
         *
         * 参数全部来自你现有变量，在进场决策点都已经算好了。
         * 每个参数对应代码里的具体位置已在注释中标注。
         *
         * @param macdHist30m     macd30.get(sigIdx).hist          —— 信号K0 的 MACD hist
         * @param difAccel        k0.dif - k1.dif                  —— DIF 加速度
         * @param macdHist1h      macd1h 对应 1H K 的 .hist        —— 1H MACD hist
         * @param pool2Score      pool2.score                       —— Pool2 投票总分
         * @param pool2Dir        pool2.dir                         —— Pool2 方向 (+1/-1/0)
         * @param tsrndScore      ov.score                          —— TSRND 外层投票总分
         * @param tsrndDirStr     ov.dirStrength                    —— TSRND 方向票强度
         * @param t1hTrend        t1hInfo.trend                     —— 1H 趋势枚举
         * @param side            进场方向                           —— Side.LONG / Side.SHORT
         * @param emaVoteScore    EmaVoteSnapshot.score             —— EMA 投票分数
         * @param atrValue        atrSig                            —— 信号K0 的 ATR 绝对值
         * @param entryPx         入场参考价                         —— sigC0.c
         * @param srGap           sr.resistance - entryPx (LONG)    —— 到撑压线的距离（点数）
         *                        或 entryPx - sr.support  (SHORT)
         * @param bodyRatio       Math.abs(sigC0.c-sigC0.o)/(sigC0.h-sigC0.l)
         * @param volumeRatio     sigC0.vol / sma20Vol              —— 量比
         */
        static Map<Feature, Integer> discretize(
                double macdHist30m,
                double difAccel,
                double macdHist1h,
                int    pool2Score,
                int    pool2Dir,
                int    tsrndScore,
                int    tsrndDirStr,
                Object t1hTrend,  // Trend1H 枚举
                Object side,      // Side 枚举
                double emaVoteScore,
                double atrValue,
                double entryPx,
                double srGap,
                double bodyRatio,
                double volumeRatio
        ) {
            Map<Feature, Integer> bins = new EnumMap<>(Feature.class);

            // ====== 1. MACD_HIST (5 bins) ======
            // ETH 30m 的 hist 典型范围约 -30 ~ +30，绝大多数在 -10 ~ +10
            // bin0: 强空 (<-8)  bin1: 弱空 (-8~0)  bin2: 弱多 (0~3)  bin3: 中多 (3~8)  bin4: 强多 (>8)
            bins.put(Feature.MACD_HIST, discretizeMacdHist(macdHist30m));

            // ====== 2. MACD_DIF_ACCEL (4 bins) ======
            // k0.dif - k1.dif：加速度通常在 -5 ~ +5 之间
            // bin0: 减速空 (<-1.5)  bin1: 微弱 (-1.5~0)  bin2: 微加速 (0~1.5)  bin3: 强加速 (>1.5)
            bins.put(Feature.MACD_DIF_ACCEL, discretizeDifAccel(difAccel));

            // ====== 3. MACD_1H_HIST (4 bins) ======
            // 1H hist 范围更大，约 -50 ~ +50
            // bin0: 强空 (<-10)  bin1: 弱空 (-10~0)  bin2: 弱多 (0~10)  bin3: 强多 (>10)
            bins.put(Feature.MACD_1H_HIST, discretizeMacd1hHist(macdHist1h));

            // ====== 4. POOL2_SCORE (4 bins) ======
            // Pool2 score 通常 0~14（你的 Pool2 有 8 个基础 + 5 个 COMBO5 + 1 个 FAILM8）
            // bin0: 弱 (<=3)  bin1: 中偏弱 (4~6)  bin2: 中偏强 (7~9)  bin3: 强 (>=10)
            bins.put(Feature.POOL2_SCORE, discretizePool2Score(pool2Score));

            // ====== 5. TSRND_SCORE (4 bins) ======
            // TSRND score 范围 0~7（2分方向 + 最多5分总体）
            // bin0: 极弱 (0~2)  bin1: 刚过线 (3~4)  bin2: 中强 (5)  bin3: 极强 (6~7)
            bins.put(Feature.TSRND_SCORE, discretizeTsrndScore(tsrndScore));

            // ====== 6. TSRND_DIR_STRENGTH (4 bins) ======
            // dirStrength 是 lookback 窗口内的累积值，可能 -50 ~ +50
            // 对做多来说，正值越大越有利；做空相反
            // 先按进场方向标准化为"有利度"
            boolean isLong = "LONG".equals(String.valueOf(side));
            int alignedStr = isLong ? tsrndDirStr : -tsrndDirStr;
            bins.put(Feature.TSRND_DIR_STRENGTH, discretizeAlignedStrength(alignedStr));

            // ====== 7. T1H_ALIGNMENT (3 bins) ======
            // bin0: 反向  bin1: 中性  bin2: 同向
            bins.put(Feature.T1H_ALIGNMENT, discretizeT1hAlignment(t1hTrend, side));

            // ====== 8. EMA_VOTE_SCORE (4 bins) ======
            // EMA score 范围约 -2.4 ~ +2.4（3个时间框架 × (base+strong)=0.8）
            // 按进场方向标准化
            double alignedEma = isLong ? emaVoteScore : -emaVoteScore;
            bins.put(Feature.EMA_VOTE_SCORE, discretizeEmaScore(alignedEma));

            // ====== 9. ATR_RELATIVE (4 bins) ======
            // atrValue / entryPx：ETH 在 2000~4000 区间，30m ATR 通常 5~50 点
            // ratio 大约 0.001 ~ 0.025
            double atrRatio = (entryPx > 0) ? atrValue / entryPx : 0.0;
            bins.put(Feature.ATR_RELATIVE, discretizeAtrRatio(atrRatio));

            // ====== 10. SR_GAP_RATIO (3 bins) ======
            // 到撑压线的距离 / ATR：表示"上方空间有多少个ATR"
            double gapRatio = (atrValue > 0) ? srGap / atrValue : 0.0;
            bins.put(Feature.SR_GAP_RATIO, discretizeSrGapRatio(gapRatio));

            // ====== 11. BODY_RATIO (3 bins) ======
            // 实体占比 0~1。趋势系统偏好大实体K线
            bins.put(Feature.BODY_RATIO, discretizeBodyRatio(bodyRatio));

            // ====== 12. VOLUME_RATIO (3 bins) ======
            // 当前K成交量 / 20K均量
            bins.put(Feature.VOLUME_RATIO, discretizeVolumeRatio(volumeRatio));

            return bins;
        }

        // ---- 各特征的离散化实现 ----

        /** MACD_HIST: 5 bins */
        static int discretizeMacdHist(double hist) {
            if (hist < -8.0) return 0;       // 强空
            if (hist < 0.0)  return 1;       // 弱空
            if (hist < 3.0)  return 2;       // 弱多（刚转正/接近零轴）
            if (hist < 8.0)  return 3;       // 中多
            return 4;                        // 强多
        }

        /** MACD_DIF_ACCEL: 4 bins */
        static int discretizeDifAccel(double accel) {
            if (accel < -1.5) return 0;      // 显著减速
            if (accel < 0.0)  return 1;      // 微减速
            if (accel < 1.5)  return 2;      // 微加速
            return 3;                        // 显著加速
        }

        /** MACD_1H_HIST: 4 bins */
        static int discretizeMacd1hHist(double hist) {
            if (Double.isNaN(hist)) return 1;  // 缺失当中性偏空处理
            if (hist < -10.0) return 0;
            if (hist < 0.0)   return 1;
            if (hist < 10.0)  return 2;
            return 3;
        }

        /** POOL2_SCORE: 4 bins */
        static int discretizePool2Score(int score) {
            if (score <= 3) return 0;        // 弱
            if (score <= 6) return 1;        // 中偏弱
            if (score <= 9) return 2;        // 中偏强
            return 3;                        // 强
        }

        /** TSRND_SCORE: 4 bins (0~7) */
        static int discretizeTsrndScore(int score) {
            if (score <= 2) return 0;        // 极弱（连方向票都没过）
            if (score <= 4) return 1;        // 刚过线（方向+部分总体）
            if (score == 5) return 2;        // 中强
            return 3;                        // 极强（6~7，多套总体都过）
        }

        /** TSRND_DIR_STRENGTH (已按方向标准化): 4 bins */
        static int discretizeAlignedStrength(int alignedStr) {
            if (alignedStr < -10) return 0;  // 强反向
            if (alignedStr < 0)   return 1;  // 弱反向
            if (alignedStr < 15)  return 2;  // 弱顺向
            return 3;                        // 强顺向
        }

        /** T1H_ALIGNMENT: 3 bins */
        static int discretizeT1hAlignment(Object t1hTrend, Object side) {
            // Trend1H: 多, 空, 中性
            // Side: LONG, SHORT
            String trend = String.valueOf(t1hTrend);
            boolean isLong = "LONG".equals(String.valueOf(side));

            if ("中性".equals(trend)) return 1;          // 中性
            if (isLong && "多".equals(trend)) return 2;   // 同向
            if (!isLong && "空".equals(trend)) return 2;  // 同向
            return 0;                                     // 反向
        }

        /** EMA_VOTE_SCORE (已按方向标准化): 4 bins */
        static int discretizeEmaScore(double aligned) {
            // 范围约 -2.4 ~ +2.4
            if (aligned < -0.5) return 0;    // 强反向
            if (aligned < 0.3)  return 1;    // 中性偏弱
            if (aligned < 1.2)  return 2;    // 中等顺向
            return 3;                        // 强顺向
        }

        /** ATR_RELATIVE: 4 bins */
        static int discretizeAtrRatio(double ratio) {
            // ETH 30m ATR/price 典型分布
            if (ratio < 0.002) return 0;     // 极低波动（不适合趋势）
            if (ratio < 0.005) return 1;     // 低波动
            if (ratio < 0.012) return 2;     // 正常波动
            return 3;                        // 高波动
        }

        /** SR_GAP_RATIO: 3 bins */
        static int discretizeSrGapRatio(double ratio) {
            if (ratio < 1.0) return 0;       // 空间不足（< 1个ATR）
            if (ratio < 2.5) return 1;       // 空间适中
            return 2;                        // 空间充足
        }

        /** BODY_RATIO: 3 bins */
        static int discretizeBodyRatio(double ratio) {
            if (ratio < 0.3) return 0;       // 十字星/小实体
            if (ratio < 0.65) return 1;      // 中等实体
            return 2;                        // 大实体
        }

        /** VOLUME_RATIO: 3 bins */
        static int discretizeVolumeRatio(double ratio) {
            if (ratio < 0.7) return 0;       // 缩量
            if (ratio < 1.5) return 1;       // 正常量
            return 2;                        // 放量
        }


        // =====================================================================
        //  第三部分：贝叶斯状态 + 核心计算
        // =====================================================================

        /**
         * 进场快照：记录进场时所有特征的 bin 值，出场时用于更新统计
         */
        static class BayesEntrySnapshot {
            final long entryTs;
            final Map<Feature, Integer> bins;                 // 进场时按“当时模型”离散化得到的 bins（用于 legacy / 诊断）
            final EnumMap<Feature, Double> raw;               // 进场时锁定的原始特征值（用于 quantile 重新拟合分箱）
            boolean isWin;
            double pnlPoints;

            double rMultiple = Double.NaN;
            BayesEntrySnapshot(long entryTs, Map<Feature, Integer> bins) {
                this(entryTs, bins, null);
            }

            BayesEntrySnapshot(long entryTs, Map<Feature, Integer> bins, EnumMap<Feature, Double> raw) {
                this.entryTs = entryTs;
                this.bins = bins;
                this.raw = raw;
            }
        }

        /**
         * 贝叶斯统计状态
         *
         * 核心数据结构：对每个 Feature 的每个 bin，
         * 分别统计它在"盈利交易"和"亏损交易"中出现了多少次。
         *
         * 内存占用极小：12 个特征 × 平均 4 bins × 2 类 = ~96 个 int
         */
        static class BayesState {
            int totalWins = 0;
            int totalLosses = 0;

            /**
             * counts[feature][bin][0] = 该 bin 在盈利交易中出现的次数
             * counts[feature][bin][1] = 该 bin 在亏损交易中出现的次数
             */
            final Map<Feature, int[][]> counts = new EnumMap<>(Feature.class);

            /** 滚动窗口（FIFO） */
            final Deque<BayesEntrySnapshot> history = new ArrayDeque<>();

            // ===== quantile 模型（滚动分位数分箱）=====
            volatile boolean qDirty = true;   // 样本有变化，模型需要重建
            volatile int qRefitCount = 0;     // 重建次数（用于报告节流）
            volatile int qNewSinceRefit = 0;  // 自上次重建后新增样本数（达到阈值才重建）

            // 最近一次重建后的统计（仅 quantile 方案使用）
            volatile int qEvidenceN = 0;
            volatile int qWins = 0;
            volatile int qLosses = 0;

            // 每个特征的分箱边界与计数（动态K：3/2/1）
            final Map<Feature, double[]> qEdges = new EnumMap<>(Feature.class);      // edges: len=(k-1)，k<=5
            final Map<Feature, Double>   qWinsorLo = new EnumMap<>(Feature.class);
            final Map<Feature, Double>   qWinsorHi = new EnumMap<>(Feature.class);
            final Map<Feature, Integer>  qK = new EnumMap<>(Feature.class);          // effective k（1/2/3/5）
            final Map<Feature, int[][]>  qCounts = new EnumMap<>(Feature.class);     // counts[bin][0=win,1=lose]

            // ===== P3：posterior 分位数阈值缓存（在 refit 时重算，避免每tick排序）=====
            volatile double[] qPosteriorSorted = null;  // 最近N笔样本的 posterior（按当前模型重算后排序）
            volatile double   qGateThr = Double.NaN;
            volatile double   qBoostThr = Double.NaN;
            volatile double   qGateQ = Double.NaN;
            volatile double   qBoostQ = Double.NaN;


            // ===== P5：posterior 分层校验（AvgR 分桶）缓存（用于 Gate by AvgR）=====
            // 由 printBayesReport() 在输出 [BAYES-CALIB] 时写入，仅使用已完成交易样本（无未来函数）
            volatile double[] calibPostHi = null; // 每桶 posterior 上界（升序，len=buckets）
            volatile double[] calibAvgR   = null; // 每桶 AvgR（len=buckets）
            volatile int[]    calibCnt    = null; // 每桶样本数（len=buckets）
            volatile int      calibTotalN = 0;    // 本次校验总样本数
            volatile int      calibBuckets= 0;    // 桶数
            volatile int      calibRefitId= 0;    // 对应的 qRefitCount（仅诊断）
            // 每个特征的“最大区分度”缓存：max|logLR|（用于过滤噪声特征）
            final Map<Feature, Double> qMaxAbsLogLR = new EnumMap<>(Feature.class);

            // ===== OPT-3: Isotonic 校准模型（PAVA：raw_posterior → calibrated_probability）=====
            volatile double[] isoTrainX = null;  // 排序后的 raw posterior（升序）
            volatile double[] isoTrainY = null;  // PAVA 校准后的 calibrated probability
            volatile int isoN = 0;               // 校准样本数

            // ===== OPT-5: 互信息（MI）特征选择缓存 =====
            final Map<Feature, Double> qMutualInfo = new EnumMap<>(Feature.class);

            // ===== OPT-11: 特征相关性矩阵缓存（用于相关性惩罚）=====
            volatile double[][] qFeatureCorrMatrix = null;  // [i][j] = |corr(feature_i, feature_j)|

            // ===== OPT-4: Kelly 参数缓存（从历史推算真实盈亏比）=====
            volatile double kellyAvgWinR = 1.2;
            volatile double kellyAvgLossR = 1.0;

            BayesState() {
                for (Feature f : Feature.values()) {
                    counts.put(f, new int[f.binCount][2]); // [bin][0=win, 1=lose]
                }
            }

            /** 深拷贝（用于回测并行场景，避免共享状态） */
            BayesState deepCopy() {
                BayesState copy = new BayesState();
                copy.totalWins = this.totalWins;
                copy.totalLosses = this.totalLosses;
                for (Feature f : Feature.values()) {
                    int[][] src = this.counts.get(f);
                    int[][] dst = copy.counts.get(f);
                    for (int b = 0; b < f.binCount; b++) {
                        dst[b][0] = src[b][0];
                        dst[b][1] = src[b][1];
                    }
                }
                // 注意：history 不深拷贝（滚动窗口只在主线程操作）
                return copy;
            }
        }



        // =====================================================================
        //  quantile 稳健分箱（滚动最近N笔交易拟合）—— 只用于 GATE/BOOST（避免压复利）
        // =====================================================================

        // quantile 方案优先使用的少量特征（低相关、可解释、避免重复证据）
        static final Feature[] Q_FEATURES = new Feature[] {
                Feature.ATR_RELATIVE,
                // MACD_HIST 已移除:诊断显示其 logLR 跨度仅 0.139(接近废票),
                // 删除后样本内 t 微升(2.969→2.984)、样本外逐笔 R 零差异。
                // 注意:此处仅移除贝叶斯特征用途;MACD-BOOST 加仓规则(MACD_HIST>5→×1.5)不受影响。
                Feature.TSRND_DIR_STRENGTH,
                Feature.SR_GAP_RATIO,
                Feature.BODY_RATIO,
                Feature.VOLUME_RATIO,
                Feature.EMA_VOTE_SCORE
        };

        static String qFamilyOf(Feature f) {
            if (f == null) return "OTHER";
            switch (f) {
                case ATR_RELATIVE: return "VOL";
                case VOLUME_RATIO: return "VOLUME";
                case MACD_HIST:
                case MACD_DIF_ACCEL:
                case MACD_1H_HIST: return "MOM";
                case TSRND_DIR_STRENGTH:
                case TSRND_SCORE:
                case POOL2_SCORE:
                case EMA_VOTE_SCORE: return "VOTE";
                case SR_GAP_RATIO:
                case T1H_ALIGNMENT: return "STRUCT";
                case BODY_RATIO: return "CANDLE";
                default: return "OTHER";
            }
        }


        // ===== P4：极弱证据 veto（只针对白名单特征；LR<阈值 且 bin样本足够 → 直接禁入）=====
        static final EnumSet<Feature> VETO_SET = parseVetoFeatures(BAYES_VETO_FEATURES);

        static EnumSet<Feature> parseVetoFeatures(String s) {
            EnumSet<Feature> set = EnumSet.noneOf(Feature.class);
            try {
                if (s == null || s.trim().isEmpty()) return set;
                String[] parts = s.split("[,;\s]+");
                for (String p : parts) {
                    if (p == null) continue;
                    String name = p.trim();
                    if (name.isEmpty()) continue;
                    try {
                        set.add(Feature.valueOf(name));
                    } catch (Throwable ignore) {
                        // ignore unknown
                    }
                }
            } catch (Throwable ignore) {}
            return set;
        }

        static String checkWeakVetoQuantile(Map<Feature, Integer> bins, BayesState state, double alpha, int minBinEvidence) {
            if (!BAYES_WEAK_VETO_ENABLED) return null;
            if (bins == null || bins.isEmpty() || state == null) return null;
            if (VETO_SET == null || VETO_SET.isEmpty()) return null;

            int W = Math.max(1, state.qWins);
            int L = Math.max(1, state.qLosses);

            double bestLr = Double.POSITIVE_INFINITY;
            String best = null;

            for (Feature f : VETO_SET) {
                // OPT-5: MI-based noise filter (fallback to max|logLR|)
                if (BAYES_USE_MI_SELECTION) {
                    Double mi = state.qMutualInfo.get(f);
                    if (mi != null && mi < BAYES_MI_THRESHOLD) continue;
                } else if (BAYES_MIN_MAX_ABS_LOGLR > 0) {
                    Double mm = state.qMaxAbsLogLR.get(f);
                    if (mm != null && mm < BAYES_MIN_MAX_ABS_LOGLR) continue;
                }

                Integer bin = bins.get(f);
                if (bin == null) continue;
                int[][] fc = state.qCounts.get(f);
                if (fc == null) continue;
                if (bin < 0 || bin >= fc.length) continue;

                int tot = fc[bin][0] + fc[bin][1];
                if (tot < Math.max(1, minBinEvidence)) continue; // 证据不足不 veto

                int nBins = Math.max(1, state.qK.getOrDefault(f, fc.length));
                double pW = (fc[bin][0] + alpha) / (W + nBins * alpha);
                double pL = (fc[bin][1] + alpha) / (L + nBins * alpha);
                double lr = (pL > 1e-12 ? pW / pL : 99.0);

                if (lr < BAYES_VETO_LR_THRESHOLD && lr < bestLr) {
                    bestLr = lr;
                    best = f.name() + ":bin" + bin + " LR=" + String.format(Locale.US, "%.2f", lr) + " tot=" + tot;
                }
            }
            return best;
        }


        static boolean isCalibReady(BayesState state) {
            if (!BAYES_GATE_BY_CALIB_AVGR) return false;
            if (state == null) return false;
            if (state.calibPostHi == null || state.calibAvgR == null || state.calibCnt == null) return false;
            if (state.calibPostHi.length == 0) return false;
            if (state.calibTotalN < Math.max(0, BAYES_CALIB_MIN_N)) return false;
            return true;
        }



        static String checkCalibAvgRGate(BayesState state, double posterior) {
            if (!isCalibReady(state)) return null;

            int buckets = state.calibPostHi.length;
            int bi = 0;
            while (bi < buckets - 1 && posterior > state.calibPostHi[bi]) bi++;

            int cnt = (bi < state.calibCnt.length ? state.calibCnt[bi] : 0);
            if (cnt < Math.max(1, BAYES_CALIB_MIN_BUCKET_N)) return null;

            double avgR = (bi < state.calibAvgR.length ? state.calibAvgR[bi] : Double.NaN);
            if (!Double.isFinite(avgR)) return null;

            if (avgR <= BAYES_CALIB_GATE_R_THR) {
                double hi = state.calibPostHi[bi];
                return String.format(Locale.US,
                        "CALIB_AVGR bucket=%d/%d avgR=%.4f<=thr%.4f n=%d post<=%.4f",
                        (bi + 1), buckets, avgR, BAYES_CALIB_GATE_R_THR, cnt, hi);
            }
            return null;
        }


        static double clamp(double v, double lo, double hi) {
            if (Double.isNaN(v)) return v;
            if (v < lo) return lo;
            if (v > hi) return hi;
            return v;
        }

        static double percentileSorted(double[] sorted, double p) {
            if (sorted == null || sorted.length == 0) return 0.0;
            if (p <= 0) return sorted[0];
            if (p >= 1) return sorted[sorted.length - 1];
            double pos = p * (sorted.length - 1);
            int i = (int) Math.floor(pos);
            int j = (int) Math.ceil(pos);
            if (i == j) return sorted[i];
            double a = sorted[i];
            double b = sorted[j];
            double t = pos - i;
            return a + (b - a) * t;
        }

        static double[] computeEdgesByK(double[] sortedWinsor, int k) {
            if (k <= 1) return new double[0];
            if (sortedWinsor == null || sortedWinsor.length == 0) return new double[0];

            if (k == 2) {
                return new double[] { percentileSorted(sortedWinsor, 0.50) };
            }
            if (k == 3) {
                return new double[] {
                        percentileSorted(sortedWinsor, 1.0 / 3.0),
                        percentileSorted(sortedWinsor, 2.0 / 3.0)
                };
            }
            // k==5
            return new double[] {
                    percentileSorted(sortedWinsor, 0.20),
                    percentileSorted(sortedWinsor, 0.40),
                    percentileSorted(sortedWinsor, 0.60),
                    percentileSorted(sortedWinsor, 0.80)
            };
        }

        static int binByEdges(double v, double[] edges) {
            if (edges == null || edges.length == 0) return 0;
            for (int i = 0; i < edges.length; i++) {
                if (v <= edges[i]) return i;
            }
            return edges.length;
        }

        static int quantileEvidenceTrades(BayesState state) {
            if (state == null || state.history == null) return 0;
            int n = 0;
            for (BayesEntrySnapshot s : state.history) {
                if (s == null || s.raw == null) continue;
                if (!s.raw.isEmpty()) n++;
            }
            return n;
        }

        /**
         * 量化分箱模型重建：只使用最近N笔“已完成交易”的 raw 特征（无未来函数）
         * - 分位数分箱（3/5 档，默认 3）
         * - 最小样本约束：bin 太稀疏则自动降级 K（5→3→2→1）
         * - winsorize：拟合边界前先做截尾（默认 1%）
         */
        static void ensureQuantileModel(BayesState state,
                                        int lookbackTrades,
                                        int desiredK,
                                        int minBinTrades,
                                        double winsorPct,
                                        int refitEvery,
                                        int reportEveryRefit,
                                        double alpha,
                                        int minBinEvidence) {
            if (state == null) return;

            // 未开启 Bayes 时也可能调用（安全兜底）
            if (state.history == null) return;

            // 只有模型过期且满足“新增样本阈值”才重建（避免每tick排序）
            if (!state.qDirty) return;
            if (state.qRefitCount > 0 && state.qNewSinceRefit < Math.max(1, refitEvery)) return;

            // 仅使用最近 lookbackTrades（updateOnTradeComplete 已经裁剪，但再保险）
            while (state.history.size() > Math.max(1, lookbackTrades)) state.history.pollFirst();

            // 收集有效样本（raw 不为空）
            ArrayList<BayesEntrySnapshot> samples = new ArrayList<>();
            int W = 0, L = 0;
            for (BayesEntrySnapshot s : state.history) {
                if (s == null || s.raw == null || s.raw.isEmpty()) continue;
                samples.add(s);
                if (s.isWin) W++; else L++;
            }

            state.qEvidenceN = samples.size();
            state.qWins = W;
            state.qLosses = L;

            // 样本太少：建一个“空模型”（所有特征K=1），posterior 一律 0.5（由 computePosteriorQuantile 再兜底）
            if (samples.size() < 30 || W < 3 || L < 3) {
                for (Feature f : Q_FEATURES) {
                    state.qK.put(f, 1);
                    state.qEdges.put(f, new double[0]);
                    state.qCounts.put(f, new int[1][2]);
                    state.qWinsorLo.put(f, Double.NEGATIVE_INFINITY);
                    state.qWinsorHi.put(f, Double.POSITIVE_INFINITY);
                }
                state.qPosteriorSorted = null;
                state.qGateThr = Double.NaN;
                state.qBoostThr = Double.NaN;
                state.qGateQ = Double.NaN;
                state.qBoostQ = Double.NaN;
                // 样本不足：清空 calib 缓存（GateByAvgR 将回退到 posterior gate 或不 gate）
                state.calibPostHi = null;
                state.calibAvgR = null;
                state.calibCnt = null;
                state.calibTotalN = samples.size();
                state.calibBuckets = 0;
                state.calibRefitId = state.qRefitCount;

                state.qDirty = false;
                state.qNewSinceRefit = 0;
                state.qRefitCount++;
                return;
            }

            int k0 = (desiredK == 5 ? 5 : 3);

            // 为每个特征分别拟合边界（各特征分布不同）
            for (Feature f : Q_FEATURES) {
                ArrayList<Double> vals = new ArrayList<>();
                for (BayesEntrySnapshot s : samples) {
                    Double v = (s.raw == null ? null : s.raw.get(f));
                    if (v == null) continue;
                    double x = v;
                    if (Double.isNaN(x) || Double.isInfinite(x)) continue;
                    vals.add(x);
                }

                if (vals.size() < 20) {
                    // 有效值太少：降级到 1 bin
                    state.qK.put(f, 1);
                    state.qEdges.put(f, new double[0]);
                    state.qCounts.put(f, new int[1][2]);
                    state.qWinsorLo.put(f, Double.NEGATIVE_INFINITY);
                    state.qWinsorHi.put(f, Double.POSITIVE_INFINITY);
                    continue;
                }

                double[] arr = new double[vals.size()];
                for (int i = 0; i < vals.size(); i++) arr[i] = vals.get(i);
                Arrays.sort(arr);

                double lo = percentileSorted(arr, Math.max(0.0, Math.min(0.49, winsorPct)));
                double hi = percentileSorted(arr, Math.max(0.51, Math.min(1.0, 1.0 - winsorPct)));
                if (lo > hi) { double t = lo; lo = hi; hi = t; }

                // winsorize（在排序数组上直接截尾，仍保持有序）
                for (int i = 0; i < arr.length; i++) arr[i] = clamp(arr[i], lo, hi);

                // 候选K：优先 k0，然后逐级降级
                int[] candidates = (k0 == 5 ? new int[]{5, 3, 2, 1} : new int[]{3, 2, 1});

                int chosenK = 1;
                double[] chosenEdges = new double[0];
                int[][] chosenCounts = new int[1][2];

                for (int candK : candidates) {
                    double[] edges = computeEdgesByK(arr, candK);
                    int[][] fc = new int[Math.max(1, candK)][2];

                    for (BayesEntrySnapshot s : samples) {
                        Double vv = (s.raw == null ? null : s.raw.get(f));
                        if (vv == null) continue;
                        double x = vv;
                        if (Double.isNaN(x) || Double.isInfinite(x)) continue;

                        x = clamp(x, lo, hi);
                        int bin = binByEdges(x, edges);
                        int col = (s.isWin ? 0 : 1);
                        fc[bin][col]++;
                    }

                    boolean ok = true;
                    if (candK > 1) {
                        for (int b = 0; b < candK; b++) {
                            int tot = fc[b][0] + fc[b][1];
                            if (tot < Math.max(1, minBinTrades)) { ok = false; break; }
                        }
                    }
                    if (ok) {
                        chosenK = candK;
                        chosenEdges = edges;
                        chosenCounts = fc;
                        break;
                    }
                }

                state.qK.put(f, chosenK);
                state.qEdges.put(f, chosenEdges);
                state.qCounts.put(f, chosenCounts);
                state.qWinsorLo.put(f, lo);
                state.qWinsorHi.put(f, hi);
                // 计算该特征的最大区分度 max|logLR|（用于过滤噪声特征，例如 VOLUME_RATIO 常见接近 0）
                try {
                    int[][] fc2 = chosenCounts;
                    int kEff = Math.max(1, chosenK);
                    int W2 = 0, L2 = 0;
                    for (int bb = 0; bb < fc2.length; bb++) { W2 += fc2[bb][0]; L2 += fc2[bb][1]; }
                    W2 = Math.max(1, W2);
                    L2 = Math.max(1, L2);
                    double maxAbs = 0.0;
                    for (int bb = 0; bb < kEff && bb < fc2.length; bb++) {
                        double pW = (fc2[bb][0] + BAYES_LAPLACE_ALPHA) / (W2 + kEff * BAYES_LAPLACE_ALPHA);
                        double pL = (fc2[bb][1] + BAYES_LAPLACE_ALPHA) / (L2 + kEff * BAYES_LAPLACE_ALPHA);
                        double lr = (pL > 1e-12 ? pW / pL : 99.0);
                        double al = Math.abs(Math.log(Math.max(1e-12, lr)));
                        if (al > maxAbs) maxAbs = al;
                    }
                    state.qMaxAbsLogLR.put(f, maxAbs);

                    // OPT-5: 计算互信息 MI(feature; win/loss)
                    if (BAYES_USE_MI_SELECTION) {
                        double mi = computeMutualInfo(fc2, W2, L2);
                        state.qMutualInfo.put(f, mi);
                    }
                } catch (Throwable ignore) {}

            }

            // OPT-11: 计算特征相关性矩阵（用于 correlationPenalty）
            if (BAYES_USE_CORR_PENALTY) {
                try {
                    computeCorrelationMatrix(state, samples);
                } catch (Throwable ignore) {}
            }

            state.qDirty = false;
            state.qNewSinceRefit = 0;
            state.qRefitCount++;

            // ===== P5：posterior 分层校验缓存（AvgR 分桶，用于 Gate by AvgR）=====
            // 说明：GateByAvgR 不要求 posterior 单调；只根据“当前 posterior 落入的桶”对应 AvgR 决策。
            // 这里在每次 refit 后重算，避免仅靠 printBayesReport() 才更新导致“阈值/桶漂移”。
            if (BAYES_GATE_BY_CALIB_AVGR) {
                try {
                    int buckets = Math.max(2, BAYES_CALIB_BUCKETS);
                    ArrayList<double[]> rws = new ArrayList<>(); // [posterior, rMultiple]
                    for (BayesEntrySnapshot s : samples) {
                        if (s == null || s.raw == null || s.raw.isEmpty()) continue;
                        Map<Feature, Integer> bb = discretizeQuantile(s.raw, state);
                        double post = computePosteriorQuantile(bb, state, alpha, minBinEvidence);
                        rws.add(new double[]{post, s.rMultiple});
                    }
                    rws.sort(Comparator.comparingDouble(a -> a[0]));
                    double[] postHi = new double[buckets];
                    double[] avgR = new double[buckets];
                    int[] cnt = new int[buckets];
                    Arrays.fill(postHi, Double.NaN);
                    Arrays.fill(avgR, Double.NaN);
                    Arrays.fill(cnt, 0);

                    for (int bi = 0; bi < buckets; bi++) {
                        int from = (int) Math.floor((double) bi * rws.size() / buckets);
                        int to = (int) Math.floor((double) (bi + 1) * rws.size() / buckets);
                        if (to <= from) continue;

                        double sumR = 0.0;
                        int rCnt = 0;
                        for (int i = from; i < to; i++) {
                            double rr = rws.get(i)[1];
                            if (Double.isFinite(rr)) { sumR += rr; rCnt++; }
                        }

                        postHi[bi] = rws.get(to - 1)[0];               // 桶上界
                        avgR[bi] = (rCnt > 0 ? (sumR / rCnt) : Double.NaN); // AvgR（仅用有效 rMultiple）
                        cnt[bi] = rCnt;                               // AvgR 的有效样本数
                    }

                    state.calibPostHi = postHi;
                    state.calibAvgR = avgR;
                    state.calibCnt = cnt;
                    state.calibTotalN = rws.size();
                    state.calibBuckets = buckets;
                    state.calibRefitId = state.qRefitCount;
                } catch (Throwable __cal) {
                    // 失败时清空，宁可回退到 posterior gate
                    state.calibPostHi = null;
                    state.calibAvgR = null;
                    state.calibCnt = null;
                    state.calibTotalN = 0;
                    state.calibBuckets = 0;
                    state.calibRefitId = state.qRefitCount;
                }
            }

            // ===== P3：posterior 分位数阈值（用最近N笔 posterior 的分布自适应）=====
            if (BAYES_USE_POSTERIOR_QUANTILE_THR) {
                try {
                    double[] posts = new double[samples.size()];
                    int pi = 0;
                    for (BayesEntrySnapshot s : samples) {
                        if (s == null || s.raw == null || s.raw.isEmpty()) continue;
                        Map<Feature, Integer> bb = discretizeQuantile(s.raw, state);
                        double post = computePosteriorQuantile(bb, state, alpha, minBinEvidence);
                        posts[pi++] = post;
                    }
                    if (pi > 0) {
                        if (pi != posts.length) posts = Arrays.copyOf(posts, pi);
                        Arrays.sort(posts);
                        state.qPosteriorSorted = posts;

                        double gq = Math.max(0.0, Math.min(0.49, BAYES_GATE_QUANTILE));
                        double bq = Math.max(0.51, Math.min(1.0, BAYES_BOOST_QUANTILE));
                        double gThr = percentileSorted(posts, gq);
                        double bThr = percentileSorted(posts, bq);

                        // 硬保护：gate/boost 阈值不要挤在一起
                        double minGap = Math.max(0.0, BAYES_THR_MIN_GAP);
                        if (bThr - gThr < minGap) {
                            double mid = (gThr + bThr) * 0.5;
                            gThr = mid - minGap * 0.5;
                            bThr = mid + minGap * 0.5;
                            gThr = Math.max(0.0, Math.min(1.0, gThr));
                            bThr = Math.max(0.0, Math.min(1.0, bThr));
                        }

                        state.qGateThr = gThr;
                        state.qBoostThr = bThr;
                        state.qGateQ = gq;
                        state.qBoostQ = bq;
                    } else {
                        state.qPosteriorSorted = null;
                        state.qGateThr = Double.NaN;
                        state.qBoostThr = Double.NaN;
                        state.qGateQ = Double.NaN;
                        state.qBoostQ = Double.NaN;
                    }
                } catch (Throwable __thr) {
                    state.qPosteriorSorted = null;
                    state.qGateThr = Double.NaN;
                    state.qBoostThr = Double.NaN;
                    state.qGateQ = Double.NaN;
                    state.qBoostQ = Double.NaN;
                }
            } else {
                state.qPosteriorSorted = null;
                state.qGateThr = Double.NaN;
                state.qBoostThr = Double.NaN;
                state.qGateQ = Double.NaN;
                state.qBoostQ = Double.NaN;
            }


            // 可选：每次 refit 给一个“短摘要”，方便你快速看出模型是否有效
            // ===== OPT-3: Isotonic calibration (PAVA) =====
            if (BAYES_USE_ISOTONIC_CALIB) {
                try {
                    ArrayList<double[]> isoPairs = new ArrayList<>();
                    for (BayesEntrySnapshot s : samples) {
                        if (s == null || s.raw == null || s.raw.isEmpty()) continue;
                        Map<Feature, Integer> bb = discretizeQuantile(s.raw, state);
                        double rawPost = computePosteriorQuantile(bb, state, alpha, minBinEvidence);
                        isoPairs.add(new double[]{rawPost, s.isWin ? 1.0 : 0.0});
                    }
                    fitIsotonicPAVA(state, isoPairs);
                } catch (Throwable __iso) {
                    state.isoTrainX = null;
                    state.isoTrainY = null;
                    state.isoN = 0;
                }
            }

            // ===== OPT-4: Kelly params from real history =====
            if (BAYES_USE_KELLY_SIZING) {
                try {
                    double sumWinR = 0; int cntWin = 0;
                    double sumLossR = 0; int cntLoss = 0;
                    for (BayesEntrySnapshot s : samples) {
                        if (s == null) continue;
                        double r = s.rMultiple;
                        if (!Double.isFinite(r)) continue;
                        if (s.isWin && r > 0) { sumWinR += r; cntWin++; }
                        else if (!s.isWin && r < 0) { sumLossR += Math.abs(r); cntLoss++; }
                    }
                    state.kellyAvgWinR = (cntWin >= 10 ? sumWinR / cntWin : BAYES_AVG_WIN_R);
                    state.kellyAvgLossR = (cntLoss >= 10 ? sumLossR / cntLoss : BAYES_AVG_LOSS_R);
                } catch (Throwable __k) {
                    state.kellyAvgWinR = BAYES_AVG_WIN_R;
                    state.kellyAvgLossR = BAYES_AVG_LOSS_R;
                }
            }

            if (reportEveryRefit > 0 && (state.qRefitCount % reportEveryRefit == 0)) {
                System.out.printf(Locale.US, "%n[BAYES-REFIT] scheme=quantile N=%d W=%d L=%d prior=%.3f k=%d minBinTrades=%d minBinEv=%d alpha=%.2f%s%s%n",
                        state.qEvidenceN, state.qWins, state.qLosses,
                        (state.qEvidenceN > 0 ? (double) state.qWins / state.qEvidenceN : 0.5),
                        k0, minBinTrades, minBinEvidence, alpha,
                        (BAYES_USE_ISOTONIC_CALIB ? " isoN=" + state.isoN : ""),
                        (BAYES_USE_KELLY_SIZING ? String.format(Locale.US, " kellyWR=%.2f/%.2f", state.kellyAvgWinR, state.kellyAvgLossR) : ""));
            }
        }

        static Map<Feature, Integer> discretizeQuantile(EnumMap<Feature, Double> raw, BayesState state) {
            EnumMap<Feature, Integer> bins = new EnumMap<>(Feature.class);
            if (raw == null || state == null) return bins;

            // OPT-11: 当启用相关性惩罚时，不使用粗暴的 FamilyCap（由相关性惩罚替代）
            boolean useFamilyCap = !BAYES_USE_CORR_PENALTY;
            int cap = Math.max(1, BAYES_FAMILY_CAP);
            HashMap<String, Integer> famCnt = new HashMap<>();

            for (Feature f : Q_FEATURES) {
                Double vv = raw.get(f);
                if (vv == null) continue;
                double x = vv;
                if (Double.isNaN(x) || Double.isInfinite(x)) continue;

                // FamilyCap 仅在未启用相关性惩罚时使用
                if (useFamilyCap) {
                    String fam = qFamilyOf(f);
                    int used = famCnt.getOrDefault(fam, 0);
                    if (used >= cap) continue;
                    famCnt.put(fam, used + 1);
                }

                // OPT-5: 优先使用互信息（MI）过滤噪声特征，回退到 max|logLR|
                if (BAYES_USE_MI_SELECTION) {
                    Double mi = state.qMutualInfo.get(f);
                    if (mi != null && mi < BAYES_MI_THRESHOLD) continue;
                } else if (BAYES_MIN_MAX_ABS_LOGLR > 0) {
                    // 旧逻辑：max|logLR| 过滤
                    Double mm = state.qMaxAbsLogLR.get(f);
                    if (mm != null && mm < BAYES_MIN_MAX_ABS_LOGLR) continue;
                }

                double lo = state.qWinsorLo.getOrDefault(f, Double.NEGATIVE_INFINITY);
                double hi = state.qWinsorHi.getOrDefault(f, Double.POSITIVE_INFINITY);
                x = clamp(x, lo, hi);

                double[] edges = state.qEdges.get(f);
                int bin = binByEdges(x, edges);
                bins.put(f, bin);
            }
            return bins;
        }


        // =============================================================================
        //  OPT-2: Beta-Binomial 自适应权重（替代硬 minBinEvidence 截断）
        //  Beta(α₀ + wins_in_bin, β₀ + losses_in_bin) → Var → confidence weight
        //  weight = max(0, 1 - 2*sqrt(variance))
        //  α₀=β₀=0.5 (Jeffrey's prior)
        // =============================================================================
        static double betaAdaptiveWeight(int winsInBin, int lossesInBin) {
            double a0 = 0.5; // Jeffrey's prior
            double b0 = 0.5;
            double aPrime = a0 + winsInBin;
            double bPrime = b0 + lossesInBin;
            double sum = aPrime + bPrime;
            double variance = (aPrime * bPrime) / (sum * sum * (sum + 1.0));
            double weight = Math.max(0.0, 1.0 - 2.0 * Math.sqrt(variance));
            return weight;
        }

        // =============================================================================
        //  OPT-3: Isotonic Regression 校准（PAVA 算法）
        //  将 raw NB posterior 映射到实际 calibrated probability
        //  训练：对历史 (rawPosterior, isWin) 按 rawPosterior 排序，用 PAVA 拟合单调递增函数
        //  预测：二分查找 + 线性插值
        // =============================================================================
        static void fitIsotonicPAVA(BayesState state, ArrayList<double[]> posteriorWinPairs) {
            if (posteriorWinPairs == null || posteriorWinPairs.size() < 30) {
                state.isoTrainX = null;
                state.isoTrainY = null;
                state.isoN = 0;
                return;
            }
            // 按 rawPosterior 排序
            posteriorWinPairs.sort(Comparator.comparingDouble(a -> a[0]));
            int n = posteriorWinPairs.size();
            double[] x = new double[n];
            double[] y = new double[n];
            for (int i = 0; i < n; i++) {
                x[i] = posteriorWinPairs.get(i)[0];
                y[i] = posteriorWinPairs.get(i)[1]; // 1.0=win, 0.0=loss
            }

            // PAVA（Pool Adjacent Violators Algorithm）
            // 确保 y 值单调递增（符合"更高 posterior → 更高实际胜率"的直觉）
            double[] w = new double[n]; // 每个 block 的权重（样本数）
            double[] yp = new double[n]; // pooled y
            Arrays.fill(w, 1.0);
            System.arraycopy(y, 0, yp, 0, n);

            int blocks = n;
            // 用 block 合并法：反复检查相邻违反单调性的 block 并合并
            boolean changed = true;
            while (changed) {
                changed = false;
                int i = 0;
                while (i < blocks - 1) {
                    if (yp[i] > yp[i + 1]) {
                        // 合并 block i 和 i+1
                        double totalW = w[i] + w[i + 1];
                        yp[i] = (yp[i] * w[i] + yp[i + 1] * w[i + 1]) / totalW;
                        w[i] = totalW;
                        // 移除 block i+1
                        for (int j = i + 1; j < blocks - 1; j++) {
                            yp[j] = yp[j + 1];
                            w[j] = w[j + 1];
                            x[j] = x[j + 1]; // x 取后一个 block 的值（代表上界）
                        }
                        blocks--;
                        changed = true;
                        // 回退检查前一个
                        if (i > 0) i--;
                    } else {
                        i++;
                    }
                }
            }

            // 保存校准模型
            state.isoTrainX = Arrays.copyOf(x, blocks);
            state.isoTrainY = Arrays.copyOf(yp, blocks);
            state.isoN = blocks;
        }

        static double isotonicCalibrate(BayesState state, double rawPosterior) {
            if (!BAYES_USE_ISOTONIC_CALIB) return rawPosterior;
            if (state == null || state.isoTrainX == null || state.isoN < 2) return rawPosterior;

            double[] xs = state.isoTrainX;
            double[] ys = state.isoTrainY;
            int n = state.isoN;

            // ===== 退化保护(只挡退化,不影响正常校准) =====
            // 病因:当 usedF=0(无特征有效分箱)时,全部训练样本的 rawPosterior 几乎相同,
            // x 轴失去区分度,PAVA 合并后 xs[0] 往往 >=0.5 且 ys[0]=0,导致正常的
            // rawPost=0.5 命中下面 "rawPosterior<=xs[0] → return ys[0]=0",被误压成0、误杀进场。
            // 判据:训练数据 x 轴跨度过窄(rawPosterior 挤成一点)→ 校准在拟合噪声,直接回退原始值。
            // 正常情况下(特征分箱有效、rawPosterior 有分布),xs 跨度足够,以下判据不触发,校准照常工作。
            double xSpan = xs[n - 1] - xs[0];
            if (!(xSpan > 1e-6)) {
                return rawPosterior; // x 轴无区分度:不可信的退化校准,回退原始后验
            }

            // 边界处理(外推):若端点校准值为极端(0或1),对训练范围外的输入不信任该外推,回退原始值,
            // 避免把范围外的中性输入打成极端;非极端端点则沿用(正常等渗外推)。
            if (rawPosterior <= xs[0]) {
                return (ys[0] <= 1e-9 || ys[0] >= 1.0 - 1e-9) ? rawPosterior : ys[0];
            }
            if (rawPosterior >= xs[n - 1]) {
                return (ys[n - 1] <= 1e-9 || ys[n - 1] >= 1.0 - 1e-9) ? rawPosterior : ys[n - 1];
            }

            // 二分查找 + 线性插值
            int lo = 0, hi = n - 1;
            while (lo < hi - 1) {
                int mid = (lo + hi) >>> 1;
                if (xs[mid] <= rawPosterior) lo = mid;
                else hi = mid;
            }
            // 线性插值
            double xRange = xs[hi] - xs[lo];
            if (xRange < 1e-12) return ys[lo];
            double t = (rawPosterior - xs[lo]) / xRange;
            return ys[lo] + t * (ys[hi] - ys[lo]);
        }

        // =============================================================================
        //  OPT-4: Kelly Criterion 仓位缩放
        //  f* = p/a - q/b  where p=winProb, q=1-p, a=avgLoss, b=avgWin
        //  实际使用 quarter-Kelly (f*/4) 提高鲁棒性
        //  返回缩放因子 [0, maxScale]
        // =============================================================================
        static double kellyPositionScale(double calibratedPosterior, double avgWinR, double avgLossR,
                                         double kellyFraction, double maxScale) {
            if (!BAYES_USE_KELLY_SIZING) {
                // 回退到旧逻辑
                return bayesPositionScale(calibratedPosterior, BAYES_MIN_POSTERIOR, BAYES_MEDIUM_POSTERIOR, BAYES_STRONG_POSTERIOR);
            }

            double p = Math.max(0.01, Math.min(0.99, calibratedPosterior));
            double q = 1.0 - p;

            // Kelly: f* = p/a - q/b (a=avgLoss, b=avgWin)
            double b = Math.max(0.01, avgWinR);
            double a = Math.max(0.01, avgLossR);
            double fStar = p / a - q / b;

            if (fStar <= 0.0) return 0.0; // 负期望值 → 不进场

            // 实际使用 fraction-Kelly（默认 quarter-Kelly）
            double fActual = fStar * kellyFraction;

            // 归一化到 position scale：fActual=0.02 → scale=1.0
            double scale = fActual / 0.02;
            return Math.max(0.0, Math.min(maxScale, scale));
        }

        // =============================================================================
        //  OPT-5: 互信息（Mutual Information）特征选择
        //  MI(X;Y) = ΣΣ P(x,y) * log[ P(x,y) / (P(x)*P(y)) ]
        //  替代 max|logLR|，全局衡量特征区分力
        // =============================================================================
        static double computeMutualInfo(int[][] fc, int totalW, int totalL) {
            int N = totalW + totalL;
            if (N < 10) return 0.0;
            int nBins = fc.length;

            double mi = 0.0;
            for (int b = 0; b < nBins; b++) {
                int w = fc[b][0];
                int l = fc[b][1];
                int binTotal = w + l;
                if (binTotal == 0) continue;

                // P(x=bin, y=win) and P(x=bin, y=loss)
                double pBinWin = (double) w / N;
                double pBinLoss = (double) l / N;
                double pBin = (double) binTotal / N;
                double pWin = (double) totalW / N;
                double pLoss = (double) totalL / N;

                if (pBinWin > 1e-12 && pBin > 1e-12 && pWin > 1e-12) {
                    mi += pBinWin * Math.log(pBinWin / (pBin * pWin));
                }
                if (pBinLoss > 1e-12 && pBin > 1e-12 && pLoss > 1e-12) {
                    mi += pBinLoss * Math.log(pBinLoss / (pBin * pLoss));
                }
            }
            return Math.max(0.0, mi); // MI 不会为负（数值误差保护）
        }

        // =============================================================================
        //  OPT-6: James-Stein 收缩估计
        //  shrunk_LR = (1-λ)*observed_LR + λ*mean_LR
        //  λ = (K-2) / (K * Σ(LR - mean)²)  (Stein's formula, K≥3)
        //  在 log 空间操作，收缩极端 LR 向均值
        // =============================================================================
        static double[] jamesSteinShrinkLR(double[] logLRs) {
            if (!BAYES_USE_JAMES_STEIN) return logLRs;
            int K = logLRs.length;
            if (K < 3) return logLRs; // Stein's inequality 需要 K≥3

            // 计算均值
            double mean = 0.0;
            for (double lr : logLRs) mean += lr;
            mean /= K;

            // 计算 Σ(LR - mean)²
            double sumSqDev = 0.0;
            for (double lr : logLRs) {
                double d = lr - mean;
                sumSqDev += d * d;
            }

            if (sumSqDev < 1e-12) return logLRs; // 所有 LR 一样，不收缩

            // λ = (K-2) / (K * Σ(LR-mean)²)，clamped to [0, 1]
            double lambda = (K - 2.0) / (K * sumSqDev);
            lambda = Math.max(0.0, Math.min(1.0, lambda));

            double[] shrunk = new double[K];
            for (int i = 0; i < K; i++) {
                shrunk[i] = (1.0 - lambda) * logLRs[i] + lambda * mean;
            }
            return shrunk;
        }

        // =============================================================================
        //  OPT-11: 特征相关性矩阵计算 + 相关性惩罚
        //  penalty = 1 - |max_corr_with_already_used_features|²
        //  corr=0 → penalty=1 (不惩罚), corr=0.9 → penalty=0.19, corr=1 → penalty=0
        // =============================================================================
        static void computeCorrelationMatrix(BayesState state, ArrayList<BayesEntrySnapshot> samples) {
            if (!BAYES_USE_CORR_PENALTY) return;
            Feature[] feats = Q_FEATURES;
            int nf = feats.length;
            double[][] corr = new double[nf][nf];

            // 收集特征值矩阵
            int n = samples.size();
            if (n < 20) {
                state.qFeatureCorrMatrix = null;
                return;
            }

            double[][] vals = new double[nf][n];
            boolean[] valid = new boolean[nf];
            Arrays.fill(valid, true);

            for (int fi = 0; fi < nf; fi++) {
                int cnt = 0;
                double sum = 0;
                for (int si = 0; si < n; si++) {
                    Double v = samples.get(si).raw == null ? null : samples.get(si).raw.get(feats[fi]);
                    if (v == null || Double.isNaN(v) || Double.isInfinite(v)) {
                        vals[fi][si] = Double.NaN;
                    } else {
                        vals[fi][si] = v;
                        sum += v;
                        cnt++;
                    }
                }
                if (cnt < 20) { valid[fi] = false; continue; }
                double mean = sum / cnt;
                // 标准化
                double sumSq = 0;
                for (int si = 0; si < n; si++) {
                    if (Double.isNaN(vals[fi][si])) vals[fi][si] = mean; // 缺失填充均值
                    vals[fi][si] -= mean;
                    sumSq += vals[fi][si] * vals[fi][si];
                }
                double std = Math.sqrt(sumSq / cnt);
                if (std < 1e-12) { valid[fi] = false; continue; }
                for (int si = 0; si < n; si++) vals[fi][si] /= std;
            }

            // 计算相关系数矩阵
            for (int i = 0; i < nf; i++) {
                corr[i][i] = 1.0;
                if (!valid[i]) continue;
                for (int j = i + 1; j < nf; j++) {
                    if (!valid[j]) continue;
                    double sum = 0;
                    for (int si = 0; si < n; si++) {
                        sum += vals[i][si] * vals[j][si];
                    }
                    double r = sum / n;
                    corr[i][j] = r;
                    corr[j][i] = r;
                }
            }
            state.qFeatureCorrMatrix = corr;
        }

        /** 计算特征 fi 相对于已使用特征集合的相关性惩罚 */
        static double correlationPenalty(BayesState state, int featureIdx, boolean[] usedFeatures) {
            if (!BAYES_USE_CORR_PENALTY || state.qFeatureCorrMatrix == null) return 1.0;
            double[][] corr = state.qFeatureCorrMatrix;
            if (featureIdx < 0 || featureIdx >= corr.length) return 1.0;

            double maxAbsCorr = 0.0;
            for (int j = 0; j < usedFeatures.length; j++) {
                if (!usedFeatures[j]) continue;
                if (j == featureIdx) continue;
                if (j >= corr[featureIdx].length) continue;
                double ac = Math.abs(corr[featureIdx][j]);
                if (ac > maxAbsCorr) maxAbsCorr = ac;
            }
            // penalty = 1 - corr²
            return Math.max(0.0, 1.0 - maxAbsCorr * maxAbsCorr);
        }

        /** 从 Q_FEATURES 数组获取特征的索引 */
        static int qFeatureIndex(Feature f) {
            for (int i = 0; i < Q_FEATURES.length; i++) {
                if (Q_FEATURES[i] == f) return i;
            }
            return -1;
        }

        static int countUsableFeaturesQuantile(Map<Feature, Integer> bins, BayesState state, int minBinEvidence) {
            if (bins == null || state == null) return 0;
            int used = 0;
            for (Map.Entry<Feature, Integer> e : bins.entrySet()) {
                Feature f = e.getKey();
                Integer bin = e.getValue();
                if (f == null || bin == null) continue;
                int[][] fc = state.qCounts.get(f);
                if (fc == null) continue;
                if (bin < 0 || bin >= fc.length) continue;
                int tot = fc[bin][0] + fc[bin][1];
                if (tot < Math.max(1, minBinEvidence)) continue;
                used++;
            }
            return used;
        }

        static double computePosteriorQuantile(Map<Feature, Integer> bins, BayesState state, double alpha, int minBinEvidence) {
            if (state == null) return 0.50;
            int W = state.qWins;
            int L = state.qLosses;
            int N = W + L;

            if (N < 30 || W < 3 || L < 3) return 0.50;
            if (bins == null || bins.isEmpty()) return 0.50;

            double logWin = Math.log((double) W / N);
            double logLose = Math.log((double) L / N);

            // OPT-6: 收集所有特征的 logLR 用于 James-Stein 收缩
            ArrayList<double[]> featureLRs = new ArrayList<>(); // [logLR_raw, weight]
            ArrayList<Feature> featureOrder = new ArrayList<>();
            boolean[] usedFeatureFlags = new boolean[Q_FEATURES.length]; // OPT-11: 跟踪已使用特征

            for (Map.Entry<Feature, Integer> e : bins.entrySet()) {
                Feature f = e.getKey();
                Integer bin = e.getValue();
                if (f == null || bin == null) continue;

                int[][] fc = state.qCounts.get(f);
                if (fc == null) continue;
                int nBins = Math.max(1, state.qK.getOrDefault(f, fc.length));

                if (bin < 0 || bin >= fc.length) continue;

                int wInBin = fc[bin][0];
                int lInBin = fc[bin][1];
                int totInBin = wInBin + lInBin;

                // OPT-2: Beta 自适应权重（替代硬截断）
                double weight;
                if (BAYES_USE_BETA_WEIGHTS) {
                    weight = betaAdaptiveWeight(wInBin, lInBin);
                    // 极低权重特征仍然跳过（噪声保护）
                    if (weight < 0.05) continue;
                } else {
                    // 旧逻辑：硬截断
                    if (totInBin < Math.max(1, minBinEvidence)) continue;
                    weight = 1.0;
                }

                double pBinWin = (wInBin + alpha) / (W + nBins * alpha);
                double pBinLose = (lInBin + alpha) / (L + nBins * alpha);
                double lr = Math.max(1e-12, pBinWin) / Math.max(1e-12, pBinLose);
                double logLR = Math.log(Math.max(1e-12, lr));

                // OPT-11: 相关性惩罚
                int fIdx = qFeatureIndex(f);
                if (BAYES_USE_CORR_PENALTY && fIdx >= 0) {
                    double corrPenalty = correlationPenalty(state, fIdx, usedFeatureFlags);
                    weight *= corrPenalty;
                    usedFeatureFlags[fIdx] = true; // 标记为已使用
                }

                featureLRs.add(new double[]{logLR, weight});
                featureOrder.add(f);
            }

            if (featureLRs.isEmpty()) return 0.50;

            // OPT-6: James-Stein 收缩（对 logLR 进行收缩）
            double[] rawLogLRs = new double[featureLRs.size()];
            for (int i = 0; i < rawLogLRs.length; i++) rawLogLRs[i] = featureLRs.get(i)[0];
            double[] shrunkLogLRs = jamesSteinShrinkLR(rawLogLRs);

            // 用收缩后的 logLR × weight 累加
            for (int i = 0; i < shrunkLogLRs.length; i++) {
                double wt = featureLRs.get(i)[1];
                double effectiveLR = shrunkLogLRs[i] * wt;
                logWin += effectiveLR;
                // logLose 用负数（因为 logLR = log(pW/pL)，所以 logLose -= logLR）
                logLose -= effectiveLR;
            }

            double maxLog = Math.max(logWin, logLose);
            double expW = Math.exp(logWin - maxLog);
            double expL = Math.exp(logLose - maxLog);
            double post = expW / (expW + expL);

            if (Double.isNaN(post) || Double.isInfinite(post)) return 0.50;
            return Math.max(0.0, Math.min(1.0, post));
        }

        static String diagStringQuantile(EnumMap<Feature, Double> raw,
                                         Map<Feature, Integer> bins,
                                         double posterior,
                                         double scale,
                                         BayesState state,
                                         double alpha,
                                         int minBinEvidence) {
            StringBuilder sb = new StringBuilder();
            sb.append("    [BAYES-Q] prior=");
            int N = Math.max(1, state.qEvidenceN);
            double prior = (N > 0 ? (double) state.qWins / N : 0.5);
            sb.append(String.format(Locale.US, "%.3f", prior));
            sb.append(" | posterior=").append(String.format(Locale.US, "%.4f", posterior));
            sb.append(" | scale=").append(String.format(Locale.US, "%.2f", scale));
            sb.append(" | usedF=").append(countUsableFeaturesQuantile(bins, state, minBinEvidence));
            sb.append("\n");

            // 始终打印raw值（即使bins为空），确保全量特征数据被记录用于离线分析
            for (Feature f : Q_FEATURES) {
                Double rv = (raw == null ? null : raw.get(f));
                if (rv == null) continue;
                double rawV = rv;

                sb.append("      ").append(f.name())
                        .append(" raw=").append(String.format(Locale.US, "%.4f", rawV));

                // 如果bins有效，追加分箱统计信息
                if (bins != null && bins.containsKey(f)) {
                    Integer bin = bins.get(f);
                    if (bin != null) {
                        int[][] fc = (state.qCounts != null ? state.qCounts.get(f) : null);
                        if (fc != null && bin >= 0 && bin < fc.length) {
                            int W = Math.max(1, state.qWins);
                            int L = Math.max(1, state.qLosses);
                            int nBins = Math.max(1, state.qK.getOrDefault(f, fc.length));
                            int tot = fc[bin][0] + fc[bin][1];
                            boolean weak = (tot < Math.max(1, minBinEvidence));
                            double pW = (fc[bin][0] + alpha) / (W + nBins * alpha);
                            double pL = (fc[bin][1] + alpha) / (L + nBins * alpha);
                            double lr = (pL > 1e-12 ? pW / pL : 99.0);
                            sb.append(" bin=").append(bin)
                                    .append(" k=").append(nBins)
                                    .append(" cnt=").append(tot)
                                    .append(weak ? " (弱证据)" : "")
                                    .append(" LR=").append(String.format(Locale.US, "%.2f", lr))
                                    .append(" W/L=").append(fc[bin][0]).append("/").append(fc[bin][1]);
                            double[] edges = state.qEdges.get(f);
                            if (edges != null && edges.length > 0) {
                                sb.append(" edges=").append(Arrays.toString(edges));
                            }
                        } else {
                            sb.append(" bin=").append(bin).append(" cnt=0 (无分箱数据)");
                        }
                    }
                } else {
                    sb.append(" (无分箱)");
                }
                sb.append("\n");
            }
            return sb.toString();
        }


        /**
         * ★ 核心：计算后验概率 P(Win | bins)
         *
         * 朴素贝叶斯 + 拉普拉斯平滑 + 对数空间运算
         *
         * @param bins       当前进场快照的离散化特征
         * @param state      贝叶斯统计状态
         * @param alpha      拉普拉斯平滑参数（推荐 1.0）
         * @return           后验概率 [0.0, 1.0]
         */
        static double computePosterior(Map<Feature, Integer> bins, BayesState state, double alpha) {
            int W = state.totalWins;
            int L = state.totalLosses;
            int N = W + L;

            // 样本不足时返回中性（不作为过滤依据）
            if (N < 30) return 0.50;

            // 先验
            double logProdWin  = Math.log((double) W / N);
            double logProdLose = Math.log((double) L / N);

            for (Feature f : Feature.values()) {
                Integer bin = bins.get(f);
                if (bin == null) continue;         // 该特征缺失 → 跳过（不影响后验）
                if (bin < 0 || bin >= f.binCount) continue; // 安全防御

                int[][] fc = state.counts.get(f);
                if (fc == null) continue;

                int nBins = f.binCount;

                // P(bin=k | Win) = (count_win[k] + α) / (totalWins + nBins * α)
                double pBinGivenWin  = (fc[bin][0] + alpha) / (W + nBins * alpha);
                double pBinGivenLose = (fc[bin][1] + alpha) / (L + nBins * alpha);

                logProdWin  += Math.log(pBinGivenWin);
                logProdLose += Math.log(pBinGivenLose);
            }

            // log-sum-exp 归一化
            double maxLog = Math.max(logProdWin, logProdLose);
            double expWin  = Math.exp(logProdWin - maxLog);
            double expLose = Math.exp(logProdLose - maxLog);
            double posterior = expWin / (expWin + expLose);

            // 安全边界
            if (Double.isNaN(posterior) || Double.isInfinite(posterior)) return 0.50;
            return Math.max(0.0, Math.min(1.0, posterior));
        }


        /**
         * ★ 更新统计（每笔交易完成后调用）
         *
         * 支持滚动窗口：超过 maxHistory 时自动淘汰最旧样本
         */
        static void updateOnTradeComplete(
                BayesState state,
                BayesEntrySnapshot snap,
                boolean isWin,
                double pnlPoints,
                double rMultiple,
                int maxHistory
        ) {
            if (state == null || snap == null) return;

            snap.isWin = isWin;
            snap.pnlPoints = pnlPoints;


            snap.rMultiple = rMultiple;
            boolean quantile = "quantile".equalsIgnoreCase(BAYES_SCHEME);

            if (!quantile) {
                // legacy：固定分箱 → 直接增量更新计数（高效）
                addSample(state, snap.bins, isWin);
                state.history.addLast(snap);

                while (state.history.size() > maxHistory) {
                    BayesEntrySnapshot old = state.history.pollFirst();
                    if (old != null) removeSample(state, old.bins, old.isWin);
                }
                return;
            }

            // quantile：滚动最近N笔交易重新拟合分箱
            // 这里不做“按bin增量计数”，因为 bin 边界会变；只维护滚动样本，模型在需要时重建。
            state.history.addLast(snap);
            while (state.history.size() > maxHistory) state.history.pollFirst();

            // 标记模型过期（下次 evalBayesForEntry 需要时会按 refitEvery 触发重建）
            state.qDirty = true;
            state.qNewSinceRefit++;
        }

        private static void addSample(BayesState state, Map<Feature, Integer> bins, boolean isWin) {
            if (isWin) state.totalWins++; else state.totalLosses++;
            int col = isWin ? 0 : 1;

            for (Map.Entry<Feature, Integer> e : bins.entrySet()) {
                int[][] fc = state.counts.get(e.getKey());
                if (fc == null) continue;
                int bin = e.getValue();
                if (bin >= 0 && bin < fc.length) {
                    fc[bin][col]++;
                }
            }
        }

        private static void removeSample(BayesState state, Map<Feature, Integer> bins, boolean isWin) {
            if (isWin) state.totalWins = Math.max(0, state.totalWins - 1);
            else       state.totalLosses = Math.max(0, state.totalLosses - 1);
            int col = isWin ? 0 : 1;

            for (Map.Entry<Feature, Integer> e : bins.entrySet()) {
                int[][] fc = state.counts.get(e.getKey());
                if (fc == null) continue;
                int bin = e.getValue();
                if (bin >= 0 && bin < fc.length) {
                    fc[bin][col] = Math.max(0, fc[bin][col] - 1);
                }
            }
        }


        // =====================================================================
        //  第四部分：仓位倍率决策 + 诊断输出
        // =====================================================================

        /**
         * 基于后验概率决定仓位倍率
         *
         * 与你现有的 voteScale (TSRND_SCALE_SCORE2/3/5) 叠加使用
         *
         * @param posterior  后验概率
         * @param minPost    最低进场概率（BAYES_MIN_POSTERIOR）
         * @param medPost    中等概率线（BAYES_MEDIUM_POSTERIOR）
         * @param strongPost 高确信线（BAYES_STRONG_POSTERIOR）
         * @return           0.0=不进场, 0.7=轻仓, 1.0=正常, 1.3=加仓
         */
        static double bayesPositionScale(double posterior, double minPost, double medPost, double strongPost) {
            if (posterior < minPost)   return 0.0;   // 不进场
            if (posterior < medPost)   return 0.7;   // 轻仓
            if (posterior < strongPost) return 1.0;  // 正常
            return 1.3;                              // 加仓
        }


        /**
         * 诊断字符串（嵌入你的 [BT-DIAG] / VERBOSE_BACKTEST 输出链）
         *
         * 包含：后验概率、仓位倍率、先验、每个特征的 bin + 似然比（LR）
         */
        static String diagString(Map<Feature, Integer> bins, double posterior,
                                 double scale, BayesState state, double alpha) {
            int N = state.totalWins + state.totalLosses;
            double prior = (N > 0 ? (double) state.totalWins / N : 0.5);

            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Locale.US,
                    "[BAYES] posterior=%.4f scale=%.2f | prior=%.3f(W=%d L=%d N=%d) | features: ",
                    posterior, scale, prior, state.totalWins, state.totalLosses, N));

            for (Feature f : Feature.values()) {
                Integer bin = bins.get(f);
                if (bin == null) { sb.append(f.name()).append("=NA "); continue; }

                int[][] fc = state.counts.get(f);
                int nBins = f.binCount;

                // 计算该 bin 的似然比
                double pW = (fc[bin][0] + alpha) / (state.totalWins + nBins * alpha);
                double pL = (fc[bin][1] + alpha) / (state.totalLosses + nBins * alpha);
                double lr = (pL > 1e-9) ? pW / pL : 99.0;

                sb.append(String.format(Locale.US, "%s=bin%d(LR=%.2f) ", f.name(), bin, lr));
            }
            return sb.toString();
        }


        /**
         * 回测报告：打印每个特征的每个 bin 的似然比热力图
         *
         * 在 BOOT_BACKTEST_PRINT_REPORT 区域调用
         */
        static void printBayesReport(BayesState state, double alpha) {
            if (state == null) return;

            boolean quantile = "quantile".equalsIgnoreCase(BAYES_SCHEME);
            if (!quantile) {
                printBayesReportLegacy(state, alpha);
                return;
            }

            // quantile：先确保模型最新（按 refitEvery 节流）
            ensureQuantileModel(state,
                    BAYES_LOOKBACK_TRADES,
                    BAYES_BIN_K,
                    BAYES_MIN_BIN_TRADES,
                    BAYES_WINSOR_PCT,
                    BAYES_REFIT_EVERY,
                    0, // 报告在这里统一打印，不在 refit 时刷屏
                    alpha,
                    BAYES_MIN_BIN_EVIDENCE);

            int N = state.qEvidenceN;
            int W = state.qWins;
            int L = state.qLosses;
            double prior = (N > 0 ? (double) W / N : 0.5);

            String mode = (BAYES_MODE == null ? "BOOST" : BAYES_MODE.trim().toUpperCase(Locale.ROOT));
            boolean allowDecision = (N >= BAYES_MIN_TRADE_EVIDENCE);

            System.out.printf(Locale.US,
                    "%n========== [BAYES-REPORT][QUANTILE] N=%d W=%d L=%d prior=%.3f | mode=%s | K=%d | minBinTrades=%d | minBinEv=%d | familyCap=%d | alpha=%.2f | label=%s rg=%.2f rb=%.2f skipNeutral=%s ==========%n",
                    N, W, L, prior, mode, (BAYES_BIN_K == 5 ? 5 : 3), BAYES_MIN_BIN_TRADES, BAYES_MIN_BIN_EVIDENCE, BAYES_FAMILY_CAP, alpha, BAYES_LABEL_MODE, BAYES_R_GOOD, BAYES_R_BAD, BAYES_SKIP_NEUTRAL_TRAIN);

            if (!allowDecision) {
                System.out.printf(Locale.US, "[BAYES-WARN] 样本不足（N=%d < minTradeEvidence=%d）→ 不会执行 GATE/BOOST，仅打印诊断。%n",
                        N, BAYES_MIN_TRADE_EVIDENCE);
            }

            // ---------- 特征分箱表 ----------
            String labelCol = ("R_QUALITY".equalsIgnoreCase(BAYES_LABEL_MODE) ? "Good%" : "Win%");
            System.out.printf(Locale.US, "%-18s | %-3s | %-30s | %-6s | %-6s | %-6s | %s%n",
                    "Feature", "K", "Edges", "Bin", "Count", labelCol, "LR(w/l)");
            System.out.println("-".repeat(110));

            for (Feature f : Q_FEATURES) {
                int k = Math.max(1, state.qK.getOrDefault(f, 1));
                double[] edges = state.qEdges.get(f);
                int[][] fc = state.qCounts.get(f);

                String edgeStr = (edges == null || edges.length == 0) ? "[]" : Arrays.toString(edges);
                if (fc == null) {
                    System.out.printf(Locale.US, "%-18s | %-3d | %-30s | %-6s | %-6s | %-6s | %s%n",
                            f.name(), k, edgeStr, "-", "-", "-", "-");
                    System.out.printf(Locale.US, "  [BAYES-WARN] %s counts 缺失%n", f.name());
                    continue;
                }

                // 计算 maxAbsLogLR 作为“区分度”提示
                double maxAbsLogLR = 0.0;
                int binsWeak = 0;
                for (int b = 0; b < fc.length; b++) {
                    int tot = fc[b][0] + fc[b][1];
                    if (tot < Math.max(1, BAYES_MIN_BIN_EVIDENCE)) binsWeak++;
                    double pW = (fc[b][0] + alpha) / (Math.max(1, W) + k * alpha);
                    double pL = (fc[b][1] + alpha) / (Math.max(1, L) + k * alpha);
                    double lr = (pL > 1e-12 ? pW / pL : 99.0);
                    double alr = Math.abs(Math.log(Math.max(1e-12, lr)));
                    if (alr > maxAbsLogLR) maxAbsLogLR = alr;
                }

                for (int b = 0; b < fc.length; b++) {
                    int cW = fc[b][0];
                    int cL = fc[b][1];
                    int tot = cW + cL;
                    double winPct = (tot > 0 ? (100.0 * cW / tot) : 0.0);

                    double pW = (cW + alpha) / (Math.max(1, W) + k * alpha);
                    double pL = (cL + alpha) / (Math.max(1, L) + k * alpha);
                    double lr = (pL > 1e-12 ? pW / pL : 99.0);

                    System.out.printf(Locale.US, "%-18s | %-3d | %-30s | %-6d | %-6d | %-6.2f | %.2f%n",
                            f.name(), k, edgeStr, b, tot, winPct, lr);
                    edgeStr = ""; // 同一特征只在第一行显示 edges
                }

                // OPT-5: Show MI value alongside max|logLR|
                Double miVal = state.qMutualInfo.get(f);
                if (k <= 1) {
                    System.out.printf(Locale.US, "  [BAYES-WARN] %s：有效分箱K=1（区分力≈0）。%n", f.name());
                } else if (BAYES_USE_MI_SELECTION && miVal != null && miVal < BAYES_MI_THRESHOLD) {
                    System.out.printf(Locale.US, "  [BAYES-NOISE] %s: MI=%.5f < thr=%.5f -> filtered as noise%n", f.name(), miVal, BAYES_MI_THRESHOLD);
                } else if (maxAbsLogLR < 0.15) {
                    System.out.printf(Locale.US, "  [BAYES-WARN] %s：区分度偏弱（max|logLR|=%.3f%s）。%n", f.name(), maxAbsLogLR,
                            (miVal != null ? String.format(Locale.US, " MI=%.5f", miVal) : ""));
                }
                if (binsWeak >= Math.max(1, k - 1)) {
                    System.out.printf(Locale.US, "  [BAYES-WARN] %s：多数bin样本<minBinEv，posterior主要由少数特征决定。%n", f.name());
                }
            }

            // ---------- posterior 分层校验（Expected R 口径：AvgR / ≥Rgood 占比是否随 posterior 变好） ----------
            ArrayList<double[]> rows = new ArrayList<>(); // [posterior, labelWin(1/0), usedF, rMultiple]
            for (BayesEntrySnapshot s : state.history) {
                if (s == null || s.raw == null || s.raw.isEmpty()) continue;
                Map<Feature, Integer> b = discretizeQuantile(s.raw, state);
                int usedF = countUsableFeaturesQuantile(b, state, BAYES_MIN_BIN_EVIDENCE);
                double post = computePosteriorQuantile(b, state, alpha, BAYES_MIN_BIN_EVIDENCE);
                rows.add(new double[]{post, s.isWin ? 1.0 : 0.0, usedF, s.rMultiple});
            }

            if (rows.size() >= 30) {
                rows.sort(Comparator.comparingDouble(a -> a[0]));
                int buckets = Math.max(2, BAYES_CALIB_BUCKETS);
                double rg = Math.abs(BAYES_R_GOOD);

                // calib arrays: 用于“posterior->桶->AvgR”的 Gate（Expected R 口径）
                double[] calibPostHiArr = new double[buckets];
                double[] calibAvgRArr = new double[buckets];
                int[] calibCntArr = new int[buckets];
                Arrays.fill(calibPostHiArr, Double.NaN);
                Arrays.fill(calibAvgRArr, Double.NaN);
                Arrays.fill(calibCntArr, 0);

                System.out.println();
                System.out.println("---------- [BAYES-CALIB] posterior 分层（按posterior排序分5组，观察 AvgR / ≥Rgood 占比是否单调） ----------");
                System.out.printf(Locale.US, "%-8s | %-6s | %-10s | %-10s | %-10s | %-10s%n",
                        "Bucket", "Count", ">=Rgood%", "AvgR", "AvgPost", "AvgUsedF");
                System.out.println("-".repeat(78));

                double lastAvgR = Double.NaN;
                int nonMono = 0;

                for (int bi = 0; bi < buckets; bi++) {
                    int from = (int) Math.floor((double) bi * rows.size() / buckets);
                    int to = (int) Math.floor((double) (bi + 1) * rows.size() / buckets);
                    if (to <= from) continue;

                    int cnt = 0;
                    int goodCnt = 0;
                    int rCnt = 0;
                    double sumR = 0.0;
                    double sumPost = 0.0;
                    double sumUsed = 0.0;

                    for (int i = from; i < to; i++) {
                        double[] r = rows.get(i);
                        cnt++;
                        double rr = r[3];
                        if (Double.isFinite(rr)) {
                            sumR += rr;
                            rCnt++;
                            if (rr >= rg) goodCnt++;
                        } else {
                            // 兼容旧快照（无 rMultiple）：退化为用 labelWin 近似统计
                            if (r[1] > 0.5) goodCnt++;
                        }
                        sumPost += r[0];
                        sumUsed += r[2];
                    }

                    double goodPct = (cnt > 0 ? (100.0 * goodCnt / cnt) : 0.0);
                    double avgR = (rCnt > 0 ? (sumR / rCnt) : 0.0);
                    double avgPost = (cnt > 0 ? sumPost / cnt : 0.0);
                    double avgUsed = (cnt > 0 ? sumUsed / cnt : 0.0);

                    // 记录桶信息（用于 Gate by AvgR）
                    calibPostHiArr[bi] = rows.get(to - 1)[0];
                    calibAvgRArr[bi] = avgR;
                    calibCntArr[bi] = rCnt; // 用于 AvgR 统计的有效样本数

                    System.out.printf(Locale.US, "%-8s | %-6d | %-10.2f | %-10.4f | %-10.4f | %-10.2f%n",
                            (bi + 1) + "/" + buckets, cnt, goodPct, avgR, avgPost, avgUsed);

                    // 单调性检查：允许少量噪声
                    if (Double.isFinite(lastAvgR) && avgR + 0.02 < lastAvgR) nonMono++;
                    lastAvgR = avgR;
                }

                // 写入 state：供 GateByAvgR 在实盘/回测时使用（不依赖 posterior 单调性）
                state.calibPostHi = calibPostHiArr;
                state.calibAvgR = calibAvgRArr;
                state.calibCnt = calibCntArr;
                state.calibTotalN = rows.size();
                state.calibBuckets = buckets;
                state.calibRefitId = state.qRefitCount;

                if (nonMono >= 2) {
                    System.out.printf(Locale.US, "[BAYES-WARN] posterior 分层 AvgR 不太单调（nonMono=%d）→ 区分力可能不足，建议只用 BOOST 或仅做报表。%n", nonMono);
                }
            } else {
                System.out.printf(Locale.US, "%n[BAYES-WARN] 可用于校验的样本太少（%d）→ 跳过 posterior 分层。%n", rows.size());
                // 样本不足：清空 calib 缓存，避免用旧桶误 Gate
                state.calibPostHi = null;
                state.calibAvgR = null;
                state.calibCnt = null;
                state.calibTotalN = rows.size();
                state.calibBuckets = 0;
                state.calibRefitId = state.qRefitCount;

            }

            // ---------- 影响评估：如果按当前阈值会拦/会加多少 ----------
            if (rows.size() >= 30) {
                int gateN = 0, boostN = 0;

                boolean wantGate = ("GATE".equals(mode) || "BOTH".equals(mode));
                boolean wantBoost = ("BOOST".equals(mode) || "BOTH".equals(mode));

                double gateThr = BAYES_GATE_POSTERIOR;
                double boostThr = BAYES_BOOST_POSTERIOR;
                boolean useQThr = (BAYES_USE_POSTERIOR_QUANTILE_THR && !Double.isNaN(state.qGateThr) && !Double.isNaN(state.qBoostThr));
                if (useQThr) {
                    gateThr = state.qGateThr;
                    boostThr = state.qBoostThr;
                }

                if (allowDecision) {
                    for (double[] r : rows) {
                        double post = r[0];
                        boolean gatedHere = false;
                        if (wantGate) {
                            boolean calibReady = isCalibReady(state);
                            if (calibReady) {
                                String cg = checkCalibAvgRGate(state, post);
                                if (cg != null) gatedHere = true;
                            } else if (post < gateThr) {
                                gatedHere = true; // 校验不足时回退 posterior gate
                            }
                        }
                        if (gatedHere) gateN++;
                        else if (wantBoost && post > boostThr) boostN++;
                    }
                }

                int vetoN = 0;
                if (allowDecision && wantGate && BAYES_WEAK_VETO_ENABLED && useQThr) {
                    for (BayesEntrySnapshot s : state.history) {
                        if (s == null || s.raw == null || s.raw.isEmpty()) continue;
                        Map<Feature, Integer> b = discretizeQuantile(s.raw, state);
                        String vr = checkWeakVetoQuantile(b, state, alpha, BAYES_MIN_BIN_EVIDENCE);
                        if (vr != null) vetoN++;
                    }
                }

                System.out.println();
                System.out.println("---------- [BAYES-IMPACT] 规则影响（按当前阈值粗估） ----------");
                String gqStr = (useQThr ? String.format(Locale.US, " (q=%.2f)", state.qGateQ) : "");
                String bqStr = (useQThr ? String.format(Locale.US, " (q=%.2f)", state.qBoostQ) : "");
                System.out.printf(Locale.US,
                        "allowDecision=%s | useQuantileThr=%s | gateThr=%.3f%s | boostThr=%.3f%s | boostScale=%.3f%n",
                        allowDecision, useQThr, gateThr, gqStr, boostThr, bqStr, BAYES_BOOST_SCALE);

                // P5：按校验分桶 AvgR 的 Gate（Expected R）信息
                if (wantGate && BAYES_GATE_BY_CALIB_AVGR) {
                    System.out.printf(Locale.US,
                            "calibGateByAvgR=%s | calibThrR=%.4f | calibBuckets=%d | calibTotalN=%d | calibMinN=%d | calibMinBucketN=%d | calibRefit=%d%n",
                            BAYES_GATE_BY_CALIB_AVGR, BAYES_CALIB_GATE_R_THR,
                            state.calibBuckets, state.calibTotalN, BAYES_CALIB_MIN_N, BAYES_CALIB_MIN_BUCKET_N, state.calibRefitId);
                }
                if (wantGate && BAYES_WEAK_VETO_ENABLED) {
                    System.out.printf(Locale.US, "weakVetoEnabled=%s | vetoLrThr=%.2f | vetoFeatures=%s | vetoWouldBlock=%d (%.2f%%)%n",
                            BAYES_WEAK_VETO_ENABLED, BAYES_VETO_LR_THRESHOLD, BAYES_VETO_FEATURES,
                            vetoN, (rows.size() > 0 ? 100.0 * vetoN / rows.size() : 0.0));
                }
                System.out.printf(Locale.US, "gateWouldBlock=%d (%.2f%%) | boostWouldApply=%d (%.2f%%)%n",
                        gateN, (rows.size() > 0 ? 100.0 * gateN / rows.size() : 0.0),
                        boostN, (rows.size() > 0 ? 100.0 * boostN / rows.size() : 0.0));

                // 经验警戒线（你说优先BOOST：重点看 boost 覆盖率）
                double gatePct = (rows.size() > 0 ? (double) gateN / rows.size() : 0.0);
                double boostPct = (rows.size() > 0 ? (double) boostN / rows.size() : 0.0);

                if (allowDecision && wantGate && gatePct > 0.30) {
                    System.out.println("[BAYES-WARN] GATE 拦截比例偏高（>30%）→ 极可能误杀机会。建议：降低 gateQuantile/或改用固定 gatePosterior、或提高 minTradeEvidence。");
                }
                if (allowDecision && wantBoost && boostPct < 0.05) {
                    System.out.println("[BAYES-WARN] BOOST 触发偏少（<5%）→ 你将很难看到增益。建议：提高 boostQuantile(如0.80~0.85) 或降低固定 boostPosterior（如0.55~0.58）。");
                }
                if (allowDecision && wantBoost && boostPct > 0.35) {
                    System.out.println("[BAYES-WARN] BOOST 覆盖过高（>35%）→ 等于长期加仓，容易放大回撤。建议：降低 boostScale 或提高 boostQuantile。");
                }
            }

            System.out.println("=".repeat(110));
        }

        static void printBayesReportLegacy(BayesState state, double alpha) {
            int N = state.totalWins + state.totalLosses;
            double prior = (N > 0 ? (double) state.totalWins / N : 0.5);

            System.out.printf(Locale.US,
                    "%n========== [BAYES-REPORT] samples=%d wins=%d losses=%d prior=%.3f ==========%n",
                    N, state.totalWins, state.totalLosses, prior);

            System.out.printf(Locale.US, "%-22s | %-5s | %-8s | %-10s | %-10s | %-6s%n",
                    "Feature", "Bin", "LR", "P(bin|Win)", "P(bin|Lose)", "Count");
            System.out.println("-".repeat(80));

            for (Feature f : Feature.values()) {
                int[][] fc = state.counts.get(f);
                for (int b = 0; b < f.binCount; b++) {
                    int cW = fc[b][0];
                    int cL = fc[b][1];
                    double pW = (cW + alpha) / (state.totalWins + f.binCount * alpha);
                    double pL = (cL + alpha) / (state.totalLosses + f.binCount * alpha);
                    double lr = (pL > 1e-9) ? pW / pL : 99.0;

                    String lrMark = lr > 1.3 ? " ★" : (lr < 0.7 ? " ✖" : "");

                    System.out.printf(Locale.US, "%-22s | bin%-2d | %6.3f%s | %10.4f | %10.4f | W=%3d L=%3d%n",
                            f.name(), b, lr, lrMark, pW, pL, cW, cL);
                }
                System.out.println("-".repeat(80));
            }

            // 信息增益排名（哪个特征最有区分力）
            System.out.println("\n[BAYES-REPORT] 特征区分力排名（基于最大 LR 偏离度）：");
            List<String> ranked = new ArrayList<>();
            for (Feature f : Feature.values()) {
                int[][] fc = state.counts.get(f);
                double maxDeviation = 0;
                for (int b = 0; b < f.binCount; b++) {
                    double pW = (fc[b][0] + alpha) / (state.totalWins + f.binCount * alpha);
                    double pL = (fc[b][1] + alpha) / (state.totalLosses + f.binCount * alpha);
                    double lr = (pL > 1e-9) ? pW / pL : 99.0;
                    maxDeviation = Math.max(maxDeviation, Math.abs(Math.log(lr)));
                }
                ranked.add(String.format(Locale.US, "  %.4f  %s", maxDeviation, f.name()));
            }
            ranked.sort(Collections.reverseOrder());
            for (String s : ranked) System.out.println(s);
            System.out.println("=".repeat(80));
        }


        // =====================================================================
        //  第五部分：辅助计算 —— 量比（volume ratio）
        // =====================================================================

        /**
         * 计算信号K0 的量比 = vol[sigIdx] / SMA20(vol)
         * 如果你的 Candle.vol 是可用的（=成交量字段），直接用。
         * 否则返回 1.0（中性 bin）。
         */
        static double calcVolumeRatio(List<Candle> m30, int sigIdx) {
            if (m30 == null || m30.isEmpty() || sigIdx < 0 || sigIdx >= m30.size()) return 1.0;
            int n = 20;
            int from = Math.max(0, sigIdx - (n - 1));
            int cnt = 0;
            double sum = 0.0;
            for (int i = from; i <= sigIdx; i++) {
                Candle k = m30.get(i);
                if (k == null) continue;
                double v = k.vol;
                if (v <= 0) continue;
                sum += v;
                cnt++;
            }
            if (cnt <= 0) return 1.0;
            double ma = sum / cnt;
            if (ma <= 1e-9) return 1.0;
            double cur = Math.max(0.0, m30.get(sigIdx).vol);
            return cur / ma;
        }



        // =====================================================================
        //  第六部分：SQLite 持久化（可选）
        // =====================================================================

        /*
         * 如果你想在重启后保留贝叶斯状态，可以用你已有的 audit DB。
         *
         * CREATE TABLE IF NOT EXISTS bayes_bin_stats (
         *     feature_name  TEXT    NOT NULL,
         *     bin_id        INTEGER NOT NULL,
         *     win_count     INTEGER DEFAULT 0,
         *     lose_count    INTEGER DEFAULT 0,
         *     PRIMARY KEY (feature_name, bin_id)
         * );
         *
         * CREATE TABLE IF NOT EXISTS bayes_meta (
         *     key   TEXT PRIMARY KEY,
         *     value TEXT
         * );
         * -- key='totalWins'  value='123'
         * -- key='totalLosses' value='89'
         *
         * 保存：
         *   void saveBayesToDb(BayesState state, Connection conn) {
         *       for (Feature f : Feature.values()) {
         *           int[][] fc = state.counts.get(f);
         *           for (int b = 0; b < f.binCount; b++) {
         *               // UPSERT: INSERT OR REPLACE INTO bayes_bin_stats ...
         *           }
         *       }
         *   }
         *
         * 加载：
         *   BayesState loadBayesFromDb(Connection conn) { ... }
         */


        // =====================================================================
        //  第七部分：嵌入指南（精确到行号）
        // =====================================================================

        /*
         *  ==========================================
         *  步骤 1：复制本文件的类到 OkxSignalAlarmOnly.java
         *  ==========================================
         *
         *  把以下内容作为 OkxSignalAlarmOnly 的内部 static 类/枚举/方法：
         *  - enum Feature { ... }
         *  - class BayesEntrySnapshot { ... }
         *  - class BayesState { ... }
         *  - 所有 discretizeXxx() 方法
         *  - discretize() 总入口
         *  - computePosterior()
         *  - updateOnTradeComplete()
         *  - bayesPositionScale()
         *  - diagString()
         *  - printBayesReport()
         *
         *
         *  ==========================================
         *  步骤 2：添加配置常量（~第107行附近，跟你的其它 sysBool/sysDouble 放一起）
         *  ==========================================
         *
         *      static final boolean USE_BAYESIAN_FILTER   = sysBool("okx.useBayesian", true);
         *      static final double  BAYES_MIN_POSTERIOR    = sysDouble("okx.bayesMinPosterior", 0.55);
         *      static final double  BAYES_STRONG_POSTERIOR = sysDouble("okx.bayesStrongPosterior", 0.72);
         *      static final double  BAYES_MEDIUM_POSTERIOR = sysDouble("okx.bayesMediumPosterior", 0.63);
         *      static final boolean BAYES_VERBOSE          = sysBool("okx.bayesVerbose", true);
         *      static final int     BAYES_LOOKBACK_TRADES  = sysInt("okx.bayesLookback", 200);
         *      static final double  BAYES_LAPLACE_ALPHA    = sysDouble("okx.bayesAlpha", 1.0);
         *
         *
         *  ==========================================
         *  步骤 3：在 EngineState 中添加字段（~第2474行）
         *  ==========================================
         *
         *      BayesState bayesState = new BayesState();
         *      BayesEntrySnapshot currentBayesSnap = null;  // 当前持仓的进场快照
         *
         *
         *  ==========================================
         *  步骤 4：在 Position 中添加字段（~第5052行）
         *  ==========================================
         *
         *      double bayesPosterior = 0.5;
         *      double bayesScale = 1.0;
         *
         *
         *  ==========================================
         *  步骤 5：在 CompletedTrade 中添加字段（~第5125行）
         *  ==========================================
         *
         *      double bayesPosterior = 0.5;
         *
         *
         *  ==========================================
         *  步骤 6：在 resetEngineForReDerive 中清理（~第2546行）
         *  ==========================================
         *
         *      e.bayesState = new BayesState();
         *      e.currentBayesSnap = null;
         *
         *
         *  ==========================================
         *  步骤 7：★ 进场逻辑中插入贝叶斯计算
         *  ==========================================
         *
         *  位置：runBacktestToState 第 ~7167 行
         *  即 "if (side != null) {" 块内，在 TSRND 投票通过之后，
         *  "MacdPoint k0 = macd30.get(sigIdx);" 这行之前。
         *
         *  --------------- 插入代码开始 ---------------
         *
         *      // ===================== 贝叶斯多档后验概率 =====================
         *      double bayesPosterior = 0.5;
         *      double bayesScale = 1.0;
         *      Map<Feature, Integer> bayesBins = null;
         *
         *      if (USE_BAYESIAN_FILTER) {
         *          // 获取 1H hist（安全取值）
         *          double hist1h = Double.NaN;
         *          if (t1hInfo != null && t1hInfo.idx >= 0
         *              && macd1h != null && t1hInfo.idx < macd1h.size()) {
         *              hist1h = macd1h.get(t1hInfo.idx).hist;
         *          }
         *
         *          // 获取 SR gap
         *          SrLevels srForBayes = calcOkxSrLevels(m30, sigIdx, OKX_SR_N);
         *          double srGap = 0;
         *          if (srForBayes.valid) {
         *              srGap = (side == Side.LONG)
         *                  ? (srForBayes.resistance - sigC0.c)
         *                  : (sigC0.c - srForBayes.support);
         *          }
         *
         *          // 获取 body ratio
         *          Candle bk0 = m30.get(sigIdx);
         *          double bRange = bk0.h - bk0.l;
         *          double bBody = Math.abs(bk0.c - bk0.o);
         *          double bodyR = (bRange > 0) ? bBody / bRange : 0.5;
         *
         *          // 获取量比（如果你的 Candle 有 vol 字段）
         *          double volRatio = 1.0;
         *          if (sigIdx >= 20) {
         *              double curVol = m30.get(sigIdx).vol;
         *              double volSum = 0;
         *              for (int vj = sigIdx - 19; vj <= sigIdx; vj++) {
         *                  volSum += m30.get(vj).vol;
         *              }
         *              double volAvg = volSum / 20.0;
         *              volRatio = (volAvg > 0) ? curVol / volAvg : 1.0;
         *          }
         *
         *          // 获取 EMA vote score（安全取值）
         *          double emaScoreVal = 0.0;
         *          if (USE_EMA_VOTE_FILTER && EMA_VOTE_CTX_BACKTEST != null) {
         *              EmaVoteSnapshot emaSnap = EMA_VOTE_CTX_BACKTEST.snapshotAt(c.ts);
         *              if (emaSnap != null) emaScoreVal = emaSnap.score;
         *          }
         *
         *          // 离散化
         *          bayesBins = MultiBinBayesFilter.discretize(
         *              macd30.get(sigIdx).hist,                // MACD hist 30m
         *              macd30.get(sigIdx).dif                  // DIF 加速度
         *                - (sigIdx >= 1 ? macd30.get(sigIdx - 1).dif : 0),
         *              hist1h,                                  // 1H MACD hist
         *              (pool2 != null ? pool2.score : 0),       // Pool2 score
         *              (pool2 != null ? pool2.dir : 0),         // Pool2 dir
         *              ov.score,                                // TSRND score
         *              ov.dirStrength,                          // TSRND dir strength
         *              t1hInfo.trend,                           // 1H 趋势
         *              side,                                    // 进场方向
         *              emaScoreVal,                             // EMA vote score
         *              atrSig,                                  // ATR
         *              sigC0.c,                                 // 入场价
         *              srGap,                                   // SR gap
         *              bodyR,                                   // body ratio
         *              volRatio                                 // volume ratio
         *          );
         *
         *          // 计算后验
         *          bayesPosterior = MultiBinBayesFilter.computePosterior(
         *              bayesBins, eng.bayesState, BAYES_LAPLACE_ALPHA);
         *          bayesScale = MultiBinBayesFilter.bayesPositionScale(
         *              bayesPosterior, BAYES_MIN_POSTERIOR, BAYES_MEDIUM_POSTERIOR, BAYES_STRONG_POSTERIOR);
         *
         *          if (BAYES_VERBOSE && !holdingRtPos) {
         *              System.out.println(MultiBinBayesFilter.diagString(
         *                  bayesBins, bayesPosterior, bayesScale, eng.bayesState, BAYES_LAPLACE_ALPHA));
         *          }
         *
         *          // 保存快照（出场时更新统计用）
         *          eng.currentBayesSnap = new BayesEntrySnapshot(c.ts, bayesBins);
         *
         *          // 概率不足 → 不进场
         *          if (bayesScale <= 0.0) {
         *              if (diagTail) System.out.printf(
         *                  "[BT-DIAG] entry=%s | ❌BAYES posterior=%.4f < min=%.2f%n",
         *                  diagTs, bayesPosterior, BAYES_MIN_POSTERIOR);
         *              continue;
         *          }
         *
         *          // ★ 用贝叶斯倍率叠加到 voteScale
         *          voteScale = voteScale * bayesScale;
         *      }
         *
         *  --------------- 插入代码结束 ---------------
         *
         *  然后在创建 Position 之后（~第7430行 "cur = new Position();" 之后），加上：
         *
         *      cur.bayesPosterior = bayesPosterior;
         *      cur.bayesScale = bayesScale;
         *
         *
         *  ==========================================
         *  步骤 8：★ 出场逻辑中更新贝叶斯统计
         *  ==========================================
         *
         *  位置：runBacktestToState 中两处 "trades.add(ct);" 之后
         *  （第一处 ~第7604行 SL/TP 出场，第二处 ~第7683行 MACD/Combo 出场）
         *
         *  在 "trades.add(ct);" 之后紧跟：
         *
         *  --------------- 插入代码开始 ---------------
         *
         *      // ★ 贝叶斯统计更新
         *      if (USE_BAYESIAN_FILTER && eng.currentBayesSnap != null) {
         *          boolean isWin = ct.pnlPoints > 0;
         *          ct.bayesPosterior = cur.bayesPosterior;
         *          MultiBinBayesFilter.updateOnTradeComplete(
         *              eng.bayesState,
         *              eng.currentBayesSnap,
         *              isWin,
         *              ct.pnlPoints,
         *              BAYES_LOOKBACK_TRADES
         *          );
         *          eng.currentBayesSnap = null;
         *      }
         *
         *  --------------- 插入代码结束 ---------------
         *
         *
         *  ==========================================
         *  步骤 9：在回测报告中打印贝叶斯热力图
         *  ==========================================
         *
         *  位置：BOOT_BACKTEST_PRINT_REPORT 的报告打印区末尾
         *
         *      if (USE_BAYESIAN_FILTER) {
         *          MultiBinBayesFilter.printBayesReport(btEng.bayesState, BAYES_LAPLACE_ALPHA);
         *      }
         *
         *
         *  ==========================================
         *  步骤 10：在 checkRealtime / scanHistory 中同步插入
         *  ==========================================
         *
         *  与步骤 7 完全相同的逻辑（复制粘贴），确保回测/实盘一致。
         *  搜索 checkRealtime 或 conflictRealtime 中的进场路径：
         *    ~第8941行（scanHistory 冲突检测）
         *    ~第9115行（checkRealtime 正常进场）
         *
         *
         *  ==========================================
         *  步骤 11：在 SELF-CHECK 中打印贝叶斯配置（可选）
         *  ==========================================
         *
         *  在 printSelfCheckOnce() 中加入：
         *
         *      System.out.println("[SELF-CHECK][BAYES] enabled=" + USE_BAYESIAN_FILTER
         *          + " minPost=" + BAYES_MIN_POSTERIOR
         *          + " medPost=" + BAYES_MEDIUM_POSTERIOR
         *          + " strongPost=" + BAYES_STRONG_POSTERIOR
         *          + " lookback=" + BAYES_LOOKBACK_TRADES
         *          + " alpha=" + BAYES_LAPLACE_ALPHA
         *          + " features=" + Feature.values().length);
         *
         */

        // ===================== JSON 持久化（用于 live_state / auditDB） =====================
        static JSONObject snapToJson(BayesEntrySnapshot s) {
            if (s == null) return null;
            JSONObject j = new JSONObject();
            j.put("entryTs", s.entryTs);
            j.put("isWin", s.isWin);
            j.put("pnlPoints", s.pnlPoints);
            j.put("rMultiple", s.rMultiple);
            JSONArray binsArr = new JSONArray();
            for (Feature f : Feature.values()) {
                Integer v = (s.bins == null ? null : s.bins.get(f));
                binsArr.put(v == null ? -1 : v);
            }
            j.put("bins", binsArr);
            return j;
        }

        static BayesEntrySnapshot snapFromJson(JSONObject o) {
            if (o == null) return null;
            long entryTs = o.optLong("entryTs", 0L);
            JSONArray binsArr = o.optJSONArray("bins");
            Map<Feature, Integer> bins = new EnumMap<>(Feature.class);
            Feature[] fs = Feature.values();
            for (int i = 0; i < fs.length; i++) {
                int v = (binsArr != null && i < binsArr.length()) ? binsArr.optInt(i, -1) : -1;
                if (v >= 0) bins.put(fs[i], v);
            }
            BayesEntrySnapshot s = new BayesEntrySnapshot(entryTs, bins);
            s.isWin = o.optBoolean("isWin", false);
            s.pnlPoints = o.optDouble("pnlPoints", 0.0);
            s.rMultiple = o.optDouble("rMultiple", Double.NaN);
            return s;
        }

        static JSONObject stateToJson(BayesState s, int maxHistory) {
            if (s == null) return null;
            JSONObject j = new JSONObject();
            j.put("totalWins", s.totalWins);
            j.put("totalLosses", s.totalLosses);

            JSONArray countsArr = new JSONArray();
            for (Feature f : Feature.values()) {
                int[][] c = s.counts.get(f);
                JSONArray bins = new JSONArray();
                for (int b = 0; b < f.binCount; b++) {
                    JSONArray wl = new JSONArray();
                    wl.put(c[b][0]);
                    wl.put(c[b][1]);
                    bins.put(wl);
                }
                countsArr.put(bins);
            }
            j.put("counts", countsArr);

            JSONArray histArr = new JSONArray();
            if (maxHistory <= 0) maxHistory = 999999;
            List<BayesEntrySnapshot> list = new ArrayList<>(s.history);
            int keep = Math.min(maxHistory, list.size());
            int from = Math.max(0, list.size() - keep);
            for (int i = from; i < list.size(); i++) {
                JSONObject one = snapToJson(list.get(i));
                if (one != null) histArr.put(one);
            }
            j.put("history", histArr);
            return j;
        }

        static BayesState stateFromJson(JSONObject o) {
            if (o == null) return new BayesState();
            BayesState s = new BayesState();
            s.totalWins = o.optInt("totalWins", 0);
            s.totalLosses = o.optInt("totalLosses", 0);

            JSONArray countsArr = o.optJSONArray("counts");
            Feature[] fs = Feature.values();
            if (countsArr != null) {
                for (int fi = 0; fi < fs.length && fi < countsArr.length(); fi++) {
                    Feature f = fs[fi];
                    JSONArray bins = countsArr.optJSONArray(fi);
                    int[][] c = s.counts.get(f);
                    for (int b = 0; b < f.binCount; b++) {
                        JSONArray wl = (bins != null && b < bins.length()) ? bins.optJSONArray(b) : null;
                        if (wl == null) continue;
                        c[b][0] = wl.optInt(0, 0);
                        c[b][1] = wl.optInt(1, 0);
                    }
                }
            }

            s.history.clear();
            JSONArray histArr = o.optJSONArray("history");
            if (histArr != null) {
                for (int i = 0; i < histArr.length(); i++) {
                    JSONObject one = histArr.optJSONObject(i);
                    BayesEntrySnapshot snap = snapFromJson(one);
                    if (snap != null) s.history.addLast(snap);
                }
            }
            return s;
        }


    }

    // ===================== 数据结构 =====================
    public static class Candle {
        public long ts;
        public double o, h, l, c;
        public double vol;     //  新增
        public int confirm;

        // 旧构造保持不变（兼容你所有旧代码）
        public Candle(long ts, double o, double h, double l, double c, int confirm) {
            this(ts, o, h, l, c, 0.0, confirm);
        }

        // 新构造：带 vol
        public Candle(long ts, double o, double h, double l, double c, double vol, int confirm) {
            this.ts = ts;
            this.o = o;
            this.h = h;
            this.l = l;
            this.c = c;
            this.vol = vol;
            this.confirm = confirm;
        }
    }

    // ===================== 虚拟入场K（用于“最后一根K0没有K1也能进场”） =====================
    // 仅用于进场模拟/展示：不参与任何指标计算，也不会写入 m30/macd 序列
    static Candle virtualEntryCandleFromK0(Candle k0) {
        if (k0 == null) return null;
        long ts = k0.ts + BAR30_MS;
        double px = k0.c; // 用 K0 收盘价当作“下一根开盘价”展示/入场参考
        return new Candle(ts, px, px, px, px, 0.0, 0);
    }


    //  用时间判断“已收盘K”，用于实时（避免 OKX confirm 延迟）
    static List<Candle> filterClosedByTime(List<Candle> raw, long barMs, long nowMs, long safeMs) {
        if (raw == null || raw.isEmpty()) return Collections.emptyList();
        ArrayList<Candle> list = new ArrayList<>(raw);
        list.sort(Comparator.comparingLong(c -> c.ts));
        long cut = nowMs - safeMs;
        ArrayList<Candle> out = new ArrayList<>();
        for (Candle c : list) {
            long end = c.ts + barMs;
            if (end <= cut) out.add(c);
        }
        return out;
    }
    // 从DB取少量K，按“时间收盘”口径找到最后一根已收盘K的开盘ts（用于等待机制，避免反复重建缓存）
    static long getLatestClosedOpenTsFromDb(String bar, long barMs, long nowMs) {
        try {
            // 只取尾部几根，成本很低
            List<Candle> tail = fetchRecentByCount(bar, 8);
            List<Candle> closed = filterClosedByTime(tail, barMs, nowMs, SAFE_CLOSE_MS);
            if (closed == null || closed.isEmpty()) return -1L;
            closed.sort(Comparator.comparingLong(c -> c.ts));
            return closed.get(closed.size() - 1).ts;
        } catch (Exception e) {
            if (DEBUG_REALTIME) {
                System.out.println("[WAIT][ERR] getLatestClosedOpenTsFromDb failed: " + e.getMessage());
            }
            return -1L;
        }
    }

    // ===================== 工具：从 m30Latest 中找到“下一根K”（ts 更大）用于 next-open 入场 =====================
    // ===================== 工具：从 latest 中找到“紧挨着的下一根K”（严格按时间轴） =====================
// closedTs：信号K0（已收盘K）的开盘时间
// 30m 系统：下一根唯一正确时间 = closedTs + BAR30_MS
    static Candle findNextCandleAfter(List<Candle> candles, long closedTs) {
        if (candles == null || candles.isEmpty()) return null;
        long targetTs = closedTs + BAR30_MS;

        ArrayList<Candle> list = new ArrayList<>(candles);
        list.sort(Comparator.comparingLong(c -> c.ts));

        for (Candle c : list) {
            if (c != null && c.ts == targetTs) return c;
        }

        if (DEBUG_REALTIME) {
            long minTs = list.get(0).ts;
            long maxTs = list.get(list.size() - 1).ts;
            System.out.println("[DEBUG next] missing next candle: closedTs="
                    + FMT_JST.format(Instant.ofEpochMilli(closedTs))
                    + " targetTs=" + FMT_JST.format(Instant.ofEpochMilli(targetTs))
                    + " latestRange=" + FMT_JST.format(Instant.ofEpochMilli(minTs))
                    + " ~ " + FMT_JST.format(Instant.ofEpochMilli(maxTs)));
        }
        return null;
    }
    // ===================== 工具：给 scanHistory 用的“补一根 next-open 占位K” =====================
    // 原因：实盘 next-open 入场会用 market/candles 拿到【下一根未收K】的开盘价；
    // 但 scanHistory/回测若只使用“已收盘K”，就会把最新一笔的入场时间/入场价左移一根（提前30m）。
    // 解决：在不引入未来函数的前提下，追加一根 next-open 占位K：
    // - ts=targetTs
    // - o=真实开盘价（来自 latest）
    // - h/l/c 全部钉死为 o（避免用到未收盘的高低收）
    static List<Candle> extendWithNextOpenPlaceholder(List<Candle> m30Closed, List<Candle> m30Latest) {
        if (m30Closed == null) return Collections.emptyList();
        ArrayList<Candle> out = new ArrayList<>(m30Closed);
        out.sort(Comparator.comparingLong(c -> c.ts));
        if (out.isEmpty()) return out;

        long lastClosedTs = out.get(out.size() - 1).ts;
        long targetTs = lastClosedTs + BAR30_MS;

        // 已经有 targetTs（说明下一根也已收盘），无需补
        if (out.get(out.size() - 1).ts >= targetTs) return out;

        Candle next = findNextCandleAfter(m30Latest, lastClosedTs);
        if (next != null && next.ts == targetTs) {
            Candle placeholder = new Candle(next.ts, next.o, next.o, next.o, next.o, next.vol, 0);
            out.add(placeholder);

            if (DEBUG_REALTIME) {
                System.out.printf(Locale.US,
                        "[FIX-HIST-ALIGN] appended next-open placeholder: open=%s close=%s o=%.2f (h/l/c frozen)\n",
                        FMT_JST.format(Instant.ofEpochMilli(targetTs)),
                        FMT_JST.format(Instant.ofEpochMilli(targetTs + BAR30_MS)),
                        next.o);
            }
        }
        return out;
    }



    // ===================== 工具：MACD 序列只做长度对齐（placeholder 不参与计算） =====================
    // 做法：基于“已收盘K”的 macd30 结果，把最后一根已收盘K的 MACD 值复制到 placeholder 上，仅用于对齐长度。
    // 这样 next-open 占位K 只负责【时间/开盘价对齐】，不会把未来开盘价带进 MACD EMA 计算。
    static List<MacdPoint> extendMacdByRepeatingLast(List<MacdPoint> macdClosed, int targetSize) {
        if (macdClosed == null) return Collections.emptyList();
        int n = macdClosed.size();
        if (targetSize <= n) return macdClosed;

        ArrayList<MacdPoint> out = new ArrayList<>(targetSize);
        out.addAll(macdClosed);

        if (n == 0) return out;
        MacdPoint last = macdClosed.get(n - 1);

        while (out.size() < targetSize) {
            MacdPoint p = new MacdPoint();
            p.dif = last.dif;
            p.dea = last.dea;
            p.hist = last.hist;
            out.add(p);
        }
        return out;
    }

    // =====================  OKX 撑压线计算（Support/Resistance） =====================
    static class SrLevels {
        double resistance;
        double support;
        int usedBars;
        boolean valid;
    }

    // 使用最近 N 根“已收盘K”（m30 列表本身就是 confirm=1 的已收盘K）计算：
    // resistance = 最高点；support = 最低点
    static SrLevels calcOkxSrLevels(List<Candle> m30, int endIdxInclusive, int n) {
        SrLevels sr = new SrLevels();
        if (m30 == null || m30.isEmpty() || endIdxInclusive < 0) return sr;
        int end = Math.min(endIdxInclusive, m30.size() - 1);
        int start = Math.max(0, end - n + 1);
        if (end - start + 1 < Math.min(10, n)) { // 太少就认为无效（避免刚启动窗口不足）
            sr.valid = false;
            sr.usedBars = end - start + 1;
            return sr;
        }
        double r = -Double.MAX_VALUE;
        double s = Double.MAX_VALUE;
        for (int k = start; k <= end; k++) {
            Candle c = m30.get(k);
            if (c.h > r) r = c.h;
            if (c.l < s) s = c.l;
        }
        sr.resistance = r;
        sr.support = s;
        sr.usedBars = end - start + 1;
        sr.valid = true;
        return sr;
    }

    static void updateOkxSrCacheForPrint(List<Candle> m30Closed, int endIdx) {
        try {
            SrLevels sr = calcOkxSrLevels(m30Closed, endIdx, OKX_SR_N);
            lastOkxSrValidForPrint = sr.valid;
            lastOkxSrUsedBarsForPrint = sr.usedBars;
            if (sr.valid) {
                lastOkxResistanceForPrint = sr.resistance;
                lastOkxSupportForPrint = sr.support;
            }
        } catch (Exception ignore) {
        }
    }

    static boolean hitOkxSrTP(Position pos, Candle c0, double srTpPx) {
        if (pos == null) return false;
        if (srTpPx <= 0 || Double.isNaN(srTpPx) || Double.isInfinite(srTpPx)) return false;
        //  你要求：必须在蜡烛线范围内才允许触发
        return (srTpPx >= c0.l && srTpPx <= c0.h);
    }


    // ===================== 放量滚动防守出场（无未来函数） =====================
    // 触发：上一根已收盘K(i-1) 放量（vol > MA(N)*Vmult），且当前已收盘K(i) 未能延续破位/突破
    // 空：若 i.low >= preLow(prev=i-1) → 破位失败 → 当前收盘出场
    // 多：若 i.high <= preHigh(prev=i-1) → 突破失败 → 当前收盘出场
    // lossGate=true：仅在浮亏时才允许触发（防止砍利润）
    static boolean hitVolRollExit(Position pos, List<Candle> m30, int i) {
        if (!USE_VOLROLL_EXIT) return false;
        if (pos == null || m30 == null) return false;
        if (i <= 0 || i >= m30.size()) return false;
        // ★ [FIX] 最小持仓根数：入场根不允许同根出场，防止 entryNotional=0 时 lossGate 误判
        if (VOLROLL_MIN_HOLD_BARS > 0) {
            int held = barsHeldSinceEntry(m30, i, pos.entryTs);
            if (held < VOLROLL_MIN_HOLD_BARS) return false;
        }
        if (VOLROLL_STEPS != 1) {
            // 当前落地为 steps=1（无状态）。若未来要做 steps=2/3，需要引入“放量基准K状态机”。
        }
        Candle prev = m30.get(i - 1);
        Candle cur  = m30.get(i);

        // 1) 放量判定：用上一根 prev 的 vol，对比 prev 结束时的 MA(N)
        double ma = volMA(m30, i - 1, VOLROLL_N);
        if (!Double.isFinite(ma) || ma <= 0) return false;
        boolean surge = prev.vol > ma * VOLROLL_VMULT;
        if (!surge) return false;

        // 2) preLow/preHigh：基于“上一根(prev)”的前三根（仅历史，不含未来）
        double preLow = preLowOfPrev(m30, i - 1, VOLROLL_PRE_BARS);
        double preHigh = preHighOfPrev(m30, i - 1, VOLROLL_PRE_BARS);
        if (!Double.isFinite(preLow) || !Double.isFinite(preHigh)) return false;

        // 3) 破位/突破失败 → 出场
        boolean fail;
        if (pos.side == Side.SHORT) {
            fail = cur.l >= preLow;
        } else {
            fail = cur.h <= preHigh;
        }
        if (!fail) return false;

        // 4) lossGate：仅浮亏时触发（用当前收盘价计算浮动盈亏）
        if (VOLROLL_LOSS_GATE) {
            if (pos.entry > 0) {
                double floatPnl;
                double notional = (pos.entryNotional > 0 ? pos.entryNotional : 0.0);
                if (notional > 0) {
                    // 有名义金额：按持仓量计算浮动盈亏
                    double qty = notional / pos.entry;
                    floatPnl = (pos.side == Side.LONG) ? qty * (cur.c - pos.entry) : qty * (pos.entry - cur.c);
                } else {
                    // entryNotional=0（状态恢复或未赋值）：退化为按点数判断，避免lossGate静默失效
                    floatPnl = (pos.side == Side.LONG) ? (cur.c - pos.entry) : (pos.entry - cur.c);
                }
                if (floatPnl >= 0) return false;
            }
        }

        if (PRINT_VOLROLL_EXIT) {
            System.out.printf(Locale.US,
                    "【放量滚动防守出场】ts=%s | side=%s | prevVol=%.2f | MA%d=%.2f | vmult=%.2f | preLow=%.2f | preHigh=%.2f | cur(H/L/C)=%.2f/%.2f/%.2f | lossGate=%s → 触发%n",
                    FMT_JST.format(Instant.ofEpochMilli(cur.ts + BAR30_MS)),
                    (pos.side == Side.LONG ? "做多" : "做空"),
                    prev.vol, VOLROLL_N, ma, VOLROLL_VMULT,
                    preLow, preHigh,
                    cur.h, cur.l, cur.c,
                    (VOLROLL_LOSS_GATE ? "是" : "否")
            );
        }

        return true;
    }

    static double volMA(List<Candle> m30, int endIdx, int n) {
        if (m30 == null || m30.isEmpty()) return Double.NaN;
        if (endIdx < 0) return Double.NaN;
        int l = Math.max(0, endIdx - n + 1);
        int r = endIdx;
        int cnt = 0;
        double sum = 0.0;
        for (int i = l; i <= r; i++) {
            double v = m30.get(i).vol;
            if (!Double.isFinite(v)) continue;
            sum += v;
            cnt++;
        }
        if (cnt < Math.max(3, n / 2)) return Double.NaN;
        return sum / cnt;
    }

    // prevIdx 指的是“上一根K”的索引；preBars=3 => 用 prevIdx-3 .. prevIdx-1
    static double preLowOfPrev(List<Candle> m30, int prevIdx, int preBars) {
        int end = prevIdx - 1;
        int start = end - preBars + 1;
        if (start < 0) return Double.NaN;
        double mn = Double.POSITIVE_INFINITY;
        for (int i = start; i <= end; i++) mn = Math.min(mn, m30.get(i).l);
        return Double.isFinite(mn) ? mn : Double.NaN;
    }

    static double preHighOfPrev(List<Candle> m30, int prevIdx, int preBars) {
        int end = prevIdx - 1;
        int start = end - preBars + 1;
        if (start < 0) return Double.NaN;
        double mx = Double.NEGATIVE_INFINITY;
        for (int i = start; i <= end; i++) mx = Math.max(mx, m30.get(i).h);
        return Double.isFinite(mx) ? mx : Double.NaN;
    }

    static class MacdPoint {
        double dif, dea, hist;

        boolean isRed() {
            return hist >= 0;
        }
    }

    // =====================  TSRND 外层投票：指标计算缓存（仅用已收盘K线，无未来函数） =====================
    static final class OuterVoteDecision {
        int score;                 // 0/2/3/4/5(=long/short+overall票累计)
        double voteScale;          // 1.0 / 1.2 / 1.3（按score档位）
        boolean dirPass;           // 方向票是否通过（=2分基础）

        // 当前“主总体票”的通过情况（由 TSRND_OVERALL_MODE 决定）
        boolean overallPass;       // 总体票通过（=+1 或 +2）

        // 当 TSRND_OVERALL_MODE=2（两套总体都算）时：分别记录两套总体是否通过
        boolean overallPass1;      // 旧总体 Top1
        boolean overallPass2;      // 新总体 Top1

        int dirStrength;           // 方向组合强度（累积）
        int overallStrength;       // 总体组合强度（累积）——旧总体
        int overallStrength2;      // 总体组合强度（累积）——新总体

        // FailMonth 总体票（COMBO7：5-bin bin/agree）
        boolean failmPass;
        int failmStrength;
        int failmBin;

        // FailMonth 额外票（已精简：SUPER_TREND_10_3.0 + PS_DON_96，两者一致才 +1）
        boolean failmCombo5Pass;

        int overallMode;           // 0/1/2
        boolean strictBlocked;     // 是否被“总体严格门”拦截
        String detail;
    }

    // ===============================
    // TSRND FAILM 额外票投票器（与挖掘器同源：ConditionFactory）
    // [重构后] 仅 2 个子指标：SuperTrend(10,3.0) + Donchian96；两者同向且非0才通过（给外层投票 +1）
    //   原 5 指标中的 RSI(mid60)/ROC48/BBWidth30 已删（共线动量 + BBWidth 方向误用）。
    // 无未来函数：只对 [0..sigIdx] 的已收盘K线求值（toExclusive = idx+1）
    // ===============================
    static final class TsRndFailmCombo5Voter {
        static final int EVAL_HISTORY_BARS = 3000;

        static volatile boolean inited = false;
        static PoolCondition PC_SUPER;
        static PoolCondition PC_PSDON;
        // [重构] 已移除 RSI14_mid60 / ROC48_thr0.01 / BBWidth30_thr0.015 三个子指标：
        //   - RSI(mid60)、ROC48 与趋势核共线（冗余动量），删；
        //   - BBWidth 为非方向性波动率，原作方向票属误用，移出（分散候选另行处理）。
        // 现仅保留 SuperTrend + Donchian 两个，consensus 同步降为 2 指标一致。

        static void ensureInit() {
            if (inited) return;
            synchronized (TsRndFailmCombo5Voter.class) {
                if (inited) return;
                try {
                    PC_SUPER = ConditionFactory.superTrend(10, 3.0);       // SUPER_TREND_10_3.0 [机制分] 4.0→3.0：对齐主力(ALL)门既有标准乘数，去私货
                    PC_PSDON = ConditionFactory.psDonchian(96);            // PS_DON_96
                } catch (Throwable t) {
                    // 反射/依赖缺失时：直接不启用，避免影响主逻辑
                    PC_SUPER = null;
                    PC_PSDON = null;
                } finally {
                    inited = true;
                }
            }
        }

        static boolean pass(List<Candle> m30, int sigIdx, Side side) {
            try {
                ensureInit();
                if (PC_SUPER == null || PC_PSDON == null) return false;
                if (m30 == null || m30.isEmpty()) return false;
                if (sigIdx < 0 || sigIdx >= m30.size()) return false;

                int start = Math.max(0, sigIdx - EVAL_HISTORY_BARS);
                List<Candle> seg = m30.subList(start, sigIdx + 1);
                int idx = sigIdx - start;

                List<Object> evalCandles = EvalCandleAdapter.toEvalCandles(seg);
                if (evalCandles == null || evalCandles.isEmpty()) return false;
                if (idx < 0 || idx >= evalCandles.size()) return false;

                int d1 = dirAt(PC_SUPER, evalCandles, idx);   // SuperTrend
                int d2 = dirAt(PC_PSDON, evalCandles, idx);   // Donchian96

                int dir = consensus2(d1, d2);
                int want = (side == Side.LONG ? 1 : -1);
                return dir == want;
            } catch (Throwable t) {
                return false;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        static int dirAt(PoolCondition pc, List<Object> evalCandles, int idx) throws Exception {
            // PoolCondition.dir(from, to) 的 to 为【toExclusive】，因此要传 idx+1
            int[] arr = pc.dir((List) evalCandles, 0, idx + 1);
            if (arr == null || arr.length == 0) return 0;
            return arr[arr.length - 1];
        }

        static int consensus2(int a, int b) {
            // 保留原一致性语义：任一中性(0)即不成立；两者同向才给方向
            if (a == 0 || b == 0) return 0;
            if (a > 0 && b > 0) return +1;
            if (a < 0 && b < 0) return -1;
            return 0;
        }
    }

    static final class TsRndCache {
        int n = 0;
        long endTs = Long.MIN_VALUE;
        double[] close;
        double[] high;
        double[] low;

        double[] ema20, ema60, ema100, ema200;
        double[] plusDI14, minusDI14;
        double[] cci20;
        double[] stochD14_3;
        double[] lastPivotHigh6_6, lastPivotLow6_6;

        // 新总体票所需：SQUEEZE20_20（ROC12/BBWidth20 已随重构移除）
        double[] squeeze20_20;
    }

    static TsRndCache TSRND_CACHE = null;

    static void ensureTsRndCache(List<Candle> m30) {
        if (m30 == null || m30.isEmpty()) return;
        int n = m30.size();
        long endTs = m30.get(n - 1).ts;
        if (TSRND_CACHE != null && TSRND_CACHE.n == n && TSRND_CACHE.endTs == endTs) return;

        TsRndCache c = new TsRndCache();
        c.n = n;
        c.endTs = endTs;
        c.close = new double[n];
        c.high = new double[n];
        c.low = new double[n];
        for (int i = 0; i < n; i++) {
            Candle k = m30.get(i);
            c.close[i] = k.c;
            c.high[i] = k.h;
            c.low[i] = k.l;
        }

        c.ema20 = buildEma(c.close, 20);
        c.ema60 = buildEma(c.close, 60);
        c.ema100 = buildEma(c.close, 100);
        c.ema200 = buildEma(c.close, 200);

        double[][] di = buildDi14(c.high, c.low, c.close, 14);
        c.plusDI14 = di[0];
        c.minusDI14 = di[1];

        c.cci20 = buildCci20(c.high, c.low, c.close, 20);
        c.stochD14_3 = buildStochD(c.high, c.low, c.close, 14, 3);

        double[][] piv = buildPivotLast(c.high, c.low, 6, 6);
        c.lastPivotHigh6_6 = piv[0];
        c.lastPivotLow6_6 = piv[1];

        // 新总体票：SQUEEZE20_20（ROC12/BBWidth20 已随重构移除）
        // 与 Pool-2 一致：SQUEEZE(20,2.0) vs Keltner(20,1.5)，1=挤压中，0=非挤压
        c.squeeze20_20 = p2_squeeze(m30, 20, 2.0, 20, 1.5);

        TSRND_CACHE = c;
    }

    static double[] buildEma(double[] close, int period) {
        int n = close.length;
        double[] ema = new double[n];
        double alpha = 2.0 / (period + 1.0);

        ema[0] = close[0];
        for (int i = 1; i < n; i++) {
            ema[i] = alpha * close[i] + (1.0 - alpha) * ema[i - 1];
        }
        return ema;
    }   //  这一行很关键

    static double[] buildRoc(double[] close, int period) {
        int n = close.length;
        double[] roc = new double[n];
        if (n <= period) return roc;

        for (int i = period; i < n; i++) {
            double prev = close[i - period];
            if (!Double.isFinite(prev) || prev == 0.0) continue;
            roc[i] = (close[i] - prev) / prev; // e.g. 0.01 = +1%
        }
        return roc;
    }


    static double[] buildBbWidth20(double[] close, int period) {
        int n = close.length;
        double[] out = new double[n];
        Arrays.fill(out, Double.NaN);
        if (period <= 1) return out;
        double sum = 0.0;
        double sumSq = 0.0;
        for (int i = 0; i < n; i++) {
            double x = close[i];
            sum += x;
            sumSq += x * x;
            if (i >= period) {
                double old = close[i - period];
                sum -= old;
                sumSq -= old * old;
            }
            if (i >= period - 1) {
                double mean = sum / period;
                double var = Math.max(0.0, sumSq / period - mean * mean);
                double std = Math.sqrt(var);
                double mid = mean;
                if (Math.abs(mid) < 1e-12) out[i] = Double.NaN;
                else out[i] = (4.0 * std) / Math.abs(mid); // (upper-lower)/mid, upper/lower=mid±2std
            }
        }
        return out;
    }




    static double[][] buildDi14(double[] high, double[] low, double[] close, int period) {
        int n = close.length;
        double[] plusDI = new double[n];
        double[] minusDI = new double[n];
        for (int i = 0; i < n; i++) { plusDI[i] = Double.NaN; minusDI[i] = Double.NaN; }
        if (n <= period) return new double[][]{plusDI, minusDI};

        double[] tr = new double[n];
        double[] plusDM = new double[n];
        double[] minusDM = new double[n];

        tr[0] = high[0] - low[0];
        plusDM[0] = 0.0;
        minusDM[0] = 0.0;

        for (int i = 1; i < n; i++) {
            double up = high[i] - high[i - 1];
            double down = low[i - 1] - low[i];
            plusDM[i] = (up > down && up > 0) ? up : 0.0;
            minusDM[i] = (down > up && down > 0) ? down : 0.0;

            double r1 = high[i] - low[i];
            double r2 = Math.abs(high[i] - close[i - 1]);
            double r3 = Math.abs(low[i] - close[i - 1]);
            tr[i] = Math.max(r1, Math.max(r2, r3));
        }

        double smTR = 0.0, smPlus = 0.0, smMinus = 0.0;
        for (int i = 1; i <= period; i++) {
            smTR += tr[i];
            smPlus += plusDM[i];
            smMinus += minusDM[i];
        }

        // Wilder smoothing
        for (int i = period + 1; i < n; i++) {
            smTR = smTR - (smTR / period) + tr[i];
            smPlus = smPlus - (smPlus / period) + plusDM[i];
            smMinus = smMinus - (smMinus / period) + minusDM[i];

            if (smTR > 0) {
                plusDI[i] = 100.0 * (smPlus / smTR);
                minusDI[i] = 100.0 * (smMinus / smTR);
            }
        }
        return new double[][]{plusDI, minusDI};
    }

    static double[] buildCci20(double[] high, double[] low, double[] close, int period) {
        int n = close.length;
        double[] cci = new double[n];
        for (int i = 0; i < n; i++) cci[i] = Double.NaN;
        if (n < period) return cci;

        double[] tp = new double[n];
        for (int i = 0; i < n; i++) tp[i] = (high[i] + low[i] + close[i]) / 3.0;

        for (int i = period - 1; i < n; i++) {
            double sum = 0.0;
            for (int j = i - period + 1; j <= i; j++) sum += tp[j];
            double sma = sum / period;

            double devSum = 0.0;
            for (int j = i - period + 1; j <= i; j++) devSum += Math.abs(tp[j] - sma);
            double meanDev = devSum / period;

            if (meanDev > 0) cci[i] = (tp[i] - sma) / (0.015 * meanDev);
            else cci[i] = 0.0;
        }
        return cci;
    }

    static double[] buildTsi(double[] close, int r, int s) {
        int n = close.length;
        double[] tsi = new double[n];
        for (int i = 0; i < n; i++) tsi[i] = Double.NaN;
        if (n < 2) return tsi;

        double[] m = new double[n];
        double[] absM = new double[n];
        m[0] = 0.0;
        absM[0] = 0.0;
        for (int i = 1; i < n; i++) {
            m[i] = close[i] - close[i - 1];
            absM[i] = Math.abs(m[i]);
        }

        double[] ema1 = buildEma(m, r);
        double[] ema2 = buildEma(ema1, s);
        double[] a1 = buildEma(absM, r);
        double[] a2 = buildEma(a1, s);

        for (int i = 0; i < n; i++) {
            if (a2[i] != 0) tsi[i] = 100.0 * (ema2[i] / a2[i]);
            else tsi[i] = 0.0;
        }
        return tsi;
    }

    static double[] buildStochD(double[] high, double[] low, double[] close, int kPeriod, int dPeriod) {
        int n = close.length;
        double[] k = new double[n];
        double[] d = new double[n];
        for (int i = 0; i < n; i++) { k[i] = Double.NaN; d[i] = Double.NaN; }
        if (n < kPeriod) return d;

        for (int i = kPeriod - 1; i < n; i++) {
            double hh = high[i];
            double ll = low[i];
            for (int j = i - kPeriod + 1; j <= i; j++) {
                hh = Math.max(hh, high[j]);
                ll = Math.min(ll, low[j]);
            }
            double denom = hh - ll;
            if (denom <= 0) k[i] = 50.0;
            else k[i] = 100.0 * (close[i] - ll) / denom;
        }

        for (int i = 0; i < n; i++) {
            if (i < kPeriod - 1 + (dPeriod - 1)) continue;
            double sum = 0.0;
            int cnt = 0;
            for (int j = i - dPeriod + 1; j <= i; j++) {
                if (!Double.isNaN(k[j])) { sum += k[j]; cnt++; }
            }
            d[i] = (cnt > 0) ? (sum / cnt) : Double.NaN;
        }
        return d;
    }

    // 返回：lastPivotHighAt[i], lastPivotLowAt[i]（i 时刻可见的“已确认 pivot”）
    static double[][] buildPivotLast(double[] high, double[] low, int left, int right) {
        int n = high.length;
        double[] lastPH = new double[n];
        double[] lastPL = new double[n];
        double ph = Double.NaN, pl = Double.NaN;

        for (int i = 0; i < n; i++) {
            int j = i - right; // 在 i 时刻可以确认 j 是否 pivot（需要右侧 right 根都已存在）
            if (j >= left && j >= 0) {
                // pivot high
                boolean isPH = true;
                double hj = high[j];
                for (int t = j - left; t <= j + right; t++) {
                    if (t < 0 || t >= n) continue;
                    if (high[t] > hj) { isPH = false; break; }
                }
                if (isPH) ph = hj;

                // pivot low
                boolean isPL = true;
                double lj = low[j];
                for (int t = j - left; t <= j + right; t++) {
                    if (t < 0 || t >= n) continue;
                    if (low[t] < lj) { isPL = false; break; }
                }
                if (isPL) pl = lj;
            }
            lastPH[i] = ph;
            lastPL[i] = pl;
        }
        return new double[][]{lastPH, lastPL};
    }

    static int sign(double x) { return x > 0 ? 1 : (x < 0 ? -1 : 0); }
    static int signInt(int x) { return x > 0 ? 1 : (x < 0 ? -1 : 0); }

    // 计算某一根K的“组件方向”（-1/0/+1）
    static int tsrndDirEma(double close, double ema) {
        if (Double.isNaN(ema)) return 0;
        return close >= ema ? 1 : -1;
    }
    static int tsrndDirAdxDi(double plusDI, double minusDI) {
        if (Double.isNaN(plusDI) || Double.isNaN(minusDI)) return 0;
        if (plusDI > minusDI) return 1;
        if (plusDI < minusDI) return -1;
        return 0;
    }
    static int tsrndDirCci(double cci) {
        if (Double.isNaN(cci)) return 0;
        if (cci > 100.0) return 1;
        if (cci < -100.0) return -1;
        return 0;
    }
    static int tsrndDirTsi(double tsi) {
        if (Double.isNaN(tsi)) return 0;
        return sign(tsi);
    }
    static int tsrndDirStochBand(double d) {
        if (Double.isNaN(d)) return 0;
        if (d <= 20.0) return 1;
        if (d >= 80.0) return -1;
        return 0;
    }
    static int tsrndDirPivot(double close, double lastPH, double lastPL) {
        if (!Double.isNaN(lastPH) && close > lastPH) return 1;
        if (!Double.isNaN(lastPL) && close < lastPL) return -1;
        return 0;
    }
    static int tsrndDirMacd(MacdPoint p) {
        if (p == null) return 0;
        if (p.hist > 0) return 1;
        if (p.hist < 0) return -1;
        return 0;
    }

    static int tsrndDirRocThr(double roc, double thr) {
        if (!Double.isFinite(roc)) return 0;
        if (roc > thr) return 1;
        if (roc < -thr) return -1;
        return 0;
    }



    static int tsrndDirBbWidthThr(double close, double ema20, double bbWidth, double thr) {
        if (!Double.isFinite(bbWidth)) return 0;
        if (bbWidth < thr) return 0;
        if (!Double.isFinite(ema20)) return 0;
        return close >= ema20 ? 1 : -1;
    }

    //  eval 同款 5-bin mapping（strength∈[-L, L]）
    static int tsrndTo5Bin(int strength, int L) {
        if (L <= 0) return 2;
        int tWeak = Math.max(1, (int) Math.ceil(0.2 * L));
        int tStrong = Math.max(tWeak + 1, (int) Math.ceil(0.6 * L));

        if (strength <= -tStrong) return 0;
        if (strength <= -tWeak) return 1;
        if (Math.abs(strength) < tWeak) return 2;
        if (strength < tStrong) return 3;
        return 4;
    }

    static boolean tsrndIsAgreeBin(Side side, int bin) {
        if (side == Side.LONG) return (bin >= 3);
        if (side == Side.SHORT) return (bin <= 1);
        return false;
    }

    // ======================================================================
    // [重构·预留构件] 以下两个方法当前【未接入任何决策路径】，不被调用，无运行时影响。
    // 它们承接「ROC 提进趋势核」「BBWidth 留作分散候选」两项意图的清洗结果，
    // 待趋势核 / 分散层正式建立后再显式接线（接线前请先做扰动+样本外验证）。
    // ======================================================================

    /**
     * 趋势核候选（方向）：单尺度 ROC，阈值改用 ATR 相对值，消除固定 0.01 的价格区间依赖。
     * @param atrPctAtI 当前bar的 ATR/价格（相对波动），由调用方传入
     * @param k         ATR 倍数（建议 0.3 起步，定值前务必扰动测试）
     * @return +1/-1/0
     * 【未接入】
     */
    static int rocAtrRelDir(double[] close, int i, int rocPeriod, double atrPctAtI, double k) {
        if (close == null || i < rocPeriod || i >= close.length) return 0;
        double base = close[i - rocPeriod];
        if (!(base > 0)) return 0;
        double roc = close[i] / base - 1.0;
        double thr = k * atrPctAtI;
        if (!Double.isFinite(roc) || !Double.isFinite(thr) || thr <= 0) return 0;
        if (roc >  thr) return +1;
        if (roc < -thr) return -1;
        return 0;
    }

    /**
     * 分散候选（非方向）：BBWidth 仅作波动率 regime 门，修正原 tsrndDirBbWidthThr 的方向误用。
     * 返回 true=带宽达到阈值（波动率足够），不含任何多空方向。
     * 接入分散层时应与趋势核做相关性检验，仅低相关才纳入。
     * 【未接入】
     */
    static boolean bbWidthRegimeGate(double bbWidth, double thr) {
        return Double.isFinite(bbWidth) && bbWidth >= thr;
    }

    static int tsrndDirSqueeze(double close, double ema20, double squeeze20_20) {
        // 1=挤压中（方向信息弱） -> 0；非挤压 -> close vs EMA20
        if (Double.isFinite(squeeze20_20) && squeeze20_20 > 0.5) return 0;
        if (!Double.isFinite(ema20)) return 0;
        return close >= ema20 ? 1 : -1;
    }


    static OuterVoteDecision evalTsRndOuterVote(List<Candle> m30, List<MacdPoint> macd30, int sigIdx, Side side, String phase, boolean silent) {
        OuterVoteDecision d = new OuterVoteDecision();
        d.score = 0;
        d.voteScale = 1.0;
        d.dirPass = false;

        d.overallMode = TSRND_OVERALL_MODE;
        d.overallPass = false;
        d.overallPass1 = false;
        d.overallPass2 = false;

        d.dirStrength = 0;
        d.overallStrength = 0;
        d.overallStrength2 = 0;
        d.failmStrength = 0;
        d.failmBin = 2;
        d.failmPass = false;

        d.failmCombo5Pass = false;

        d.strictBlocked = false;
        d.detail = "";

        if (!USE_TSRND_OUTER_VOTE) {
            d.dirPass = true;
            d.score = 2;
            d.voteScale = TSRND_SCALE_SCORE2;
            return d;
        }
        if (m30 == null || macd30 == null) return d;
        if (sigIdx <= 0 || sigIdx >= m30.size()) return d;

        ensureTsRndCache(m30);
        if (TSRND_CACHE == null) return d;
        TsRndCache c = TSRND_CACHE;

        int lookback = TSRND_LOOKBACK_BARS;
        int start = Math.max(0, sigIdx - lookback + 1);
        int overallStrength1 = 0;
        int overallStrength2 = 0;
        int failmStrength = 0;
        int dirStrength = 0;

        for (int i = start; i <= sigIdx; i++) {
            int sOverall1 = 0;
            int sOverall2 = 0;

            if (TSRND_OVERALL_MODE == 0 || TSRND_OVERALL_MODE == 2) {
                // 旧总体 Top1（已移除 TSI25_13）：EMA20+EMA60+EMA100+EMA200+ADX14_DI+CCI20_thr100
                sOverall1 += tsrndDirEma(c.close[i], c.ema20[i]);
                sOverall1 += tsrndDirEma(c.close[i], c.ema60[i]);
                sOverall1 += tsrndDirEma(c.close[i], c.ema100[i]);
                sOverall1 += tsrndDirEma(c.close[i], c.ema200[i]);
                sOverall1 += tsrndDirAdxDi(c.plusDI14[i], c.minusDI14[i]);
                sOverall1 += tsrndDirCci(c.cci20[i]);
                overallStrength1 += sOverall1;
            }

            if (TSRND_OVERALL_MODE == 1 || TSRND_OVERALL_MODE == 2) {
                // 新总体 Top1（已移除 ROC12 / TSI25_13）：EMA60+EMA100+EMA200+ADX14_DI+SQUEEZE20_20
                sOverall2 += tsrndDirEma(c.close[i], c.ema60[i]);
                sOverall2 += tsrndDirEma(c.close[i], c.ema100[i]);
                sOverall2 += tsrndDirEma(c.close[i], c.ema200[i]);
                sOverall2 += tsrndDirAdxDi(c.plusDI14[i], c.minusDI14[i]);
                sOverall2 += tsrndDirSqueeze(c.close[i], c.ema20[i], (c.squeeze20_20 != null ? c.squeeze20_20[i] : Double.NaN));
                overallStrength2 += sOverall2;
            }

            // FailMonth 总体票（已移除 ROC12 / TSI25_13 / BBWidth20）：EMA60+EMA100+EMA200+STOCH14_3_band
            if (TSRND_FAILM_VOTER_ENABLE) {
                int sFailm = 0;
                sFailm += tsrndDirEma(c.close[i], c.ema60[i]);
                sFailm += tsrndDirEma(c.close[i], c.ema100[i]);
                sFailm += tsrndDirEma(c.close[i], c.ema200[i]);
                sFailm += tsrndDirStochBand(c.stochD14_3[i]);
                failmStrength += sFailm;
            }

            int sDir = 0;
            if (side == Side.LONG) {
                // Long Top1：EMA100+ADX14_DI+STOCH14_3_band+CCI20_thr100+PS_PIVOT_6_6
                sDir += tsrndDirEma(c.close[i], c.ema100[i]);
                sDir += tsrndDirAdxDi(c.plusDI14[i], c.minusDI14[i]);
                sDir += tsrndDirStochBand(c.stochD14_3[i]);
                sDir += tsrndDirCci(c.cci20[i]);
                sDir += tsrndDirPivot(c.close[i], c.lastPivotHigh6_6[i], c.lastPivotLow6_6[i]);
            } else {
                // Short Top1（已移除 TSI25_13）：EMA100+EMA200+MACD12_26_9+CCI20_thr100
                sDir += tsrndDirEma(c.close[i], c.ema100[i]);
                sDir += tsrndDirEma(c.close[i], c.ema200[i]);
                sDir += tsrndDirMacd(macd30.get(i));
                sDir += tsrndDirCci(c.cci20[i]);
            }
            dirStrength += sDir;
        }

        d.overallStrength = overallStrength1;
        d.overallStrength2 = overallStrength2;
        d.failmStrength = failmStrength;
        d.dirStrength = dirStrength;
        d.overallMode = TSRND_OVERALL_MODE;

        int dirDir = signInt(dirStrength);
        int want = (side == Side.LONG ? 1 : -1);

        d.dirPass = (dirDir == want);

        // 总体票：按模式决定（0=旧，1=新，2=两套都算）
        d.overallPass1 = false;
        d.overallPass2 = false;
        if (TSRND_OVERALL_MODE == 0) {
            int overallDir1 = signInt(overallStrength1);
            d.overallPass = (overallDir1 == want);
        } else if (TSRND_OVERALL_MODE == 1) {
            int overallDir2 = signInt(overallStrength2);
            d.overallPass = (overallDir2 == want);
        } else {
            int overallDir1 = signInt(overallStrength1);
            int overallDir2 = signInt(overallStrength2);
            d.overallPass1 = (overallDir1 == want);
            d.overallPass2 = (overallDir2 == want);
            d.overallPass = (d.overallPass1 || d.overallPass2);
        }

        // FailMonth 票：用 eval 同款 5-bin 规则判断 agree
        if (TSRND_FAILM_VOTER_ENABLE) {
            int L = lookback * 4; // [重构] 子指标数 7→4，归一化上限同步更新，否则分箱阈值偏松
            d.failmBin = tsrndTo5Bin(failmStrength, L);
            d.failmPass = tsrndIsAgreeBin(side, d.failmBin);
        } else {
            d.failmBin = 2;
            d.failmPass = false;
        }

        // FailMonth 额外票（COMBO5）：按挖掘器同源 ConditionFactory 求当前 bar 的一致方向
        // 无未来函数：只对 [0..sigIdx] 的已收盘K线求值（toExclusive = idx+1）
        d.failmCombo5Pass = TsRndFailmCombo5Voter.pass(m30, sigIdx, side);

        if (d.dirPass) d.score += 2;
        // 总体票（原有两套）
        if (TSRND_OVERALL_MODE == 2) {
            if (d.overallPass1) d.score += 1;
            if (d.overallPass2) d.score += 1;
        } else {
            if (d.overallPass) d.score += 1;
        }
        // FailMonth 总体票（新增第三个总体票）
        if (d.failmPass) d.score += 1;

        // FailMonth 额外票（FAILM_COMBO5：新增第四个总体票）
        if (d.failmCombo5Pass) d.score += 1;

        // 严格门：要求总体票也通过（否则当作无信号）
        if (TSRND_OVERALL_STRICT_GATE && d.dirPass && !(d.overallPass || d.failmPass || d.failmCombo5Pass)) {
            d.strictBlocked = true;
            d.score = 0;
        }
        if (d.score >= 6) d.voteScale = TSRND_SCALE_SCORE5;
        else if (d.score >= 5) d.voteScale = TSRND_SCALE_SCORE3;
        else d.voteScale = TSRND_SCALE_SCORE2;

        d.detail = String.format(Locale.US,
                "phase=%s side=%s mode=%d score=%d scale=%.1f dirStr=%d oStr1=%d oStr2=%d failmStr=%d failmBin=%d failmPass=%s failm5Pass=%s dirPass=%s oPass=%s o1=%s o2=%s strict=%s",
                phase,
                (side == Side.LONG ? "LONG" : "SHORT"),
                d.overallMode,
                d.score, d.voteScale,
                d.dirStrength, d.overallStrength, d.overallStrength2,
                d.failmStrength, d.failmBin, String.valueOf(d.failmPass),
                String.valueOf(d.failmCombo5Pass),
                String.valueOf(d.dirPass),
                String.valueOf(d.overallPass),
                String.valueOf(d.overallPass1),
                String.valueOf(d.overallPass2),
                String.valueOf(d.strictBlocked)
        );

        if (!silent && d.score < TSRND_SCORE_THRESHOLD) {
            System.out.println("【TSRND 外层投票拦截】" + d.detail);
        }
        return d;
    }


    enum Side { LONG, SHORT }
    enum PullbackMode { ATR, FIXED }
    enum Trend1H { 多, 空, 中性 }
    enum ExpectedMetric { SR_GAP, AVG_RANGE }

    static class Trend1HInfo {
        Trend1H trend;
        boolean isFirstNeutralBar;
        boolean isSecondTrendBarAfterNeutral; // “进入中性期的第一根”
        double hist;
        double prevHist;
        int idx; // 当前使用的1H K线索引

        // ===== 新增：用于打印的中间判断结果 =====
        int oscCount;
        int posCnt;
        int negCnt;
        int zeroCnt;

        boolean neutralByAbs;
        boolean neutralByOsc;
        boolean neutralByFlip;
        boolean neutral;
    }

    static class Position {
        public double mult;
        Side side;
        double entry;
        long entryTs;
        long signalTs; // 信号K0开盘时间（用于打印对齐）
        double entryNotional = 0.0; //  入场时锁定的下单名义(USDT)
        boolean isVirtual = false; // 策略A：手动进场虚拟持仓

        //  OKX 撑压线（入场时锁定，用于止盈 & 入场距离过滤）
        double okxResistanceAtEntry = Double.NaN;
        double okxSupportAtEntry = Double.NaN;
        double okxGapAtEntry = Double.NaN; // 入场价到目标撑/压的距离（点数）

        //  本仓位使用的止损点数（固定底线28 \+ 动态ATR）
        double slPoints = SL_POINTS;

        // ===== SRMF（入场时锁定）=====
        double entryEquity = 0.0;     // 入场时资金
        double entryTierMult = 1.0;   // 入场时 SRMF 档位倍率
        double entryMult = 1.0;       // 入场时 最终 mult = 档位倍率 * 投票倍率
        // ===== 信号来源（入场时锁定，用于回测按来源归因统计）=====
        String entrySource = "";       // MACD3K / RANGE_BK / DOUBLE_TB / OKX_COMBO / "" (未知)
        // ===== TSRND 外层投票（入场时锁定，用于复盘/输出）=====
        int entryVoteScore = 0;        // 2 或 3
        double entryVoteScale = 1.0;   // 1.0 / 1.2 / 1.3
        boolean entryVoteDirPass = false;
        boolean entryVoteOverallPass = false;
        String entryVoteDetail = "";
        double slPointsAtEntry = SL_POINTS; // 入场时止损点数（用于R倍数与资金盈亏换算）
        String regimeAtEntry = "STABLE_4";
        double trendPtsAtEntry = 0.0;

        // ===== BAYES（入场时锁定）=====
        double bayesPosterior = 0.5;
        double bayesScale = 1.0;


        //  MFE / MAE（点数）
        double mfe = 0.0; // 最大浮盈点
        double mae = 0.0; // 最大浮亏点（这里用“点数幅度”，越大代表越不利）
    }



    static String posCoreLine(EngineState st) {
        if (st == null) return "pos=null";
        Position p = st.pos;

        // lastProcessedSigTs：记录“本轮推进到的最新信号K0开盘时间”
        // 为了便于你对齐理解，这里同时展示 (开)/(收=entryTs)。
        String lp = (st.lastProcessedSigTs > 0
                ? (fmtOpen(st.lastProcessedSigTs) + "(开)/" + fmtClose(st.lastProcessedSigTs, BAR30_MS) + "(收)")
                : "NA");

        if (p == null) {
            return "posSide=NONE entryTs=NA entryPx=NA isVirtual=false slPts=NA lastProcessedSigTs=" + lp;
        }

        String sig = (p.signalTs > 0
                ? (fmtOpen(p.signalTs) + "(开)/" + fmtClose(p.signalTs, BAR30_MS) + "(收=entryTs)")
                : "NA");

        return String.format(Locale.US,
                "posSide=%s entryTs=%s entryPx=%.2f isVirtual=%s slPts=%.2f signalTs=%s lastProcessedSigTs=%s",
                (p.side == null ? "NA" : p.side),
                fmtTs(p.entryTs),
                p.entry,
                p.isVirtual,
                p.slPoints,
                sig,
                lp
        );
    }

    static void debugPrintPosCore(String tag, EngineState st) {
        if (!DEBUG_POS_TRACE) return;
        System.out.println("[POS-CORE][" + tag + "] " + posCoreLine(st));
    }

    static class CompletedTrade {
        long enterTs, exitTs;
        long signalTs; // 信号K0开盘时间（用于打印对齐）
        Side side;
        double entry, exit;
        double pnlPoints;
        double grossPnlPoints;
        double feePoints;
        boolean isStop;
        String reason;

        // ===== SRMF / 资金曲线（动态R=2%） =====
        double entryEquity;
        double exitEquity;
        double pnlUSDT;
        double rMultiple;     // 本笔相对风险金额（entryEquity*RISK_PCT*mult）的R倍数
        double mult;
        double slPointsUsed;
        String regime;
        double trendPtsAfter;

        // ===== BAYES（入场后验概率，用于复盘/统计）=====
        double bayesPosterior = 0.5;


        double mfe;
        double mae;

        // ===== 信号来源（从 Position 搬入，用于回测按来源归因统计）=====
        String entrySource = "";   // MACD3K / RANGE_BK / DOUBLE_TB / OKX_COMBO / "" (未知)
    }

    static class EnterOpportunity {
        long ts;
        Trend1H t1h;
        Side side;
        double entryRef;

        // ===== 回撤入场（仅用于提示/回测统计）=====
        boolean pbEnabled = false;
        String pbMode = "";
        double atrAtSignal = 0.0;
        double pbAtrPts = 0.0;
        double pbFixedPts = 0.0;
        double pbUsedPts = 0.0;
        double pbLimitPx = 0.0;
        boolean pbFilled = false;
        boolean pbPending = false; // 实盘：下一根未收盘，无法判定成交/未成交
        double entryFillPx = 0.0;
        double dif1, dif2, dea1, dea2;
        int diffInt;
    }

    static class ExitOpportunity {
        long ts;
        Side side;
        double price;
        String reason;
    }

    // ===================== MAIN =====================


    // ---------------- SELF-CHECK / DEBUG helpers ----------------
    private static void log1(String s) {
        System.out.println(s);
    }

    public static void main(String[] args) throws Exception {
        printBuildTag();
        printSelfCheckOnce();
        if (STRICT_SSOT) {
            File f = new File(DB_FILE);
            if (!f.isAbsolute()) {
                throw new IllegalArgumentException("strictSsot=true 时 candlesDb 必须是绝对路径：-Dokx.candlesDb=... | current=" + DB_FILE);
            }
        }
        initAuditDbAndState();
        loadRtAuditIfAny();
        loadHistAuditIfAny();


        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        if (USE_DB_CACHE) {
            CandleDb.init();
            long nowMs0 = System.currentTimeMillis();
            // strictSsot=true 时：本进程必须只读 DB（由独立 Puller 负责写入）
            if (!STRICT_SSOT) {
                // 1) 启动时：只在 DB 很空时做一次“多年补齐”（避免每次启动都全量重刷）
                CandleDb.syncLastYearsIfNeeded(INST_ID, "30m", DB_KEEP_YEARS, nowMs0);
                CandleDb.syncLastYearsIfNeeded(INST_ID, "1H",  DB_KEEP_YEARS, nowMs0);
                // 2) 开局先补最近一段（增量写入）+ 不变量校验
                CandleDb.ingestRecent(INST_ID, "30m", 1200, nowMs0);
                CandleDb.ingestRecent(INST_ID, "1H",  800,  nowMs0);
            }
        }
        printBootStatusOnce();
        new Thread(() -> {
            try { Thread.sleep(200); } catch (Exception ignore) {}
            //alarmOnceInTick(  BAR30_MS);
        }).start();
        // 启动回测：一年（打印进/出场条件 + 月度胜率）
        // Route A：当 VIEW_USE_REPLAY=true 时，默认跳过 boot 推演回测（否则会把“推演进场=22:30”误当成实盘事实）
        // 如需研究推演，可加启动参数：-Dokx.bootResearchBacktest=true
        if (!VIEW_USE_REPLAY || BOOT_RESEARCH_BACKTEST) {
            if (RUN_COMPARE_ON_BOOT) {
                runCompareBacktestOnBoot();
                if (COMPARE_EXIT_AFTER) return;
            } else {
                runBacktestOneYearOnBoot();
            }
        } else {
            System.out.println("[BOOT] VIEW_USE_REPLAY=true -> skip boot backtest; view will load from replay db.");
            try { AuditDb.init(); syncViewFromReplayDb(); } catch (Exception e) {
                System.out.println("[BOOT] replay sync failed: " + e.getMessage());
            }
        }


        initAutoTradeIfEnabled();

        // role: puller=写库进程（strictSsot 必须为 false）；engine=只读策略进程（strictSsot 建议为 true）
        String role = sysStr("okx.role", "engine").toLowerCase(Locale.ROOT);
        if (role.contains("pull")) {
            if (STRICT_SSOT) {
                System.out.println("[PULLER][WARN] strictSsot=true 将禁止写库；请用 -Dokx.strictSsot=false 启动 puller。");
            }
            if (!USE_DB_CACHE) {
                System.out.println("[PULLER][WARN] USE_DB_CACHE=false 时无法写入 SQLite；请开启 DB 缓存。");
            }
            runPullerForever();
            return;
        }

        // engine：开局也跑一次实时刷新（否则要等到下一个 :00/:30 才有输出）
        try {
            runOnce();
        } catch (Exception e) {
            System.out.println("[BOOT] runOnce error: " + e.getMessage());
            e.printStackTrace();
        }

        schedule();
    }

    // 30m 入场判定（仅此方法被修改）
// 规则：
// K2、K1、K0 三根 DIF 均 > 0
// 且严格递增：K2 < K1 < K0
// 且中间涨幅 (K1 - K2) > 1
// ===============================

    // ===============================
    // 30m 入场判定（DIF/DEA 原逻辑 + OKX 组合入场兜底）
    // 先跑你原来的 DIF/DEA 逻辑：若返回 LONG/SHORT 则直接返回；
    // 若返回 null，则进入 OKX 组合：
    //  1) 主力(ALL)：SAR_0.02_0.2 + SUPER_TREND_10_3.0 + PS_NRX_8（同向且非0 -> 允许多/空）
    //  2) 空头专用(SHORT)：STOCH14_3_band + FISHER_P10（两者均为 -1 -> 允许空）
    // ===============================
    static final ThreadLocal<List<Candle>> ENTRY_M30_CTX = new ThreadLocal<>();

    // ★ [BT-DIAG] 信号来源追踪
    static final ThreadLocal<String> LAST_ENTRY_SOURCE = ThreadLocal.withInitial(() -> "");

    static Side entrySideByDifDea(List<Candle> m30, List<MacdPoint> macd30, int i) {
        ENTRY_M30_CTX.set(m30);
        LAST_ENTRY_SOURCE.set("");
        try {
            Side side = entrySideByDifDeaCore(macd30, i);
            if (side != null) return side;
            Side combo = OkxComboEntryDecision.decide(m30, i);
            // decide() 内部已按分支设了 OKX_MAIN / OKX_SHORT_FB；此处仅在它未设时兜底打通用标签
            if (combo != null && LAST_ENTRY_SOURCE.get().isEmpty()) LAST_ENTRY_SOURCE.set("OKX_COMBO");
            return combo;
        } finally {
            ENTRY_M30_CTX.remove();
        }
    }

    // 兼容旧调用：如果外部还在调用旧签名（仅 DIF/DEA），保持可编译
    static Side entrySideByDifDea(List<MacdPoint> macd30, int i) {
        return entrySideByDifDeaCore(macd30, i);
    }

    static Side entrySideByDifDeaCore(List<MacdPoint> macd30, int i) {

        //  可切换：三连K确认 or 两连K确认（默认三连，不改你当前逻辑）
        if (USE_3K_ENTRY_CONFIRM) {

            // 防越界（保持你原有的安全风格）
            if (i < 2) return null;

            MacdPoint k0 = macd30.get(i);     // 刚确认收盘K
            MacdPoint k1 = macd30.get(i - 1); // 上一根
            MacdPoint k2 = macd30.get(i - 2); // 上上根

            // ===== LONG：三根 DIF 阶梯递增 + 中间涨幅>1 =====
            if (k2.dif > 0 && k1.dif > 0 && k0.dif > 0 &&
                    k2.dif < k1.dif
                    && k1.dif < k0.dif
                    && (k0.dif - k1.dif) >0) {

                LAST_ENTRY_SOURCE.set("MACD3K");
                return Side.LONG;
            }

            // ===== SHORT（保持你原来的逻辑，不动）=====
            //  如果你原文件里 SHORT 是别的写法，
            // 请把下面这段替换回你原本的 SHORT 判断即可

            // 空：K1.dea<0 && K2.dea<0 && int(K1.dea - K2.dea) >= 2
            if (k2.dea < 0 && k1.dea < 0 && k0.dea < 0&&
                    k2.dea > k1.dea &&
                    k1.dea > k0.dea &&
                    (k1.dea - k0.dea) > 0) {
                // if ((int)(k1.dea - k2.dea) >= 0.88)
                LAST_ENTRY_SOURCE.set("MACD3K");
                return Side.SHORT;
            }


            // 3K 不触发 -> 新入场：K12 形态（ID44 / ID40）
            // 规则来自 4年30mK 挖掘：只用 [0..i] 已收盘K（SSOT），无未来函数


            // 3K 不触发 -> 兜底1：横盘箱体突破（组合AB）
// - LONG：窗口横盘(isFlat) + 上破 + 回踩可选
// - SHORT：pivot 箱体 + 下破（默认不强制回踩）
// 无未来函数：pivot 只允许 pivotIdx+R<=i；其它判断只用 [0..i] 已收盘K
            if (USE_RANGE_BREAKOUT_ENTRY) {
                List<Candle> m30 = ENTRY_M30_CTX.get();
                Side rb = RangeBreakoutEntryDecision.decide(m30, i);
                if (rb != null) { LAST_ENTRY_SOURCE.set("RANGE_BK"); return rb; }
            }

// 兜底2：双顶/双底（突破后回抽入场）

            // - 仅做“入场方向”判定；止损/止盈/出场由你原系统负责（本段不插手）
            // - 无未来函数：只使用 [0..i] 已收盘K；形态拐点用“右侧确认”确保不偷看 i+1 之后
            if (USE_DOUBLE_PATTERN_ENTRY) {
                List<Candle> m30 = ENTRY_M30_CTX.get();
                Side dt = DoubleTopBottomEntryDecision.decide(m30, i);
                if (dt != null) { LAST_ENTRY_SOURCE.set("DOUBLE_TB"); return dt; }
            }

            return null;

        } else {
            // ===============================
            // 0.4 投票机制（来自 1_PatternIndicatorsMinerMain.java）：needVotes=ceil(k*0.40)
            // 最新组合：MA60 + ENV_SMA_20_0.02_BREAK + FISHER_P10 + TSI25_13 + PS_DON_96
            // 无未来函数：只使用 [0..i] 的已收盘K线（dir 计算用 to=idx+1）
            // ===============================

            List<Candle> m30 = ENTRY_M30_CTX.get();
            if (m30 == null || m30.isEmpty()) return null;
            if (i < 0 || i >= m30.size()) return null;

            return OkxVote04ComboEntryDecision.decide(m30, i);
        }
    }

    // ===============================
    // K12 形态入场（ID44 / ID40）—— 只做 LONG
    // 口径：
    // - 信号在第 i 根30mK收盘后生成（只用 [0..i] confirm=1 已收盘K）
    // - 由主流程在 i+1 开盘进场（符合 SSOT & 无未来函数）
    // -------------------------------
    // ID44（温和不弱 + 近6根回撤 + 近3根止跌）：
    //   ret12 > -0.061%
    //   ret6  <= -0.390%
    //   ret3  > -0.067%
    //   upCount(12) <= 6
    //
    // ID40（温和不弱 + 中度回撤 + 区间下半部继续下压）：
    //   ret12 > -0.061%
    //   ret6  in (-0.925%, -0.390%]   (即 ret6<=-0.390% && ret6>-0.925%)
    //   ret3  <= -0.067%
    //   closePos(12) <= 0.517
    //   breakPct <= -1.401%   (close 相对前11根最高价的跌幅)
    // ===============================
    static final boolean USE_K12_ID44_ENTRY = sysBool("okx.useK12Id44Entry", false);
    static final boolean USE_K12_ID40_ENTRY = sysBool("okx.useK12Id40Entry", true);
    static final boolean PRINT_K12_ID_ENTRY = sysBool("okx.printK12IdEntry", false);

    static final class K12PatternId40Id44EntryDecision {

        static Side decide(List<Candle> m30, int i) {
            try {
                if (m30 == null || m30.isEmpty()) return null;
                if (i < 11) return null;
                if (i >= m30.size()) return null;

                // ID44 first (higher freq)
                if (USE_K12_ID44_ENTRY && hitId44(m30, i)) {
                    if (PRINT_K12_ID_ENTRY) {
                        System.out.println("[K12-ID44] LONG @ " + fmtOpen(m30.get(i).ts + BAR30_MS)
                                + " | sig=" + fmtOpen(m30.get(i).ts));
                    }
                    return Side.LONG;
                }

                // ID40 (stronger under fee in our mining)
                if (USE_K12_ID40_ENTRY && hitId40(m30, i)) {
                    if (PRINT_K12_ID_ENTRY) {
                        System.out.println("[K12-ID40] LONG @ " + fmtOpen(m30.get(i).ts + BAR30_MS)
                                + " | sig=" + fmtOpen(m30.get(i).ts));
                    }
                    return Side.LONG;
                }

                return null;
            } catch (Throwable t) {
                return null;
            }
        }

        static boolean hitId44(List<Candle> m30, int i) {
            // ret12 = C[i]/C[i-11]-1
            double c0 = m30.get(i).c;
            double c11 = m30.get(i - 11).c;
            if (c11 <= 0) return false;
            double ret12 = c0 / c11 - 1.0;

            double c5 = m30.get(i - 5).c;
            if (c5 <= 0) return false;
            double ret6 = c0 / c5 - 1.0;

            double c2 = m30.get(i - 2).c;
            if (c2 <= 0) return false;
            double ret3 = c0 / c2 - 1.0;

            // upCount(12)
            int up = 0;
            for (int k = i - 11; k <= i; k++) {
                Candle x = m30.get(k);
                if (x.c > x.o) up++;
            }

            return (ret12 > -0.00061)
                    && (ret6 <= -0.00390)
                    && (ret3 > -0.00067)
                    && (up <= 6);
        }

        static boolean hitId40(List<Candle> m30, int i) {
            double c0 = m30.get(i).c;

            double c11 = m30.get(i - 11).c;
            if (c11 <= 0) return false;
            double ret12 = c0 / c11 - 1.0;

            double c5 = m30.get(i - 5).c;
            if (c5 <= 0) return false;
            double ret6 = c0 / c5 - 1.0;

            double c2 = m30.get(i - 2).c;
            if (c2 <= 0) return false;
            double ret3 = c0 / c2 - 1.0;

            // closePos(12)
            double ll = Double.POSITIVE_INFINITY;
            double hh = 0.0;
            for (int k = i - 11; k <= i; k++) {
                Candle x = m30.get(k);
                if (x.l < ll) ll = x.l;
                if (x.h > hh) hh = x.h;
            }
            double denom = hh - ll;
            if (denom <= 0) return false;
            double closePos = (c0 - ll) / denom;

            // breakPct relative to prev 11 bars high
            double hhPrev = 0.0;
            for (int k = i - 11; k <= i - 1; k++) {
                Candle x = m30.get(k);
                if (x.h > hhPrev) hhPrev = x.h;
            }
            if (hhPrev <= 0) return false;
            double breakPct = (c0 - hhPrev) / hhPrev;

            return (ret12 > -0.00061)
                    && (ret6 <= -0.00390) && (ret6 > -0.00925)
                    && (ret3 <= -0.00067)
                    && (closePos <= 0.517)
                    && (breakPct <= -0.01401);
        }
    }


    // ===============================
    // 双顶/双底（突破 + 回抽）入场兜底
    // 仅返回方向（Side），不管止损止盈与出场。
    // 无未来函数：
    // - 形态拐点用 (L=2,R=2) 的“右侧确认”pivot（只允许 pivotIdx+R <= i）
    // - 突破/回抽检测只扫描 [0..i] 的已收盘K
    // ===============================
    static final boolean USE_DOUBLE_PATTERN_ENTRY = sysBool("okx.useDoublePatternEntry", true);
    static final boolean PRINT_DOUBLE_PATTERN_ENTRY = sysBool("okx.printDoublePatternEntry", false);



    // ===============================
    // 横盘箱体突破（组合AB）兜底入场
    // -------------------------------
    // A（LONG）：窗口横盘(isFlat) + 上破（breakPct） + 回踩可选（requireRetest）
    // B（SHORT）：pivot 箱体（双高/双低近似水平） + 下破（breakPct） + 回踩可选（默认不强制）
    // 仅返回方向 Side；止损/止盈/出场仍由你原系统负责（这里不插手）
    // 无未来函数：
    // - A：只用 [i-lookback .. i] 已收盘K（窗口不含未来）
    // - B：pivot 只允许 pivotIdx + pivR <= i（右侧确认，不偷看未来）
    // ===============================
    static final boolean USE_RANGE_BREAKOUT_ENTRY = sysBool("okx.useRangeBreakoutEntry", true);
    static final boolean PRINT_RANGE_BREAKOUT_ENTRY = sysBool("okx.printRangeBreakoutEntry", false);

    // A（LONG）
    static final int    RANGE_A_LOOKBACK        = sysInt   ("okx.rangeA.lookback", 10);
    static final double RANGE_A_SLOPE_THRESH    = sysDouble("okx.rangeA.slopeThresh", 0.20);
    static final double RANGE_A_BREAK_PCT       = sysDouble("okx.rangeA.breakPct", 0.002);
    static final boolean RANGE_A_REQUIRE_RETEST = sysBool  ("okx.rangeA.requireRetest", true);
    static final double RANGE_A_RETEST_TOL_PCT  = sysDouble("okx.rangeA.retestTolPct", 0.002);
    static final int    RANGE_A_MAX_RETEST_BARS = sysInt   ("okx.rangeA.maxRetestBars", 96);

    // B（SHORT）
    static final int    RANGE_B_LOOKBACK        = sysInt   ("okx.rangeB.lookback", 300);
    static final int    RANGE_PIV_L             = sysInt   ("okx.rangeB.pivL", 2);
    static final int    RANGE_PIV_R             = sysInt   ("okx.rangeB.pivR", 2);
    static final int    RANGE_B_MIN_PIV_PAIRS   = sysInt   ("okx.rangeB.minPivPairs", 2);
    static final double RANGE_B_LEVEL_TOL_PCT   = sysDouble("okx.rangeB.levelTolPct", 0.003);
    static final double RANGE_B_BREAK_PCT       = sysDouble("okx.rangeB.breakPct", 0.002);
    static final boolean RANGE_B_REQUIRE_RETEST = sysBool  ("okx.rangeB.requireRetest", true);
    static final double RANGE_B_RETEST_TOL_PCT  = sysDouble("okx.rangeB.retestTolPct", 0.002);
    static final int    RANGE_B_MAX_RETEST_BARS = sysInt   ("okx.rangeB.maxRetestBars", 96);

    static final class RangeBreakoutEntryDecision {

        // 轻量缓存：回测循环里同一根 m30 列表会被反复调用，避免每根K重复算 pivots
        static volatile PivotCache LAST = null;

        static Side decide(List<Candle> m30, int i) {
            try {
                if (m30 == null || m30.isEmpty()) return null;
                if (i < 0 || i >= m30.size()) return null;

                // A：LONG
                if (detectFlatWindowUp(m30, i)) {
                    if (PRINT_RANGE_BREAKOUT_ENTRY) {
                        System.out.println("[RANGE-ENTRY] LONG @ " + fmtOpen(m30.get(i).ts + BAR30_MS)
                                + " | sig=" + fmtOpen(m30.get(i).ts) + " | tag=FLAT_WINDOW_BREAK_UP");
                    }
                    return Side.LONG;
                }

                // B：SHORT
                PivotCache pc = ensurePivotCache(m30);
                if (pc != null && detectPivotRangeDown(m30, i, pc)) {
                    if (PRINT_RANGE_BREAKOUT_ENTRY) {
                        System.out.println("[RANGE-ENTRY] SHORT @ " + fmtOpen(m30.get(i).ts + BAR30_MS)
                                + " | sig=" + fmtOpen(m30.get(i).ts) + " | tag=HORIZONTAL_RANGE_BREAK_DOWN");
                    }
                    return Side.SHORT;
                }

                return null;
            } catch (Throwable t) {
                // 兜底：不要让新增形态影响主流程
                return null;
            }
        }

        // ---------------------------
        // A（LONG）：窗口横盘 + 上破
        // ---------------------------
        static boolean detectFlatWindowUp(List<Candle> cs, int i) {
            if (i < RANGE_A_LOOKBACK + 1) return false;

            double upper = -1e18;
            double lower =  1e18;
            double[] highs = new double[RANGE_A_LOOKBACK];
            double[] lows  = new double[RANGE_A_LOOKBACK];

            // window: [i-lookback .. i-1]
            for (int k = 0; k < RANGE_A_LOOKBACK; k++) {
                Candle ck = cs.get(i - RANGE_A_LOOKBACK + k);
                highs[k] = ck.h;
                lows[k]  = ck.l;
                if (ck.h > upper) upper = ck.h;
                if (ck.l < lower) lower = ck.l;
            }

            double range = upper - lower;
            if (!(range > 0.0)) return false;

            double thr = RANGE_A_SLOPE_THRESH * range / RANGE_A_LOOKBACK;
            if (Math.abs(slopeLR(highs)) >= thr) return false;
            if (Math.abs(slopeLR(lows))  >= thr) return false;

            double close = cs.get(i).c;
            if (!(close >= upper * (1.0 + RANGE_A_BREAK_PCT))) return false;

            if (RANGE_A_REQUIRE_RETEST) {
                if (!hasValidRetest(cs, i, upper, true, RANGE_A_RETEST_TOL_PCT, RANGE_A_MAX_RETEST_BARS)) return false;
            }

            return true;
        }

        // ---------------------------
        // B（SHORT）：pivot 箱体 + 下破
        // ---------------------------
        static boolean detectPivotRangeDown(List<Candle> cs, int i, PivotCache pc) {
            if (pc.lowPivots == null || pc.highPivots == null) return false;
            if (pc.lowPivots.length < 2 || pc.highPivots.length < 2) return false;

            // pivotLimit：只允许 pivotIdx + RANGE_PIV_R <= i（右侧确认）
            int pivotLimit = i - RANGE_PIV_R;
            if (pivotLimit <= RANGE_PIV_L) return false;

            int fromIdx = Math.max(0, i - RANGE_B_LOOKBACK);

            int[] highs2 = pickLastTwo(pc.highPivots, fromIdx, pivotLimit);
            int[] lows2  = pickLastTwo(pc.lowPivots,  fromIdx, pivotLimit);
            if (highs2 == null || lows2 == null) return false;

            int h1i = highs2[0], h2i = highs2[1];
            int l1i = lows2[0],  l2i = lows2[1];

            // 至少需要 N 组 pivot（这里我们用“必须存在两个 pivot high + 两个 pivot low”）
            if (RANGE_B_MIN_PIV_PAIRS > 2) {
                // 如果你以后把 minPivPairs 调大，这里可以扩展成更一般的选择器；
                // 目前组合AB只用最近两组，保持与回测对齐。
                return false;
            }

            double h1 = cs.get(h1i).h;
            double h2 = cs.get(h2i).h;
            double res = (h1 + h2) * 0.5;
            if (!(res > 0.0)) return false;
            if (Math.abs(h2 - h1) / res > RANGE_B_LEVEL_TOL_PCT) return false;

            double l1 = cs.get(l1i).l;
            double l2 = cs.get(l2i).l;
            double sup = (l1 + l2) * 0.5;
            if (!(sup > 0.0)) return false;
            if (Math.abs(l2 - l1) / sup > RANGE_B_LEVEL_TOL_PCT) return false;

            if (!(res > sup)) return false;

            double close = cs.get(i).c;
            if (!(close <= sup * (1.0 - RANGE_B_BREAK_PCT))) return false;

            if (RANGE_B_REQUIRE_RETEST) {
                if (!hasValidRetest(cs, i, sup, false, RANGE_B_RETEST_TOL_PCT, RANGE_B_MAX_RETEST_BARS)) return false;
            }
            return true;
        }

        // ---------------------------
        // pivots cache
        // ---------------------------
        static final class PivotCache {
            final List<Candle> key;
            final int size;
            final long lastTs;
            final int[] lowPivots;
            final int[] highPivots;

            PivotCache(List<Candle> key, int size, long lastTs, int[] lowPivots, int[] highPivots) {
                this.key = key;
                this.size = size;
                this.lastTs = lastTs;
                this.lowPivots = lowPivots;
                this.highPivots = highPivots;
            }
        }

        static PivotCache ensurePivotCache(List<Candle> cs) {
            if (cs == null || cs.isEmpty()) return null;
            int sz = cs.size();
            long lastTs = cs.get(sz - 1).ts;
            PivotCache pc = LAST;
            if (pc != null && pc.key == cs && pc.size == sz && pc.lastTs == lastTs) return pc;

            synchronized (RangeBreakoutEntryDecision.class) {
                pc = LAST;
                if (pc != null && pc.key == cs && pc.size == sz && pc.lastTs == lastTs) return pc;

                PivotCache built = buildPivotCache(cs);
                LAST = built;
                return built;
            }
        }

        static PivotCache buildPivotCache(List<Candle> cs) {
            try {
                int sz = cs.size();
                if (sz <= (RANGE_PIV_L + RANGE_PIV_R + 2)) return new PivotCache(cs, sz, cs.get(sz - 1).ts, new int[0], new int[0]);

                IntArray lows = new IntArray();
                IntArray highs = new IntArray();

                for (int idx = RANGE_PIV_L; idx <= sz - 1 - RANGE_PIV_R; idx++) {
                    if (isPivotLow(cs, idx, RANGE_PIV_L, RANGE_PIV_R)) lows.add(idx);
                    if (isPivotHigh(cs, idx, RANGE_PIV_L, RANGE_PIV_R)) highs.add(idx);
                }

                return new PivotCache(cs, sz, cs.get(sz - 1).ts, lows.toArray(), highs.toArray());
            } catch (Throwable t) {
                return null;
            }
        }

        static boolean isPivotHigh(List<Candle> cs, int idx, int L, int R) {
            double v = cs.get(idx).h;
            for (int k = idx - L; k <= idx + R; k++) {
                if (k == idx) continue;
                if (cs.get(k).h >= v) return false; // strict
            }
            return true;
        }

        static boolean isPivotLow(List<Candle> cs, int idx, int L, int R) {
            double v = cs.get(idx).l;
            for (int k = idx - L; k <= idx + R; k++) {
                if (k == idx) continue;
                if (cs.get(k).l <= v) return false; // strict
            }
            return true;
        }

        static int[] pickLastTwo(int[] pivots, int fromIdx, int toIdx) {
            if (pivots == null || pivots.length < 2) return null;
            int pos = upperBound(pivots, toIdx) - 1;
            int newest = -1, older = -1;
            for (; pos >= 0; pos--) {
                int idx = pivots[pos];
                if (idx < fromIdx) break;
                if (newest < 0) newest = idx;
                else { older = idx; break; }
            }
            if (older < 0 || newest < 0) return null;
            return new int[]{older, newest};
        }

        static int upperBound(int[] a, int x) {
            int lo = 0, hi = a.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (a[mid] <= x) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }

        // ---------------------------
        // utilities
        // ---------------------------
        static double slopeLR(double[] arr) {
            int n = arr.length;
            double sx = 0, sy = 0, sxy = 0, sx2 = 0;
            for (int i = 0; i < n; i++) {
                sx += i;
                sy += arr[i];
                sxy += i * arr[i];
                sx2 += (double) i * i;
            }
            double denom = n * sx2 - sx * sx;
            if (denom == 0) return 0.0;
            return (n * sxy - sx * sy) / denom;
        }

        static boolean nearPct(double a, double b, double tolPct) {
            if (!(b > 0.0)) return false;
            return Math.abs(a - b) / b <= tolPct;
        }

        static boolean hasValidRetest(List<Candle> cs, int i, double level, boolean isBull,
                                      double tolPct, int maxBars) {
            int from = Math.max(0, i - Math.max(1, maxBars));
            for (int k = from; k <= i; k++) {
                Candle ck = cs.get(k);
                boolean touch = nearPct(ck.l, level, tolPct) || nearPct(ck.h, level, tolPct) || nearPct(ck.c, level, tolPct);
                if (!touch) continue;
                if (isBull) {
                    if (ck.c >= level) return true;
                } else {
                    if (ck.c <= level) return true;
                }
            }
            return false;
        }

        // 小工具：int 动态数组（避免引入额外依赖）
        static final class IntArray {
            int[] a = new int[64];
            int sz = 0;
            void add(int v) {
                if (sz >= a.length) a = Arrays.copyOf(a, a.length * 2);
                a[sz++] = v;
            }
            int[] toArray() { return Arrays.copyOf(a, sz); }
        }
    }

    static final class DoubleTopBottomEntryDecision {

        // pivot 确认：用右侧 R 根确认（避免未来函数）
        static final int PIV_L = 2;
        static final int PIV_R = 2;

        // 形态/突破/回抽参数（轻量且稳健，默认贴近你图示的“回抽小止损”打法）
        static final double LEVEL_TOL_PCT = sysDouble("okx.doublePatternLevelTolPct", 0.0025); // 两次顶/底高度容差（0.3%）
        static final double BREAK_PCT = sysDouble("okx.doublePatternBreakPct", 0.0035);       // 突破确认（0.2%）
        static final double RETEST_TOL_PCT = sysDouble("okx.doublePatternRetestTolPct", 0.0018); // 回抽触及容差（0.2%）

        static final int MAX_LOOKBACK_BARS = (int) sysLong("okx.doublePatternMaxLookbackBars", 900); // 约25天（30m）
        static final int MAX_RETEST_BARS = (int) sysLong("okx.doublePatternMaxRetestBars", 72);       // 突破后2天内回抽
        static final int MAX_P2_CANDIDATES = (int) sysLong("okx.doublePatternMaxP2", 8);
        static final int MAX_P1_CANDIDATES = (int) sysLong("okx.doublePatternMaxP1", 12);

        static volatile PivotCache LAST = null;

        static Side decide(List<Candle> m30, int i) {
            try {
                if (m30 == null || m30.isEmpty()) return null;
                if (i < 0 || i >= m30.size()) return null;

                // pivot 必须“已确认”：pivotIdx + PIV_R <= i
                int pivotLimit = i - PIV_R;
                if (pivotLimit <= PIV_L) return null;

                PivotCache pc = ensure(m30);
                if (pc == null) return null;

                Side s;
                s = tryDoubleBottom(m30, i, pivotLimit, pc.lowPivots);
                if (s != null) return s;
                s = tryDoubleTop(m30, i, pivotLimit, pc.highPivots);
                return s;
            } catch (Throwable t) {
                return null;
            }
        }

        // -------- LONG：双底 -> 突破颈线 -> 回抽触及颈线并收回颈线上方 --------
        static Side tryDoubleBottom(List<Candle> m30, int i, int pivotLimit, int[] lowPivots) {
            if (lowPivots == null || lowPivots.length == 0) return null;
            int pos2 = upperBoundInt(lowPivots, pivotLimit) - 1;
            if (pos2 < 1) return null;

            Candle cur = m30.get(i);

            int tried2 = 0;
            outerDTDB_LONG:
            for (int p2pos = pos2; p2pos >= 1 && tried2 < MAX_P2_CANDIDATES; p2pos--, tried2++) {
                int p2 = lowPivots[p2pos];
                if (i - p2 > MAX_LOOKBACK_BARS) break;

                double l2 = m30.get(p2).l;

                int tried1 = 0;
                for (int p1pos = p2pos - 1; p1pos >= 0 && tried1 < MAX_P1_CANDIDATES; p1pos--, tried1++) {
                    int p1 = lowPivots[p1pos];
                    if (p2 - p1 < 6) continue; // 形态间隔太近，忽略
                    if (i - p1 > MAX_LOOKBACK_BARS) break outerDTDB_LONG;

                    double l1 = m30.get(p1).l;
                    double avg = (l1 + l2) * 0.5;
                    if (!(avg > 0.0)) continue;
                    if (Math.abs(l2 - l1) / avg > LEVEL_TOL_PCT) continue;

                    // 颈线：两底之间的最高点（用 high 最大值）——包含 p1/p2 自身（避免颈线低估）
                    double neckline = -1.0;
                    for (int k = p1; k <= p2; k++) {
                        double h = m30.get(k).h;
                        if (h > neckline) neckline = h;
                    }
                    if (!(neckline > 0.0)) continue;

                    // 突破：p2 之后出现“收盘价 > 颈线*(1+BREAK_PCT)”
                    int brk = -1;
                    double brkThr = neckline * (1.0 + BREAK_PCT);
                    for (int k = p2 + 1; k <= i - 1; k++) {
                        if (m30.get(k).c > brkThr) { brk = k; break; }
                    }
                    if (brk < 0) continue;
                    if (i - brk > MAX_RETEST_BARS) continue;

                    // 突破后到当前K之前，不得出现 close 再次跌穿颈线（否则形态失效）
                    boolean breakInvalidated = false;
                    for (int k = brk + 1; k <= i - 1; k++) {
                        if (m30.get(k).c < neckline) { breakInvalidated = true; break; }
                    }
                    if (breakInvalidated) continue;

                    // 回抽：当前K 触及颈线附近，且没有深穿；并且收盘回到颈线上方
                    if (cur.l >= neckline * (1.0 - RETEST_TOL_PCT)
                            && cur.l <= neckline * (1.0 + RETEST_TOL_PCT)
                            && cur.c >= neckline) {
                        if (PRINT_DOUBLE_PATTERN_ENTRY) {
                            System.out.printf("[ENTRY][DTDB] LONG i=%d ts=%s p1=%d p2=%d neck=%.2f brk=%d\n",
                                    i, FMT_JST_SHORT.format(Instant.ofEpochMilli(cur.ts)), p1, p2, neckline, brk);
                        }
                        return Side.LONG;
                    }
                }
            }
            return null;
        }

        // -------- SHORT：双顶 -> 跌破颈线 -> 回抽触及颈线并收回颈线下方 --------
        static Side tryDoubleTop(List<Candle> m30, int i, int pivotLimit, int[] highPivots) {
            if (highPivots == null || highPivots.length == 0) return null;
            int pos2 = upperBoundInt(highPivots, pivotLimit) - 1;
            if (pos2 < 1) return null;

            Candle cur = m30.get(i);

            int tried2 = 0;
            outerDTDB_SHORT:
            for (int p2pos = pos2; p2pos >= 1 && tried2 < MAX_P2_CANDIDATES; p2pos--, tried2++) {
                int p2 = highPivots[p2pos];
                if (i - p2 > MAX_LOOKBACK_BARS) break;

                double h2 = m30.get(p2).h;

                int tried1 = 0;
                for (int p1pos = p2pos - 1; p1pos >= 0 && tried1 < MAX_P1_CANDIDATES; p1pos--, tried1++) {
                    int p1 = highPivots[p1pos];
                    if (p2 - p1 < 6) continue;
                    if (i - p1 > MAX_LOOKBACK_BARS) break outerDTDB_SHORT;

                    double h1 = m30.get(p1).h;
                    double avg = (h1 + h2) * 0.5;
                    if (!(avg > 0.0)) continue;
                    if (Math.abs(h2 - h1) / avg > LEVEL_TOL_PCT) continue;

                    // 颈线：两顶之间的最低点（用 low 最小值）——包含 p1/p2 自身（避免颈线高估/低估）
                    double neckline = Double.POSITIVE_INFINITY;
                    for (int k = p1; k <= p2; k++) {
                        double l = m30.get(k).l;
                        if (l < neckline) neckline = l;
                    }
                    if (!(neckline > 0.0) || Double.isInfinite(neckline)) continue;

                    // 跌破：p2 之后出现“收盘价 < 颈线*(1-BREAK_PCT)”
                    int brk = -1;
                    double brkThr = neckline * (1.0 - BREAK_PCT);
                    for (int k = p2 + 1; k <= i - 1; k++) {
                        if (m30.get(k).c < brkThr) { brk = k; break; }
                    }
                    if (brk < 0) continue;
                    if (i - brk > MAX_RETEST_BARS) continue;

                    // 突破后到当前K之前，不得出现 close 再次涨穿颈线（否则形态失效）
                    boolean breakInvalidated = false;
                    for (int k = brk + 1; k <= i - 1; k++) {
                        if (m30.get(k).c > neckline) { breakInvalidated = true; break; }
                    }
                    if (breakInvalidated) continue;

                    // 回抽：当前K 触及颈线附近，且没有高穿；并且收盘回到颈线下方
                    if (cur.h >= neckline * (1.0 - RETEST_TOL_PCT)
                            && cur.h <= neckline * (1.0 + RETEST_TOL_PCT)
                            && cur.c <= neckline) {
                        if (PRINT_DOUBLE_PATTERN_ENTRY) {
                            System.out.printf("[ENTRY][DTDB] SHORT i=%d ts=%s p1=%d p2=%d neck=%.2f brk=%d\n",
                                    i, FMT_JST_SHORT.format(Instant.ofEpochMilli(cur.ts)), p1, p2, neckline, brk);
                        }
                        return Side.SHORT;
                    }
                }
            }
            return null;
        }

        // -------- pivot cache（只依赖本地邻域，不会使用 i+1 之后的数据；使用时再限制 pivotIdx+R<=i） --------
        static PivotCache ensure(List<Candle> m30) {
            int sz = m30.size();
            long lastTs = m30.get(sz - 1).ts;
            PivotCache pc = LAST;
            if (pc != null && pc.key == m30 && pc.size == sz && pc.lastTs == lastTs) return pc;

            synchronized (DoubleTopBottomEntryDecision.class) {
                pc = LAST;
                if (pc != null && pc.key == m30 && pc.size == sz && pc.lastTs == lastTs) return pc;
                PivotCache built = build(m30);
                LAST = built;
                return built;
            }
        }

        static PivotCache build(List<Candle> m30) {
            try {
                int sz = m30.size();
                IntBuf lows = new IntBuf(Math.max(32, sz / 20));
                IntBuf highs = new IntBuf(Math.max(32, sz / 20));

                for (int j = PIV_L; j <= sz - 1 - PIV_R; j++) {
                    double lj = m30.get(j).l;
                    double hj = m30.get(j).h;

                    boolean isLow = true;
                    boolean isHigh = true;
                    for (int k = j - PIV_L; k <= j + PIV_R; k++) {
                        if (k == j) continue;
                        if (m30.get(k).l < lj) isLow = false;
                        if (m30.get(k).h > hj) isHigh = false;
                        if (!isLow && !isHigh) break;
                    }
                    if (isLow) lows.add(j);
                    if (isHigh) highs.add(j);
                }

                return new PivotCache(m30, sz, m30.get(sz - 1).ts, lows.toArray(), highs.toArray());
            } catch (Throwable t) {
                return null;
            }
        }

        static int upperBoundInt(int[] a, int x) {
            int lo = 0, hi = a.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (a[mid] <= x) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }

        static final class PivotCache {
            final List<Candle> key;
            final int size;
            final long lastTs;
            final int[] lowPivots;
            final int[] highPivots;
            PivotCache(List<Candle> key, int size, long lastTs, int[] lowPivots, int[] highPivots) {
                this.key = key;
                this.size = size;
                this.lastTs = lastTs;
                this.lowPivots = (lowPivots == null ? new int[0] : lowPivots);
                this.highPivots = (highPivots == null ? new int[0] : highPivots);
            }
        }

        // 轻量 int 动态数组
        static final class IntBuf {
            int[] a;
            int n;
            IntBuf(int cap) { a = new int[Math.max(8, cap)]; }
            void add(int v) {
                if (n >= a.length) a = Arrays.copyOf(a, a.length * 2);
                a[n++] = v;
            }
            int[] toArray() { return Arrays.copyOf(a, n); }
        }
    }

    // ===============================
    // OKX 组合入场（兜底）：主力(ALL) + 空头专用(SHORT)
    // - 主力(ALL)：SAR_0.02_0.2 + SUPER_TREND_10_3.0 + PS_NRX_8（3个方向同向且非0 -> 允许多/空）
    // - 空头专用(SHORT)：STOCH14_3_band + FISHER_P10（两者均为 -1 -> 允许空）
    //
    // 重点：PS_NRX_8 使用 ConditionFactory.psInsideNrx(8) —— 与挖掘器 100% 同源（PriceStructureLibrary.insideOrNRxBreakout）
    // 无未来函数：只使用传入的已收盘K线序列（不读取 i+1）
    // ===============================
    static final class OkxComboEntryDecision {

        // 为避免实时频繁反射/构造全量 Candle，这里只取最近 N 根做计算（足够覆盖 SuperTrend/PSAR/Stoch/Fisher 的 warmup）
        // 若你后续想更“完全等价于挖掘器全样本”，把这个值调大即可（比如 10000）
        static final int EVAL_HISTORY_BARS = 3072;

        // OKX 组合（固定参数，与你挖掘结果一致）
        static final PoolCondition PC_SAR =
                ConditionFactory.psarTrend(0.02, 0.2);      // id= SAR_0.02_0.2
        static final PoolCondition PC_SUPER =
                ConditionFactory.superTrend(10, 3.0);       // id= SUPER_TREND_10_3.0
        static final PoolCondition PC_PSNX =
                ConditionFactory.psInsideNrx(8);            // id= PS_NRX_8  (完美复制)
        static final PoolCondition PC_STOCH_BAND =
                ConditionFactory.stochBand(14, 3, 80, 20);  // id= STOCH14_3_band
        static final PoolCondition PC_FISHER =
                ConditionFactory.fisherPrice(10);           // id= FISHER_P10

        static Side decide(List<Candle> m30, int i) {
            try {
                if (m30 == null || m30.isEmpty()) return null;
                if (i < 0 || i >= m30.size()) return null;

                int start = Math.max(0, i - EVAL_HISTORY_BARS);
                List<Candle> seg = m30.subList(start, i + 1);
                int idx = i - start;

                List<Object> evalCandles = EvalCandleAdapter.toEvalCandles(seg);
                if (evalCandles == null || evalCandles.isEmpty()) return null;
                if (idx < 0 || idx >= evalCandles.size()) return null;

                int sarDir   = dirAt(PC_SAR, evalCandles, idx);
                int superDir = dirAt(PC_SUPER, evalCandles, idx);
                int psDir    = dirAt(PC_PSNX, evalCandles, idx);

                // ① 主力(ALL)：三者同向且非0
                int mainDir = consensus3(sarDir, superDir, psDir);
                if (mainDir > 0) { LAST_ENTRY_SOURCE.set("OKX_MAIN"); return Side.LONG; }
                if (mainDir < 0) { LAST_ENTRY_SOURCE.set("OKX_MAIN"); return Side.SHORT; }

                // ② 空头兜底(OKX_SHORT_FB) 已删除：归因统计证实其 17 笔仅赚 12.37 点、
                //    均笔 0.73 点、盈亏比 1.20，做无用功还吃手续费，且为不对称逻辑私货。
                //    删后主力门变为纯对称的"①三者共识"进场。
                //    （PC_STOCH_BAND / PC_FISHER 若别处不再引用，可作无害死代码删除。）

                return null;
            } catch (Throwable t) {
                // 兜底：不让组合逻辑影响你原本的系统稳定性
                return null;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        static int dirAt(PoolCondition pc, List<Object> evalCandles, int idx) throws Exception {
            // 用 [0..idx] 的范围算，保证 warmup 足够（不读取未来）
            int[] arr = pc.dir((List) evalCandles, 0, idx);
            if (arr == null || arr.length == 0) return 0;
            return arr[arr.length - 1];
        }

        static int consensus3(int a, int b, int c) {
            if (a == 0 || b == 0 || c == 0) return 0;
            if (a > 0 && b > 0 && c > 0) return +1;
            if (a < 0 && b < 0 && c < 0) return -1;
            return 0;
        }
    }

    // ===============================
    // 把本系统 Candle -> eval.model.Candle（通过反射，避免侵入式改动）
    // 只要 eval.model.Candle 存在（你的挖掘器已经在用），这里就能 0 侵入地复用 ConditionFactory/PoolCondition。
    // ===============================


    // ===============================
    // OKX 挖掘组合入场（0.4 投票制）：
    // 组合：MA60 + ENV_SMA_20_0.02_BREAK + FISHER_P10 + TSI25_13 + PS_DON_96
    // 投票机制：needVotes = ceil(k * 0.40)，k=5 => need=2；若同一根同时满足多/空，则判为冲突 -> null
    // 无未来函数：只使用传入的已收盘K线 [0..idx]（dir 计算的 to=idx+1，不读取 idx+1）
    // ===============================
    static final class OkxVote04ComboEntryDecision {

        // 仅取最近 N 根做计算（覆盖 MA60/TSI/Donchian 的 warmup），不读取未来
        static final int EVAL_HISTORY_BARS = 3000;

        static final double VOTE_RATIO = 0.60;     // 你挖掘器的 --comboVoteRatio=0.40
        static final int COMBO_MIN_VOTES = -1;     // 你挖掘器默认 -1（走 ratio 计算）

        static final String BAR = "30m";

        static final String[] IDS = new String[]{
                "MA60",
                "ENV_SMA_20_0.02_BREAK",
                "FISHER_P10",
                "TSI25_13",
                "PS_DON_96"
        };

        static volatile boolean inited = false;
        static final PoolCondition[] PCS = new PoolCondition[IDS.length];

        static void ensureInit() {
            if (inited) return;
            synchronized (OkxVote04ComboEntryDecision.class) {
                if (inited) return;
                try {
                    List<PoolCondition> conds =
                            ConditionFactory.defaultConditionsWithInst(INST_ID, BAR);
                    if (conds != null) {
                        Map<String, PoolCondition> map = new HashMap<>();
                        for (PoolCondition c : conds) {
                            if (c == null) continue;
                            String id = null;
                            try { id = c.id(); } catch (Throwable ignore) {}
                            if (id != null && !id.isBlank()) map.put(id.trim(), c);
                        }
                        for (int i = 0; i < IDS.length; i++) {
                            PCS[i] = map.get(IDS[i]);
                        }
                    }
                } catch (Throwable ignore) {
                    // keep null; caller will return null
                } finally {
                    inited = true;
                }
            }
        }

        static Side decide(List<Candle> m30, int sigIdx) {
            try {
                if (m30 == null || m30.isEmpty()) return null;
                if (sigIdx < 0 || sigIdx >= m30.size()) return null;

                ensureInit();
                for (PoolCondition pc : PCS) {
                    if (pc == null) return null; // 指标不全则不启用（避免误判）
                }

                int start = Math.max(0, sigIdx - EVAL_HISTORY_BARS);
                List<Candle> seg = m30.subList(start, sigIdx + 1);
                int idx = sigIdx - start;

                List<Object> evalCandles = EvalCandleAdapter.toEvalCandles(seg);
                if (evalCandles == null || evalCandles.isEmpty()) return null;
                if (idx < 0 || idx >= evalCandles.size()) return null;

                int needVotes = calcNeedVotes(PCS.length, COMBO_MIN_VOTES, VOTE_RATIO);

                int longVotes = 0;
                int shortVotes = 0;
                for (PoolCondition pc : PCS) {
                    int d = dirAt(pc, evalCandles, idx);
                    if (d > 0) longVotes++;
                    else if (d < 0) shortVotes++;
                }

                // resolve conflicts (both long & short) -> drop
                if (longVotes >= needVotes && shortVotes >= needVotes) return null;
                if (longVotes >= needVotes) return Side.LONG;
                if (shortVotes >= needVotes) return Side.SHORT;
                return null;
            } catch (Throwable t) {
                return null;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        static int dirAt(PoolCondition pc, List<Object> evalCandles, int idx) throws Exception {
            // PoolCondition.dir(from, to) 的 to 为【toExclusive】，因此要传 idx+1
            int[] arr = pc.dir((List) evalCandles, 0, idx + 1);
            if (arr == null || arr.length == 0) return 0;
            return arr[arr.length - 1];
        }

        // 复制自 1_PatternIndicatorsMinerMain.calcNeedVotes（保持一致）
        static int calcNeedVotes(int k, int comboMinVotes, double comboVoteRatio) {
            int need;
            if (comboMinVotes > 0) need = Math.min(k, comboMinVotes);
            else {
                if (!Double.isFinite(comboVoteRatio)) comboVoteRatio = 1.0;
                if (comboVoteRatio <= 0.0) need = 1;
                else need = (int) Math.ceil(k * comboVoteRatio);
                if (need < 1) need = 1;
                if (need > k) need = k;
            }
            return need;
        }
    }

    static final class EvalCandleAdapter {

        static volatile boolean inited = false;
        static Class<?> evalCandleCls;
        static Constructor<?> ctor7;
        static Constructor<?> ctor6;
        static Constructor<?> ctor5;
        static Constructor<?> ctor0;

        static Field fTs, fO, fH, fL, fC, fVol, fConfirm;
        static Field fOpenTs, fOpenTsMs;

        static void init() {
            if (inited) return;
            synchronized (EvalCandleAdapter.class) {
                if (inited) return;
                try {
                    evalCandleCls = Class.forName("eval.model.Candle");

                    // 尝试常见构造器
                    ctor7 = findCtor(evalCandleCls, long.class, double.class, double.class, double.class, double.class, double.class, int.class);
                    ctor6 = findCtor(evalCandleCls, long.class, double.class, double.class, double.class, double.class, int.class);
                    ctor5 = findCtor(evalCandleCls, long.class, double.class, double.class, double.class, double.class);
                    ctor0 = findCtor(evalCandleCls);

                    // 尝试常见字段名（没有就留 null）
                    fTs      = findField(evalCandleCls, "ts");
                    fOpenTs  = findField(evalCandleCls, "openTs");
                    fOpenTsMs= findField(evalCandleCls, "openTsMs");

                    fO   = findField(evalCandleCls, "o");
                    if (fO == null) fO = findField(evalCandleCls, "open");
                    fH   = findField(evalCandleCls, "h");
                    if (fH == null) fH = findField(evalCandleCls, "high");
                    fL   = findField(evalCandleCls, "l");
                    if (fL == null) fL = findField(evalCandleCls, "low");
                    fC   = findField(evalCandleCls, "c");
                    if (fC == null) fC = findField(evalCandleCls, "close");

                    fVol = findField(evalCandleCls, "vol");
                    if (fVol == null) fVol = findField(evalCandleCls, "v");
                    if (fVol == null) fVol = findField(evalCandleCls, "volume");

                    fConfirm = findField(evalCandleCls, "confirm");

                } catch (Throwable ignore) {
                    // 留给调用方兜底
                } finally {
                    inited = true;
                }
            }
        }

        static Constructor<?> findCtor(Class<?> cls, Class<?>... p) {
            try {
                Constructor<?> c = cls.getDeclaredConstructor(p);
                c.setAccessible(true);
                return c;
            } catch (Throwable t) {
                return null;
            }
        }

        static Field findField(Class<?> cls, String name) {
            try {
                Field f = cls.getDeclaredField(name);
                f.setAccessible(true);
                return f;
            } catch (Throwable t) {
                return null;
            }
        }

        static List<Object> toEvalCandles(List<Candle> seg) {
            init();
            if (evalCandleCls == null) return null;

            ArrayList<Object> out = new ArrayList<>(seg.size());
            for (Candle c : seg) {
                Object ec = newEvalCandle(c);
                if (ec == null) return null;
                out.add(ec);
            }
            return out;
        }

        static Object newEvalCandle(Candle c) {
            try {
                // 1) 优先走构造器（最快）
                if (ctor7 != null) {
                    return ctor7.newInstance(c.ts, c.o, c.h, c.l, c.c, c.vol, c.confirm);
                }
                if (ctor6 != null) {
                    return ctor6.newInstance(c.ts, c.o, c.h, c.l, c.c, c.confirm);
                }
                if (ctor5 != null) {
                    return ctor5.newInstance(c.ts, c.o, c.h, c.l, c.c);
                }
                if (ctor0 == null) return null;

                // 2) 无参构造 + set 字段
                Object ec = ctor0.newInstance();

                // 时间戳字段
                if (fTs != null)       fTs.setLong(ec, c.ts);
                else if (fOpenTsMs != null) fOpenTsMs.setLong(ec, c.ts);
                else if (fOpenTs != null)   fOpenTs.setLong(ec, c.ts);

                if (fO != null) fO.setDouble(ec, c.o);
                if (fH != null) fH.setDouble(ec, c.h);
                if (fL != null) fL.setDouble(ec, c.l);
                if (fC != null) fC.setDouble(ec, c.c);
                if (fVol != null) fVol.setDouble(ec, c.vol);
                if (fConfirm != null) fConfirm.setInt(ec, c.confirm);

                return ec;
            } catch (Throwable t) {
                return null;
            }
        }
    }


    /**
     *  计算“某一根K作为入场K(entryIdx)”时，按【与真实入场完全一致】的过滤链得到入场方向。
     * - 入场K = m30[entryIdx]（next-open）
     * - 信号K0 = m30[entryIdx-1]（上一根已收盘）
     * - silent=true 时不打印任何拦截日志，仅返回 side/null
     */
    static Side computeEntrySideAtIndex(
            List<Candle> m30, List<MacdPoint> macd30,
            List<Candle> h2,  List<MacdPoint> macd1h,
            int entryIdx,
            boolean silent
    ) {
        if (m30 == null || macd30 == null) return null;
        if (entryIdx <= 0 || entryIdx > m30.size()) return null;  // ✅ 允许 entryIdx == m30.size()（无K1，虚拟下一根开盘）

        int sigIdx = entryIdx - 1; // 信号K0索引（上一根已收盘）
        if (sigIdx < 2) return null;

        Candle entryC;
        if (entryIdx == m30.size()) {
            // ✅ 没有 K1：用 K0 收盘时刻 + BAR30_MS 构造“虚拟下一根开盘”（仅用于进场模拟/展示）
            Candle k0 = m30.get(entryIdx - 1);
            entryC = virtualEntryCandleFromK0(k0);
        } else {
            entryC = m30.get(entryIdx);
        }
        Candle sigC = m30.get(sigIdx);

        // 亚洲盘过滤：按入场K时间对齐（与实盘一致）
        if (isAsiaLowLiquidityTime(entryC.ts)) return null;

        // 先给出方向（仅基于 DIF/DEA）
        Side side = entrySideByDifDea(m30, macd30, sigIdx);

        //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1）
        //  ⚠️ entryIdx 允许 == m30.size()（无K1），此时不能再 m30.get(entryIdx)；
        //  用 entryC.ts（虚拟下一根开盘时间=K0.close+BAR30_MS）即可与实盘/回测时间口径一致。
        side = applyDecemberNoEntryGate(side, m30, sigIdx, entryC.ts, "回看", silent);
        if (side == null) return null;

        // ---- Pool-2 投票机（并行，不改原逻辑；可选硬过滤） ----
        Pool2VoteResult pool2 = evalPool2Vote(m30, macd30, sigIdx, side, silent, "BACKTEST_ENTRY");
        if (USE_POOL2_ENTRY_FILTER && pool2 != null && !pool2.matchSide(side)) {
            if (!silent) {
                // 复用系统已有的 blockEntry(side, ts, phase, reason) 输出格式
                side = blockEntry(side, entryC.ts, "回测", "POOL2投票不一致(硬过滤): " + pool2.toOneLine());
            }
            return null;
        }
        // 1H 过滤：按入场K时间对齐（与实盘一致）
        Trend1HInfo t1hInfo = null;
        if (USE_1H_FILTER) {
            t1hInfo = get1HTrendInfoAt(h2, macd1h, entryC.ts);

            if (t1hInfo.neutral) side = null;
            if (t1hInfo.trend == Trend1H.中性) side = null;
            if (FILTER_1H_NEUTRAL_FIRST_BAR && t1hInfo.isFirstNeutralBar) side = null;

            if (FILTER_1H_SECOND_TREND_BAR && t1hInfo.isSecondTrendBarAfterNeutral) {
                side = null;
            }

            if (side == Side.LONG  && t1hInfo.trend != Trend1H.多) side = null;
            if (side == Side.SHORT && t1hInfo.trend != Trend1H.空) side = null;
        }

        if (side == null) return null;

        //  解法三：市场状态机（Trend / Range）
        if (USE_MARKET_REGIME_FILTER) {
            int h1Idx = -1;
            if (t1hInfo != null) h1Idx = t1hInfo.idx;
                // BUG FIX: 与回测口径对齐。entryC.ts=K1开盘时刻，使用 entryC.ts（K1开盘/入场边界时刻）对应当下可用的最后已收盘1H，避免超前到K1收盘造成回测/实盘分叉。
            else if (h2 != null && !h2.isEmpty()) h1Idx = regimeSafeH1Idx(h2, entryC.ts);

            // ✅硬校验：强制锚点 1H 必须在 entryC.ts 前已收盘，否则回退到安全 idx，并打印硬检查
            h1Idx = enforceRegimeAnchorH1Idx(h2, entryC.ts, h1Idx, "BT/ENTRY_SIDE@entryC.ts");

            // 调试：确认 02:30 的机会不会错误用到 02:00-03:00 这根 1H
            debugPrintRegimePick1H("BT/ENTRY_SIDE@entryC.ts", entryC.ts, h2, h1Idx);

            MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
            if (regime == MarketRegime.RANGE) {
                if ((!silent) || ((IN_SCAN_HISTORY || IN_BOOT_BACKTEST) && BT_PRINT_BLOCKED)) {
                    System.out.printf(Locale.US,
                            "【状态机过滤-回看】ts=%s | regime=RANGE → 趋势系统不进场 | %s%n",
                            FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                            formatRegimeInfo(h2, h1Idx)
                    );
                    if ((IN_SCAN_HISTORY || IN_BOOT_BACKTEST) && BT_PRINT_BLOCKED) {
                        System.out.printf(Locale.US,
                                "【回测-进场(被过滤:REGIME=RANGE)】 时间=%s | entryTs=%s | %s%n",
                                FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                formatRegimeInfo(h2, h1Idx)
                        );
                    }
                }
                return null;
            }
        }

        // 质量过滤：检查“信号K0”（上一根已收盘K）
        if (USE_QUALITY_FILTER && !passQuality(m30, sigIdx)) return null;

        //  EMA 投票过滤（entryIdx 的开盘时间=entryC.ts）
        if (USE_EMA_VOTE_FILTER) {
            EmaVoteContext ctx = (EMA_VOTE_CTX_BACKTEST != null ? EMA_VOTE_CTX_BACKTEST : EMA_VOTE_CTX_REALTIME);
            if (ctx != null) {
                Side s2 = applyEmaVoteFilter(side, entryC.ts, ctx, true, "回看");
                if (s2 == null) return null;
            }
        }

        //  进场参考价：统一用「信号K0收盘价」作为进场价（实盘/回测一致，避免 next-open 偏差）
        //  注意：entryC.o 仍可用于展示 nextOpen，但不作为进场价/过滤门槛的基准
        final double entryRefPx = sigC.c;

        //  解法一：手续费 / 波动门槛（用信号K0及之前数据计算，避免未来函数）
        double atrSig = calcAtr(m30, sigIdx, FEE_VOL_ATR_N);
        if (!passFeeVolGuard(atrSig, entryRefPx)) {
            if (!silent) {
                System.out.printf(Locale.US,
                        "【手续费/波动过滤-回看】ts=%s | %s → 不进场%n",
                        FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                        feeVolGuardReason(atrSig, entryRefPx)
                );
            }
            return null;
        }




        // 解法一.5：slPts/entry 的分位数门槛（动态阈值，只用过去数据）
        if (USE_SL_PCT_Q_GATE) {
            SlPctGateResult g = evalSlPctQuantileGate(m30, sigIdx, entryRefPx);
            if (!g.pass) {
                if (!silent) {
                    System.out.printf(Locale.US,
                            "【SL%%分位过滤-回看】ts=%s | slPts=%.2f | entry=%.2f | slPct=%.5f > q%.0f=%.5f (n=%d, lb=%d) → 不进场%n",
                            FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                            g.slPts, entryRefPx, g.slPct,
                            (SL_PCT_Q * 100.0), g.qVal, g.sampleN, SL_PCT_Q_LOOKBACK_BARS
                    );
                }
                return null;
            }
        }
        // ===== TSRND 外层投票（用于冲突检查时，也保持一致：方向票不过则视为无信号）=====
        if (USE_TSRND_OUTER_VOTE && side != null) {
            OuterVoteDecision ov = evalTsRndOuterVote(m30, macd30, sigIdx, side, "回看", silent);
            if (ov.score < TSRND_SCORE_THRESHOLD) return null;
        }

        return side;
    }

    // 空：K1.dea<0 && K2.dea<0 && int(K1.dea - K2.dea) >= 2

    // =====================  止盈：TP（全平） =====================
    static boolean hitTP2(Position pos, Candle c) {
        if (pos == null) return false;
        if (TP2_TRIGGER <= 0) return false;

        if (pos.side == Side.LONG) {
            return (c.h - pos.entry) >= TP2_TRIGGER;
        } else {
            return (pos.entry - c.l) >= TP2_TRIGGER;
        }
    }

    static double tp2FillPrice(Position pos) {
        if (pos.side == Side.LONG) return pos.entry + TP2_TRIGGER;
        else return pos.entry - TP2_TRIGGER;
    }

    // =====================  更新 MFE / MAE（每根K都更新一次） =====================
    static void updateMfeMae(Position pos, Candle c) {
        if (pos == null) return;

        if (pos.side == Side.LONG) {
            double fav = c.h - pos.entry;   // 最大浮盈
            double adv = pos.entry - c.l;   // 最大浮亏幅度（点）
            if (fav > pos.mfe) pos.mfe = fav;
            if (adv > pos.mae) pos.mae = adv;
        } else {
            double fav = pos.entry - c.l;
            double adv = c.h - pos.entry;
            if (fav > pos.mfe) pos.mfe = fav;
            if (adv > pos.mae) pos.mae = adv;
        }
    }

    // =====================  你新定义的 MACD“颜色反转” =====================
    // macdStrength = 2 * ( |dif| + |dea| )
    static double macdStrength(MacdPoint m) {
        return 2.0 * (Math.abs(m.dif) + Math.abs(m.dea));
    }

    // 多：当前strength < 上根strength；空：当前strength > 上根strength
    static boolean isMacdColorReversal(List<MacdPoint> macd, int i, Side side) {
        if (macd == null) return false;
        int n = macd.size();
        if (n < 2) return false;
        if (i < 1) return false;

        // 关键修复：回测最后一根“虚拟入场K”会让 i==n（m30.size），但 macd 只有 n 个点（0..n-1）
        // 虚拟K不参与指标计算：这里按最后一根已闭合K的 macd 作为当前点
        int idx = i;
        if (idx >= n) idx = n - 1;
        int prevIdx = idx - 1;
        if (prevIdx < 0) return false;

        double cur = macdStrength(macd.get(idx));
        double prev = macdStrength(macd.get(prevIdx));
        if (side == Side.LONG) return cur < prev;
        return cur < prev;
    }

    // ===== 美盘时段过滤：开启后只在美股开盘(09:30-16:00 美东, 周一~周五)时段开仓 =====
    // 注意：只按“周中 + 时段”判断，未排除美股节假日(感恩节/独立日等)；这些日子仍会放行(影响极小)。
    static final boolean US_SESSION_ONLY = sysBool("okx.usSessionOnly", true);
    static final ZoneId US_ZONE = ZoneId.of("America/New_York");
    static boolean isOutsideUsMarketHours(long tsMs) {
        ZonedDateTime z = Instant.ofEpochMilli(tsMs).atZone(US_ZONE);
        DayOfWeek dow = z.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) return true; // 周末
        int mins = z.getHour() * 60 + z.getMinute();
        return (mins < 9 * 60 + 30) || (mins >= 16 * 60);                       // 盘外(09:30前 / 16:00后)
    }

    static boolean isAsiaLowLiquidityTime(long tsMs) {
        // [新增] 美盘时段过滤(独立开关)：开启后，非美股开盘时段一律拦截开仓（不影响已持仓的止盈/止损/管理）
        if (US_SESSION_ONLY && isOutsideUsMarketHours(tsMs)) return true;
        if (!FILTER_ASIA_LOW_LIQ) return false;

        ZonedDateTime z = Instant.ofEpochMilli(tsMs).atZone(ZONE);
        int h = z.getHour();
        DayOfWeek dow = z.getDayOfWeek();

        // 原有时间过滤
        boolean block1 = (h >= 19);
        boolean block2 = (h == 6);
        boolean block7 = (h == 7);
        boolean block8 = (h == 8);
        boolean block9 = (h == 9);
        boolean block15 = (h >= 15);

        // 周一到周四 05:00 禁用
        boolean monToThu5 =
                (dow == DayOfWeek.MONDAY
                        || dow == DayOfWeek.TUESDAY
                        || dow == DayOfWeek.WEDNESDAY
                        || dow == DayOfWeek.THURSDAY)
                        && h == 5;


        // 周六周日 08:00 以后禁用
        boolean weekendMorningToEvening =
                ((dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) && h >= 8);

        return weekendMorningToEvening
                || monToThu5
                || block1
                || block2
                || block7
                || block8
                || block9
                || block15;
    }

    // =====================  新增：12月禁止开仓（只影响进场；月份判断只用30m-K1避免未来函数） =====================
    static boolean isDecemberByK1(List<Candle> m30, int sigIdx) {
        if (!DISABLE_DECEMBER_NEW_ENTRY) return false;
        if (m30 == null || m30.isEmpty()) return false;
        int k1Idx = sigIdx - 1; // 只用K1（信号K0的上一根）判断月份
        if (k1Idx < 0 || k1Idx >= m30.size()) return false;
        long ts = m30.get(k1Idx).ts;
        int month = Instant.ofEpochMilli(ts).atZone(ZONE).getMonthValue();
        return month == 12;
    }

    static Side applyDecemberNoEntryGate(Side side, List<Candle> m30, int sigIdx, long tsForPrint, String phase, boolean silent) {
        if (side == null) return null;
        if (!DISABLE_DECEMBER_NEW_ENTRY) return side;
        if (!isDecemberByK1(m30, sigIdx)) return side;
        if (silent) return null;
        return blockEntry(side, tsForPrint, phase, "12月禁止开仓（按30m-K1月份判断）");
    }

    // ===================== 回测开仓日期窗口门（样本外/时间切分用）=====================
    // 把 "yyyy-MM-dd" 解析为当日 00:00（ZONE时区）的 epoch ms；空串返回哨兵值。
    static long parseWindowMs(String ymd, long fallback) {
        if (ymd == null || ymd.trim().isEmpty()) return fallback;
        try {
            return java.time.LocalDate.parse(ymd.trim())
                    .atStartOfDay(ZONE).toInstant().toEpochMilli();
        } catch (Throwable t) {
            System.out.println("[BT-WINDOW][WARN] 日期解析失败，按不限制处理: " + ymd);
            return fallback;
        }
    }
    static final long BT_WIN_START_MS = parseWindowMs(BT_ENTRY_WINDOW_START, Long.MIN_VALUE);
    static final long BT_WIN_END_MS   = parseWindowMs(BT_ENTRY_WINDOW_END,   Long.MAX_VALUE);

    // 仅按"开仓信号K的时间"判断是否在允许窗口内。窗口外只挡开仓，不影响数据遍历/持仓管理。
    static Side applyBtEntryWindowGate(Side side, long entryTsMs) {
        if (side == null) return null;
        if (BT_WIN_START_MS == Long.MIN_VALUE && BT_WIN_END_MS == Long.MAX_VALUE) return side; // 未设窗口=零变更
        if (entryTsMs < BT_WIN_START_MS || entryTsMs >= BT_WIN_END_MS) return null;            // 窗口外：不开仓
        return side;
    }
    // =====================  1H 动能结构判断（替代部分绝对阈值） =====================
    static boolean is1HMomentumNeutral(List<MacdPoint> macd1h, int i) {
        if (i < 4) return true; // 数据不足时保守当中性

        // 最近三段 dif 增量
        double d1 = macd1h.get(i - 1).dif - macd1h.get(i - 2).dif;
        double d2 = macd1h.get(i - 2).dif - macd1h.get(i - 3).dif;
        double d3 = macd1h.get(i - 3).dif - macd1h.get(i - 4).dif;

        // 同向但不再增强（动能走平或衰减）
        boolean noAcceleration =
                Math.abs(d1) <= Math.abs(d2) &&
                        Math.abs(d2) <= Math.abs(d3);

        return noAcceleration ;
    }
    // =====================  新增：1H 中性判定升级 + 中性第一根识别 =====================
    static Trend1HInfo get1HTrendInfoAt(List<Candle> h2, List<MacdPoint> macd1h, long ts30m) {
        Trend1HInfo info = new Trend1HInfo();
        info.trend = Trend1H.中性;
        info.isFirstNeutralBar = false;
        info.isSecondTrendBarAfterNeutral = false;
        info.hist = 0.0;
        info.prevHist = 0.0;
        info.idx = -1;

        // 仅使用“在 ts30m 时刻已收盘”的 1H K线（避免未来函数）
        if (h2 == null || h2.isEmpty() || macd1h == null || macd1h.isEmpty()) {
            info.neutral = true;
            return info;
        }

        // ts30m 对齐到最后一根“已收盘1H”：k1h.ts + 1H <= ts30m
        long tsNeed = ts30m - BAR1H_MS;
        int idx0 = upperBoundCandle(h2, tsNeed) - 1;
        if (idx0 < 2) { // 需要至少 k0/k1/k2 三根
            info.neutral = true;
            return info;
        }
        if (idx0 >= macd1h.size()) idx0 = macd1h.size() - 1; // 保险：防越界
        if (idx0 < 2) { info.neutral = true; return info; }

        info.idx = idx0;

        // 1H“最新已收盘”三根：k0(当前)、k1(上一根)、k2(上上根)
        MacdPoint k0 = macd1h.get(idx0);
        MacdPoint k1 = macd1h.get(idx0 - 1);
        MacdPoint k2 = macd1h.get(idx0 - 2);

        info.hist = k0.hist;
        info.prevHist = k1.hist;

        // ===== 阈值：用最近 N 根 1H 的 DIF/DEA 绝对值平均值替代固定 10（无未来函数）=====
        // 注意：这里“平均阈值”只使用 <= idx0 的已收盘数据，不会使用未来K线。
        int end = FILTER_1H_AVG_INCLUDE_SELF ? idx0 : (idx0 - 1);
        if (end < 0) { info.neutral = true; return info; }
        int start = Math.max(0, end - (FILTER_1H_AVG_N - 1));

        double sumAbsDif = 0.0, sumAbsDea = 0.0;
        int cnt = 0;
        for (int i = start; i <= end; i++) {
            MacdPoint p = macd1h.get(i);
            sumAbsDif += Math.abs(p.dif);
            sumAbsDea += Math.abs(p.dea);
            cnt++;
        }
        double difTh = (cnt > 0) ? (sumAbsDif / cnt) : 0.0;
        double deaTh = (cnt > 0) ? (sumAbsDea / cnt) : 0.0;

        // ===== 新 1H 过滤逻辑（仅此一套；旧逻辑已清理）=====
        // 多：k0.dif > 阈值 且 k0.dif > k1.dif > k2.dif
        boolean longOk = (k0.dif > difTh*1.5) && (k0.dif > k1.dif) && (k1.dif > k2.dif);

        // 空：k0.dea < -阈值 且 k0.dea < k1.dea < k2.dea
        boolean shortOk = (k0.dea < -deaTh*1.5) && (k0.dea < k1.dea) && (k1.dea < k2.dea);

        // 默认空头优先
        if (shortOk) {
            info.trend = Trend1H.空;
            info.neutral = false;
        } else if (longOk) {
            info.trend = Trend1H.多;
            info.neutral = false;
        } else {
            info.trend = Trend1H.中性;
            info.neutral = true;
        }

        // 旧字段保留（仅用于打印/兼容；本逻辑不再使用这些中性细分原因）
        info.neutralByAbs = false;
        info.neutralByOsc = false;
        info.neutralByFlip = false;

        return info;
    }

    //==============   //新增信号质量过滤规则调用方法===========
    static double calcAvgRangePoints(List<Candle> ks, int endIdxInclusive, int n){
        if (ks == null || ks.isEmpty() || n <= 0) return 0.0;
        int end = Math.min(endIdxInclusive, ks.size() - 1);
        if (end < 0) return 0.0;

        int start = Math.max(0, end - n + 1);
        double sum = 0.0;
        int cnt = 0;
        for (int i = start; i <= end; i++) {
            Candle k = ks.get(i);
            double r = k.h - k.l;
            if (Double.isFinite(r) && r > 0) {
                sum += r;
                cnt++;
            }
        }
        return cnt == 0 ? 0.0 : (sum / cnt);
    }

    static double getMinRangeNeed(List<Candle> ks, int sigIdx){
        if (!USE_DYNAMIC_MIN_RANGE) return MIN_RANGE_POINTS;
        int end = MIN_RANGE_INCLUDE_SELF ? sigIdx : (sigIdx - 1);
        double v = calcAvgRangePoints(ks, end, MIN_RANGE_LOOKBACK_N);
        if (!Double.isFinite(v) || v <= 0) v = MIN_RANGE_POINTS; // fallback（极端情况下）
        return v;
    }

    static boolean passQuality(List<Candle> ks, int sigIdx){
        if (ks == null || sigIdx < 0 || sigIdx >= ks.size()) return false;
        Candle k1 = ks.get(sigIdx);

        double range = k1.h - k1.l;
        if (range <= 0) return false;

        double body = Math.abs(k1.c - k1.o);
        double bodyRatio = body / range;

        // 规则A：实体占比过滤（避免上影下影乱扫）
        if (bodyRatio < MIN_BODY_RATIO) return false;

        // 规则B：最小波动过滤（阈值=最近N根平均波动，无未来函数参与）
        double minRangeNeed = getMinRangeNeed(ks, sigIdx);
        if (range < minRangeNeed) return false;

        return true;
    }

    // 兼容：旧签名（不建议使用，因无法计算“最近N根”动态阈值）
    static boolean passQuality(Candle k1){
        double range = k1.h - k1.l;
        if (range <= 0) return false;
        double body = Math.abs(k1.c - k1.o);
        double bodyRatio = body / range;
        if (bodyRatio < MIN_BODY_RATIO) return false;
        if (range < MIN_RANGE_POINTS) return false;
        return true;
    }

    // 统一的“进场阻断”工具：打印原因并返回 null（用于尾盘/回看/实盘保持一致的过滤输出）
    static Side blockEntry(Side side, long ts, String phase, String reason) {
        if (side == null) return null;
        if (!DEBUG_FILTER_TRACE && ("回测".equals(phase) || "回看".equals(phase) || "BACKTEST".equals(phase))) {
            return null;
        }
        String dir = (side == Side.LONG ? "做多" : "做空");
        System.out.println("【" + phase + "进场过滤】" + reason + " ｜已屏蔽方向=" + dir
                + " ｜时间=" + FMT_JST_SHORT.format(Instant.ofEpochMilli(ts)));
        return null;
    }

    // 仅用于打印解释：当前1H波动 vs 分位数阈值
    static String marketRegimeReason(List<Candle> h1, int endIdx) {
        if (!USE_MARKET_REGIME_FILTER) return "disabled";
        if (h1 == null || h1.isEmpty()) return "no_1h_data";
        if (endIdx < 0 || endIdx >= h1.size()) return "bad_idx";
        double cur = h1.get(endIdx).h - h1.get(endIdx).l;
        if (!Double.isFinite(cur) || cur <= 0) return "curRange_invalid";

        int start = Math.max(0, endIdx - REGIME_LOOKBACK_BARS_1H + 1);
        ArrayList<Double> ranges = new ArrayList<>();
        for (int i = start; i <= endIdx; i++) {
            double r = h1.get(i).h - h1.get(i).l;
            if (Double.isFinite(r) && r > 0) ranges.add(r);
        }
        int minNeed = Math.max(50, REGIME_LOOKBACK_BARS_1H / 4);
        if (ranges.size() < minNeed) {
            return String.format(Locale.US,
                    "insufficient ranges=%d < %d (lookback=%d)",
                    ranges.size(), minNeed, REGIME_LOOKBACK_BARS_1H);
        }
        Collections.sort(ranges);
        int qIdx = (int) Math.floor((ranges.size() - 1) * REGIME_TREND_RANGE_Q);
        qIdx = Math.max(0, Math.min(qIdx, ranges.size() - 1));
        double thr = ranges.get(qIdx);

        return String.format(Locale.US,
                "curRange=%.2f vs thr(q=%.2f)=%.2f | lookback=%d used=%d",
                cur, REGIME_TREND_RANGE_Q, thr, REGIME_LOOKBACK_BARS_1H, ranges.size());
    }

    // ===================== 回测：按闹钟结构的入/出场判定 =====================
    static List<CompletedTrade> runBacktestToState(EngineState eng, List<Candle> m30, List<MacdPoint> macd30,
                                                   List<Candle> h2,  List<MacdPoint> macd1h) {
        if (eng == null) eng = new EngineState();
        //  回测前清空“最近机会/最近10笔”缓存，保证回测后辅助输出不为空
        eng.lastEnterOpp = null;
        eng.lastExitOpp = null;
        eng.lastTrades.clear();
        eng.equity = capital;
        eng.srmf = SRMFState.init(capital);
        eng.pos = null;


        //  EMA 多时间投票上下文（回测）
        EMA_VOTE_CTX_BACKTEST = (USE_EMA_VOTE_FILTER ? EmaVoteContext.build(m30, h2) : null);

        List<CompletedTrade> trades = new ArrayList<>();
        Position cur = null;
        // RT持仓：用于决定是否抑制回测视图的“入场打印”（避免与你实时持仓冲突）
        // [FIX-3] IN_BOOT_BACKTEST/IN_SCAN_HISTORY 时强制 false：
        // 完整推演必须打印进场日志，不能因 LIVE_ENGINE.pos 残留而全部抑制
        boolean holdingRtPos = (LIVE_ENGINE != null && LIVE_ENGINE.pos != null && LIVE_ENGINE.pos.side != null)
                && !IN_BOOT_BACKTEST && !IN_SCAN_HISTORY;

        long skipEntrySignalTsAfterStop = -1L; //  止损后：禁止同一信号K再进场（禁止一根K线交易）
        long cooldownUntilEntryTsAfterStop = -1L; //  止损冷却：在此时间前不允许再入场（entryTs=下一根K开盘）

        long skipEntrySignalTsAfterTp = -1L; //  止盈后：禁止同一信号K再进场（禁止一根K线交易）
        long cooldownUntilEntryTsAfterTp = -1L; //  止盈冷却：在此时间前不允许再入场（entryTs=下一根K开盘）
        for (int i = 1; i <= m30.size(); i++) {
            // 入场K（next-open）：
            // - 常规：c = m30[i]（真实下一根K）
            // - 关键修复：当 i==m30.size() 时，构造一个“虚拟入场K”（ts=最后一根K0.ts+BAR30_MS, open=K0.close）
            //   这样历史回看/推演不依赖 placeholder 也能在“下一根开盘时间”产生入场机会（仅用于入场模拟，不参与指标计算）
            Candle c;
            boolean virtualEntry = false;
            if (i < m30.size()) {
                c = m30.get(i);
            } else {
                virtualEntry = true;
                Candle k0 = m30.get(m30.size() - 1);
                c = new Candle(k0.ts + BAR30_MS, k0.c, k0.c, k0.c, k0.c, 0.0, 0);
            }


            // 逐K推进去重标记：记录本轮已推进到的最新“信号K0”（上一根已收盘K）
            // 目的：让 [POS-CORE] 的 lastProcessedSigTs 不再长期为 -1，并用于实盘/回测一致的“同K去重”语义。
            // 口径：signalTs = m30[i-1].ts（上一根已收盘K 的开盘时间）；与 Position.signalTs/回放库字段保持一致。
            if (i - 1 >= 0 && i - 1 < m30.size()) {
                Candle __sigK0 = m30.get(i - 1);
                if (__sigK0 != null) {
                    eng.lastProcessedSigTs = __sigK0.ts;
                }
            }
            // 若已持仓而又到了虚拟入场K（没有后续真实K可用于出场判定），直接结束回测循环
            if (virtualEntry && cur != null) {
                // [FIX] OPEN_AT_BT_END：将未平仓持仓写入 trades/eng.pos，防止：
                // 1) __bt.pos=null → buildSyncEvent 误判出场 → 触发出场误闹钟
                // 2) lastEnterOpp/最近10笔 看不到这笔进场记录
                Candle k0Last = m30.get(m30.size() - 1);
                CompletedTrade ct = new CompletedTrade();
                ct.enterTs        = cur.entryTs;
                ct.signalTs       = cur.signalTs;
                ct.side           = cur.side;
                ct.entry          = cur.entry;
                ct.exit           = k0Last.c;
                ct.exitTs         = k0Last.ts + BAR30_MS;
                ct.isStop         = false;
                ct.entrySource    = cur.entrySource;
                ct.reason         = "OPEN_AT_BT_END（回测结束时仍持仓，尚未出场）";
                double unrealPnl  = (cur.side == Side.LONG)
                        ? (ct.exit - ct.entry) : (ct.entry - ct.exit);
                double feePts     = okxFeePoints(ct.entry, ct.exit);
                ct.grossPnlPoints = unrealPnl;
                ct.feePoints      = feePts;
                ct.pnlPoints      = unrealPnl - feePts;
                ct.entryEquity    = cur.entryEquity;
                ct.exitEquity     = eng.equity;
                ct.pnlUSDT        = 0.0;
                ct.mult           = cur.entryMult;
                ct.slPointsUsed   = cur.slPointsAtEntry;
                ct.regime         = cur.regimeAtEntry;
                ct.rMultiple      = ct.pnlPoints / Math.max(1e-9, cur.slPointsAtEntry);
                ct.trendPtsAfter  = SRMF.getTrendPts(eng.srmf);
                ct.bayesPosterior = cur.bayesPosterior;
                ct.mfe            = cur.mfe;
                ct.mae            = cur.mae;
                trades.add(ct);
                recordClosedTradeForView(eng, ct);
                eng.pos = cur;
                if (VERBOSE_BACKTEST && !holdingRtPos) {
                    System.out.printf(Locale.US,
                            "[SRMF-持仓(BT末尾)] 时间=%s | 方向=%s | 入场=%.2f | 末K收盘=%.2f | 未实现=%.2f点(%+.2f点扣费) | MFE=%.2f MAE=%.2f%n",
                            FMT_JST.format(Instant.ofEpochMilli(ct.exitTs)),
                            (cur.side == Side.LONG ? "做多" : "做空"),
                            ct.entry, ct.exit, ct.grossPnlPoints, ct.pnlPoints,
                            ct.mfe, ct.mae);
                }
                break;
            }

            // 注意：t1hInfo 在“无持仓找进场”时，应以【入场K】时间对齐。
            // 你是 next-open 入场，如果用信号K0(上一根)的 ts 去取 1H (>neutral) ，
            // 会出现 00:30 的 1H 被判 neutral，从而把 01:00 这一根入场K错杀，
            // 结果拖到 01:30 才进（你截图里遇到的情况）。
            Trend1HInfo t1hInfo = new Trend1HInfo();
            t1hInfo.trend = Trend1H.中性;

            // ========== 没持仓：找进场 ==========
            if (cur == null) {
                //  统一对位规则（年回测 / 最近10次 / 实盘）
                // 信号：上一根已收盘K = sigIdx
                // 入场：下一根K(i) 的 open（next-open）
                int sigIdx = i - 1;
                if (sigIdx < 2) { continue; }

                // ★ [BT-DIAG] 尾部诊断：最后6根K打印完整过滤链
                boolean diagTail = (IN_BOOT_BACKTEST || IN_SCAN_HISTORY) && (i >= m30.size() - 5);
                // [临时诊断] 强制打印指定时间(入场K c.ts)的完整过滤链：
                //   用法：-Dokx.diagForceTs=2026-05-28 03:00   （匹配入场K开盘时间前缀，可只写到分钟/小时/天）
                //   不设置该参数时不影响任何行为（DIAG_FORCE_TS 为空 → 不触发）。
                if (DIAG_FORCE_TS != null && !DIAG_FORCE_TS.isEmpty()) {
                    String _cts = FMT_JST.format(Instant.ofEpochMilli(c.ts));
                    if (_cts.startsWith(DIAG_FORCE_TS)) diagTail = true;
                }
                String diagTs = diagTail ? FMT_JST.format(Instant.ofEpochMilli(c.ts)) : "";
                String diagSig = diagTail ? FMT_JST.format(Instant.ofEpochMilli(m30.get(sigIdx).ts)) : "";

                //  止损冷却：止损后等待 N 根30m K 再允许开仓（与实盘一致）
                if (blockedByStoplossCooldown(c.ts, cooldownUntilEntryTsAfterStop)) {
                    if (VERBOSE_BACKTEST && PRINT_STOPLOSS_COOLDOWN_BLOCK) {
                        long remainBars = Math.max(0, (cooldownUntilEntryTsAfterStop - c.ts + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【止损冷却拦截-回测】候选入场=%s | cooldownUntil=%s | remainBars=%d → 不进场%n",
                                FMT_JST.format(Instant.ofEpochMilli(c.ts)),
                                FMT_JST.format(Instant.ofEpochMilli(cooldownUntilEntryTsAfterStop)),
                                remainBars
                        );
                    }
                    continue;
                }

                //  止盈冷却：止盈后等待 N 根30m K 再允许开仓（与实盘一致）
                if (blockedByTakeprofitCooldown(c.ts, cooldownUntilEntryTsAfterTp)) {
                    if (VERBOSE_BACKTEST && PRINT_TAKEPROFIT_COOLDOWN_BLOCK) {
                        long remainBars = Math.max(0, (cooldownUntilEntryTsAfterTp - c.ts + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【止盈冷却拦截-回测】候选入场=%s | cooldownUntil=%s | remainBars=%d → 不进场%n",
                                FMT_JST.format(Instant.ofEpochMilli(c.ts)),
                                FMT_JST.format(Instant.ofEpochMilli(cooldownUntilEntryTsAfterTp)),
                                remainBars
                        );
                    }
                    continue;
                }

                //  止损后：禁止使用“刚刚触发止损的那根信号K”在下一根立刻再入场（禁止一根K线交易）
                if (skipEntrySignalTsAfterStop == m30.get(sigIdx).ts) {
                    if (VERBOSE_BACKTEST) {
                        System.out.printf(Locale.US,
                                "【止损后禁止同K再进】信号时间=%s | 本次入场K=%s | 说明=上一根触发止损，下一根放弃该入场机会%n",
                                FMT_JST.format(Instant.ofEpochMilli(m30.get(sigIdx).ts)),
                                FMT_JST.format(Instant.ofEpochMilli(c.ts))
                        );
                    }
                    skipEntrySignalTsAfterStop = -1L;
                    continue;
                }

                //  止盈后：禁止使用“刚刚触发止盈的那根信号K”在下一根立刻再入场（禁止一根K线交易）
                if (skipEntrySignalTsAfterTp == m30.get(sigIdx).ts) {
                    if (VERBOSE_BACKTEST) {
                        System.out.printf(Locale.US,
                                "【止盈后禁止同K再进】信号时间=%s | 本次入场K=%s | 说明=上一根触发止盈，下一根放弃该入场机会%n",
                                FMT_JST.format(Instant.ofEpochMilli(m30.get(sigIdx).ts)),
                                FMT_JST.format(Instant.ofEpochMilli(c.ts))
                        );
                    }
                    skipEntrySignalTsAfterTp = -1L;
                    continue;
                }
                Candle sigC = m30.get(sigIdx); // 信号K0（上一根已收盘K）

                //  亚洲盘过滤：按入场K(i)时间对齐（与实盘/最近10次一致）
                if (isAsiaLowLiquidityTime(c.ts)) {
                    if (diagTail) System.out.printf("[BT-DIAG] entry=%s sig=%s | ❌亚洲盘%n", diagTs, diagSig);
                    continue;
                }

                //  1H趋势信息：按【入场K(i)】时间对齐（修复 01:00 被 00:30 的1H neutral 错杀）
                if (USE_1H_FILTER && !h2.isEmpty() && !macd1h.isEmpty()) {
                    t1hInfo = get1HTrendInfoAt(h2, macd1h, c.ts) /* 按入场K时间取1H */;
                }

                Side side = entrySideByDifDea(m30, macd30, sigIdx);
                String entrySource = LAST_ENTRY_SOURCE.get();
                if (diagTail) {
                    MacdPoint _dk0 = macd30.get(sigIdx);
                    MacdPoint _dk1 = (sigIdx >= 1) ? macd30.get(sigIdx - 1) : null;
                    MacdPoint _dk2 = (sigIdx >= 2) ? macd30.get(sigIdx - 2) : null;
                    Candle _dSigC = m30.get(sigIdx);
                    System.out.printf(Locale.US,
                            "[BT-DIAG] entry=%s sig=%s(c=%.2f) virt=%s | K0dif=%.2f K1dif=%.2f K2dif=%.2f K0dea=%.2f K1dea=%.2f K2dea=%.2f | side=%s src=%s%n",
                            diagTs, diagSig, _dSigC.c, (virtualEntry ? "Y" : "N"),
                            _dk0.dif, (_dk1 != null ? _dk1.dif : 0), (_dk2 != null ? _dk2.dif : 0),
                            _dk0.dea, (_dk1 != null ? _dk1.dea : 0), (_dk2 != null ? _dk2.dea : 0),
                            (side != null ? side.name() : "NONE"),
                            (entrySource.isEmpty() ? "-" : entrySource)
                    );
                }


                //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1）
                boolean __probe = isProbeTs(c.ts);
                if (__probe) {
                    System.out.println("[PROBE][BT] entryTs=" + fmtTs(c.ts) + " sigIdx=" + sigIdx);
                }

                //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1）
                side = applyDecemberNoEntryGate(side, m30, sigIdx, c.ts, "回测", !__probe);
                if (__probe && side == null) {
                    System.out.println("[PROBE][BT] blocked: DecemberNoEntryGate");
                }

                //  回测日期窗口门（样本外/时间切分用；未设窗口时零影响）
                side = applyBtEntryWindowGate(side, c.ts);

                // =====================  R1 冷却硬门（回测/实盘同构） =====================
                if (blockedByR1VetoCooldown(eng, c.ts, side)) {
                    if (RV_PRINT_BLOCK) {
                        long until = getR1CooldownUntilTs(eng, side);
                        long lastVeto = getR1VetoTs(eng, side);
                        long remainBars = Math.max(0, (until - c.ts + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【R1冷却】%s 剩余%d根K，不进场 | now=%s | lastVeto=%s | until=%s%n",
                                (side == Side.LONG ? "LONG" : "SHORT"),
                                remainBars,
                                FMT_JST.format(Instant.ofEpochMilli(c.ts)),
                                (lastVeto > 0 ? FMT_JST.format(Instant.ofEpochMilli(lastVeto)) : "-"),
                                (until > 0 ? FMT_JST.format(Instant.ofEpochMilli(until)) : "-")
                        );
                    }
                    continue;
                }

                if (side == null) {
                    if (diagTail) System.out.printf("[BT-DIAG] entry=%s sig=%s | ⚪无信号(MACD/Dec/R1后side=null)%n", diagTs, diagSig);
                    continue;
                }

                // ---- Pool-2 投票机（与实盘一致；可选硬过滤） ----
                Pool2VoteResult pool2 = evalPool2Vote(m30, macd30, sigIdx, side, !__probe, "BACKTEST");
                if (USE_POOL2_ENTRY_FILTER && pool2 != null && !pool2.matchSide(side)) {
                    if (diagTail) System.out.printf("[BT-DIAG] entry=%s | side=%s | ❌Pool2 %s%n", diagTs, side, pool2.toOneLine());
                    if (__probe) System.out.println("[PROBE][BT] blocked: POOL2 hard filter: " + pool2.toOneLine());
                    continue;
                }
                if (diagTail && pool2 != null) System.out.printf("[BT-DIAG]   Pool2=✅ %s%n", pool2.toOneLine());
                if (USE_1H_FILTER) {
                    // 1H 中性过滤
                    if (t1hInfo.neutral) side = null;
                    if (t1hInfo.trend == Trend1H.中性) side = null;
                    if (FILTER_1H_NEUTRAL_FIRST_BAR && t1hInfo.isFirstNeutralBar) side = null;

                    // 第二根趋势K过滤（保留你原输出）
                    if (FILTER_1H_SECOND_TREND_BAR && t1hInfo.isSecondTrendBarAfterNeutral) {
                        System.out.printf("[BLOCK 2ND TREND] ts=%s t1h=%s idx=%d hist=%.4f prev=%.4f%n",
                                Instant.ofEpochMilli(c.ts).atZone(ZONE),
                                t1hInfo.trend, t1hInfo.idx, t1hInfo.hist, t1hInfo.prevHist);
                        side = null;
                    }

                    if (side == Side.LONG && t1hInfo.trend != Trend1H.多) side = null;
                    if (side == Side.SHORT && t1hInfo.trend != Trend1H.空) side = null;
                }
                if (__probe && side == null) {
                    System.out.println("[PROBE][BT] blocked: 1H filter (neutral/first/second/dir mismatch)");
                }
                //  解法三：市场状态机过滤（趋势态才允许趋势系统进场）
                if (side != null && USE_MARKET_REGIME_FILTER) {
                    int h1Idx = -1;
                    long entryTsForRegime = c.ts; // c=入场K(K1)，c.ts=K1开盘=K0收盘=实际进场边界（不能再+BAR30_MS，否则超前到K1收盘，引入未来函数）
                    if (USE_1H_FILTER) h1Idx = t1hInfo.idx;
                    else if (h2 != null && !h2.isEmpty()) {
                        h1Idx = regimeSafeH1Idx(h2, entryTsForRegime);
                    }

                    // ✅硬校验：强制锚点 1H 必须在 entryTsForRegime 前已收盘，否则回退到安全 idx，并打印硬检查
                    h1Idx = enforceRegimeAnchorH1Idx(h2, entryTsForRegime, h1Idx, "BT/ENTRY@c.ts+30m");

                    // 调试：打印该 entryTs 对应被选中的 1H，确认不会选到未来 1H（例如 02:30 不应选 02:00-03:00）
                    debugPrintRegimePick1H("BT/ENTRY@c.ts+30m", entryTsForRegime, h2, h1Idx);

                    MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
                    if (regime == MarketRegime.RANGE) {
                        if (diagTail) System.out.printf("[BT-DIAG] entry=%s | ❌Regime=RANGE%n", diagTs);
                        long entryTs = entryTsForRegime;
                        if ((IN_SCAN_HISTORY || IN_BOOT_BACKTEST) && BT_PRINT_BLOCKED) {
                            System.out.printf(Locale.US,
                                    "【回测-进场(被过滤:REGIME=RANGE)】 时间=%s | %s%n",
                                    FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                                    marketRegimeReason(h2, h1Idx)
                            );
                        }
                        if (__probe) System.out.println("[PROBE][BT] blocked: MarketRegime=RANGE | " + marketRegimeReason(h2, h1Idx));
                        side = null;
                    }
                }

                //  质量过滤：检查“信号K0”（上一根已收盘K）
                if (side != null) {
                    Candle k0Candle = m30.get(sigIdx);
                    if (USE_QUALITY_FILTER && !passQuality(m30, sigIdx)) {
                        double range = k0Candle.h - k0Candle.l;
                        double body = Math.abs(k0Candle.c - k0Candle.o);
                        double bodyRatio = (range <= 0 ? 0 : body / range);

                        double minRangeNeed = getMinRangeNeed(m30, sigIdx);
                        System.out.printf(
                                "【质量过滤拦截】%n" +
                                        "时间：%s%n" +
                                        "方向：%s%n" +
                                        "实体比例：%.2f %s 最低要求 %.2f%n" +
                                        "波动范围：%.2f %s 最低要求 %.2f%n" +
                                        "→ 本次信号已过滤%n%n",
                                FMT_JST.format(Instant.ofEpochMilli(k0Candle.ts + BAR30_MS)),
                                side,
                                bodyRatio,
                                (bodyRatio >= MIN_BODY_RATIO ? "≥" : "<"),
                                MIN_BODY_RATIO,
                                range,
                                (range >= minRangeNeed ? "≥" : "<"),
                                minRangeNeed
                        );

                        side = null;
                    }
                }
                //  EMA 投票过滤（入场时间=下一根30m开盘=c.ts）
                if (side != null && USE_EMA_VOTE_FILTER && EMA_VOTE_CTX_BACKTEST != null) {
                    EmaVoteSnapshot _emaSnap = diagTail ? EMA_VOTE_CTX_BACKTEST.snapshotAt(c.ts) : null;
                    Side _sideBeforeEma = side;
                    side = applyEmaVoteFilter(side, c.ts, EMA_VOTE_CTX_BACKTEST, false, "回测");
                    if (diagTail) {
                        double _emaThr = 3.0 * (EMA_VOTE_BASE + EMA_VOTE_STRONG_EXTRA) * EMA_VOTE_SCORE_PCT;
                        if (side == null) {
                            System.out.printf(Locale.US,
                                    "[BT-DIAG] entry=%s | ❌EMA投票 was=%s score=%.2f thr=%.2f pos=%d neg=%d v30=%.2f v1h=%.2f v4h=%.2f%n",
                                    diagTs, _sideBeforeEma, _emaSnap.score, _emaThr, _emaSnap.posCnt, _emaSnap.negCnt,
                                    _emaSnap.v30, _emaSnap.v1h, _emaSnap.v4h);
                        } else {
                            System.out.printf(Locale.US,
                                    "[BT-DIAG]   EMA=✅ score=%.2f pos=%d neg=%d v30=%.2f v1h=%.2f v4h=%.2f%n",
                                    _emaSnap.score, _emaSnap.posCnt, _emaSnap.negCnt,
                                    _emaSnap.v30, _emaSnap.v1h, _emaSnap.v4h);
                        }
                    }
                }
                if (side != null) {

                    // =====================  TSRND 外层投票：方向(2分) + 总体(+1) => 名义倍率 *1.0/*1.2 =====================
                    OuterVoteDecision ov = evalTsRndOuterVote(m30, macd30, sigIdx, side, "回测", true);
                    if (USE_TSRND_OUTER_VOTE) {
                        if (ov.score < TSRND_SCORE_THRESHOLD) {
                            if (diagTail) System.out.printf("[BT-DIAG] entry=%s | ❌TSRND score=%d < thr=%d%n", diagTs, ov.score, TSRND_SCORE_THRESHOLD);
                            continue;
                        }
                    }
                    int voteScore = ov.score;
                    double voteScale = ov.voteScale;
                    if (diagTail) System.out.printf("[BT-DIAG]   TSRND=✅ score=%d | 全部过滤器通过 → ENTER %s (src=%s)%n", voteScore, side, entrySource);
                    MacdPoint k0 = macd30.get(sigIdx);      // 信号K0（已收盘）
                    MacdPoint k1 = macd30.get(sigIdx - 1);  // 信号K1（已收盘）
                    int entryIdx = sigIdx + 1;                 //  next-open 的K索引
                    Candle entryC;                                  // 入场K = 信号K0 的下一根（其开盘时刻=K0收盘时刻）
                    if (entryIdx == m30.size()) {
                        // ✅ 没有 K1：允许用 K0 收盘时刻 + BAR30_MS 构造“虚拟下一根开盘”
                        // 仅用于进场模拟/展示，不参与任何指标计算，也不会写入 m30/macd
                        Candle k0c = m30.get(sigIdx);
                        entryC = virtualEntryCandleFromK0(k0c);
                    } else if (entryIdx > m30.size()) {
                        continue; // 防御：不应发生
                    } else {
                        entryC = m30.get(entryIdx);
                    }
                    Candle sigC0 = m30.get(sigIdx);          // 信号K0（已收盘）
                    double entryPx = sigC0.c;                // ★ 回测入场价：用 K0 收盘价（与提醒在 K0 收盘时刻一致）
                    double entryPxNextOpen = entryC.o;       // 仅对照：下一根开盘价
                    // =====================  OKX 撑压线入场限制：差 60 点以内不进（回测一致） =====================
                    SrLevels sr = calcOkxSrLevels(m30, sigIdx, OKX_SR_N);
                    if (USE_OKX_SR_ENTRY_FILTER && sr.valid) {
                        double dynSlPtsForGate = calcSlPointsForIndex(m30, sigIdx); // K0及之前（避免未来函数）
                        double minGapPts = okxSrEntryMinGapPts(dynSlPtsForGate);
                        double gap = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                        if (gap < minGapPts) {
                            if (diagTail) System.out.printf(Locale.US, "[BT-DIAG] entry=%s | ❌SR过滤 gap=%.2f < min=%.2f%n", diagTs, gap, minGapPts);
                            if (VERBOSE_BACKTEST) {
                                System.out.printf(Locale.US,
                                        "    【OKX撑压过滤拦截】ts=%s side=%s entry=%.2f | R=%.2f S=%.2f | gap=%.2f < %.2f → 不进场%n",
                                        FMT_JST.format(Instant.ofEpochMilli(c.ts + BAR30_MS)),
                                        side, entryPx,
                                        sr.resistance, sr.support,
                                        gap, minGapPts
                                );
                            }
                            continue;
                        }
                    }

                    //  回撤入场：仅用信号K0及之前数据计算（避免未来函数）
                    double atrSig = calcAtr(m30, sigIdx, ATR_N);
                    //  解法一：手续费/波动门槛（ATR至少覆盖K倍往返手续费）
                    if (USE_FEE_VOL_GUARD) {
                        double atrForGuard = (FEE_VOL_ATR_N == ATR_N) ? atrSig : calcAtr(m30, sigIdx, FEE_VOL_ATR_N);
                        if (!passFeeVolGuard(atrForGuard, entryPx)) {
                            if (false) {
                                System.out.printf(Locale.US,
                                        "【手续费/波动过滤-回测】ts=%s | side=%s | entry=%.2f | %s → 不进场%n",
                                        FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                        side, entryPx,
                                        feeVolGuardReason(atrForGuard, entryPx)
                                );
                            }
                            continue;
                        }
                    }

                    // 解法一.5：slPts/entry 的分位数门槛（动态阈值，只用过去数据）
                    if (USE_SL_PCT_Q_GATE) {
                        SlPctGateResult g = evalSlPctQuantileGate(m30, sigIdx, entryPx);
                        if (!g.pass) {
                            if (false) {
                                System.out.printf(Locale.US,
                                        "【SL%%分位过滤-回测】ts=%s | side=%s | slPts=%.2f | entry=%.2f | slPct=%.5f > q%.0f=%.5f (n=%d, lb=%d) → 不进场%n",
                                        FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                        side,
                                        g.slPts, entryPx, g.slPct,
                                        (SL_PCT_Q * 100.0), g.qVal, g.sampleN, SL_PCT_Q_LOOKBACK_BARS
                                );
                            }
                            continue;
                        }
                    }

                    double pbAtrPts = atrSig * PULLBACK_ATR_MULT;
                    double pbFixedPts = PULLBACK_FIXED_POINTS;
                    double pbUsedPts = (PULLBACK_MODE == PullbackMode.ATR ? pbAtrPts : pbFixedPts);
                    double pbLimitPx = (side == Side.LONG) ? (entryPx - pbUsedPts) : (entryPx + pbUsedPts);
                    boolean pbEnabled = ENABLE_PULLBACK_ENTRY_BACKTEST;
                    boolean pbFilled = !pbEnabled;
                    double entryFillPx = entryPx;

                    if (pbEnabled) {
                        // 订单仅在【入场K(entryC)】触达限价才成交（不触达则本次信号不成交）
                        if (side == Side.LONG) pbFilled = (entryC.l <= pbLimitPx);
                        else                   pbFilled = (entryC.h >= pbLimitPx);

                        if (pbFilled) {
                            entryFillPx = pbLimitPx;
                        }
                    }
                    // ===== [修复] R1风控否决提前到【回撤挂单】/【回测-进场】打印之前 =====
                    // 原来 R1 检查在打印之后：日志显示"进场"后才否决，造成混乱且
                    // 导致 pbEnabled=true 时 continue 绕过了回撤成交路径。
                    // 修复：R1 否决则直接 continue，不输出任何"进场"类日志。
                    // ===== 风控票（R1~R4）：只用信号K0已收盘K线（sigIdx），避免未来函数 =====
                    RiskVoteResult rvRisk = null;
                    double tierMultNow = SRMF.getCurrentMult(eng.srmf);
                    double entryMultNow = tierMultNow * voteScale;
                    if (USE_RISK_VOTE) {
                        Candle __sigC0 = (sigIdx >= 0 && sigIdx < m30.size()) ? m30.get(sigIdx) : null;
                        double __refPx = (__sigC0 != null ? __sigC0.c : entryPx);
                        rvRisk = evalRiskVote(m30, h2, sigIdx, side, __refPx, tierMultNow, voteScale,
                                (holdingRtPos || !VERBOSE_BACKTEST), "回测");
                        if (rvRisk != null && rvRisk.r1Veto) {
                            if (diagTail) System.out.printf("[BT-DIAG] entry=%s | ❌R1否决 %s%n", diagTs, rvRisk.detail);
                            markR1Veto(eng, entryC.ts, side);
                            if (VERBOSE_BACKTEST && !holdingRtPos) {
                                System.out.printf(Locale.US,
                                        "【R1反向长影线-否决】ts=%s | %s → 不进场 | %s%n",
                                        FMT_JST.format(Instant.ofEpochMilli(entryC.ts)), side, rvRisk.detail);
                            }
                            continue;
                        }
                        entryMultNow = (rvRisk != null ? rvRisk.multAfter : entryMultNow);
                    }



                    // ===== BAYES：多特征朴素贝叶斯 过滤/仓位缩放（不改信号，仅调 entryMultNow）=====
                    double bayesPosterior = 0.5;
                    double bayesScale = 1.0;
                    Map<MultiBinBayesFilter.Feature, Integer> bayesBins = null;
                    EnumMap<MultiBinBayesFilter.Feature, Double> bayesRaw = null;
                    if (USE_BAYESIAN_FILTER && side != null) {
                        double __emaScore = 0.0;
                        try {
                            if (EMA_VOTE_CTX_BACKTEST != null) {
                                EmaVoteSnapshot __es = EMA_VOTE_CTX_BACKTEST.snapshotAt(entryC.ts);
                                if (__es != null) __emaScore = __es.score;
                            }
                        } catch (Throwable ignore) {}

                        double __srGapPts = 0.0;
                        try {
                            if (sr != null && sr.valid) {
                                __srGapPts = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                            }
                        } catch (Throwable ignore) {}

                        BayesEvalResult __bd = evalBayesForEntry(eng, m30, macd30, macd1h, t1hInfo, sigIdx, side, pool2, ov,
                                entryC.ts, __emaScore, atrSig, entryPx, __srGapPts,
                                (holdingRtPos || !VERBOSE_BACKTEST), "回测");
                        if (__bd != null) {
                            bayesPosterior = __bd.posterior;
                            bayesScale = __bd.scale;
                            bayesBins = __bd.bins;
                            bayesRaw = __bd.raw;
                            if (bayesScale <= 0.0) {
                                if (diagTail) System.out.printf("[BT-DIAG] entry=%s | ❌BAYES(%s/%s) posterior=%.3f < thr=%.3f%n", diagTs, BAYES_SCHEME, BAYES_MODE, bayesPosterior, ("SCALE".equalsIgnoreCase(BAYES_MODE) ? BAYES_MIN_POSTERIOR : BAYES_GATE_POSTERIOR));
                                continue;
                            }
                            entryMultNow *= bayesScale;
                        }
                    }

                    // ===== MACD动量仓位动态调整（无未来函数：k0已收盘，diffInt用k0/k1）=====
                    if (USE_MACD_POS_SIZING) {
                        double macdHist = k0.hist;
                        int diffIntForScale = (side == Side.LONG)
                                ? (int)(k0.dif - k1.dif)
                                : (int)(k0.dea - k1.dea);
                        if (macdHist > MACD_BOOST_THRESHOLD) {
                            entryMultNow *= MACD_BOOST_SCALE;
                            if (VERBOSE_BACKTEST) System.out.printf(Locale.US,
                                    "[MACD-BOOST][回测] ts=%s MACD_HIST=%.2f>%.1f → 加仓×%.1f%n",
                                    FMT_JST.format(Instant.ofEpochMilli(entryC.ts)), macdHist, MACD_BOOST_THRESHOLD, MACD_BOOST_SCALE);
                        } else if (diffIntForScale < 0) {
                            entryMultNow *= MACD_REDUCE_SCALE;
                            if (VERBOSE_BACKTEST) System.out.printf(Locale.US,
                                    "[MACD-REDUCE][回测] ts=%s diffInt=%d<0 → 减仓×%.1f%n",
                                    FMT_JST.format(Instant.ofEpochMilli(entryC.ts)), diffIntForScale, MACD_REDUCE_SCALE);
                        }
                    }

//  回撤入场诊断打印：无论是否成交都打印一次，方便你确认逻辑是否真的启用
                    if (!holdingRtPos) {
                        if (pbEnabled) {
                            System.out.printf(Locale.US,
                                    "    【回撤挂单】entryRef=%.2f limit=%.2f | used=%.2f mode=%s | ATR=%.2f | entryK(H/L)=%.2f/%.2f%n",
                                    entryPx, pbLimitPx, pbUsedPts, PULLBACK_MODE, atrSig, entryC.h, entryC.l
                            );
                        } else {
                            System.out.printf(Locale.US,
                                    "    【回撤挂单】OFF（按K0收盘=K1开盘进场）entryRef=%.2f | used(仅打印)=%.2f mode=%s | ATR=%.2f%n",
                                    entryPx, pbUsedPts, PULLBACK_MODE, atrSig
                            );
                        }
                    }

                    int diffInt = (side == Side.LONG)
                            ? (int) (k0.dif - k1.dif)
                            : (int) (k0.dea - k1.dea);
                    boolean suppressBtEnterPrint = holdingRtPos || ((!IN_SCAN_HISTORY && !IN_BOOT_BACKTEST) && (eng == LIVE_ENGINE) && btEnterLocked() && (entryC.ts != LOCKED_BT_ENTER_TS));
                    // scanHistory 刷新时：默认只打印“尾部附近”的机会（避免全历史刷屏），并且不受 BT_ENTER_LOCK 抑制
                    // 旧实现用 sigIdx vs m30.size()，当窗口很长（20399）时会把中段的真实机会全部抑制掉（例如 02-12 00:00）。
                    // 新实现改为：以 scanHistory 本次推演的 tailClose 为基准，打印最近 N 小时内的入场事件（N 默认 36h，可调）。
                    if (IN_SCAN_HISTORY && BT_SCAN_ONLY_TAIL_PRINT) {
                        // boundaryCloseTs 仅在 K-USED 打印函数里存在；此处用 entryC.ts 作为本次机会的 boundaryClose（=K0收盘=下一根开盘时刻）
                        long tailClose = (SCAN_TAIL_CLOSE_TS != Long.MIN_VALUE) ? SCAN_TAIL_CLOSE_TS : entryC.ts;
                        long lookbackMs = (long) BT_SCAN_PRINT_LOOKBACK_HOURS * 3600_000L;
                        if (entryC.ts < (tailClose - lookbackMs)) {
                            suppressBtEnterPrint = true;
                        }
                    }
                    if (!suppressBtEnterPrint) {
                        System.out.printf(Locale.US,
                                "【回测-进场】 时间=%s | 方向=%s | 1H=%s | 参考入场(K0收盘=K1开盘)=%.2f | dif(本根/前一)=%.6f/%.6f | dea(本根/前一)=%.6f/%.6f | 动量diffInt=%d%n",
                                FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                (side == Side.LONG ? "做多" : "做空"),
                                (USE_1H_FILTER ? (t1hInfo.trend + (t1hInfo.isFirstNeutralBar ? "(NEUTRAL_FIRST)" : "")) : "OFF"),
                                entryPx,
                                k0.dif, k1.dif, k0.dea, k1.dea,
                                diffInt
                        );


                        // ===== 回测尾部进场响铃：仅在最新机会更新时响一次（不影响任何逻辑）=====
                        if (isTailSigIdx(sigIdx, (m30 == null ? 0 : m30.size()))) {
                            btAlarmOnceOnEnter(entryC.ts, sigIdx, "SCAN");
                            if (!IN_SCAN_HISTORY && !IN_BOOT_BACKTEST) {
                                lockBtEnter(entryC.ts, sigIdx);
                            }
                        }


// ===== K-USED：回测/推演入场时打印本次实际用到的K及窗口 =====
                        Candle k0C = (m30 != null && sigIdx >= 0 && sigIdx < m30.size()) ? m30.get(sigIdx) : null;
                        Candle k1C = entryC; // 回测入场K（下一根开盘进场）
                        printKUsedEntry("SCAN", "ENTER", (k0C != null ? (k0C.ts + BAR30_MS) : entryC.ts), m30, sigIdx, k1C, entryPx, false, macd30);

                        //  更新“最近一次进场机会”（回测/实盘统一显示）
                        {
                            EnterOpportunity opp = new EnterOpportunity();
                            opp.ts = entryC.ts;
                            opp.side = side;
                            opp.entryRef = entryPx;
                            opp.pbEnabled = pbEnabled;
                            opp.pbMode = PULLBACK_MODE.name();
                            opp.atrAtSignal = atrSig;
                            opp.pbAtrPts = pbAtrPts;
                            opp.pbFixedPts = pbFixedPts;
                            opp.pbUsedPts = pbUsedPts;
                            opp.pbLimitPx = pbLimitPx;
                            opp.pbFilled = pbFilled;
                            opp.pbPending = false;
                            opp.entryFillPx = entryFillPx;
                            opp.dif1 = k0.dif;
                            opp.dif2 = k1.dif;
                            opp.dea1 = k0.dea;
                            opp.dea2 = k1.dea;
                            opp.diffInt = diffInt;
                            opp.t1h = t1hInfo.trend;
                            eng.lastEnterOpp = opp;

                            // ===== HIST_AUDIT：用于对齐检查（回测/回看）=====
                            if (DEBUG_ALIGN) {
                                AlignAudit ha = new AlignAudit();
                                ha.src = "HIST/SCAN";
                                ha.nowMs = System.currentTimeMillis();
                                Candle sigCC = (m30 != null && sigIdx >= 0 && sigIdx < m30.size()) ? m30.get(sigIdx) : null;
                                ha.signalTs = (sigCC != null ? sigCC.ts : (entryC.ts - BAR30_MS));
                                ha.sigCloseTs = ha.signalTs + BAR30_MS;
                                ha.sigConfirm = (sigCC != null ? sigCC.confirm : 1);
                                ha.sigO = (sigCC != null ? sigCC.o : 0.0);
                                ha.sigC = (sigCC != null ? sigCC.c : entryPx);
                                ha.entryTs = entryC.ts; // next-open（入场K开盘）
                                ha.entryRefPx = entryPx; // 默认=K0收盘
                                ha.k1OpenPx = entryC.o;   // K1.open（若存在）
                                ha.note = "scanHistory/backtest enter";
                                LAST_HIST_AUDIT = ha;
                                saveHistAudit(ha);
                            }

                        }

                    } else {
                        // 已锁定首个回测进场展示：后续刷新出现的新机会不再打印/不再响铃（避免滚屏）
                    }

                    //  回撤入场：回测开关打开但未触达 limit
                    //  - 默认：未成交则不入场（成交触达为准，避免未来函数）
                    //  - 若开启 MANUAL_VIRTUAL_POS_ON_ALERT：与实盘“响铃即视为已进场”一致 → 这里进入“虚拟持仓态”
                    if (pbEnabled && !pbFilled) {
                        //  PULLBACK_SKIP_ON_NOFILL=true：未成交一律跳过(本次不进场)，与实盘“响铃即持仓”解耦；
                        //    下一根K若信号仍成立，循环会自然重新评估并按新K0收盘价重新挂限价(fresh 重报价)。
                        //  PULLBACK_SKIP_ON_NOFILL=false：维持旧口径(受 MANUAL_VIRTUAL_POS_ON_ALERT 控制)。
                        boolean skipOnNoFill = PULLBACK_SKIP_ON_NOFILL || !MANUAL_VIRTUAL_POS_ON_ALERT;
                        if (skipOnNoFill) {
                            System.out.printf(Locale.US,
                                    "    【回撤未成交→跳过】entryRef=%.2f limit=%.2f | used=%.2f mode=%s | ATR=%.2f | 说明=本根未触达回撤限价，本次不成交；下一根信号仍成立则重新挂%n",
                                    entryPx, pbLimitPx, pbUsedPts, PULLBACK_MODE, atrSig
                            );
                            continue;
                        } else {
                            System.out.printf(Locale.US,
                                    "    【回撤未成交→虚拟持仓】entryRef=%.2f limit=%.2f | used=%.2f mode=%s | ATR=%.2f | 说明=下一根K未触达回撤限价，但为保持与实盘一致，仍按提醒事件进入持仓态%n",
                                    entryPx, pbLimitPx, pbUsedPts, PULLBACK_MODE, atrSig
                            );
                            //  虚拟持仓：以 entryRef 作为成交价进入（后续止损/止盈/出场按同规则推演）
                            entryFillPx = entryPx;
                        }
                    }

                    //  回撤入场：成交时提示成交价
                    if (pbEnabled && pbFilled) {
                        System.out.printf(Locale.US,
                                "    【回撤成交】entryRef=%.2f limit=%.2f | used=%.2f mode=%s | ATR=%.2f | 成交价=%.2f%n",
                                entryPx, pbLimitPx, pbUsedPts, PULLBACK_MODE, atrSig, entryFillPx
                        );
                    }

                    cur = new Position();
                    cur.side = side;
                    cur.entry = entryFillPx;
                    cur.bayesPosterior = bayesPosterior;
                    cur.bayesScale = bayesScale;
                    cur.isVirtual = MANUAL_VIRTUAL_POS_ON_ALERT; // 与实盘“响铃即持仓态”口径一致
                    sigC = (sigIdx >= 0 && sigIdx < m30.size()) ? m30.get(sigIdx) : null;
                    cur.signalTs = (sigC != null ? sigC.ts : (entryC.ts - BAR30_MS)); // 信号K0开盘

                    cur.entryTs = entryC.ts;
                    if (USE_BAYESIAN_FILTER && bayesBins != null) {
                        eng.currentBayesSnap = new MultiBinBayesFilter.BayesEntrySnapshot(cur.entryTs, bayesBins, bayesRaw);
                    } else {
                        eng.currentBayesSnap = null;
                    }
                    cur.slPoints = calcSlPointsForIndex(m30, sigIdx); //  用信号K0对齐动态SL
                    //  SRMF：锁定入场时的 mult / regime / trendPts / equity
                    cur.entryEquity = eng.equity;
                    cur.slPointsAtEntry = cur.slPoints;
                    //  入场时锁定 OKX 撑压线（回测一致，用于止盈/复盘）
                    SrLevels srLocked = calcOkxSrLevels(m30, sigIdx, OKX_SR_N);
                    if (srLocked.valid) {
                        cur.okxResistanceAtEntry = srLocked.resistance;
                        cur.okxSupportAtEntry = srLocked.support;
                        cur.okxGapAtEntry = (cur.side == Side.LONG) ? (srLocked.resistance - cur.entry) : (cur.entry - srLocked.support);
                    }
                    cur.entryVoteScore = voteScore;
                    cur.entryVoteScale = voteScale;
                    cur.entryVoteDirPass = ov.dirPass;
                    cur.entryVoteOverallPass = ov.overallPass;
                    cur.entrySource = entrySource;   // 锁定本笔信号来源（用于回测按来源归因）
                    cur.entryVoteDetail = ov.detail;
                    cur.entryTierMult = tierMultNow;
                    cur.entryMult = entryMultNow;
                    if (rvRisk != null) {
                        if (cur.entryVoteDetail == null) cur.entryVoteDetail = "";
                        cur.entryVoteDetail = (cur.entryVoteDetail.trim().isEmpty() ? rvRisk.detail : (cur.entryVoteDetail + " | " + rvRisk.detail));
                    }
                    cur.regimeAtEntry = SRMF.getRegimeName(eng.srmf);
                    cur.trendPtsAtEntry = SRMF.getTrendPts(eng.srmf);
                    double entryRiskAmt = cur.entryEquity * RISK_PCT * cur.entryMult;
                    double entryNotional = entryRiskAmt * entryFillPx / Math.max(1e-9, cur.slPointsAtEntry);
                    cur.entryNotional = entryNotional;
                    if (VERBOSE_BACKTEST && !holdingRtPos) System.out.printf(Locale.US, "【SRMF-进场】 时间=%s | 入场开盘=%.2f | 模式=%s | trendPts=%.2f | 倍数=%.2f(*%.1f) | 票=%d | 止损点=%.0f | 风险R=%.2f | 名义=%.0f | 资金=%.2f%n",
                            FMT_JST.format(Instant.ofEpochMilli(cur.entryTs)), cur.entry,
                            cur.regimeAtEntry, cur.trendPtsAtEntry, cur.entryMult, cur.entryVoteScale, cur.entryVoteScore, cur.slPointsAtEntry,
                            entryRiskAmt, entryNotional, cur.entryEquity);
                    cur.mfe = 0.0;
                    cur.mae = 0.0;
                    eng.pos = cur;
                }
                //  允许“入场根”触发出场：
                // 关键修复：若本轮是“虚拟入场K”（i==m30.size），其 OHLC 是用 K0.close 构造的
                // 仅用于产生 entryTs 机会，不应再在同一根虚拟K上评估 SL/TP/MACD 出场（也避免 macd 越界）
                if (virtualEntry) break;

                // 允许“入场根”触发出场：如果本根没开仓就 continue；若已开仓则落入下面持仓段执行 SL/TP（SL 优先）。
                if (cur == null) continue;
            }

            // ========== 持仓：先更新 MFE/MAE ==========
            updateMfeMae(cur, c);

            //  出场优先级（硬→软）：SL -> TP全平 -> OKX撑压止盈 -> 放量滚动防守出场 -> 其它出场 -> MACD出场
            boolean stop = hitStopLoss(cur, c);
            boolean tp2  = (!stop) && hitTP2(cur, c);

            double okxTpPx = (cur.side == Side.LONG) ? cur.okxResistanceAtEntry : cur.okxSupportAtEntry;
            boolean okxSrTp = (!stop) && USE_OKX_SR_TP && hitOkxSrTP(cur, c, okxTpPx);

            if (stop || tp2 || okxSrTp) {
                CompletedTrade ct = new CompletedTrade();
                ct.enterTs = cur.entryTs;
                ct.signalTs = cur.signalTs;
                ct.side = cur.side;
                ct.entry = cur.entry;
                ct.entrySource = cur.entrySource;

                if (stop) {
                    ct.isStop = true;
                    ct.reason = "止损触发（SL=" + (int)Math.round(cur.slPoints) + "点）";
                    ct.exitTs = c.ts + BAR30_MS; //  时间对齐到收盘(=下一根开盘)
                    ct.exit = stopFillPrice(cur);
                    skipEntrySignalTsAfterStop = c.ts; //  记录本根K为信号K，下一根禁止用它再入场
                    cooldownUntilEntryTsAfterStop = calcCooldownUntilEntryTsAfterStop(ct.exitTs); //  止损冷却
                } else {
                    ct.isStop = false;
                    ct.exitTs = c.ts + BAR30_MS; //  时间对齐到收盘(=下一根开盘)

                    double tp2Px = tp2FillPrice(cur);
                    double srPx  = okxTpPx;

                    if (tp2 && okxSrTp) {
                        //  多个止盈同时触发：仍按“更先触发”的近似规则选择“哪一个原因”，
                        // 但若选择的是 OKX 撑压线止盈，则【出场价/出场时间】按当前K线收盘确认（避免盘中/盘尾冲突）。
                        boolean chooseTp2;
                        if (cur.side == Side.LONG) chooseTp2 = (tp2Px <= srPx);   // LONG：更小价更先触发
                        else                      chooseTp2 = (tp2Px >= srPx);   // SHORT：更大价更先触发

                        if (chooseTp2) {
                            ct.exit = tp2Px;
                            ct.reason = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                        } else {
                            ct.exit = c.c; //  SR 仍按盘中触价判定，但“以收盘价出”
                            ct.reason = (cur.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                        }
                    } else if (okxSrTp) {
                        ct.exit = c.c; //  SR 仍按盘中触价判定，但“以收盘价出”
                        ct.reason = (cur.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                    } else {
                        ct.exit = tp2Px;
                        ct.reason = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                    }
                }
                //  合并保护（必须在结算前）：TP全平若“同一时间出入场冲突”则继续持仓（不结算/不更新SRMF/不写回测）；止损永远退场
                if (tp2 || okxSrTp) {
                    // [BUG-FIX-2026-06] 继续持仓判断不再依赖“下一根真实K是否已收盘”。
                    // 原因：旧守卫 (i+1)<m30.size() 让末尾K首刷必出场、次刷(下一根收盘后)必继续持仓，
                    // 造成“出场后30分钟刷新又变回持仓”的两轮翻转。
                    // computeEntrySideAtIndex 只用 K<=i 数据 + 入场时间戳（entryRefPx=sigC.c，从不读 entryC 价），
                    // 且支持 entryIdx==m30.size() 的虚拟下一根开盘，故首刷=次刷，结果稳定。
                    // 索引修正：exitTs=open(i+1)，同一时刻成交的进场信号K=i、入场K=i+1 → entryIdx=i+1（sigIdx=i）。
                    if ((i + 1) <= m30.size()) {
                        Side nextSide = computeEntrySideAtIndex(m30, macd30, h2, macd1h, i + 1, true);

                        if (nextSide != null) {
                            System.out.printf(Locale.US,
                                    "【继续持仓（同一时间出入场冲突）】时间=%s | 当前持仓=%s | 入场时间=%s 入场价=%.2f | 当前价=%.2f | 名义=%.0fU | 本次不出不进%n",
                                    FMT_JST.format(Instant.ofEpochMilli(ct.exitTs)),
                                    (cur.side == Side.LONG ? "做多" : "做空"),
                                    FMT_JST.format(Instant.ofEpochMilli(cur.entryTs)),
                                    cur.entry,
                                    c.c,
                                    (cur.entryNotional > 0 ? cur.entryNotional : 0.0)
                            );
                            // 不结算、不清仓 → 直接继续持仓（否则会出现“收益叠加”）
                            continue;
                        }
                    }
                }

                //  止盈冷却：仅在“真正出场”后才记录（若上面冲突→继续持仓，则不会执行到这里）
                if (!stop && (tp2 || okxSrTp)) {
                    skipEntrySignalTsAfterTp = c.ts; //  本根K作为信号K，下一根禁止用它立刻再入场
                    cooldownUntilEntryTsAfterTp = calcCooldownUntilEntryTsAfterTakeProfit(ct.exitTs); //  允许再次入场的最早 entryTs
                }

                // 结算剩余（全仓一次性结算）
                double remainPnl = (ct.side == Side.LONG)
                        ? (ct.exit - ct.entry)
                        : (ct.entry - ct.exit) ;
                double feePts = okxFeePoints(ct.entry, ct.exit);
                ct.grossPnlPoints = remainPnl;
                ct.feePoints = feePts;
                ct.pnlPoints = remainPnl - feePts;
                //  资金盈亏换算（动态R=2%）：pnlUSDT = pnlPts * (entryEquity*RISK_PCT*mult) / slPts
                double riskAmt = cur.entryEquity * RISK_PCT * cur.entryMult;
                double pnlUSDT = ct.pnlPoints * riskAmt / Math.max(1e-9, cur.slPointsAtEntry);
                eng.equity += pnlUSDT;

                ct.entryEquity = cur.entryEquity;
                ct.exitEquity = eng.equity;
                ct.pnlUSDT = pnlUSDT;
                ct.mult = cur.entryMult;
                ct.slPointsUsed = cur.slPointsAtEntry;
                ct.regime = cur.regimeAtEntry;
                ct.rMultiple = ct.pnlPoints / Math.max(1e-9, cur.slPointsAtEntry);
                ct.trendPtsAfter = 0.0; // 下面 onTradeClosed 后回填

                //  用历史回放更新 SRMF（用于下一笔 mult / 环境状态）
                eng.srmf = SRMF.reduce(eng.srmf, ct, eng.equity);
                ct.trendPtsAfter = SRMF.getTrendPts(eng.srmf);
                ct.mfe = cur.mfe;
                ct.mae = cur.mae;
                if (VERBOSE_BACKTEST) System.out.printf(Locale.US, "【SRMF-出场】 时间=%s | 方向=%s | 毛=%.2f点 | 费=%.2f点 | 净=%.2f点(%.2fU) | R倍=%.2f | 倍数=%.2f | 止损点=%.0f | 资金=%.2f | trendPts=%.2f | 原因=%s | MFE=%.2f MAE=%.2f%n",
                        FMT_JST.format(Instant.ofEpochMilli(ct.exitTs)),
                        (ct.side==Side.LONG?"做多":"做空"),
                        ct.grossPnlPoints, ct.feePoints, ct.pnlPoints, ct.pnlUSDT, ct.rMultiple, ct.mult, ct.slPointsUsed, ct.exitEquity, ct.trendPtsAfter, ct.reason,
                        ct.mfe, ct.mae);

                //  统一记录：最近一次出场机会 + 最近10笔（止损/TP/OKX SR 都要计入）
                recordClosedTradeForView(eng, ct);

                // BT_ENTER_LOCK: 出场后允许下一次重新锁定（防止下一次机会仍被锁屏蔽）
                unlockBtEnterIfNeeded("tradeClosed");

                trades.add(ct);
                cur = null;
                eng.pos = null;
                continue;
            }
            // [FIX] 进场当根不允许 VolRoll/exitCombo/macdRev 触发（与止损/止盈一致）
            boolean isEntryBar = (cur != null && cur.entryTs == c.ts + BAR30_MS);
            boolean volRollExit = (!stop) && (!tp2) && (!okxSrTp) && !isEntryBar && hitVolRollExit(cur, m30, i);
            boolean exitCombo7 = (!stop) && (!tp2) && (!okxSrTp) && !isEntryBar && (!volRollExit) && ExitCombo7Miner.shouldExit(m30, i, cur.side, "回测");
            boolean exitCombo2 = (!stop) && (!tp2) && (!okxSrTp) && !isEntryBar && (!volRollExit) && (!exitCombo7) && ExitCombo2Miner.shouldExit(m30, i, cur.side, "回测");
            boolean macdRev = (!stop) && (!tp2) && (!okxSrTp) && !isEntryBar && (!volRollExit) && (!exitCombo7) && (!exitCombo2) && shouldExitByMacdWithGuards(m30, macd30, i, cur, c.c, "回测", c.ts + BAR30_MS);

            if (volRollExit || exitCombo7 || exitCombo2 || macdRev) {
                CompletedTrade ct = new CompletedTrade();
                ct.enterTs = cur.entryTs;
                ct.signalTs = cur.signalTs;
                ct.side = cur.side;
                ct.entry = cur.entry;

                ct.isStop = false;
                ct.reason = volRollExit ? "放量滚动防守出场" : (exitCombo7 ? ExitCombo7Miner.REASON : (exitCombo2 ? ExitCombo2Miner.REASON : "MACD颜色反转出场"));
                ct.entrySource = cur.entrySource;
                ct.exitTs = c.ts + BAR30_MS;
                ct.exit = c.c;
                //  出场时间对齐到“下一根开盘”(K+1 open)。若同一时间也满足入场，则按你的要求：不出不进=继续持仓。
                // [BUG-FIX-2026-06] 同上：继续持仓判断不再依赖“下一根真实K是否已收盘”，并修正 i→i+1（sigIdx=i）。
                if ((i + 1) <= m30.size()) {
                    Side nextSide = computeEntrySideAtIndex(m30, macd30, h2, macd1h, i + 1, true);
                    if (nextSide != null) {
                        double holdRisk = cur.entryEquity * RISK_PCT * cur.entryMult;
                        double holdNotional = (cur.slPointsAtEntry > 0 ? (holdRisk * cur.entry / cur.slPointsAtEntry) : 0);

                        System.out.printf(Locale.US,
                                "[继续持仓] ts=%s | 出入场同一时间→忽略本次出场/入场信号 | 当前持仓=%s | 入场=%s @ %.2f | 风险=%.2fUSDT | 名义=%.0fUSDT %n",
                                Instant.ofEpochMilli(ct.exitTs).atZone(ZONE),
                                (cur.side == Side.LONG ? "做多" : "做空"),
                                Instant.ofEpochMilli(cur.entryTs).atZone(ZONE),
                                cur.entry,
                                holdRisk,
                                holdNotional
                        );
                        // 按要求：不更新回测交易列表 / 最近机会 / 最近10笔 / 持仓状态（继续持仓）
                        continue;
                    }
                }

                double remainPnl = (ct.side == Side.LONG)
                        ? (ct.exit - ct.entry)
                        : (ct.entry - ct.exit) ;
                double feePts = okxFeePoints(ct.entry, ct.exit);
                ct.grossPnlPoints = remainPnl;
                ct.feePoints = feePts;
                ct.pnlPoints = remainPnl - feePts;
                //  资金盈亏换算（动态R=2%）：pnlUSDT = pnlPts * (entryEquity*RISK_PCT*mult) / slPts
                double riskAmt = cur.entryEquity * RISK_PCT * cur.entryMult;
                double pnlUSDT = ct.pnlPoints * riskAmt / Math.max(1e-9, cur.slPointsAtEntry);
                eng.equity += pnlUSDT;

                ct.entryEquity = cur.entryEquity;
                ct.exitEquity = eng.equity;
                ct.pnlUSDT = pnlUSDT;
                ct.mult = cur.entryMult;
                ct.slPointsUsed = cur.slPointsAtEntry;
                ct.regime = cur.regimeAtEntry;
                ct.rMultiple = ct.pnlPoints / Math.max(1e-9, cur.slPointsAtEntry);
                ct.trendPtsAfter = 0.0; // 下面 onTradeClosed 后回填

                //  用历史回放更新 SRMF（用于下一笔 mult / 环境状态）
                eng.srmf = SRMF.reduce(eng.srmf, ct, eng.equity);
                ct.trendPtsAfter = SRMF.getTrendPts(eng.srmf);
                ct.mfe = cur.mfe;
                ct.mae = cur.mae;
                if (VERBOSE_BACKTEST) System.out.printf(Locale.US, "【SRMF-出场】 时间=%s | 方向=%s | 毛=%.2f点 | 费=%.2f点 | 净=%.2f点(%.2fU) | R倍=%.2f | 倍数=%.2f | 止损点=%.0f | 资金=%.2f | trendPts=%.2f | 原因=%s | MFE=%.2f MAE=%.2f%n",
                        FMT_JST.format(Instant.ofEpochMilli(ct.exitTs)),
                        (ct.side==Side.LONG?"做多":"做空"),
                        ct.grossPnlPoints, ct.feePoints, ct.pnlPoints, ct.pnlUSDT, ct.rMultiple, ct.mult, ct.slPointsUsed, ct.exitEquity, ct.trendPtsAfter, ct.reason,
                        ct.mfe, ct.mae);

                //  统一记录：最近一次出场机会 + 最近10笔（包含止损/TP/形态/MACD等所有出场）
                recordClosedTradeForView(eng, ct);

                // BT_ENTER_LOCK: 出场后允许下一次重新锁定（防止下一次机会仍被锁屏蔽）
                unlockBtEnterIfNeeded("tradeClosed");
                trades.add(ct);

                //  MACD 止盈也纳入“止盈冷却”：出场后等待 N 根K线才允许重新进场
                eng.lastTpSigTs = c.ts;
                skipEntrySignalTsAfterTp = c.ts;
                cooldownUntilEntryTsAfterTp = calcCooldownUntilEntryTsAfterTakeProfit(ct.exitTs);
                if (VERBOSE_BACKTEST && PRINT_TAKEPROFIT_COOLDOWN_BLOCK) {
                    System.out.printf(Locale.US,
                            "[TP-COOLDOWN][MACD] exitTs=%s => allowEntryTs>=%s (bars=%d)%n",
                            FMT_JST_SHORT.format(Instant.ofEpochMilli(ct.exitTs)),
                            FMT_JST_SHORT.format(Instant.ofEpochMilli(cooldownUntilEntryTsAfterTp)),
                            TAKEPROFIT_COOLDOWN_BARS);
                }

                cur = null;
                eng.pos = null;
            }
        }
        EMA_VOTE_CTX_BACKTEST = null;
        return trades;
    }

    // 兼容旧调用：不传 EngineState 时，用临时状态执行回测（不污染 LIVE_ENGINE / VIEW_ENGINE）
    static List<CompletedTrade> runBacktestAlarmLogic(List<Candle> m30, List<MacdPoint> macd30,
                                                      List<Candle> h2,  List<MacdPoint> macd1h) {
        EngineState tmp = new EngineState();
        return runBacktestToState(tmp, m30, macd30, h2, macd1h);
    }


    // 回测统计输出（按你提供的 da_1770564188144.txt 格式）
// 注意：为了不污染策略逻辑，这里只做“展示层”增强。
    static void printBacktestReport(List<CompletedTrade> trades) {
        printBacktestReport(trades, LIVE_ENGINE, "LIVE");
    }

    static void printBacktestReport(List<CompletedTrade> trades, EngineState eng, String tag) {
        System.out.println();
        System.out.println("========== 回测结果 ==========" + (tag == null ? "" : (" [" + tag + "]")));

        int n = (trades == null ? 0 : trades.size());
        long win = (trades == null ? 0 : trades.stream().filter(t -> t.pnlPoints > 0).count());
        double total = (trades == null ? 0 : trades.stream().mapToDouble(t -> t.pnlPoints).sum());
        double totalGross = (trades == null ? 0 : trades.stream().mapToDouble(t -> t.grossPnlPoints).sum());
        double totalFee = (trades == null ? 0 : trades.stream().mapToDouble(t -> t.feePoints).sum());
        double avg = (n == 0 ? 0 : total / n);

        double maxDd = (trades == null ? 0 : calcMaxDrawdownPoints(trades));
        int maxLoseStreak = (trades == null ? 0 : calcMaxLoseStreak(trades));

        System.out.println("交易次数 = " + n);
        System.out.println("胜率    = " + (n == 0 ? "0.00%" : String.format(Locale.US, "%.2f%%", (win * 100.0 / n))) + " (" + win + "/" + n + ")");
        System.out.println("总盈亏(点) = " + String.format(Locale.US, "净%.2f | 毛%.2f | 手续费%.2f", total, totalGross, totalFee));
        System.out.println("平均每笔(点) = " + String.format(Locale.US, "%.4f", avg));
        System.out.println("平均手续费(点) = " + String.format(Locale.US, "%.4f", (n == 0 ? 0 : totalFee / n)));
        System.out.println("最大回撤(点) = " + String.format(Locale.US, "%.2f", maxDd));
        System.out.println("最大连亏(笔) = " + maxLoseStreak);

        // ===== 资金曲线（动态R=2% + mult）统计（如果 trades 已填充 pnlUSDT/exitEquity） =====
        boolean hasEquity = (trades != null) && trades.stream().anyMatch(t -> t.exitEquity != 0.0);
        if (hasEquity) {
            double startEq = capital;
            double endEq = trades.get(trades.size() - 1).exitEquity;
            double totalUsdt = trades.stream().mapToDouble(t -> t.pnlUSDT).sum();
            double maxDdAmt = calcMaxDrawdownUSDT(trades, startEq);
            double maxDdPct = calcMaxDrawdownPct(trades, startEq); // 逐步峰值，早期回撤正确计算
            System.out.println();
            System.out.println("========== EQUITY（动态R=2%） ==========");
            System.out.println("初始资金 = " + String.format(Locale.US, "%.2f", startEq));
            System.out.println("最终资金 = " + String.format(Locale.US, "%.2f", endEq));
            System.out.println("总盈亏(USDT) = " + String.format(Locale.US, "%.2f", totalUsdt));
            System.out.println("最大回撤(USDT) = " + String.format(Locale.US, "%.2f", maxDdAmt));
            System.out.println("最大回撤(%)  ≈ " + String.format(Locale.US, "%.2f", maxDdPct) + "%");
        }

        // 兼容：老版本这里用 LIVE_ENGINE；现在允许传入 eng（避免回测报告打印“实盘状态”）
        EngineState e = (eng == null ? LIVE_ENGINE : eng);
        SRMFState s = (e == null ? null : e.srmf);
        if (s == null) s = LIVE_ENGINE.srmf;

        System.out.println("SRMF状态 = regime=" + SRMF.getRegimeName(s)
                + " trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts(s))
                + " mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult(s))
                + (APPLY_OKX_FEE ? (" | fee双边=" + String.format(Locale.US, "%.4f%%", OKX_FEE_RATE_PER_SIDE * 2 * 100)) : " | fee=OFF"));

        //  回撤入场参考：每半小时打印 ATR 回撤值 & 固定回撤值（不改变原入场条件/提醒）
        double pbAtrForPrint = lastAtrCache;
        if (pbAtrForPrint <= 0 && lastM30ClosedForPrint != null && lastM30ClosedForPrint.size() >= 2) {
            pbAtrForPrint = calcAtr(lastM30ClosedForPrint, lastM30ClosedEndIdxForPrint, ATR_N);
        }
        double pbAtrPtsPrint = pbAtrForPrint * PULLBACK_ATR_MULT;
        double pbUsedPtsPrint = (PULLBACK_MODE == PullbackMode.ATR ? pbAtrPtsPrint : PULLBACK_FIXED_POINTS);

        System.out.printf(Locale.US,
                "回撤入场参数：回测开关=%s | mode=%s | ATR(30m,%d)=%.2f | ATR回撤=%.2f(x%.2f) | 固定回撤=%.2f | 使用回撤=%.2f%n",
                (ENABLE_PULLBACK_ENTRY_BACKTEST ? "ON" : "OFF"),
                PULLBACK_MODE,
                ATR_N,
                pbAtrForPrint,
                pbAtrPtsPrint,
                PULLBACK_ATR_MULT,
                PULLBACK_FIXED_POINTS,
                pbUsedPtsPrint
        );

        if (e != null && e.lastEnterOpp != null && e.lastEnterOpp.side != null) {
            double lim = (e.lastEnterOpp.side == Side.LONG)
                    ? (e.lastEnterOpp.entryRef - pbUsedPtsPrint)
                    : (e.lastEnterOpp.entryRef + pbUsedPtsPrint);
            System.out.printf(Locale.US,
                    "回撤入场参考价：%s limit=%.2f （参考进场价=%.2f %s %.2f）%n",
                    (e.lastEnterOpp.side == Side.LONG ? "做多" : "做空"),
                    lim,
                    e.lastEnterOpp.entryRef,
                    (e.lastEnterOpp.side == Side.LONG ? "-" : "+"),
                    pbUsedPtsPrint
            );
        }

        //  快速观察：稳健4档 / 进攻5档 的“参考名义”（按 A：已收K收盘价=最后一根已收K close）
        double obsPrice = lastPriceCache;
        double obsSl = Math.max(1e-9, lastSlPointsCache);
        if (obsPrice > 0) {
            double obsBaseRisk = capital * RISK_PCT;
            double tp = SRMF.getTrendPts(s);
            double m4 = stable4MultByTrendPts(tp);
            double m5 = attack5MultByTrendPts(tp);
            double n4 = (obsBaseRisk * m4) * obsPrice / obsSl;
            double n5 = (obsBaseRisk * m5) * obsPrice / obsSl;
            System.out.printf(Locale.US, "名义参考(按已收K收盘价=%.2f, SL=%.0f点)：稳健4档(mult=%.2f)=%.0fU | 进攻5档(mult=%.2f)=%.0fU%n",
                    obsPrice, obsSl, m4, n4, m5, n5);
        }

        System.out.println();
        System.out.println("========== MONTHLY（按入场月统计胜率） ==========");
        Map<String, List<CompletedTrade>> byMonth = new LinkedHashMap<>();
        DateTimeFormatter ym = DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZONE);

        if (trades != null) {
            for (CompletedTrade t : trades) {
                String key = ym.format(Instant.ofEpochMilli(t.enterTs));
                byMonth.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
            }
        }

        if (byMonth.isEmpty()) {
            System.out.println("（全年无交易触发）");
        } else {
            for (Map.Entry<String, List<CompletedTrade>> en : byMonth.entrySet()) {
                List<CompletedTrade> list = en.getValue();
                int cnt = list.size();
                long w = list.stream().filter(x -> x.pnlPoints > 0).count();
                double sum = list.stream().mapToDouble(x -> x.pnlPoints).sum();
                double wr = (cnt == 0 ? 0 : (w * 100.0 / cnt));

                System.out.printf(Locale.US, "%s | 笔数=%d | 胜率=%.2f%% | 盈亏点=%.2f%n",
                        en.getKey(), cnt, wr, sum);
            }
        }


        // ===== BAYES 诊断报告（仅打印，不影响回测结果）=====
        if (USE_BAYESIAN_FILTER && BAYES_PRINT_LR_REPORT && eng != null && eng.bayesState != null) {
            try {
                System.out.println("【BAYES-LR 摘要】（posterior=Win|features bins 的似然比）");
                MultiBinBayesFilter.printBayesReport(eng.bayesState, BAYES_LAPLACE_ALPHA);
            } catch (Throwable __t) {
                System.out.println("[BAYES][WARN] printBayesReport failed: " + __t.getMessage());
            }
        }

        // ===== 信号来源归因统计（由 PRINT_SOURCE_STATS 开关控制；默认关闭）=====
        if (PRINT_SOURCE_STATS && trades != null && !trades.isEmpty()) {
            printSourceStats(trades);
        }
        // Pool-2 诊断打印（由 PRINT_POOL2_DIAG 控制；默认关闭）
        if (PRINT_POOL2_DIAG) {
            Pool2DiagStats.print();
        }

        System.out.println("=====================================");
        System.out.println();
    }

    // 按入场信号来源(entrySource)分组，统计每个来源的成绩。
    // 仅遍历一次已有成交列表做汇总打印，无额外计算重负担；由 PRINT_SOURCE_STATS 控制是否调用。
    static void printSourceStats(List<CompletedTrade> trades) {
        int total = trades.size();
        // 用 LinkedHashMap 保持来源首次出现顺序，便于阅读
        Map<String, List<CompletedTrade>> bySrc = new LinkedHashMap<>();
        for (CompletedTrade t : trades) {
            String src = (t.entrySource == null || t.entrySource.isEmpty()) ? "(未知)" : t.entrySource;
            bySrc.computeIfAbsent(src, k -> new ArrayList<>()).add(t);
        }

        System.out.println();
        System.out.println("---------- 信号来源归因 ----------");
        System.out.printf(Locale.US, "%-12s %6s %7s %7s %12s %10s %8s%n",
                "来源", "笔数", "占比", "胜率", "总盈亏(点)", "均笔(点)", "盈亏比");

        for (Map.Entry<String, List<CompletedTrade>> e : bySrc.entrySet()) {
            List<CompletedTrade> list = e.getValue();
            int n = list.size();
            long win = list.stream().filter(t -> t.pnlPoints > 0).count();
            double sum = list.stream().mapToDouble(t -> t.pnlPoints).sum();
            double avg = (n == 0 ? 0.0 : sum / n);
            // 盈亏比 = 平均盈利点 / 平均亏损点(绝对值)；无亏损笔时记为 NaN(显示 -)
            double grossWin = list.stream().filter(t -> t.pnlPoints > 0).mapToDouble(t -> t.pnlPoints).sum();
            double grossLossAbs = Math.abs(list.stream().filter(t -> t.pnlPoints < 0).mapToDouble(t -> t.pnlPoints).sum());
            long winCnt = win;
            long loseCnt = list.stream().filter(t -> t.pnlPoints < 0).count();
            double avgWin = (winCnt == 0 ? 0.0 : grossWin / winCnt);
            double avgLossAbs = (loseCnt == 0 ? 0.0 : grossLossAbs / loseCnt);
            String plRatio = (avgLossAbs == 0.0) ? "-" : String.format(Locale.US, "%.2f", avgWin / avgLossAbs);

            System.out.printf(Locale.US, "%-12s %6d %6.1f%% %6.1f%% %12.2f %10.4f %8s%n",
                    e.getKey(), n,
                    (total == 0 ? 0.0 : n * 100.0 / total),
                    (n == 0 ? 0.0 : win * 100.0 / n),
                    sum, avg, plRatio);
        }
        System.out.println("----------------------------------");

        // ===== 统计C：月度 × 信号源 盈亏表（检验是否依赖特定行情）=====
        printMonthlyBySource(trades);
    }

    // 统计C：按月 × 信号源，看每月每个源的笔数/盈亏/胜率。
    // 目的：检验过拟合——利润是均匀分布还是集中在某几个月（集中=依赖特定行情=危险）。
    static void printMonthlyBySource(List<CompletedTrade> trades) {
        if (trades == null || trades.isEmpty()) return;
        DateTimeFormatter ym = DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZONE);

        // 月份 -> (来源 -> 该来源该月的交易列表)
        java.util.TreeMap<String, java.util.LinkedHashMap<String, List<CompletedTrade>>> grid = new java.util.TreeMap<>();
        java.util.LinkedHashSet<String> allSources = new java.util.LinkedHashSet<>();
        for (CompletedTrade t : trades) {
            String mon = ym.format(Instant.ofEpochMilli(t.enterTs));
            String src = (t.entrySource == null || t.entrySource.isEmpty()) ? "(未知)" : t.entrySource;
            allSources.add(src);
            grid.computeIfAbsent(mon, k -> new java.util.LinkedHashMap<>())
                    .computeIfAbsent(src, k -> new ArrayList<>()).add(t);
        }

        System.out.println();
        System.out.println("---------- 统计C：月度 × 信号源 盈亏（点）----------");
        // 表头
        StringBuilder hdr = new StringBuilder(String.format(Locale.US, "%-9s", "月份"));
        for (String s : allSources) hdr.append(String.format(Locale.US, " %14s", s));
        hdr.append(String.format(Locale.US, " %14s", "当月合计"));
        System.out.println(hdr.toString());

        double[] colTotal = new double[allSources.size()];
        double grandTotal = 0;
        for (Map.Entry<String, java.util.LinkedHashMap<String, List<CompletedTrade>>> e : grid.entrySet()) {
            StringBuilder row = new StringBuilder(String.format(Locale.US, "%-9s", e.getKey()));
            double monthTotal = 0;
            int ci = 0;
            for (String s : allSources) {
                List<CompletedTrade> list = e.getValue().get(s);
                if (list == null || list.isEmpty()) {
                    row.append(String.format(Locale.US, " %14s", "·"));
                } else {
                    double sum = list.stream().mapToDouble(t -> t.pnlPoints).sum();
                    long w = list.stream().filter(t -> t.pnlPoints > 0).count();
                    int n = list.size();
                    // 格式：盈亏(笔数,胜率%)
                    row.append(String.format(Locale.US, " %+8.0f(%d,%.0f%%)", sum, n, (n == 0 ? 0.0 : w * 100.0 / n)));
                    monthTotal += sum;
                    colTotal[ci] += sum;
                }
                ci++;
            }
            row.append(String.format(Locale.US, " %+14.1f", monthTotal));
            grandTotal += monthTotal;
            System.out.println(row.toString());
        }
        // 合计行
        StringBuilder tot = new StringBuilder(String.format(Locale.US, "%-9s", "源合计"));
        for (int i = 0; i < colTotal.length; i++) tot.append(String.format(Locale.US, " %+14.0f", colTotal[i]));
        tot.append(String.format(Locale.US, " %+14.1f", grandTotal));
        System.out.println(tot.toString());
        System.out.println("  说明：单元格=盈亏点(笔数,胜率)；· 表示该月该源无交易。");
        System.out.println("  看点：某源利润是否集中在少数几个月？集中=依赖特定行情=过拟合嫌疑。");
        System.out.println("------------------------------------------------");
    }

    static double calcMaxDrawdownPoints(List<CompletedTrade> trades) {
        double eq = 0, peak = 0, maxDd = 0;
        for (CompletedTrade t : trades) {
            eq += t.pnlPoints;
            if (eq > peak) peak = eq;
            double dd = peak - eq;
            if (dd > maxDd) maxDd = dd;
        }
        return maxDd;
    }

    static int calcMaxLoseStreak(List<CompletedTrade> trades) {
        int cur = 0, mx = 0;
        for (CompletedTrade t : trades) {
            if (t.pnlPoints < 0) { cur++; mx = Math.max(mx, cur); }
            else cur = 0;
        }
        return mx;
    }
    // 计算全程资金曲线峰值（用于 MaxDD% 分母）
// 优先使用 trade.exitEquity（更准确）；若未填则退化为 startEq + 累加 pnlUSDT
    static double calcPeakEquity(List<CompletedTrade> trades, double startEq) {
        double eq = startEq;
        double peak = startEq;

        for (CompletedTrade t : trades) {
            if (t.exitEquity != 0.0) {
                eq = t.exitEquity;
            } else {
                eq += t.pnlUSDT;
            }
            if (eq > peak) peak = eq;
        }
        return peak;
    }

    // 计算最大回撤金额（USDT）
// 同样优先用 exitEquity；否则用 pnlUSDT 累加构造资金曲线
    static double calcMaxDrawdownUSDT(List<CompletedTrade> trades, double startEq) {
        double eq = startEq;
        double peak = startEq;
        double maxDd = 0.0;

        for (CompletedTrade t : trades) {
            if (t.exitEquity != 0.0) eq = t.exitEquity;
            else eq += t.pnlUSDT;
            if (eq > peak) peak = eq;
            double dd = peak - eq;
            if (dd > maxDd) maxDd = dd;
        }
        return maxDd;
    }

    // 最大回撤%（逐步峰值算法，早期小回撤也能正确计算）
    static double calcMaxDrawdownPct(List<CompletedTrade> trades, double startEq) {
        double eq = startEq;
        double peak = startEq;
        double maxDdPct = 0.0;

        for (CompletedTrade t : trades) {
            if (t.exitEquity != 0.0) eq = t.exitEquity;
            else eq += t.pnlUSDT;
            if (eq > peak) peak = eq;
            if (peak > 1e-9) {
                double ddPct = (peak - eq) / peak * 100.0;
                if (ddPct > maxDdPct) maxDdPct = ddPct;
            }
        }
        return maxDdPct;
    }
    // ===================== DB优先：拉K线（按时间范围） =====================
    // - USE_DB_CACHE=true：优先读SQLite；不够再走网络分页补齐；只入库已收盘(confirm=1)
    // - USE_DB_CACHE=false：直接走网络分页
    static List<Candle> fetchRangeCandles(String instId, String bar, long startMs, long endMs) throws Exception {
        if (!USE_DB_CACHE) return fetchRangeCandlesNet(instId, bar, startMs, endMs);
        if (STRICT_SSOT) {
            CandleDb.init();
            return CandleDb.loadRange(instId, bar, startMs, endMs, true);
        }
        CandleDb.init();
        // 先读 DB（SSOT）。若 DB 还没覆盖到该区间，则补一次网络并写回 DB，再读。
        List<Candle> db = CandleDb.loadRange(instId, bar, startMs, endMs, true);
        if (db == null || db.isEmpty()) {
            List<Candle> net = fetchRangeCandlesNet(instId, bar, startMs, endMs);
            CandleDb.upsertNormalized(instId, bar, net, System.currentTimeMillis());
            CandleDb.validateInvariants(instId, bar, CandleDb.barMs(bar), System.currentTimeMillis());
            db = CandleDb.loadRange(instId, bar, startMs, endMs, true);
        }
        return db;
    }

    // ===================== 网络：拉K线（按时间范围分页） =====================
    public static List<Candle> fetchRangeCandlesNet(String instId, String bar, long startMs, long endMs) throws Exception {
        TreeMap<Long, Candle> map = new TreeMap<>();

        Long after = null;
        int guard = 0;

        long lastOldest = -1, lastNewest = -1;

        while (true) {
            String url = buildHistoryCandlesUrl(instId, bar, after, LIMIT);
            String json = fetch(url);
            List<Candle> page = parseCandles(json);
            if (page.isEmpty()) break;

            page.sort(Comparator.comparingLong(c -> c.ts));
            long oldest = page.get(0).ts;
            long newest = page.get(page.size() - 1).ts;

            if (oldest == lastOldest && newest == lastNewest) {
                System.out.println("[WARN] page not moving (after stuck), break");
                break;
            }
            lastOldest = oldest; lastNewest = newest;

            for (Candle c : page) map.put(c.ts, c);

            System.out.println(bar + " page=" + page.size()
                    + " oldest=" + Instant.ofEpochMilli(oldest)
                    + " newest=" + Instant.ofEpochMilli(newest)
                    + " total=" + map.size());

            if (oldest <= startMs) break;

            after = oldest - 1;

            Thread.sleep(Math.max(250, SLEEP_MS));
            if (++guard > GUARD_MAX) {
                System.out.println("[WARN] guard break " + bar);
                break;
            }
        }

        if (map.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(map.subMap(startMs, true, endMs, true).values());
    }


    // ===================== Puller（写库进程） =====================
    // 以 5 秒为默认频率增量写入 SQLite（可用 -Dokx.pullerPollMs=5000 调整）
    // 启动建议：-Dokx.role=puller -Dokx.strictSsot=false
    static void runPullerForever() {
        if (!USE_DB_CACHE) {
            System.out.println("[PULLER] USE_DB_CACHE=false -> nothing to do.");
            return;
        }
        try { CandleDb.init(); } catch (Exception e) { throw new RuntimeException(e); }

        final long pollMs = sysLong("okx.pullerPollMs", 5000);
        final int bars30m = (int) sysLong("okx.pullerBars30m", 600);
        final int bars1h  = (int) sysLong("okx.pullerBars1h",  400);

        System.out.println("[PULLER] started. pollMs=" + pollMs + " inst=" + INST_ID +
                " bars30m=" + bars30m + " bars1h=" + bars1h + " strictSsot=" + STRICT_SSOT);

        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        es.scheduleAtFixedRate(() -> {
            try {
                long nowMs = System.currentTimeMillis();
                // 写库：增量拉取并 upsert SQLite（30m + 1H）
                CandleDb.ingestRecent(INST_ID, "30m", Math.max(200, bars30m), nowMs);
                CandleDb.ingestRecent(INST_ID, "1H",  Math.max(200, bars1h),  nowMs);
            } catch (Throwable t) {
                System.out.println("[PULLER] ingest error: " + t.getMessage());
            }
        }, 0, Math.max(1000, pollMs), TimeUnit.MILLISECONDS);
    }

    // ===================== 定时 =====================
    static void schedule() {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

        long period = 30L * 60 * 1000;

        //  只在 :00:07 / :30:07 跑一次（收盘后确认：只用已收K）
        long delayPost = calcInitialDelayPostClose();

        es.scheduleAtFixedRate(() -> {
            try { runOnce(); } catch (Exception e) { e.printStackTrace(); }
        }, delayPost, period, TimeUnit.MILLISECONDS);
    }

    // 收盘后确认（:00:07 / :30:07）
    static long calcInitialDelayPostClose() {
        ZonedDateTime now = ZonedDateTime.now(ZONE);

        ZonedDateTime next = now.withSecond(POST_CLOSE_SECOND).withNano(0);
        int m = now.getMinute();

        if (m < 30) next = next.withMinute(30);
        else next = next.plusHours(1).withMinute(0);

        if (!next.isAfter(now)) next = next.plusMinutes(30);
        return Duration.between(now, next).toMillis();
    }

    // ===================== 主逻辑（实时刷新） =====================
    static void runOnce() throws Exception {
        if (!tryEnterRunOnce(Thread.currentThread().getName())) return;
        try {
            // 防止指标缓存跨轮次无限增长导致 OOM：每次刷新先清一次（本轮内仍可复用缓存）
            if (CLEAR_SERIES_CACHE_EACH_RUN) {
                try { IndicatorSeriesCache.clear(); } catch (Throwable ignore) {}
            }

            long now = System.currentTimeMillis();

            //  使用稳定缓存（启动1Y回测作为起算点），定时仅做增量合并，避免EMA/MACD漂移导致回看分叉
            syncCache(now);
            List<Candle> m30 = snapshotCache(CACHE_M30);
            List<Candle> h2  = snapshotCache(CACHE_1H);

            //  30m：不要只靠 confirm（有时会延迟一根），用时间判断已收盘（对齐 :00:07/:30:07）
            m30 = filterClosedByTime(m30, BAR30_MS, now, SAFE_CLOSE_MS);

            //  1H：不要只靠 confirm（有时会延迟一根），用时间判断已收盘（对齐 :00:07/:30:07）
            h2  = filterClosedByTime(h2, BAR1H_MS, now, SAFE_CLOSE_MS);

            Candle last30 = (m30.isEmpty() ? null : m30.get(m30.size() - 1));

            //  保存本次“已收盘30m”供后续打印使用（避免顺序变化导致动态止损打印为0）
            lastM30ClosedForPrint = m30;
            lastM30ClosedEndIdxForPrint = m30.size() - 1;
            updateOkxSrCacheForPrint(m30, m30.size() - 1);
            if (m30.size() >= 2) updateDynamicSlCache(m30, m30.size() - 1);

            //  数据不足时也要尽量输出“最近机会/最近10笔/持仓/SRMF”
            // 极端不足（<80）才直接 return，避免数组越界
            if (m30.size() < 80) {
                // 定时刷新打印：BT/RT 实际使用的最新 30m/1H K（开盘/收盘时间）
                long bc = (last30 == null ? 0L : (last30.ts + BAR30_MS));
                updateBtRtKUsageForPrint(bc, m30, h2);

                printRefreshHeader(now, last30, LIVE_ENGINE.pos);
                System.out.println("[WARN] 数据严重不足：30m=" + m30.size() + " 1H=" + h2.size() + "（无法稳定计算信号）");
                System.out.println("提示：多半是限流/网络/分页卡住，把 SLEEP_MS 提到 450~650 再试。");
                return;
            }
            if (m30.size() < 300) {
                System.out.println("[WARN] 数据偏少：30m=" + m30.size() + " 1H=" + h2.size()
                        + "（仍继续输出 最近机会/最近10笔/持仓/SRMF；但历史样本少，信号质量参考性会下降）");
            }

            //  强制对齐“这次应该用的那根已收K”
            // now=12:07 -> 期望 last30.ts=11:30（它的收盘=12:00）
            // 期望“已收盘”的 30m K：用 (now - SAFE_CLOSE_MS) 先落到时间格，再退一格，避免 14:30:01 这类刚到边界但仍未收盘的误判
            long t = now - SAFE_CLOSE_MS;
            long boundary = (t / BAR30_MS) * BAR30_MS;   // 边界: ...:00 / ...:30
            long expectedOpen = boundary - BAR30_MS;     // 期望已收K的开盘时间
            long expectedClose = expectedOpen + BAR30_MS;

            if (DEBUG_REALTIME) {
                log1("[WAIT] expectedOpen(30m)=" + fmtTs(expectedOpen) + " (close@" + fmtTs(expectedClose) + "), safeCloseMs=" + SAFE_CLOSE_MS);
            }

            // 若 OKX / DB 还没把“应该已收盘”的那根写成 confirm=1，就短暂等待并重读 DB。
            // 关键：等待阶段不要反复 syncCache / clearCache（会导致假 skip / 晚一根 / 控制台刷屏）。
            int tries = WAIT_OKX_REFRESH_TRIES;
            int waitTry = 0;
            boolean ok = false;
            for (int i = 0; i < tries; i++) {
                long now2 = System.currentTimeMillis();
                long lastClosed = getLatestClosedOpenTsFromDb("30m", BAR30_MS, now2);
                if (lastClosed >= expectedOpen) { ok = true; break; }

                if (DEBUG_REALTIME) {
                    log1("[WAIT] dbLastClosed(30m)=" + (lastClosed <= 0 ? "null" : fmtTs(lastClosed)) + " < expectedOpen=" + fmtTs(expectedOpen)
                            + " => sleep " + WAIT_OKX_REFRESH_MS + "ms (try " + (i + 1) + "/" + tries + ")");
                }
                try { waitTry = i + 1; Thread.sleep(WAIT_OKX_REFRESH_MS); } catch (InterruptedException ie) { /* ignore */ }
            }

            if (!ok) {
                // 仍未追上：本轮只 skip，不改变状态机；下轮继续等
                if (DEBUG_REALTIME) {
                    log1("[WAIT][SKIP] still not refreshed: expectedOpen=" + fmtTs(expectedOpen));
                }
                requestSkipRetry(expectedOpen, last30, "DB未刷新到期望已收K(confirm=1)");
                return;
            }

            // 等到 DB 追上后，再做一次 full sync（只做一次）
            syncCache(System.currentTimeMillis());

            // 重新快照（非常关键：syncCache 只更新 CACHE_*，之前的 m30/h2 是旧快照）
            long now3 = System.currentTimeMillis();
            m30 = snapshotCache(CACHE_M30);
            h2  = snapshotCache(CACHE_1H);

            // 再次按时间过滤为“已收盘K”（不依赖 confirm，避免 OKX confirm 延迟导致错过一根）
            m30 = filterClosedByTime(m30, BAR30_MS, now3, SAFE_CLOSE_MS);
            h2  = filterClosedByTime(h2, BAR1H_MS, now3, SAFE_CLOSE_MS);

            // 重新拿一次 last30（此时应匹配 expectedOpen）
            last30 = m30.isEmpty() ? null : m30.get(m30.size() - 1);
            if (last30 == null || last30.ts != expectedOpen) {
                requestSkipRetry(expectedOpen, last30, "DB已刷新但last30仍不匹配(可能窗口过小/排序异常)");
                return;
            }

            //  等待刷新后：m30/last30 可能已被重拉，这里同步刷新“打印/下单用”的缓存
            // （保证 SRMF 下单名义使用“本次定时刷新最终的动态止损点数”，且仅基于已收盘K0，无未来函数）
            lastM30ClosedForPrint = m30;
            lastM30ClosedEndIdxForPrint = m30.size() - 1;
            updateOkxSrCacheForPrint(m30, lastM30ClosedEndIdxForPrint);
            if (m30.size() >= 2) updateDynamicSlCache(m30, lastM30ClosedEndIdxForPrint);
            // v30: 保存一份“本轮已收盘K线快照”（时间胶囊），用于之后回放/对齐实盘
            persistSnapshotsIfEnabled(System.currentTimeMillis(), expectedClose, m30, h2);
            //  若本次发生过“等待OKX刷新”，打印最终K0动态止损点数，方便肉眼核对
            if (waitTry > 0) {
                System.out.printf(Locale.US, "最终K0动态止损点数=%.2f%n", lastSlPointsCache);
            }
            if (last30 != null) lastPriceCache = last30.c;
            //  EMA 多时间投票上下文（只用已收盘K；4H 由 30m 聚合）
            if (USE_EMA_VOTE_FILTER) {
                EMA_VOTE_CTX_REALTIME = EmaVoteContext.build(m30, h2);
            } else {
                EMA_VOTE_CTX_REALTIME = null;
            }


            // 定时刷新打印：记录本轮（截至 expectedClose）BT/RT 实际使用的最新 30m/1H K（开盘/收盘时间）
            updateBtRtKUsageForPrint(expectedClose, m30, h2);
            List<MacdPoint> macd30 = calcMacd(m30);
            List<MacdPoint> macd1h = (h2.isEmpty() ? Collections.emptyList() : calcMacd(h2));

            //  latest：用于 next-open 入场（实盘） & 实盘提示 nextOpen 展示（缺失也不影响信号）
            List<Candle> m30Latest = fetchLatestCandles("30m", 10);

            //  回看（最近10笔 / LAST ENTER/EXIT）
            // 关键修复：历史回看/研究推演 **不再喂 placeholder**（placeholder 只用于展示 K1.open，不参与任何指标计算/历史扫描）
            // 回看推演（VIEW_ENGINE）：scanHistory 仅需要“已收盘K线 + 对齐后的指标序列”，即可严格模拟“下一根开盘进场”
            // - 信号发生在 K0 收盘（即 K1 开盘）
            // - 入场时间 entryTs = c0.ts + BAR30_MS
            // - 入场价格：有 K1 则用 K1.open；没有 K1 则用 K0.close（兜底）
// ===== 一致性核心：用“回测推演(Backtest)”驱动“实时持仓(RT)” =====
// 目标：同一份数据、同一套规则，BT 的入场/出场/持仓必须与 RT 完全一致。
            Position __oldPos = (LIVE_ENGINE == null ? null : LIVE_ENGINE.pos);

            EngineState __bt = new EngineState();
// 用完整推演得到“截至本轮已收盘K”的唯一真相状态（BT SSOT）
            IN_BOOT_BACKTEST = true;

            List<CompletedTrade> __btTrades = runBacktestToState(__bt, m30, macd30, h2, macd1h);
            IN_BOOT_BACKTEST = false;

// ====== 修复：确保 BT 状态包含 lastExitOpp / lastTrades（用于 BT→RT 同步打印与对齐）======
// 某些分支下（例如 scanHistory 与 bootBacktest 分离、或未来再改动时），可能出现 __bt.lastExitOpp/lastTrades 未被填充，
// 导致同步出场价格退化为 entry。这里用 __btTrades 兜底重建“最近一次出场/最近N笔”缓存，保证同步价格/原因正确。
            if (__bt != null && __btTrades != null && !__btTrades.isEmpty()) {
                CompletedTrade __last = __btTrades.get(__btTrades.size() - 1);
                if (__bt.lastExitOpp == null && __last.exitTs > 0 && Double.isFinite(__last.exit)) {
                    __bt.lastExitOpp = new ExitOpportunity();
                    __bt.lastExitOpp.ts = __last.exitTs;
                    __bt.lastExitOpp.side = __last.side;
                    __bt.lastExitOpp.price = __last.exit;
                    __bt.lastExitOpp.reason = __last.reason;
                }
                if (__bt.lastTrades.isEmpty()) {
                    int __start = Math.max(0, __btTrades.size() - LAST_N);
                    for (int __k = __start; __k < __btTrades.size(); __k++) {
                        __bt.lastTrades.addLast(__btTrades.get(__k));
                    }
                }
            }

// 覆盖前硬校验：打印并核对 BT/RT 使用的 30m/1H K（防止“用到未来1H/错位1H”）
// 说明：本架构下 RT 以 BT 为准，所以这里主要用于证明 BT 选择的 K 满足“<= boundaryClose 已收盘”。
            maybePrintSyncHardCheck(__oldPos, __bt, m30, h2, expectedClose);

// 将 BT 推演结果同步为 RT 持仓核心（RT 服从 BT）
            syncLiveEngineFromBacktest(__bt);

// 仍保留 scanHistory 作为“展示层输出”（最近10笔/最近机会），但它不再决定 RT 持仓
            scanHistory(m30, macd30, h2, macd1h);

// 生成一次“同步事件”（用于响铃/中文提示；真正的持仓变化由 BT 决定）
            EngineEvent __syncEv = buildSyncEvent(__oldPos, LIVE_ENGINE.pos, __bt);
            dispatchSignalEvent(__syncEv);

// ===== [BUG FIX] BT->RT 同步进场后补充触发自动下单 =====
// 问题根因：buildSyncEvent/dispatchSignalEvent 只做响铃+打印，完全绕过了 checkRealtime 里
// 负责自动下单的代码块（autoEntryPending + AUTO_ES.submit）。
// 修复：在 BT 同步检测到新进场（__oldPos==null && LIVE_ENGINE.pos!=null）时，
// 补充调用 trySubmitAutoEntryFromBtSync，与 checkRealtime 路径的下单行为保持一致。
            if (AUTO_TRADE_ENABLED
                    && __syncEv != null
                    && __syncEv.kind == SignalKind.ENTER
                    && __oldPos == null
                    && LIVE_ENGINE.pos != null
                    && LIVE_ENGINE.pos.side != null) {
                trySubmitAutoEntryFromBtSync(LIVE_ENGINE.pos, m30, macd30, h2);
            }

// ===== [BUG FIX] BT->RT 同步出场后补充触发自动平仓 =====
// 问题根因：buildSyncEvent/dispatchSignalEvent 只做响铃+打印，完全绕过了 checkRealtime 里
// 负责自动平仓的代码块（autoClosePending + AUTO_ES.submit）。
// 修复：在 BT 同步检测到出场（__oldPos!=null && LIVE_ENGINE.pos==null）时，
// 补充调用 trySubmitAutoCloseFromBtSync，与 checkRealtime 路径的平仓行为保持一致。
            if (AUTO_TRADE_ENABLED
                    && __syncEv != null
                    && __syncEv.kind == SignalKind.EXIT
                    && __oldPos != null
                    && __oldPos.side != null
                    && (LIVE_ENGINE.pos == null || LIVE_ENGINE.pos.side == null)) {
                trySubmitAutoCloseFromBtSync(__oldPos, __syncEv.alarmKeyTs, __syncEv.alarmKeyTs, "BT-SYNC-EXIT");
            }

            // 可选：策略已无仓但 OKX 仍有仓 -> 自动补偿平仓（默认开：-Dokx.autoCloseOrphanFlat=true）
            maybeAutoCloseOrphanPositionWhenStrategyFlat();

// ===== RT/HIST 对比：本轮结束时再对齐一次（此时应当一致；若不一致直接报警）=====
            alignCompare();

//  再打印（保证持仓状态、最近机会等和提示后的状态一致）
            printRefreshHeader(now, last30, LIVE_ENGINE.pos);

            printIndicatorSnapshotLite(m30, macd30, h2, macd1h);
            printLastOppTemplate();
            printLastTradesTemplate();
            printSrmfBlock();
        } finally {
            leaveRunOnce();
        }
    }


    // ===================== BT 驱动 RT：同步工具 =====================
    static void syncLiveEngineFromBacktest(EngineState bt) {
        if (bt == null) return;
        // LIVE_ENGINE 是 static final，永远不为 null；这里不允许重新赋值

        // 同步持仓
        LIVE_ENGINE.pos = (bt.pos == null ? null : copyPosition(bt.pos));

        // 同步 SRMF / equity
        LIVE_ENGINE.srmf = bt.srmf;
        LIVE_ENGINE.equity = bt.equity;

        // 同步“展示层”缓存
        LIVE_ENGINE.lastEnterOpp = bt.lastEnterOpp;
        LIVE_ENGINE.lastExitOpp  = bt.lastExitOpp;
        LIVE_ENGINE.lastTrades.clear();
        LIVE_ENGINE.lastTrades.addAll(bt.lastTrades);

        // 同步冷却与去重（以 BT 推演为准）
        LIVE_ENGINE.lastProcessedSigTs = bt.lastProcessedSigTs;
        LIVE_ENGINE.lastStopSigTs = bt.lastStopSigTs;
        LIVE_ENGINE.lastTpSigTs   = bt.lastTpSigTs;
        LIVE_ENGINE.stopCooldownUntilEntryTs = bt.stopCooldownUntilEntryTs;
        LIVE_ENGINE.tpCooldownUntilEntryTs   = bt.tpCooldownUntilEntryTs;
    }

    // ===================== [BUG FIX] BT->RT 同步进场自动下单 =====================
    // 当 BT 推演发现新进场、并通过 syncLiveEngineFromBacktest 同步到 LIVE_ENGINE.pos 时，
    // 此方法负责触发与 checkRealtime 路径完全一致的自动下单逻辑。
    // 核心参数从 pos（已由 BT 填充）中读取，避免重复计算/不一致。
    static void trySubmitAutoEntryFromBtSync(Position pos, List<Candle> m30, List<MacdPoint> macd30, List<Candle> h2) {
        if (!AUTO_TRADE_ENABLED) return;
        if (pos == null || pos.side == null) return;

        long nowMs = System.currentTimeMillis();

        // 幂等锁：同一 entryTs 在有效期内只允许触发一次
        if (autoEntryLockedEntryTs == pos.entryTs && nowMs < autoEntryExpireAtMs) {
            System.out.println("[BT-SYNC-AUTO] 幂等跳过：entryTs=" + fmtOpen(pos.entryTs) + " 已在有效期内");
            return;
        }

        // 若已有挂单/平仓任务进行中，不重复提交
        if (autoEntryPending) {
            System.out.println("[BT-SYNC-AUTO] 跳过：autoEntryPending=true，入场挂单进行中");
            return;
        }
        if (autoClosePending) {
            System.out.println("[BT-SYNC-AUTO] 跳过：autoClosePending=true，平仓任务进行中");
            return;
        }

        // 入场前查实盘仓位，已有仓位则跳过
        try {
            OkxAutoTradeBridge.PositionSnap ps = AUTO.getPositionSnapPublic(INST_ID);
            if (ps != null && ps.hasPos && ps.sz > 0) {
                System.out.println("[BT-SYNC-AUTO] 跳过：已有实盘仓位 sz=" + ps.sz);
                return;
            }
        } catch (Exception ignore) {}

        // 从 pos 提取参数（BT 推演已经算好了 entryMult/slPoints/notional）
        Side side = pos.side;
        double entryPx = pos.entry;
        double slPts = pos.slPoints > 0 ? pos.slPoints : pos.slPointsAtEntry;
        if (slPts <= 0 && m30 != null && !m30.isEmpty()) {
            slPts = calcSlPointsForIndex(m30, m30.size() - 1);
        }
        double notionalUsdt = pos.entryNotional;
        // 如果 BT 里 notional 没填（老版本兼容），重新算一次
        if (notionalUsdt <= 0) {
            double mult = pos.entryMult > 0 ? pos.entryMult : 1.0;
            NotionalDecision nd = calcEntryNotional(mult, entryPx, slPts);
            notionalUsdt = nd.notionalUsdt;
        }

        long expireAtMs = nowMs + 15L * 60L * 1000L;
        long entryTs = pos.entryTs;

        System.out.printf(Locale.US,
                "[BT-SYNC-AUTO] 触发自动下单 | side=%s | entryPx=%.2f | slPts=%.2f | notional=%.0f | entryTs=%s%n",
                side, entryPx, slPts, notionalUsdt, fmtOpen(entryTs));

        autoEntryPending = true;
        autoEntryExpireAtMs = expireAtMs;
        autoEntryLockedEntryTs = entryTs;

        Side finalSide = side;
        double finalEntryPx = entryPx;
        double finalSlPts = slPts;
        double finalNotional = notionalUsdt;

        autoEntryFuture = AUTO_ES.submit(() -> {
            try {
                OkxAutoTradeBridge.OpenResult r = AUTO.tryOpenNetIsolatedUntilExpireWithSL(
                        INST_ID, finalSide, finalNotional, finalEntryPx, finalSlPts, expireAtMs, 0L, entryTs
                );
                if (r != null && r.ok && r.filled) {
                    double avg = (r.avgPx > 0 ? r.avgPx : finalEntryPx);
                    System.out.printf(Locale.US, "[BT-SYNC-AUTO] ENTRY filled ✅ avgPx=%.2f ordId=%s%n", avg, r.ordId);
                } else {
                    System.out.println("[BT-SYNC-AUTO] ENTRY not filled / canceled: " + (r == null ? "null" : r.msg));
                }
            } catch (Exception e) {
                System.out.println("[BT-SYNC-AUTO] ENTRY exception: " + e.getMessage());
            } finally {
                autoEntryPending = false;
                autoEntryOrdId = null;
            }
        });
    }

    // ===================== [BUG FIX] BT->RT 同步出场自动平仓（post-only 轮询） =====================
    // 场景：BT 推演在 boundaryClose 发现出场，并通过 buildSyncEvent/dispatchSignalEvent 提示+响铃；
    // 但该路径不会经过 checkRealtime 里的 AUTO-EXIT 提交块，导致“有闹钟/提醒，但不自动平仓”。
    //
    // 修复：
    // 1) 当 BT 同步检测到出场（__oldPos!=null && LIVE_ENGINE.pos==null）时，立即补充触发自动平仓；
    // 2) 额外做一个“孤儿仓位修复”（策略已无仓但 OKX 仍有仓）可选开关，避免因为一次网络失败就永远不再尝试平仓。
    //
    // 平仓采用 OkxAutoTradeBridge.tryCloseNetIsolated：post-only 轮询（默认 30s），超时则按桥接逻辑降级处理。
    static void trySubmitAutoCloseFromBtSync(Position oldPos, long idemKey, long exitTsForPrint, String tag) {
        if (!AUTO_TRADE_ENABLED) return;
        if (oldPos == null || oldPos.side == null) return;

        // 仍在进行中的平仓任务，不重复提交
        if (autoClosePending) {
            System.out.println("[BT-SYNC-AUTO] 跳过：autoClosePending=true，平仓任务进行中");
            return;
        }

        long nowMs = System.currentTimeMillis();

        // 幂等锁：同一 key 在有效期内只允许触发一次（避免短时间内反复提交）
        if (autoCloseLockedKey == idemKey && nowMs < autoCloseExpireAtMs) {
            String tsStr = (exitTsForPrint > 0 ? fmtJst(exitTsForPrint) : "n/a");
            System.out.println("[BT-SYNC-AUTO] 幂等跳过：exitTs=" + tsStr + " key=" + idemKey + " tag=" + tag);
            return;
        }

        // 策略补偿平仓前：先查实盘是否仍有仓位（无仓则直接跳过）
        OkxAutoTradeBridge.PositionSnap ps = null;
        try { ps = AUTO.getPositionSnapPublic(INST_ID); } catch (Exception ignore) {}
        if (ps == null || !ps.hasPos || ps.sz <= 0) {
            System.out.println("[BT-SYNC-AUTO] 跳过：实盘无仓，tag=" + tag);
            return;
        }

        // vm 参数：你现在用 -Dokx.makerOffset 控制（与 checkRealtime AUTO-EXIT 保持一致）
        double vm = sysDouble("okx.makerOffset", 0.03);
        long ttlMs = sysLong("okx.makerTtlMs", 30_000L);
        long expireAtMs = nowMs + ttlMs + 5_000L;

        Side closeSide = oldPos.side;
        String tsStr = (exitTsForPrint > 0 ? fmtJst(exitTsForPrint) : "n/a");

        System.out.printf(Locale.US,
                "[BT-SYNC-AUTO] 提交平仓: exitTs=%s | key=%d | tag=%s | side=%s | vm(makerOffset)=%.6f | ttl=%dms | okxSz=%.4f%n",
                tsStr,
                idemKey,
                tag,
                (closeSide == Side.LONG ? "做多" : "做空"),
                vm, ttlMs,
                ps.sz);

        autoClosePending = true;
        autoCloseLockedKey = idemKey;
        autoCloseExpireAtMs = expireAtMs;

        Side finalCloseSide = closeSide;
        double finalVm = vm;
        long finalTtlMs = ttlMs;

        autoCloseFuture = AUTO_ES.submit(() -> {
            try {
                // 注意：这里是“策略出场”补偿平仓，非止损；止损由 SL algo 单处理
                AUTO.tryCloseNetIsolated(INST_ID, finalCloseSide, false, finalVm, finalTtlMs, 0, 0L, true);
                System.out.println("[BT-SYNC-AUTO] 平仓任务结束（已触发桥接 close 流程） tag=" + tag);
            } catch (Exception e) {
                System.out.println("[BT-SYNC-AUTO] 平仓异常 tag=" + tag + " : " + e.getMessage());
                e.printStackTrace(System.out);
            } finally {
                autoClosePending = false;
                autoCloseFuture = null;
            }
            return null;
        });
    }

    // 可选：策略已无仓但 OKX 仍有仓时，自动补偿平仓（默认开：-Dokx.autoCloseOrphanFlat=true）
    static void maybeAutoCloseOrphanPositionWhenStrategyFlat() {
        if (!AUTO_TRADE_ENABLED) return;
        if (!sysBool("okx.autoCloseOrphanFlat", true)) return;

        if (autoClosePending) return;
        if (LIVE_ENGINE.pos != null && LIVE_ENGINE.pos.side != null) return;

        OkxAutoTradeBridge.PositionSnap ps = null;
        try { ps = AUTO.getPositionSnapPublic(INST_ID); } catch (Exception ignore) {}
        if (ps == null || !ps.hasPos || ps.sz <= 0 || ps.side == null) return;

        long nowMs = System.currentTimeMillis();
        final long ORPHAN_KEY = -777777777777L;

        // 失败会在 ttl+5s 后允许重试
        if (autoCloseLockedKey == ORPHAN_KEY && nowMs < autoCloseExpireAtMs) return;

        Position tmp = new Position();
        tmp.side = ps.side;

        System.out.printf(Locale.US,
                "[ORPHAN-AUTO-CLOSE] 策略=无仓 但OKX仍有仓 -> 提交平仓 | sz=%.4f | side=%s%n",
                ps.sz, (ps.side == Side.LONG ? "多" : "空"));

        trySubmitAutoCloseFromBtSync(tmp, ORPHAN_KEY, nowMs, "ORPHAN-FLAT");
    }


    // ===================== 覆盖前硬校验：BT/RT 用K对齐检查 =====================
    static void maybePrintSyncHardCheck(Position oldPos, EngineState bt, List<Candle> m30, List<Candle> h2, long boundaryCloseHint) {
        if (bt == null) return;
        Position newPos = bt.pos;
        Side oldSide = (oldPos == null ? null : oldPos.side);
        Side newSide = (newPos == null ? null : newPos.side);

        // 无变化：不打印
        if (Objects.equals(oldSide, newSide)
                && (oldPos == null || newPos == null || oldPos.entryTs == newPos.entryTs)) {
            return;
        }

        // 选一个 boundaryClose 用于校验
        long boundaryCloseTs = 0L;
        String kind = "CHANGE";
        if (oldSide == null && newSide != null) {
            kind = "ENTER";
            boundaryCloseTs = newPos.entryTs;
        } else if (oldSide != null && newSide == null) {
            kind = "EXIT";
            ExitOpportunity xo = bt.lastExitOpp;
            if (xo != null && xo.ts > 0) boundaryCloseTs = xo.ts;
            if (boundaryCloseTs <= 0 && bt.lastTrades != null && !bt.lastTrades.isEmpty()) {
                CompletedTrade lastCt = null;
                for (CompletedTrade ct : bt.lastTrades) lastCt = ct;
                if (lastCt != null) boundaryCloseTs = lastCt.exitTs;
            }
        } else {
            // flip
            kind = "FLIP";
            boundaryCloseTs = (newPos != null ? newPos.entryTs : 0L);
        }
        if (boundaryCloseTs <= 0) {
            // 兜底：若 BT 未能给出 exitTs（lastExitOpp/lastTrades 为空），用本轮 expectedClose 作为校验边界。
            // 这样即使同步层走了兜底路径，你也能看到本轮 boundaryClose 上 BT 选择的 30m/1H 是否正确（是否未来1H）。
            if (boundaryCloseHint > 0) boundaryCloseTs = boundaryCloseHint;
        }
        if (boundaryCloseTs <= 0) return;

        // 对齐到 30m boundary（确保打印一致）
        boundaryCloseTs = floorToBar(boundaryCloseTs, BAR30_MS);

        // BT used 30m：K0 = [boundary-30m, boundary]
        int bt30 = lastClosedCandleIdx(m30, boundaryCloseTs, BAR30_MS);
        Candle bt30c = (bt30 >= 0 && bt30 < m30.size()) ? m30.get(bt30) : null;

        // BT used 1H：最后一根已收盘 1H（严格 <= boundaryClose）
        int bt1h = (h2 == null || h2.isEmpty()) ? -1 : regimeSafeH1Idx(h2, boundaryCloseTs);
        Candle bt1hc = (bt1h >= 0 && bt1h < (h2 == null ? 0 : h2.size())) ? h2.get(bt1h) : null;

        // RT used（理论上与 BT 一致；此处打印同一套选法，便于你核对“有没有选到 02:00-03:00”）
        int rt30 = bt30;
        Candle rt30c = bt30c;
        int rt1h = bt1h;
        Candle rt1hc = bt1hc;

        System.out.printf(Locale.US,
                "[SYNC-HARD-CHECK][%s] boundaryClose=%s%n" +
                        "  BT 30m idx=%d %s%n" +
                        "  BT 1H  idx=%d %s%n" +
                        "  RT 30m idx=%d %s%n" +
                        "  RT 1H  idx=%d %s%n",
                kind,
                FMT_JST.format(Instant.ofEpochMilli(boundaryCloseTs)),
                bt30, briefCandle(bt30c, BAR30_MS),
                bt1h, briefCandle(bt1hc, BAR1H_MS),
                rt30, briefCandle(rt30c, BAR30_MS),
                rt1h, briefCandle(rt1hc, BAR1H_MS)
        );

        // 未来函数硬断言（只打印，不中断）：1H 必须已收盘
        if (bt1hc != null) {
            long closeTs = bt1hc.ts + BAR1H_MS;
            if (closeTs > boundaryCloseTs) {
                System.out.printf(Locale.US,
                        "[SYNC-HARD-CHECK][WARN] picked 1H not closed! pickedClose=%s > boundaryClose=%s (will cause future-leak)\n",
                        FMT_JST.format(Instant.ofEpochMilli(closeTs)),
                        FMT_JST.format(Instant.ofEpochMilli(boundaryCloseTs))
                );
            }
        }
    }

    static long floorToBar(long ts, long barMs) {
        return (ts / barMs) * barMs;
    }

    static String briefCandle(Candle c, long barMs) {
        if (c == null) return "(null)";
        long open = c.ts;
        long close = c.ts + barMs;
        return String.format(Locale.US,
                "ts=%s/%s confirm=%d o=%.2f c=%.2f",
                FMT_JST.format(Instant.ofEpochMilli(open)) + "(开)",
                FMT_JST.format(Instant.ofEpochMilli(close)) + "(收)",
                c.confirm,
                c.o,
                c.c
        );
    }


    // ===================== 定时刷新显示：BT/RT 使用的最新K线（开盘/收盘时间） =====================
    // 说明：
    // - 回测推演(BT)：runBacktestToState 推演到“本轮已收盘K”（expectedClose）为止
    // - 实盘触发(RT)：本架构下 RT 以 BT 为准，但这里仍分别打印，方便核对/未来扩展
    static final boolean PRINT_BT_RT_K_EACH_REFRESH = sysBool("okx.printBtRtKEachRefresh", true);

    static volatile long LAST_BT_RT_BOUNDARY_CLOSE_FOR_PRINT = 0L;
    static volatile Candle LAST_BT_30M_FOR_PRINT = null;
    static volatile Candle LAST_BT_1H_FOR_PRINT = null;
    static volatile Candle LAST_RT_30M_FOR_PRINT = null;
    static volatile Candle LAST_RT_1H_FOR_PRINT = null;

    static void updateBtRtKUsageForPrint(long expectedCloseTs, List<Candle> m30, List<Candle> h2) {
        if (!PRINT_BT_RT_K_EACH_REFRESH) return;
        if (expectedCloseTs <= 0) return;

        // 统一对齐到 30m “收盘边界”，避免显示抖动
        long boundaryCloseTs = floorToBar(expectedCloseTs, BAR30_MS);
        LAST_BT_RT_BOUNDARY_CLOSE_FOR_PRINT = boundaryCloseTs;

        int bt30 = lastClosedCandleIdx(m30, boundaryCloseTs, BAR30_MS);
        Candle bt30c = (m30 != null && bt30 >= 0 && bt30 < m30.size()) ? m30.get(bt30) : null;

        int bt1h = (h2 == null || h2.isEmpty()) ? -1 : regimeSafeH1Idx(h2, boundaryCloseTs);
        Candle bt1hc = (h2 != null && bt1h >= 0 && bt1h < h2.size()) ? h2.get(bt1h) : null;

        LAST_BT_30M_FOR_PRINT = bt30c;
        LAST_BT_1H_FOR_PRINT = bt1hc;

        // 当前架构：RT 服从 BT（同一套 K 选择）。仍保留独立字段，便于未来做 RT/BT 差异对比。
        LAST_RT_30M_FOR_PRINT = bt30c;
        LAST_RT_1H_FOR_PRINT = bt1hc;
    }

    static EngineEvent buildSyncEvent(Position oldPos, Position newPos, EngineState bt) {
        Side oldSide = (oldPos == null ? null : oldPos.side);
        Side newSide = (newPos == null ? null : newPos.side);

        // 无变化：不打印/不响铃
        if (Objects.equals(oldSide, newSide)
                && (oldPos == null || newPos == null || oldPos.entryTs == newPos.entryTs)) {
            return null;
        }

        // 进场
        if (oldSide == null && newSide != null) {
            String txt = String.format(Locale.US,
                    "【一致性同步-进场】BT→RT | 时间=%s | 方向=%s | 入场=%.2f | signalTs=%s%n",
                    FMT_JST.format(Instant.ofEpochMilli(newPos.entryTs)),
                    (newSide == Side.LONG ? "做多" : "做空"),
                    newPos.entry,
                    (newPos.signalTs > 0 ? FMT_JST.format(Instant.ofEpochMilli(newPos.signalTs)) : "NA")
            );
            return new EngineEvent(SignalKind.ENTER, newPos.entryTs, true, txt);
        }

        // 出场：优先用 bt.lastExitOpp，其次用 bt.lastTrades 的最后一笔（避免 lastExitOpp 丢失导致用 entry 当 exit）
        if (oldSide != null && newSide == null) {
            ExitOpportunity xo = (bt == null ? null : bt.lastExitOpp);

            long ts = 0L;
            String reason = null;
            double px = 0.0;

            if (xo != null && xo.ts > 0) {
                ts = xo.ts;
                reason = xo.reason;
                px = xo.price;
            } else if (bt != null && bt.lastTrades != null && !bt.lastTrades.isEmpty()) {
                CompletedTrade lastCt = null;
                for (CompletedTrade ct : bt.lastTrades) lastCt = ct; // deque last
                if (lastCt != null) {
                    ts = lastCt.exitTs;
                    reason = lastCt.reason;
                    px = lastCt.exit;
                }
            }

            if (ts <= 0) ts = System.currentTimeMillis();
            if (reason == null) reason = "BT同步出场";
            if (px == 0.0 && oldPos != null) px = oldPos.entry; // 最后兜底，至少别崩

            String txt = String.format(Locale.US,
                    "【一致性同步-出场】BT→RT | 时间=%s | 方向=%s | 出场=%.2f | 原因=%s%n",
                    FMT_JST.format(Instant.ofEpochMilli(ts)),
                    (oldSide == Side.LONG ? "做多" : "做空"),
                    px,
                    reason
            );
            return new EngineEvent(SignalKind.EXIT, ts, true, txt);
        }

        // 反手/重建：按进场打印
        String txt = String.format(Locale.US,
                "【一致性同步-变更】BT→RT | old=%s new=%s | entryTs=%s px=%.2f%n",
                (oldSide == null ? "NONE" : oldSide.name()),
                (newSide == null ? "NONE" : newSide.name()),
                (newPos == null ? "NA" : FMT_JST.format(Instant.ofEpochMilli(newPos.entryTs))),
                (newPos == null ? 0.0 : newPos.entry)
        );
        long keyTs = (newPos != null && newPos.entryTs > 0 ? newPos.entryTs : System.currentTimeMillis());
        return new EngineEvent(SignalKind.ENTER, keyTs, true, txt);
    }


    static Position copyPosition(Position p) {
        if (p == null) return null;
        Position q = new Position();
        q.mult = p.mult;
        q.side = p.side;
        q.entry = p.entry;
        q.entryTs = p.entryTs;
        q.signalTs = p.signalTs;
        q.entryNotional = p.entryNotional;
        q.isVirtual = p.isVirtual;

        q.okxResistanceAtEntry = p.okxResistanceAtEntry;
        q.okxSupportAtEntry = p.okxSupportAtEntry;
        q.okxGapAtEntry = p.okxGapAtEntry;

        q.slPoints = p.slPoints;

        q.entryEquity = p.entryEquity;
        q.entryTierMult = p.entryTierMult;
        q.entryMult = p.entryMult;

        q.entryVoteScore = p.entryVoteScore;
        q.entryVoteScale = p.entryVoteScale;
        q.entryVoteDirPass = p.entryVoteDirPass;
        q.entryVoteOverallPass = p.entryVoteOverallPass;
        q.entryVoteDetail = p.entryVoteDetail;

        q.slPointsAtEntry = p.slPointsAtEntry;
        q.regimeAtEntry = p.regimeAtEntry;
        q.trendPtsAtEntry = p.trendPtsAtEntry;

        q.mfe = p.mfe;
        q.mae = p.mae;
        return q;
    }
    // ===================== 扫历史（用于最近10笔/last enter/exit） =====================
    // ===================== 扫历史（用于：最近10笔 / 最近机会 / 与实盘一致） =====================
    // 说明：之前这里有一套“简化回放”逻辑，缺少【出场=下一根开盘 & 同一时刻又触发入场】的冲突保护，
    // 会导致你看到的“分叉”（同一时间先出再进）。现在直接复用 runBacktestAlarmLogic，保证与开局一年回测一致。
    // ===== Route A / 双轨制：View 走“实盘回放”，研究回测仍可走推演 =====
    static Trend1H parseTrend1H(String s) {
        if (s == null) return Trend1H.中性;
        if (s.contains("多")) return Trend1H.多;
        if (s.contains("空")) return Trend1H.空;
        return Trend1H.中性;
    }

    static void syncViewFromReplayDb() {
        long nowMs = System.currentTimeMillis();
        if (!AUDIT_DB_ENABLED) return;

        // 1) 同步最近 N 笔“实盘链路”成交
        try {
            VIEW_ENGINE.lastTrades.clear();
            List<CompletedTrade> recent = AuditDb.readRecentRtTradesAsCompletedTrades(LAST_N);
            if (recent != null && !recent.isEmpty()) {
                Collections.reverse(recent); // 变为从旧到新
                for (CompletedTrade ct : recent) {
                    if (ct == null) continue;
                    while (VIEW_ENGINE.lastTrades.size() >= LAST_N) VIEW_ENGINE.lastTrades.removeFirst();
                    VIEW_ENGINE.lastTrades.addLast(ct);
                }
                CompletedTrade last = recent.get(recent.size() - 1);
                if (last != null) {
                    VIEW_ENGINE.lastExitOpp = new ExitOpportunity();
                    VIEW_ENGINE.lastExitOpp.ts = last.exitTs;
                    VIEW_ENGINE.lastExitOpp.side = last.side;
                    VIEW_ENGINE.lastExitOpp.price = last.exit;
                    VIEW_ENGINE.lastExitOpp.reason = last.reason;
                    if (last.exitEquity > 0) VIEW_ENGINE.equity = last.exitEquity;
                }
            }
        } catch (Exception e) {
            System.out.println("[REPLAY] sync trades failed: " + e.getMessage());
        }

        // 2) 同步最近一次“实盘进场触发”
        try {
            RtTrigger rt = AuditDb.readLastRtTrigger();
            if (rt != null) {
                EnterOpportunity e = new EnterOpportunity();
                e.ts = rt.entryTs;
                e.side = rt.side;
                e.entryRef = rt.entryRef;
                e.diffInt = rt.diffInt;
                e.dif1 = rt.dif;
                e.dif2 = rt.prevDif;
                e.dea1 = rt.dea;
                e.dea2 = rt.prevDea;
                e.t1h = parseTrend1H(rt.t1h);
                VIEW_ENGINE.lastEnterOpp = e;

                // 让对齐比较使用“回放(HIST)”而不是推演，避免出现“实盘=00点、回测=22点30”的错觉
                AlignAudit a = new AlignAudit();
                a.src = "HIST/REPLAY";
                a.nowMs = nowMs;
                a.signalTs = rt.signalTs;
                a.entryTs = rt.entryTs;
                a.entryRefPx = rt.entryRef;
                a.k1OpenPx = rt.nextOpen;
                a.note = rt.note;
                LAST_HIST_AUDIT = a;
                saveHistAudit(a);

                // ✅ 与实盘一致：若开启“提醒即持仓”(MANUAL_VIRTUAL_POS_ON_ALERT)，回放/回看也立刻进入虚拟持仓态
                if (MANUAL_VIRTUAL_POS_ON_ALERT) {
                    long lastExitTs = -1L;
                    if (!VIEW_ENGINE.lastTrades.isEmpty()) {
                        CompletedTrade lastCt = VIEW_ENGINE.lastTrades.peekLast();
                        if (lastCt != null) lastExitTs = lastCt.exitTs;
                    }
                    boolean newerThanLastTrade = (lastExitTs < 0 || rt.entryTs > lastExitTs);
                    if (newerThanLastTrade) {
                        Position p = new Position();
                        p.side = rt.side;
                        p.entryTs = rt.entryTs;
                        p.signalTs = rt.signalTs;
                        // 入场价口径：优先 nextOpen（K1.open），拿不到就用 entryRef（K0.close）
                        p.entry = (rt.nextOpen > 0 ? rt.nextOpen : rt.entryRef);
                        p.isVirtual = true;
                        p.slPointsAtEntry = SL_POINTS;
                        p.slPoints = SL_POINTS;
                        VIEW_ENGINE.pos = p;
                        VIEW_ENGINE.lastProcessedSigTs = rt.signalTs;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[REPLAY] sync last trigger failed: " + e.getMessage());
        }

        // 兼容：让外部读取到最新“回放”资金
        equitySim = VIEW_ENGINE.equity;

        // 提示：当前 View 来自回放（避免误把 boot 推演输出当成“实盘事实”）
        if (VIEW_USE_REPLAY) {
            String le = (VIEW_ENGINE.lastEnterOpp == null ? "null"
                    : (fmtJst(VIEW_ENGINE.lastEnterOpp.ts) + " " + VIEW_ENGINE.lastEnterOpp.side));
            System.out.println("[REPLAY] synced: trades=" + VIEW_ENGINE.lastTrades.size() + " lastEnter=" + le);
        }
    }

    static void scanHistory(List<Candle> m30, List<MacdPoint> macd30, List<Candle> h2, List<MacdPoint> macd1h) {
        // 回看/推演窗口单独用更大的读库窗口，避免 lastTrade/opp 断档导致 HIST_AUDIT 缺失
        //（不影响实时：实时仍然用 ENGINE_BARS_* 的小窗口）
        if (SCAN_HISTORY_USE_LARGE_WINDOW && USE_DB_CACHE) {
            try {
                long nowMs = System.currentTimeMillis();
                List<Candle> m30Raw = CandleDb.loadLatest(INST_ID, "30m", Math.max(200, SCAN_BARS_30M), true);
                List<Candle> h2Raw  = CandleDb.loadLatest(INST_ID, "1H",  Math.max(200, H2_LOAD_BARS_1H),  true);

                // 只用“已收盘区”参与指标计算/推演
                List<Candle> m30Closed = filterClosedByTime(m30Raw, BAR30_MS, nowMs, SAFE_CLOSE_MS);
                List<Candle> h2Closed  = filterClosedByTime(h2Raw,  BAR1H_MS, nowMs, SAFE_CLOSE_MS);

                if (m30Closed != null && m30Closed.size() >= 60 && h2Closed != null && h2Closed.size() >= 60) {

                    // ===================== 关键修复：DB 不能覆盖更“新”的内存窗口 =====================
                    // 场景：拉取器与闹钟/回测若使用了不同工作目录，会导致 DB_FILE 指向不同的 ./okx_candles.db；
                    // 此时 DB 可能是“旧库”（比如只到 2026-01-30），而入参 m30/h2（来自实时链路）却已到 2026-02-12。
                    // 如果这里无脑用 DB 覆盖，就会造成“刷新后推演推不出 02-12 00:00”的假象。
                    long dbTailClose30 = m30Closed.get(m30Closed.size() - 1).ts + BAR30_MS;
                    long inTailClose30 = (m30 != null && !m30.isEmpty()) ? (m30.get(m30.size() - 1).ts + BAR30_MS) : -1L;
                    long expectedClose30 = alignLastClosed(nowMs, BAR30_MS, SAFE_CLOSE_MS);
                    boolean dbStaleVsNow = (expectedClose30 > 0 && dbTailClose30 > 0 && dbTailClose30 + 2*BAR30_MS <= expectedClose30);


                    // 允许 DB 比入参略新/略旧一根以内；超过则视为“DB 可能落后/跑错库”，优先用入参。
                    boolean dbLooksStale = dbStaleVsNow || (inTailClose30 > 0 && dbTailClose30 > 0 && (dbTailClose30 + BAR30_MS) < inTailClose30);

                    if (dbLooksStale) {
                        System.out.println("[SCAN][DB-STALE] (dbStaleVsNow=" + fmtTs(dbTailClose30)
                                + " < input tailClose=" + fmtTs(inTailClose30)
                                + " | DB_FILE=" + DB_FILE
                                + " | keep input window for re-derive");
                        // 保持入参 m30/h2（通常来自实时链路/内存），并重新计算 MACD，确保推演基于最新数据
                        macd30 = calcMacd(m30);
                        macd1h = calcMacd(h2);
                    } else {
                        m30 = m30Closed;
                        h2  = h2Closed;
                        // 使用与实时一致的 MACD 计算入口；输入已被 filterClosedByTime 限制为“已收盘区”
                        macd30 = calcMacd(m30);
                        macd1h = calcMacd(h2);
                    }
                }
            } catch (Throwable ignore) {
                // fallback: 使用调用方传入的窗口（不阻塞实盘）
            }
        }

        if (m30 == null || m30.size() < 50 || macd30 == null || macd30.size() < 50) return;

        // Route A：View 默认走“实盘回放”，确保【实盘没触发 → View/回放也不会有】
        if (VIEW_USE_REPLAY) {
            syncViewFromReplayDb();

            // ✅ replay 仅展示：scanHistory 仍必须跑“推演回测”（与实时口径对齐）
            // 这里强制跑 RESEARCH_ENGINE，不再受 RESEARCH_BACKTEST_PARALLEL 开关影响
            resetEngineForReDerive(RESEARCH_ENGINE);
            IN_SCAN_HISTORY = true;
            SCAN_TAIL_CLOSE_TS = calcTailCloseTs(m30);
            try {
                runBacktestToState(RESEARCH_ENGINE, m30, macd30, h2, macd1h);
            } finally {
                IN_SCAN_HISTORY = false;
            }

            // 对齐 RT：打印推演核心状态（回测口径）
            debugPrintPosCore("SCAN-END", RESEARCH_ENGINE);
            // 额外打印回放展示状态（来自 replay DB），方便对照
            debugPrintPosCore("SCAN-END-VIEW", VIEW_ENGINE);
            return;
        }

        // 研究/推演回测：仍然允许用同一套逻辑推 4 年（但它不是“实盘一致回放”）
        // ✅ 历史回看只写 VIEW_ENGINE（SSOT），绝不污染 LIVE_ENGINE
        resetEngineForReDerive(VIEW_ENGINE);
        IN_SCAN_HISTORY = true;
        SCAN_TAIL_CLOSE_TS = calcTailCloseTs(m30);
        try {
            runBacktestToState(VIEW_ENGINE, m30, macd30, h2, macd1h);
        } finally {
            IN_SCAN_HISTORY = false;
        }
        // 兼容：让外部（OkxPagingAlarm）读取到最新“回看推演”资金
        equitySim = VIEW_ENGINE.equity;
        debugPrintPosCore("SCAN-END", VIEW_ENGINE);
    }


    /**
     *  冲突保护专用：按“实盘进场过滤链”判断在同一时间(evalTs=K0收盘后的下一根开盘)是否也会触发进场。
     * 目的：当出现“出场 + 同一时间进场”时，决定是否继续持仓（避免同K反复）。
     *
     * 注意：
     * - 不打印任何过滤/拦截日志（silent），只返回 side / null
     * - 过滤链覆盖：亚洲盘过滤、1H过滤(neutral/first/second/方向)、状态机、质量过滤、手续费/波动门槛、止损冷却、OKX撑压入场差过滤
     */
    static Side computeEntrySideForConflictRealtime(
            EngineState eng,
            List<Candle> m30, List<MacdPoint> macd30,
            List<Candle> h2,  List<MacdPoint> macd1h,
            Trend1HInfo t1hInfo,
            List<Candle> m30Latest,
            int sigIdx
    ) {
        if (eng == null) eng = LIVE_ENGINE;
        if (m30 == null || macd30 == null || m30.isEmpty()) return null;
        if (sigIdx < 0 || sigIdx >= m30.size()) return null;
        Candle c0 = m30.get(sigIdx);
        long entryTs = c0.ts + BAR30_MS;

        Side side = entrySideByDifDea(m30, macd30, sigIdx);

        //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1；冲突保护链也一致）
        side = applyDecemberNoEntryGate(side, m30, sigIdx, entryTs, "实盘冲突", true);
        if (side == null) return null;

        // ---- Pool-2 投票机（与实盘一致；可选硬过滤） ----
        Pool2VoteResult pool2 = evalPool2Vote(m30, macd30, sigIdx, side, true, "REALTIME_CONFLICT");
        if (USE_POOL2_ENTRY_FILTER && side != null && pool2 != null && !pool2.matchSide(side)) return null;

        // 亚洲盘低流动性过滤（按入场时间）
        if (isAsiaLowLiquidityTime(entryTs)) return null;

        // 1H 过滤（t1hInfo 已按 entryTs 取值）
        if (USE_1H_FILTER) {
            if (t1hInfo == null) return null;
            if (t1hInfo.neutral) return null;
            if (t1hInfo.trend == Trend1H.中性) return null;
            if (FILTER_1H_NEUTRAL_FIRST_BAR && t1hInfo.isFirstNeutralBar) return null;
            if (FILTER_1H_SECOND_TREND_BAR && t1hInfo.isSecondTrendBarAfterNeutral) return null;
            if (side == Side.LONG  && t1hInfo.trend != Trend1H.多) return null;
            if (side == Side.SHORT && t1hInfo.trend != Trend1H.空) return null;
        }

        // 解法三：市场状态机过滤（趋势态才允许进场）
        if (USE_MARKET_REGIME_FILTER) {
            int h1Idx = -1;
            if (USE_1H_FILTER && t1hInfo != null) h1Idx = t1hInfo.idx;
                // BUG FIX: 与回测口径对齐。entryTs=c0.ts+BAR30_MS（K1开盘），使用 entryTs（K1开盘/入场边界时刻）对应当下可用的最后已收盘1H，避免超前到K1收盘造成回测/实盘分叉。
            else if (h2 != null && !h2.isEmpty()) h1Idx = regimeSafeH1Idx(h2, entryTs);

            // ✅硬校验：强制锚点 1H 必须在 entryTs 前已收盘，否则回退到安全 idx，并打印硬检查
            h1Idx = enforceRegimeAnchorH1Idx(h2, entryTs, h1Idx, "RT/CONFLICT_ENTRY@entryTs");

            debugPrintRegimePick1H("RT/CONFLICT_ENTRY@entryTs", entryTs, h2, h1Idx);
            MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
            if (regime == MarketRegime.RANGE) {
                if ((IN_SCAN_HISTORY || IN_BOOT_BACKTEST) && BT_PRINT_BLOCKED) {
                    System.out.printf(Locale.US,
                            "【回测-进场(被过滤:REGIME=RANGE)】 时间=%s | entryTs=%s | %s%n",
                            FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                            FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                            formatRegimeInfo(h2, h1Idx)
                    );
                }
                return null;
            }
        }

        // 质量过滤：检查信号K0（已收盘）
        if (USE_QUALITY_FILTER && !passQuality(m30, sigIdx)) return null;

        //  EMA 投票过滤（入场时间=下一根30m开盘 entryTs）
        if (USE_EMA_VOTE_FILTER) {
            EmaVoteContext ctx = (EMA_VOTE_CTX_REALTIME != null ? EMA_VOTE_CTX_REALTIME : EmaVoteContext.build(m30, h2));
            if (ctx != null) {
                Side s2 = applyEmaVoteFilter(side, entryTs, ctx, true, "实盘冲突");
                if (s2 == null) return null;
            }
        }

        // next-open：仅用于展示/对照，不再硬依赖（缺失时用 entryRef 代替展示）
        Candle entryC = findNextCandleAfter(m30Latest, c0.ts);
        double entryPx = c0.c;               // ★ 用 K0 收盘价
        double entryPxNextOpen = (entryC != null ? entryC.o : entryPx);   // 仅对照：下一根开盘价

        // 解法一：手续费/波动门槛（用信号K0及之前数据算ATR，无未来函数）
        if (USE_FEE_VOL_GUARD) {
            double atrSig = calcAtr(m30, sigIdx, FEE_VOL_ATR_N);
            if (!passFeeVolGuard(atrSig, entryPx)) return null;
        }


        // 解法一.5：slPts/entry 的分位数门槛（动态阈值，只用过去数据）
        if (USE_SL_PCT_Q_GATE) {
            SlPctGateResult g = evalSlPctQuantileGate(m30, sigIdx, entryPx);
            if (!g.pass) return null;
        }

        // 止损冷却（止损后等待N根K才允许进场）
        if (blockedByStoplossCooldown(entryTs, eng.stopCooldownUntilEntryTs)) return null;

        // OKX 撑压线入场限制：差 60 点以内不进（与实盘入场一致）
        SrLevels sr = calcOkxSrLevels(m30, sigIdx, OKX_SR_N);
        if (USE_OKX_SR_ENTRY_FILTER && sr.valid) {
            double dynSlPtsForGate = calcSlPointsForIndex(m30, sigIdx); // K0及之前（避免未来函数）
            double minGapPts = okxSrEntryMinGapPts(dynSlPtsForGate);
            double gap = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
            if (gap < minGapPts) return null;
        }


        // ===== TSRND 外层投票（用于冲突检查时，也保持一致：方向票不过则视为无信号）=====
        if (USE_TSRND_OUTER_VOTE && side != null) {
            OuterVoteDecision ov = evalTsRndOuterVote(m30, macd30, sigIdx, side, "冲突实时", true);
            if (ov.score < TSRND_SCORE_THRESHOLD) return null;
        }

        return side;
    }

    // ===================== 实时提醒（只看最后一根已收30m） =====================
    static
    EngineEvent checkRealtime(List<Candle> m30, List<MacdPoint> macd30,
                              List<Candle> h2,  List<MacdPoint> macd1h,
                              List<Candle> m30Latest) {

        int i = m30.size() - 1;
        Candle c0 = m30.get(i);

        long nowMsForAlign = System.currentTimeMillis();
        if (DEBUG_ALIGN) {
            alignPrintTail("RT-30m", nowMsForAlign, m30, BAR30_MS);
            alignPrintTail("RT-1H",  nowMsForAlign, h2,  BAR1H_MS);
        }



        // ===================== AUTO-TRADE 状态输出（每 30m 定时 runOnce 都会进来一次） =====================
        if (AUTO_TRADE_ENABLED) {
            long nowMs = System.currentTimeMillis();
            if (autoEntryPending) {
                double left = (autoEntryExpireAtMs - nowMs) / 1000.0;
                System.out.printf(Locale.US, "【OKX-AUTO】状态：入场挂单中... 剩余%.1fs%n", left);
            } else if (autoClosePending) {
                System.out.println("【OKX-AUTO】状态：平仓中...");
            } else if (LIVE_ENGINE.pos != null) {
                System.out.printf(Locale.US, "【OKX-AUTO】状态：持仓中 side=%s entry=%.2f slPts=%.2f notional=%.2f%n",
                        (LIVE_ENGINE.pos.side == Side.LONG ? "做多" : "做空"), LIVE_ENGINE.pos.entry, LIVE_ENGINE.pos.slPointsAtEntry, LIVE_ENGINE.pos.entryNotional);
            } else {
                System.out.println("【OKX-AUTO】状态：空仓");
            }
        }


        Trend1HInfo t1hInfo = new Trend1HInfo();
        t1hInfo.trend = Trend1H.中性;
        if (USE_1H_FILTER && !h2.isEmpty() && !macd1h.isEmpty()) {
            t1hInfo = get1HTrendInfoAt(h2, macd1h, c0.ts + 1800000L) /* 按下一根30m开盘时间取1H */;
        }

        if (LIVE_ENGINE.pos == null) {
            //  若上一根信号K刚触发止损，本轮禁止用同一根信号K再进场（手动重复运行也不会同K再入）
            if (LIVE_ENGINE.lastStopSigTs == c0.ts) {
                System.out.printf(Locale.US,
                        "【止损后禁止同K再进】信号时间=%s | 说明=上一根触发止损，本次放弃同K入场机会%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts))
                );
                return null;
            }

            //  若上一根信号K刚触发止盈，本轮禁止用同一根信号K再进场（手动重复运行也不会同K再入）
            if (LIVE_ENGINE.lastTpSigTs == c0.ts) {
                System.out.printf(Locale.US,
                        "【止盈后禁止同K再进】信号时间=%s | 说明=上一根触发止盈，本次放弃同K入场机会%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts))
                );
                return null;
            }

// v28：入场挂单期间禁止重复下单（不影响策略提醒）
            boolean autoCanSubmitEntry = true;
            if (AUTO_TRADE_ENABLED) {
                if (autoEntryPending || autoClosePending) {
                    autoCanSubmitEntry = false;
                }
                // entryTs 幂等：同一 entryTs 在有效期内不重复触发（避免刷新抖动/重启重复）
                long _nowMs = System.currentTimeMillis();
                if (autoEntryLockedEntryTs == (c0.ts + BAR30_MS) && _nowMs < autoEntryExpireAtMs) {
                    autoCanSubmitEntry = false;
                }
            }

//  信号只用“最后一根已收盘K”（索引 i）
            Side side = entrySideByDifDea(m30, macd30, i);


            //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1）
            side = applyDecemberNoEntryGate(side, m30, i, c0.ts + BAR30_MS, "实盘", false);
            // ---- Pool-2 投票机（并行，不改原逻辑；可选硬过滤） ----
            Pool2VoteResult pool2 = evalPool2Vote(m30, macd30, i, side, false, "REALTIME");
            if (USE_POOL2_ENTRY_FILTER && side != null && pool2 != null && !pool2.matchSide(side)) {
                // 实盘：硬过滤（方向不一致则禁止进场）
                side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "POOL2投票不一致(硬过滤): " + pool2.toOneLine());
            }
            //  新增：进场过滤（只影响进场）
            if (side != null && isAsiaLowLiquidityTime(c0.ts + 1800000L) /* 按入场时间 */) {
                side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "亚洲盘低流动性时段");
            }

            if (USE_1H_FILTER && side != null) {
                // 与回测一致：neutral 一律不进场
                if (t1hInfo.neutral) side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "1H 被判为 neutral（中性期）");
                if (t1hInfo.trend == Trend1H.中性) side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "1H 趋势=中性（不允许进场）");
                if (FILTER_1H_NEUTRAL_FIRST_BAR && t1hInfo.isFirstNeutralBar) side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "1H 进入中性期的第一根（禁止开仓）");

                // 第二根趋势K禁止：中性→趋势后的第2根趋势K（且方向一致）不允许开仓（保持你原输出）
                if (FILTER_1H_SECOND_TREND_BAR && t1hInfo.isSecondTrendBarAfterNeutral) {
                    System.out.printf("[BLOCK 2ND TREND] ts=%s t1h=%s idx=%d hist=%.4f prev=%.4f%n",
                            Instant.ofEpochMilli(c0.ts + BAR30_MS).atZone(ZONE),
                            t1hInfo.trend, t1hInfo.idx, t1hInfo.hist, t1hInfo.prevHist);
                    side = null;
                }

                if (side == Side.LONG && t1hInfo.trend != Trend1H.多) side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "1H 方向不匹配（要求=多）");
                if (side == Side.SHORT && t1hInfo.trend != Trend1H.空) side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "1H 方向不匹配（要求=空）");
            }

            //  解法三：市场状态机过滤（趋势态才允许趋势系统进场）
            if (side != null && USE_MARKET_REGIME_FILTER) {
                int h1Idx = -1;
                if (USE_1H_FILTER) h1Idx = t1hInfo.idx;
                    // BUG FIX: 与回测口径对齐。
                    // 回测/实盘统一：regime 只能使用 <= entryTs 已收盘的数据。
                    // entryTs=K1开盘=c0.ts+BAR30_MS，因此这里使用 c0.ts+BAR30_MS（不再超前到K1收盘）。
                else if (h2 != null && !h2.isEmpty()) h1Idx = regimeSafeH1Idx(h2, c0.ts + BAR30_MS);

                // ✅硬校验：强制锚点 1H 必须在 entryTs 前已收盘，否则回退到安全 idx，并打印硬检查
                h1Idx = enforceRegimeAnchorH1Idx(h2, c0.ts + BAR30_MS, h1Idx, "RT/ENTRY@c0.ts+30m");

                debugPrintRegimePick1H("RT/ENTRY@c0.ts+30m", c0.ts + BAR30_MS, h2, h1Idx);

                MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
                if (regime == MarketRegime.RANGE) {
                    side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "状态机=RANGE（低波动震荡期/弱势月份剥离）");
                }
            }

            //  新增：质量过滤（与回测一致，且复用你原输出模板，不改）

            // ✅ CROSS-CHECK：实时侧算出的 side 必须与回测口径一致
            // 规则：实时算出 side 后，用同一份数据走回测口径 computeEntrySideAtIndex(...) 再确认；
            // 若不一致：打印差异 + K-USED 窗口，并且 side 直接服从回测结果（回测 null => 实时不响铃）
            Side btSide = computeEntrySideAtIndex(m30, macd30, h2, macd1h, i, true);
            if (btSide != side) {
                System.out.println(String.format(Locale.US,
                        "[CROSS-CHECK][DIFF] rt=%s bt=%s | sigIdx=%d entryIdx=%d | entryTs=%s | k0Close=%.2f",
                        String.valueOf(side), String.valueOf(btSide),
                        i, (i + 1),
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                        c0.c));

                Candle k1Used = null;
                int k1Idx = i + 1;
                if (m30 != null && k1Idx >= 0 && k1Idx < m30.size()) k1Used = m30.get(k1Idx);
                boolean usedFallback = (k1Used == null);
                printKUsedEntry("RT", "CROSS", c0.ts + BAR30_MS, m30, i, k1Used, c0.c, usedFallback, macd30);

                side = btSide; // ✅ 服从回测口径
            }
            side = btSide; // ✅ 即使 rtSide=null，也要服从回测口径

            if (side != null) {
                Candle k0Candle = m30.get(i); // K0（本次进场用的收盘K）
                if (USE_QUALITY_FILTER && !passQuality(m30, i)) {
                    double range = k0Candle.h - k0Candle.l;
                    double body = Math.abs(k0Candle.c - k0Candle.o);
                    double bodyRatio = (range <= 0 ? 0 : body / range);

                    double minRangeNeed = getMinRangeNeed(m30, i);
                   /* System.out.printf(
                            "【质量过滤拦截】%n" +
                                    "时间：%s%n" +
                                    "方向：%s%n" +
                                    "实体比例：%.2f %s 最低要求 %.2f%n" +
                                    "波动范围：%.2f %s 最低要求 %.2f%n" +
                                    "→ 本次信号已过滤%n%n",
                            FMT_JST.format(Instant.ofEpochMilli(m30.get(i).ts)),
                            side,
                            bodyRatio,
                            (bodyRatio >= MIN_BODY_RATIO ? "≥" : "<"),
                            MIN_BODY_RATIO,
                            range,
                            (range >= minRangeNeed ? "≥" : "<"),
                            minRangeNeed
                    );*/

                    side = null;
                }
            }            //  EMA 投票过滤（入场时间=下一根30m开盘）
            if (side != null && USE_EMA_VOTE_FILTER && EMA_VOTE_CTX_REALTIME != null) {
                side = applyEmaVoteFilter(side, c0.ts + BAR30_MS, EMA_VOTE_CTX_REALTIME, false, "实盘");
            }

            // ===================== AUTO-ENTRY 计划打印（开局 & 每次定时刷新都打印） =====================
            // 目的：你能在控制台直接看到：open/方向/挂单价/notional/止损，不需要等真正下单才打印。
            // 注意：这里是“计划预览”，后面仍可能被 fee/冷却/撑压/仓位检查 等拦截。
            if (PRINT_AUTO_ENTRY_PLAN_EACH_REFRESH) {
                long sigTs = c0.ts + BAR30_MS; // 入场对齐时间=下一根30m开盘
                if (side == null) {
                    System.out.printf(Locale.US,
                            "【AUTO-ENTRY计划】%s | 本轮无入场信号（或已被过滤）%n",
                            FMT_JST.format(Instant.ofEpochMilli(sigTs)));
                } else {
                    Candle k0 = m30.get(i);
                    Candle entryC = findNextCandleAfter(m30Latest, k0.ts);
                    double entryPx = k0.c;            // ★ 用 K0 收盘价
                    double entryPxNextOpen = (entryC != null ? entryC.o : entryPx); // 仅对照：下一根开盘价（缺失则用 entryRef 展示）

                    double slPts = calcSlPointsForIndex(m30, i);

                    // 外层投票（用于 notional 的 voteScale；与你原逻辑一致；silent=true 避免刷屏）
                    OuterVoteDecision ov2 = evalTsRndOuterVote(m30, macd30, i, side, "实时", true);
                    double tierMult = SRMF.getCurrentMult(LIVE_ENGINE.srmf);
                    double entryMult = tierMult * ov2.voteScale;

                    // 风控票（R1~R4）：只用信号K0已收盘K线（i），避免未来函数
                    RiskVoteResult rvRisk = null;
                    if (USE_RISK_VOTE) {
                        Candle __sigC0 = (i >= 0 && i < m30.size()) ? m30.get(i) : null;
                        double __refPx = (__sigC0 != null ? __sigC0.c : entryPx);
                        RiskVoteResult __rv = evalRiskVote(m30, h2, i, side, __refPx, tierMult, ov2.voteScale, true, "实时-预览");
                        rvRisk = __rv;
                        if (__rv != null) {
                            if (__rv.r1Veto) {
                                entryMult = 0.0; // 直接否决：预览阶段展示为“0名义”
                            } else {
                                entryMult = __rv.multAfter;
                            }
                        }
                    }

                    // 入场名义：优先使用 -Dokx.testNotional>0 的固定模式；否则默认按 SRMF 风险反推名义

                    // ===== BAYES：多特征朴素贝叶斯（预览：只影响 entryMult/名义展示，不改其它链路）=====
                    double bayesPosterior = 0.5;
                    double bayesScale = 1.0;
                    if (USE_BAYESIAN_FILTER && side != null) {
                        double __emaScore = 0.0;
                        try {
                            if (EMA_VOTE_CTX_REALTIME != null) {
                                EmaVoteSnapshot __es = EMA_VOTE_CTX_REALTIME.snapshotAt(sigTs);
                                if (__es != null) __emaScore = __es.score;
                            }
                        } catch (Throwable ignore) {}
                        double __atr = 0.0;
                        try { __atr = calcAtr(m30, i, ATR_N); } catch (Throwable ignore) {}
                        double __srGapPts = 0.0;
                        try {
                            SrLevels __sr = calcOkxSrLevels(m30, i, OKX_SR_N);
                            if (__sr != null && __sr.valid) {
                                __srGapPts = (side == Side.LONG) ? (__sr.resistance - entryPx) : (entryPx - __sr.support);
                            }
                        } catch (Throwable ignore) {}
                        BayesEvalResult __bd = evalBayesForEntry(LIVE_ENGINE, m30, macd30, macd1h, t1hInfo, i, side, pool2, ov2,
                                sigTs, __emaScore, __atr, entryPx, __srGapPts,
                                true, "实时-预览");
                        if (__bd != null) {
                            bayesPosterior = __bd.posterior;
                            bayesScale = __bd.scale;
                            if (bayesScale <= 0.0) entryMult = 0.0;
                            else entryMult *= bayesScale;
                        }
                    }

                    // ===== MACD动量仓位动态调整（实盘预览）=====
                    if (USE_MACD_POS_SIZING) {
                        MacdPoint __m0rt = (i >= 0 && i < macd30.size()) ? macd30.get(i) : null;
                        MacdPoint __m1rt = (i >= 1 && i-1 < macd30.size()) ? macd30.get(i-1) : null;
                        if (__m0rt != null) {
                            if (__m0rt.hist > MACD_BOOST_THRESHOLD) {
                                entryMult *= MACD_BOOST_SCALE;
                            } else if (__m1rt != null) {
                                int __di = (side == Side.LONG)
                                        ? (int)(__m0rt.dif - __m1rt.dif)
                                        : (int)(__m0rt.dea - __m1rt.dea);
                                if (__di < 0) entryMult *= MACD_REDUCE_SCALE;
                            }
                        }
                    }

                    NotionalDecision nd = calcEntryNotional(entryMult, entryPx, slPts);
                    double notionalUsdt = nd.notionalUsdt;
                    // ===== 记录“最终mult/名义/自动下单传参”快照（用于 SRMF 下单参数区块打印核对） =====
                    try {
                        int __rv = 0;
                        double __cut = 1.0;
                        String __rvDetail = "";
                        if (rvRisk != null) {
                            __rv = rvRisk.riskVote;
                            if (rvRisk.multBefore > 1e-12) __cut = rvRisk.multAfter / rvRisk.multBefore;
                            __rvDetail = rvRisk.detail;
                        }
                        double __eqUsed = (LIVE_ENGINE != null && LIVE_ENGINE.equity > 0.0) ? LIVE_ENGINE.equity : capital;
                        double __baseRisk = __eqUsed * RISK_PCT;

                        LAST_NOTIONAL_DEBUG.src = "实时";
                        LAST_NOTIONAL_DEBUG.side = side;
                        LAST_NOTIONAL_DEBUG.sigTs = (m30 != null && i >= 0 && i < m30.size() && m30.get(i) != null) ? m30.get(i).ts : (sigTs - BAR30_MS);
                        LAST_NOTIONAL_DEBUG.entryTs = sigTs;

                        LAST_NOTIONAL_DEBUG.tierMult = tierMult;
                        LAST_NOTIONAL_DEBUG.voteScore = ov2.score;
                        LAST_NOTIONAL_DEBUG.voteScale = ov2.voteScale;
                        LAST_NOTIONAL_DEBUG.riskVote = __rv;
                        LAST_NOTIONAL_DEBUG.riskCut = __cut;
                        LAST_NOTIONAL_DEBUG.entryMultFinal = entryMult;

                        LAST_NOTIONAL_DEBUG.ndMode = (nd != null ? nd.mode : "?");
                        LAST_NOTIONAL_DEBUG.baseNotional = (nd != null ? nd.baseNotional : 0.0);
                        LAST_NOTIONAL_DEBUG.equityUsed = __eqUsed;
                        LAST_NOTIONAL_DEBUG.baseRisk = __baseRisk;
                        LAST_NOTIONAL_DEBUG.entryPx = entryPx;
                        LAST_NOTIONAL_DEBUG.slPts = slPts;

                        LAST_NOTIONAL_DEBUG.computedNotional = notionalUsdt;
                        // 初始先记为计算值；真正提交前会再覆盖一次
                        LAST_NOTIONAL_DEBUG.sentToAutoNotional = notionalUsdt;
                        LAST_NOTIONAL_DEBUG.autoTradeEnabled = AUTO_TRADE_ENABLED;
                        LAST_NOTIONAL_DEBUG.autoCanSubmitEntry = autoCanSubmitEntry;
                        LAST_NOTIONAL_DEBUG.detail = (__rvDetail == null ? "" : __rvDetail);
                    } catch (Exception ignore) {}

                    double rawLimit;
                    if (ENTRY_USE_REF_OFFSET) {
                        rawLimit = (side == Side.LONG) ? (entryPx - ENTRY_REF_OFFSET) : (entryPx + ENTRY_REF_OFFSET);
                    } else {
                        rawLimit = (side == Side.LONG) ? (entryPx - ENTRY_LEGACY_OFFSET) : (entryPx + ENTRY_LEGACY_OFFSET);
                    }
                    double slTriggerPx = (side == Side.LONG) ? (rawLimit - slPts) : (rawLimit + slPts);
                    long expireAtMs = System.currentTimeMillis() + 15L * 60L * 1000L;

                    printAutoEntryPlan(sigTs, side, entryPx, rawLimit, notionalUsdt, slPts, slTriggerPx,
                            entryMult, tierMult, ov2.voteScale, expireAtMs, nd);
                    printNotionalCheckLine("计划预览");
                }
            }
            if (side != null) {
                Candle k0 = m30.get(i); // 信号K0（已收盘）
                MacdPoint m0 = macd30.get(i);
                MacdPoint m1 = macd30.get(i - 1);

                //  入场对齐：entryTs 直接用 K0 收盘时间；next-open 仅用于展示/对照（缺失则用 entryRef 展示）
                long entryTs = k0.ts + BAR30_MS;
                Candle entryC = findNextCandleAfter(m30Latest, k0.ts);
                double entryPx = k0.c;             // ★ 用 K0 收盘价
                double entryPxNextOpen = (entryC != null ? entryC.o : entryPx); // 仅对照：下一根开盘价

                //  解法一：手续费/波动门槛（不够覆盖往返手续费则不提示进场）
                if (USE_FEE_VOL_GUARD) {
                    double atrSigLive = calcAtr(m30, i, FEE_VOL_ATR_N);
                    if (!passFeeVolGuard(atrSigLive, entryPx)) {
                        side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "手续费/波动不足：" + feeVolGuardReason(atrSigLive, entryPx));
                        return null;
                    }
                }



                // 解法一.5：slPts/entry 的分位数门槛（动态阈值，只用过去数据）
                if (USE_SL_PCT_Q_GATE) {
                    SlPctGateResult g = evalSlPctQuantileGate(m30, i, entryPx);
                    if (!g.pass) {
                        String reason = String.format(Locale.US,
                                "SL%%过高：slPts=%.2f entry=%.2f slPct=%.5f > q%.0f=%.5f (lb=%d n=%d)",
                                g.slPts, entryPx, g.slPct,
                                (SL_PCT_Q * 100.0), g.qVal, SL_PCT_Q_LOOKBACK_BARS, g.sampleN
                        );
                        side = blockEntry(side, entryTs, "实盘", reason);
                        return null;
                    }
                }
                // =====================  止损冷却：止损后等待 N 根K 再允许开仓 =====================
                if (blockedByStoplossCooldown(entryTs, LIVE_ENGINE.stopCooldownUntilEntryTs)) {
                    if (PRINT_STOPLOSS_COOLDOWN_BLOCK) {
                        long remainBars = Math.max(0, (LIVE_ENGINE.stopCooldownUntilEntryTs - entryTs + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【止损冷却拦截-实盘】候选入场=%s | cooldownUntil=%s | remainBars=%d → 不进场%n",
                                FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                                FMT_JST.format(Instant.ofEpochMilli(LIVE_ENGINE.stopCooldownUntilEntryTs)),
                                remainBars
                        );
                    }
                    return null;
                }

                // =====================  止盈冷却：止盈后等待 N 根K 再允许开仓 =====================
                if (blockedByTakeprofitCooldown(entryTs, LIVE_ENGINE.tpCooldownUntilEntryTs)) {
                    if (PRINT_TAKEPROFIT_COOLDOWN_BLOCK) {
                        long remainBars = Math.max(0, (LIVE_ENGINE.tpCooldownUntilEntryTs - entryTs + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【止盈冷却拦截-实盘】候选入场=%s | cooldownUntil=%s | remainBars=%d → 不进场%n",
                                FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                                FMT_JST.format(Instant.ofEpochMilli(LIVE_ENGINE.tpCooldownUntilEntryTs)),
                                remainBars
                        );
                    }
                    return null;
                }


                // =====================  R1 冷却硬门（回测/实盘同构） =====================
                if (side != null && blockedByR1VetoCooldown(LIVE_ENGINE, entryTs, side)) {
                    if (RV_PRINT_BLOCK) {
                        long until = getR1CooldownUntilTs(LIVE_ENGINE, side);
                        long lastVeto = getR1VetoTs(LIVE_ENGINE, side);
                        long remainBars = Math.max(0, (until - entryTs + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【R1冷却】%s 剩余%d根K，不进场 | now=%s | lastVeto=%s | until=%s%n",
                                (side == Side.LONG ? "LONG" : "SHORT"),
                                remainBars,
                                FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                                (lastVeto > 0 ? FMT_JST.format(Instant.ofEpochMilli(lastVeto)) : "-"),
                                (until > 0 ? FMT_JST.format(Instant.ofEpochMilli(until)) : "-")
                        );
                    }
                    return null;
                }

                // =====================  OKX 撑压线入场限制：差 60 点以内不进 =====================
                SrLevels sr = calcOkxSrLevels(m30, i, OKX_SR_N);
                if (USE_OKX_SR_ENTRY_FILTER && sr.valid) {
                    double dynSlPtsForGate = calcSlPointsForIndex(m30, i); // K0及之前（避免未来函数）
                    double minGapPts = okxSrEntryMinGapPts(dynSlPtsForGate);
                    double gap = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                    if (gap < minGapPts) {
                        System.out.printf(Locale.US,
                                "【OKX撑压过滤拦截】%n时间：%s%n方向：%s%n参考入场：%.2f（下一根开盘）%nResistance=%.2f | Support=%.2f | gap=%.2f < %.2f → 不进场%n%n",
                                FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                                (side == Side.LONG ? "做多" : "做空"),
                                entryPx,
                                sr.resistance, sr.support,
                                gap, minGapPts
                        );
                        return null;
                    }
                }

                // ===== R1 硬否决：反向长影线出现 => 直接不进场（必须在任何“锁定/响铃/自动下单/虚拟持仓”之前） =====
                if (side != null && USE_RISK_VOTE && RV_R1_VETO) {
                    Candle __sigC0 = (i >= 0 && i < m30.size()) ? m30.get(i) : null; // 信号K0（已收盘）
                    if (__sigC0 != null && isReverseLongWick(__sigC0, side)) {
                        markR1Veto(LIVE_ENGINE, c0.ts + BAR30_MS, side);
                        if (RV_PRINT_BLOCK) {
                            System.out.printf(Locale.US,
                                    "【R1反向长影线-否决】ts=%s | %s → 不进场%n",
                                    FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)), side);
                        }
                        return null;
                    }
                }




                // ===================== 统一计算：SRMF mult / notional / Bayes（AUTO + 手动共用） =====================
                // 说明：
                //  - 这些变量会在后面的 AUTO 下单 / 手动响铃 / SRMF 入场锁定 中复用
                //  - 默认 USE_BAYESIAN_FILTER=false，不破坏原行为；开启后是否“否决”由 okx.bayesMode 决定（BOOST 不否决）
                double slPts = calcSlPointsForIndex(m30, i);

                // 外层投票：用于 voteScale（silent=true 避免刷屏；结果与 silent=false 等价）
                OuterVoteDecision ov2 = evalTsRndOuterVote(m30, macd30, i, side, "实时", true);
                double tierMult = SRMF.getCurrentMult(LIVE_ENGINE.srmf);
                double entryMult = tierMult * ov2.voteScale;

                // 风控票（R1~R4）：只用信号K0已收盘K线（i），避免未来函数
                RiskVoteResult rvRisk = null;
                if (USE_RISK_VOTE) {
                    Candle __sigC0 = (i >= 0 && i < m30.size()) ? m30.get(i) : null;
                    double __refPx = (__sigC0 != null ? __sigC0.c : entryPx);
                    rvRisk = evalRiskVote(m30, h2, i, side, __refPx, tierMult, ov2.voteScale, false, "实时");
                    if (rvRisk != null) {
                        if (rvRisk.r1Veto) {
                            markR1Veto(LIVE_ENGINE, c0.ts + BAR30_MS, side);
                            // 与 AUTO 路径一致：R1 否决时直接 return，避免后续响铃/计划预览造成误解
                            if (RV_PRINT_BLOCK) {
                                System.out.printf(Locale.US,
                                        "【R1反向长影线-否决】ts=%s | %s → 不进场 | %s%n",
                                        FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)), side, rvRisk.detail);
                            }
                            return null;
                        } else {
                            entryMult = rvRisk.multAfter;
                        }
                    }
                }

                // ===== BAYES：多特征朴素贝叶斯（可否决 + 仓位缩放；与回测同构）=====
                double bayesPosterior = 0.5;
                double bayesScale = 1.0;
                Map<MultiBinBayesFilter.Feature, Integer> bayesBins = null;
                EnumMap<MultiBinBayesFilter.Feature, Double> bayesRaw = null;
                if (USE_BAYESIAN_FILTER && side != null) {
                    double __emaScore = 0.0;
                    try {
                        if (EMA_VOTE_CTX_REALTIME != null) {
                            EmaVoteSnapshot __es = EMA_VOTE_CTX_REALTIME.snapshotAt(entryTs);
                            if (__es != null) __emaScore = __es.score;
                        }
                    } catch (Throwable ignore) {}
                    double __atr = 0.0;
                    try { __atr = calcAtr(m30, i, ATR_N); } catch (Throwable ignore) {}
                    double __srGapPts = 0.0;
                    try {
                        if (sr != null && sr.valid) {
                            __srGapPts = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                        }
                    } catch (Throwable ignore) {}

                    BayesEvalResult __bd = evalBayesForEntry(LIVE_ENGINE, m30, macd30, macd1h, t1hInfo, i, side, pool2, ov2,
                            entryTs, __emaScore, __atr, entryPx, __srGapPts,
                            false, "实时");
                    if (__bd != null) {
                        bayesPosterior = __bd.posterior;
                        bayesScale = __bd.scale;
                        bayesBins = __bd.bins;
                        bayesRaw = __bd.raw;
                        if (bayesScale <= 0.0) {
                            if (BAYES_VERBOSE) {
                                System.out.printf(Locale.US, "【BAYES拦截】时间=%s | side=%s | posterior=%.4f < thr=%.4f → 不进场%n",
                                        FMT_JST.format(Instant.ofEpochMilli(entryTs)),
                                        side,
                                        bayesPosterior,
                                        ("SCALE".equalsIgnoreCase(BAYES_MODE) ? BAYES_MIN_POSTERIOR : BAYES_GATE_POSTERIOR)
                                );
                            }
                            return null;
                        }
                        entryMult *= bayesScale;
                    }
                }

                // ===== MACD动量仓位动态调整（实盘下单，与回测同构）=====
                if (USE_MACD_POS_SIZING) {
                    MacdPoint __m0rt = (i >= 0 && i < macd30.size()) ? macd30.get(i) : null;
                    MacdPoint __m1rt = (i >= 1 && i-1 < macd30.size()) ? macd30.get(i-1) : null;
                    if (__m0rt != null) {
                        if (__m0rt.hist > MACD_BOOST_THRESHOLD) {
                            entryMult *= MACD_BOOST_SCALE;
                            System.out.printf(Locale.US,
                                    "[MACD-BOOST][实盘] ts=%s MACD_HIST=%.2f>%.1f → 加仓×%.1f%n",
                                    FMT_JST.format(Instant.ofEpochMilli(entryTs)), __m0rt.hist, MACD_BOOST_THRESHOLD, MACD_BOOST_SCALE);
                        } else if (__m1rt != null) {
                            int __di = (side == Side.LONG)
                                    ? (int)(__m0rt.dif - __m1rt.dif)
                                    : (int)(__m0rt.dea - __m1rt.dea);
                            if (__di < 0) {
                                entryMult *= MACD_REDUCE_SCALE;
                                System.out.printf(Locale.US,
                                        "[MACD-REDUCE][实盘] ts=%s diffInt=%d<0 → 减仓×%.1f%n",
                                        FMT_JST.format(Instant.ofEpochMilli(entryTs)), __di, MACD_REDUCE_SCALE);
                            }
                        }
                    }
                }

                // 入场名义：优先使用 -Dokx.testNotional>0 的固定模式；否则默认按 SRMF 风险反推名义
                NotionalDecision nd = calcEntryNotional(entryMult, entryPx, slPts);
                double notionalUsdt = nd.notionalUsdt;

// ===================== AUTO-TRADE 入场（post-only 单挂；取消/偏离可自动撤单重挂，细节由 OkxAutoTradeBridge 参数控制） =====================
                if (AUTO_TRADE_ENABLED) {
                    long nowMs = System.currentTimeMillis();
                    // 若正在平仓/已在挂入场：不重复下单，但本轮仍然走策略提醒/回测口径
                    if (autoClosePending) {
                        System.out.println("【OKX-AUTO】当前正在平仓中：本轮仍会提醒，但不提交入场单");
                        autoCanSubmitEntry = false;
                    }
                    if (autoEntryPending) {
                        long left = autoEntryExpireAtMs - nowMs;
                        System.out.printf(Locale.US, "【OKX-AUTO】入场挂单进行中... 剩余%.1fs | 本轮仍会提醒，但不重复提交下单%n", left / 1000.0);
                        autoCanSubmitEntry = false;
                    }
                    // slPts/ov2/tierMult/entryMult/bayes/nd/notionalUsdt 已在上方“共用区块”计算

                    // 打印 1~5（每次 30m 定时拉K线触发信号时都会输出）
                    double rawLimit;
                    if (ENTRY_USE_REF_OFFSET) {
                        rawLimit = (side == Side.LONG) ? (entryPx - ENTRY_REF_OFFSET) : (entryPx + ENTRY_REF_OFFSET);
                    } else {
                        rawLimit = (side == Side.LONG) ? (entryPx - ENTRY_LEGACY_OFFSET) : (entryPx + ENTRY_LEGACY_OFFSET);
                    }
                    double slTriggerPx = (side == Side.LONG) ? (rawLimit - slPts) : (rawLimit + slPts);
                    long expireAtMs = nowMs + 15L * 60L * 1000L;
                    printAutoEntryPlan(c0.ts + BAR30_MS, side, entryPx, rawLimit, notionalUsdt, slPts, slTriggerPx,
                            entryMult, tierMult, ov2.voteScale, expireAtMs, nd);
                    printNotionalCheckLine("入场计划");

                    // 入场前再查一次实盘仓位（更快反馈；桥接层也会再查一次）
                    try {
                        OkxAutoTradeBridge.PositionSnap ps = AUTO.getPositionSnapPublic(INST_ID);
                        if (ps != null && ps.hasPos && ps.sz > 0) {
                            System.out.println("【OKX-AUTO】ENTRY skip：检测到已有仓位 sz=" + ps.sz);
                            return null;
                        }
                    } catch (Exception ignore) {}


                    // ===================== 修复：AUTO 模式也必须响铃 + 文字提示 =====================
                    // 之前 AUTO_TRADE_ENABLED=true 时这里直接 return，导致“有机会但不响铃/无文字提示”。
                    // 现在：只要触发入场信号（且本轮确实准备尝试开仓），先输出提醒并同步 LIVE_ENGINE.lastEnterOpp，再去走自动下单。
                    Position sigPos = new Position();
                    sigPos.side = side;
                    sigPos.entry = entryPx;
                    sigPos.signalTs = (m30 != null && i >= 0 && i < m30.size() && m30.get(i) != null) ? m30.get(i).ts : (entryTs - BAR30_MS);

                    sigPos.entryTs = entryTs;
                    sigPos.slPoints = slPts;
                    sigPos.slPointsAtEntry = slPts;
                    sigPos.entryEquity = capital;
                    sigPos.mfe = 0.0;
                    sigPos.mae = 0.0;
                    sigPos.entryVoteScore = ov2.score;
                    sigPos.entryVoteScale = ov2.voteScale;
                    sigPos.entryVoteDirPass = ov2.dirPass;
                    sigPos.entryVoteOverallPass = ov2.overallPass;
                    sigPos.entryVoteDetail = ov2.detail;
                    sigPos.entryTierMult = tierMult;
                    sigPos.entryMult = entryMult;
                    sigPos.bayesPosterior = bayesPosterior;
                    sigPos.bayesScale = bayesScale;
                    sigPos.regimeAtEntry = SRMF.getRegimeName(LIVE_ENGINE.srmf);
                    sigPos.trendPtsAtEntry = SRMF.getTrendPts(LIVE_ENGINE.srmf);
                    sigPos.entryNotional = notionalUsdt;
                    if (sr.valid) {
                        sigPos.okxResistanceAtEntry = sr.resistance;
                        sigPos.okxSupportAtEntry = sr.support;
                        sigPos.okxGapAtEntry = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                    }
                    System.out.println("【SRMF 入场锁定-信号】方向=" + (sigPos.side == Side.LONG ? "做多" : "做空")
                            + "｜入场=" + String.format(Locale.US, "%.2f", sigPos.entry)
                            + "｜止损点数=" + String.format(Locale.US, "%.2f", sigPos.slPoints)
                            + "｜档位模式=" + SRMF.regimeCn(sigPos.regimeAtEntry)
                            + "｜趋势点数(pts)=" + String.format(Locale.US, "%.2f", sigPos.trendPtsAtEntry)
                            + "｜mult(档位)=" + String.format(Locale.US, "%.2f*%.1f=%.2f", sigPos.entryTierMult, sigPos.entryVoteScale, sigPos.entryMult)
                            + "｜票=" + sigPos.entryVoteScore);
                    printEnterAlertSummary(sigPos, m30.get(i), t1hInfo, m0, m1, getMinRangeNeed(m30, i));
                    Candle sigC0 = m30.get(i);
                    AlignAudit a = alignMake("RT", nowMsForAlign, sigC0, entryC, entryPx, sigPos.signalTs, "RT enter-trigger");
                    if (!rtLockEntryAlert(a)) {
                        // 去重/防抖：不响铃、不覆盖最近一次机会
                        return null;
                    }

                    long enterAlarmKeyTs = c0.ts + BAR30_MS;

                    int diffInt = (side == Side.LONG) ? (int)(m0.dif - m1.dif) : (int)(m0.dea - m1.dea);

                    LIVE_ENGINE.lastEnterOpp = new EnterOpportunity();
                    LIVE_ENGINE.lastEnterOpp.ts = entryTs; // next-open（入场K开盘）
                    LIVE_ENGINE.lastEnterOpp.side = side;
                    LIVE_ENGINE.lastEnterOpp.t1h = (USE_1H_FILTER ? t1hInfo.trend : Trend1H.中性);
                    LIVE_ENGINE.lastEnterOpp.entryRef = entryPx;
                    LIVE_ENGINE.lastEnterOpp.dif1 = m0.dif;
                    LIVE_ENGINE.lastEnterOpp.dif2 = m1.dif;
                    LIVE_ENGINE.lastEnterOpp.dea1 = m0.dea;
                    LIVE_ENGINE.lastEnterOpp.dea2 = m1.dea;
                    // v30-A：持久化“实盘触发记录”（即闹钟/文字提示的那一刻）
                    String t1hStrForAudit = "OFF";
                    if (USE_1H_FILTER) {
                        t1hStrForAudit = t1hInfo.trend
                                + (t1hInfo.isFirstNeutralBar ? "(NEUTRAL_FIRST)" : "")
                                + String.format(Locale.US, " hist=%.4f prev=%.4f", t1hInfo.hist, t1hInfo.prevHist);
                    }
                    persistRtTriggerIfEnabled(nowMsForAlign, sigC0, entryTs, side, entryPx, entryPxNextOpen,
                            m0, m1, diffInt, t1hStrForAudit, "RT entry alert");
                    persistLiveState("rt_entry_alert");
                    LIVE_ENGINE.lastEnterOpp.diffInt = diffInt;

                    LIVE_ENGINE.lastEnterOpp.pbMode = PULLBACK_MODE.name();
                    LIVE_ENGINE.lastEnterOpp.pbEnabled = ENABLE_PULLBACK_ENTRY_BACKTEST;
                    LIVE_ENGINE.lastEnterOpp.pbPending = true;
                    double atrSigLiveOpp = calcAtr(m30, i, ATR_N);
                    LIVE_ENGINE.lastEnterOpp.atrAtSignal = atrSigLiveOpp;
                    LIVE_ENGINE.lastEnterOpp.pbAtrPts = atrSigLiveOpp * PULLBACK_ATR_MULT;
                    LIVE_ENGINE.lastEnterOpp.pbFixedPts = PULLBACK_FIXED_POINTS;
                    LIVE_ENGINE.lastEnterOpp.pbUsedPts = (PULLBACK_MODE == PullbackMode.ATR ? LIVE_ENGINE.lastEnterOpp.pbAtrPts : LIVE_ENGINE.lastEnterOpp.pbFixedPts);
                    LIVE_ENGINE.lastEnterOpp.pbLimitPx = (side == Side.LONG) ? (entryPx - LIVE_ENGINE.lastEnterOpp.pbUsedPts) : (entryPx + LIVE_ENGINE.lastEnterOpp.pbUsedPts);
                    LIVE_ENGINE.lastEnterOpp.pbFilled = false;
                    LIVE_ENGINE.lastEnterOpp.entryFillPx = 0.0;

                    // ===== AUTO-TRADE：仅执行，不影响策略状态/提醒 =====
                    if (AUTO_TRADE_ENABLED && autoCanSubmitEntry) {
                        autoEntryPending = true;
                        autoEntryExpireAtMs = expireAtMs;
                        autoEntryLockedEntryTs = entryTs;
                        Side finalSide = side;
                        double finalNotional = notionalUsdt;
                        try {
                            LAST_NOTIONAL_DEBUG.sentToAutoNotional = finalNotional;
                            LAST_NOTIONAL_DEBUG.autoCanSubmitEntry = autoCanSubmitEntry;
                        } catch (Exception ignore) {}
                        printNotionalCheckLine("提交前");
                        double finalEntryPx = entryPx;
                        double finalSlPts = slPts;
                        autoEntryFuture = AUTO_ES.submit(() -> {
                            try {
                                OkxAutoTradeBridge.OpenResult r = AUTO.tryOpenNetIsolatedUntilExpireWithSL(
                                        INST_ID, finalSide, finalNotional, finalEntryPx, finalSlPts, expireAtMs, 0L, entryTs
                                );
                                if (r != null && r.ok && r.filled) {
                                    double avg = (r.avgPx > 0 ? r.avgPx : finalEntryPx);
                                    System.out.printf(Locale.US, "【OKX-AUTO】ENTRY filled ✅ avgPx=%.2f ordId=%s%n", avg, r.ordId);
                                } else {
                                    System.out.println("【OKX-AUTO】ENTRY not filled / canceled: " + (r == null ? "null" : r.msg));
                                }
                            } catch (Exception e) {
                                System.out.println("【OKX-AUTO】ENTRY exception: " + e.getMessage());
                                e.printStackTrace();
                            } finally {
                                autoEntryPending = false;
                                autoEntryOrdId = null;
                            }
                        });
                    }

                }

                LIVE_ENGINE.pos = new Position();
                LIVE_ENGINE.pos.isVirtual = MANUAL_VIRTUAL_POS_ON_ALERT; // 策略A：响铃即视为已进场
                LIVE_ENGINE.pos.side = side;
                LIVE_ENGINE.pos.entry = entryPx;
                LIVE_ENGINE.pos.signalTs = (m30 != null && i >= 0 && i < m30.size() && m30.get(i) != null) ? m30.get(i).ts : (entryTs - BAR30_MS);

                LIVE_ENGINE.pos.entryTs = entryTs;
                LIVE_ENGINE.pos.slPoints = calcSlPointsForIndex(m30, i);
                LIVE_ENGINE.pos.entryEquity = capital; //  实盘仅提醒：用当前 capital 作为参考资金（避免打印为0）
                LIVE_ENGINE.pos.slPointsAtEntry = LIVE_ENGINE.pos.slPoints; //  锁定入场止损点数                //  入场时锁定 OKX 撑压线（用于止盈/复盘）
                if (sr.valid) {
                    LIVE_ENGINE.pos.okxResistanceAtEntry = sr.resistance;
                    LIVE_ENGINE.pos.okxSupportAtEntry = sr.support;
                    LIVE_ENGINE.pos.okxGapAtEntry = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                }
                LIVE_ENGINE.pos.mfe = 0.0;
                LIVE_ENGINE.pos.mae = 0.0;
                LIVE_ENGINE.pos.bayesPosterior = bayesPosterior;
                LIVE_ENGINE.pos.bayesScale = bayesScale;
                if (USE_BAYESIAN_FILTER && bayesBins != null) {
                    LIVE_ENGINE.currentBayesSnap = new MultiBinBayesFilter.BayesEntrySnapshot(entryTs, bayesBins, bayesRaw);
                } else {
                    LIVE_ENGINE.currentBayesSnap = null;
                }
                //  SRMF 入场锁定（用于复盘，避免之后状态变化）
                OuterVoteDecision ov = evalTsRndOuterVote(m30, macd30, i, side, "实时", false);
                if (USE_TSRND_OUTER_VOTE && ov.score < TSRND_SCORE_THRESHOLD) {
                    System.out.println("【TSRND 外层投票拦截】" + ov.detail);
                    // 修复：拦截时必须清理刚创建的持仓状态（避免“无效持仓”导致后续链路错乱）
                    LIVE_ENGINE.pos = null;
                    LIVE_ENGINE.currentBayesSnap = null;
                    return null;
                }
                LIVE_ENGINE.pos.entryVoteScore = ov.score;
                LIVE_ENGINE.pos.entryVoteScale = ov.voteScale;
                LIVE_ENGINE.pos.entryVoteDirPass = ov.dirPass;
                LIVE_ENGINE.pos.entryVoteOverallPass = ov.overallPass;
                LIVE_ENGINE.pos.entryVoteDetail = ov.detail;
                LIVE_ENGINE.pos.entryTierMult = SRMF.getCurrentMult(LIVE_ENGINE.srmf);
                LIVE_ENGINE.pos.entryMult = entryMult;
                LIVE_ENGINE.pos.entryNotional = notionalUsdt;
                LIVE_ENGINE.pos.regimeAtEntry = SRMF.getRegimeName(LIVE_ENGINE.srmf);
                LIVE_ENGINE.pos.trendPtsAtEntry = SRMF.getTrendPts(LIVE_ENGINE.srmf);
                //  放这里：触发进场信号时输出“入场锁定”的 SRMF 状态（最准确）
                System.out.println("【SRMF 入场锁定】方向=" + (LIVE_ENGINE.pos.side == Side.LONG ? "做多" : "做空")
                        + "｜入场=" + String.format(Locale.US, "%.2f", LIVE_ENGINE.pos.entry)
                        + "｜止损点数=" + String.format(Locale.US, "%.2f", LIVE_ENGINE.pos.slPoints)
                        + "｜档位模式=" + SRMF.regimeCn(LIVE_ENGINE.pos.regimeAtEntry)
                        + "｜趋势点数(pts)=" + String.format(Locale.US, "%.2f", LIVE_ENGINE.pos.trendPtsAtEntry)
                        + "｜mult(档位)=" + String.format(Locale.US, "%.2f*%.1f=%.2f", LIVE_ENGINE.pos.entryTierMult, LIVE_ENGINE.pos.entryVoteScale, LIVE_ENGINE.pos.entryMult)
                        + "｜票=" + LIVE_ENGINE.pos.entryVoteScore);
                //  只新增摘要（不改你原输出）
                printEnterAlertSummary(LIVE_ENGINE.pos, m30.get(i), t1hInfo, macd30.get(i), macd30.get(i - 1), getMinRangeNeed(m30, i));

                long enterAlarmKeyTs = c0.ts + BAR30_MS;

                int diffInt = (side == Side.LONG) ? (int)(m0.dif - m1.dif) : (int)(m0.dea - m1.dea);

                // ===== 修复：实时触发进场时，同步更新“最近一次机会（回看）” =====
                // scanHistory 只能使用已收K线；当信号来自“最后一根已收K”时，回看拿不到 next-open（未收K开盘），会导致 LAST ENTER 落后一根。
                // 这里在实时触发进场提醒时，用 next-open 直接写入 LIVE_ENGINE.lastEnterOpp，保证“响铃/文字提示”与“最近一次机会”一致。
                LIVE_ENGINE.lastEnterOpp = new EnterOpportunity();
                LIVE_ENGINE.lastEnterOpp.ts = entryTs; // next-open（入场K开盘）
                LIVE_ENGINE.lastEnterOpp.side = side;
                LIVE_ENGINE.lastEnterOpp.t1h = (USE_1H_FILTER ? t1hInfo.trend : Trend1H.中性);
                LIVE_ENGINE.lastEnterOpp.entryRef = entryPx;

                LIVE_ENGINE.lastEnterOpp.dif1 = m0.dif;
                LIVE_ENGINE.lastEnterOpp.dif2 = m1.dif;
                LIVE_ENGINE.lastEnterOpp.dea1 = m0.dea;
                LIVE_ENGINE.lastEnterOpp.dea2 = m1.dea;
                LIVE_ENGINE.lastEnterOpp.diffInt = diffInt;

                // 回撤入场：实盘此时“下一根K未收盘”，无法判断是否触达限价；标记为 Pending（待确认）
                LIVE_ENGINE.lastEnterOpp.pbMode = PULLBACK_MODE.name();
                LIVE_ENGINE.lastEnterOpp.pbEnabled = ENABLE_PULLBACK_ENTRY_BACKTEST;
                LIVE_ENGINE.lastEnterOpp.pbPending = true;
                double atrSigLiveOpp = calcAtr(m30, i, ATR_N);
                LIVE_ENGINE.lastEnterOpp.atrAtSignal = atrSigLiveOpp;
                LIVE_ENGINE.lastEnterOpp.pbAtrPts = atrSigLiveOpp * PULLBACK_ATR_MULT;
                LIVE_ENGINE.lastEnterOpp.pbFixedPts = PULLBACK_FIXED_POINTS;
                LIVE_ENGINE.lastEnterOpp.pbUsedPts = (PULLBACK_MODE == PullbackMode.ATR ? LIVE_ENGINE.lastEnterOpp.pbAtrPts : LIVE_ENGINE.lastEnterOpp.pbFixedPts);
                LIVE_ENGINE.lastEnterOpp.pbLimitPx = (side == Side.LONG) ? (entryPx - LIVE_ENGINE.lastEnterOpp.pbUsedPts) : (entryPx + LIVE_ENGINE.lastEnterOpp.pbUsedPts);
                LIVE_ENGINE.lastEnterOpp.pbFilled = false;
                LIVE_ENGINE.lastEnterOpp.entryFillPx = 0.0;


// ===== K-USED：打印本次入场实际用到的K（K0/P0/K1）及窗口 =====
                Candle k1Used = null;
                if (m30 != null) {
                    int k1Idx = i + 1;
                    if (k1Idx >= 0 && k1Idx < m30.size()) {
                        Candle c = m30.get(k1Idx);
                        if (c != null && c.ts == entryTs) k1Used = c;
                    }
                }
                boolean usedFallback = (k1Used == null);
                printKUsedEntry("RT", "ENTER", c0.ts + BAR30_MS, m30, i, k1Used, LIVE_ENGINE.pos.entry, usedFallback, macd30);

                StringBuilder sbEnter = new StringBuilder();
                sbEnter.append(String.format(Locale.US,
                        "【进场提醒】 时间=%s | 方向=%s | 1H=%s | 参考入场(K0收盘)=%.2f | nextOpen(未收K开盘)=%.2f | dif(本根/前一)=%.6f/%.6f | dea(本根/前一)=%.6f/%.6f | 动量diffInt=%d%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                        (side == Side.LONG ? "做多" : "做空"),
                        (USE_1H_FILTER ? (t1hInfo.trend + (t1hInfo.isFirstNeutralBar ? "(NEUTRAL_FIRST)" : "")
                                + String.format(Locale.US, "(hist=%.4f prev=%.4f)", t1hInfo.hist, t1hInfo.prevHist)) : "OFF"),
                        LIVE_ENGINE.pos.entry,
                        entryPxNextOpen,
                        m0.dif, m1.dif, m0.dea, m1.dea,
                        diffInt
                ));
                if (ENABLE_PULLBACK_ENTRY_LIVE_PRINT) {
                    double atrSigLive = calcAtr(m30, i, ATR_N);
                    double pbAtrPtsLive = atrSigLive * PULLBACK_ATR_MULT;
                    double pbUsedPtsLive = (PULLBACK_MODE == PullbackMode.ATR ? pbAtrPtsLive : PULLBACK_FIXED_POINTS);
                    double pbLimitPxLive = (side == Side.LONG) ? (entryPx - pbUsedPtsLive) : (entryPx + pbUsedPtsLive);
                    sbEnter.append(String.format(Locale.US,
                            "【回撤入场参考】mode=%s | ATR(30m,%d)=%.2f | ATR回撤=%.2f(x%.2f) | 固定回撤=%.2f | 使用回撤=%.2f | limit=%.2f%n",
                            PULLBACK_MODE, ATR_N,
                            atrSigLive,
                            pbAtrPtsLive, PULLBACK_ATR_MULT,
                            PULLBACK_FIXED_POINTS,
                            pbUsedPtsLive,
                            pbLimitPxLive
                    ));
                }
                EngineEvent enterEv = EngineEvent.enter(enterAlarmKeyTs, sbEnter.toString());
                return enterEv;

            }
        }

        if (LIVE_ENGINE.pos != null) {
            //  关键修复：如果本轮刚用“下一根开盘价”入场（entryTs=下一根K开盘），
            // 则不能用“信号K0(c0)”的高低点去判断止损/止盈（这些高低点发生在入场之前）。
            // 必须等到下一根已收K（即 c0.ts >= entryTs）再开始判断出场。
            if (c0.ts < LIVE_ENGINE.pos.entryTs) return null;

            updateMfeMae(LIVE_ENGINE.pos, c0);

            boolean stop = hitStopLoss(LIVE_ENGINE.pos, c0);
            boolean tp2  = (!stop) && hitTP2(LIVE_ENGINE.pos, c0);

            //  OKX 撑压线止盈：多→压力线；空→支撑线（入场时已锁定）
            double okxTpPx = (LIVE_ENGINE.pos.side == Side.LONG) ? LIVE_ENGINE.pos.okxResistanceAtEntry : LIVE_ENGINE.pos.okxSupportAtEntry;
            boolean okxSrTp = (!stop) && USE_OKX_SR_TP && hitOkxSrTP(LIVE_ENGINE.pos, c0, okxTpPx);

            boolean volRollExit = (!stop) && (!tp2) && (!okxSrTp) && hitVolRollExit(LIVE_ENGINE.pos, m30, i);
            boolean exitCombo7 = (!stop) && (!tp2) && (!okxSrTp) && (!volRollExit) && ExitCombo7Miner.shouldExit(m30, i, LIVE_ENGINE.pos.side, "实盘");
            boolean exitCombo2 = (!stop) && (!tp2) && (!okxSrTp) && (!volRollExit) && (!exitCombo7) && ExitCombo2Miner.shouldExit(m30, i, LIVE_ENGINE.pos.side, "实盘");
            boolean macdRev = (!stop && !tp2 && !okxSrTp && !volRollExit && !exitCombo7 && !exitCombo2) && shouldExitByMacdWithGuards(m30, macd30, i, LIVE_ENGINE.pos, c0.c, "实盘", c0.ts + BAR30_MS);
//  冲突保护：同一时间出现“出场 + 同一时间进场”时，保持持仓不处理（避免同K反复）
            // 说明：对“TP全平/OKX撑压止盈/反转出场”(tp2 / okxSrTp / macdRev)启用；止损仍然优先执行。
            if (!stop && (tp2 || okxSrTp || volRollExit || exitCombo7 || exitCombo2 || macdRev)) {
                long evalTs = c0.ts + BAR30_MS;
                // ★ 修复：改用与回测（runBacktestToState）完全相同的 computeEntrySideAtIndex(i+1)，
                //   保证实盘继续持仓判断与回测结果 100% 一致，避免回测决定继续持仓但实盘仍响铃出场的分叉。
                // [BUG-FIX-2026-06] 实参由 i 修正为 i+1：i=m30.size()-1 时 i+1=m30.size() → 走虚拟下一根开盘，
                //   sigIdx=i（刚收盘的信号K，即造成出场那根），与回测两处继续持仓判断完全对齐。
                Side btNextSide = computeEntrySideAtIndex(m30, macd30, h2, macd1h, i + 1, true);
                if (btNextSide != null) {
                    double baseRisk = LIVE_ENGINE.pos.entryEquity * RISK_PCT;
                    double riskAmt  = baseRisk * LIVE_ENGINE.pos.entryMult;
                    double notional = riskAmt * LIVE_ENGINE.pos.entry / Math.max(1e-9, LIVE_ENGINE.pos.slPointsAtEntry);
                    String exitReason = okxSrTp ? (LIVE_ENGINE.pos.side == Side.LONG ? "OKX压力线止盈" : "OKX支撑线止盈")
                            : (tp2 ? "TP全平" : (volRollExit ? "放量滚动防守"
                            : (exitCombo7 ? "EXIT_COMBO7" : (exitCombo2 ? "EXIT_COMBO2" : "MACD颜色反转"))));
                    System.out.println();
                    System.out.printf(Locale.US,
                            "【继续持仓】时间=%s｜回测决策=不出场｜出场信号=%s｜下一根进场方向=%s｜与回测保持一致，不出场不响铃%n",
                            FMT_JST.format(Instant.ofEpochMilli(evalTs)),
                            exitReason,
                            (btNextSide == Side.LONG ? "做多" : "做空")
                    );
                    System.out.printf(Locale.US,
                            "当前仓位：方向=%s｜入场时间=%s｜入场价=%.2f｜止损点=%.0f｜倍数=%.2f(*%.1f)｜风险=%.2f USDT｜名义=%.0f USDT｜regime=%s｜trendPts=%.2f%n",
                            (LIVE_ENGINE.pos.side == Side.LONG ? "做多" : "做空"),
                            FMT_JST.format(Instant.ofEpochMilli(LIVE_ENGINE.pos.entryTs)),
                            LIVE_ENGINE.pos.entry,
                            LIVE_ENGINE.pos.slPointsAtEntry,
                            LIVE_ENGINE.pos.entryMult,
                            LIVE_ENGINE.pos.entryVoteScale,
                            riskAmt,
                            notional,
                            LIVE_ENGINE.pos.regimeAtEntry,
                            LIVE_ENGINE.pos.trendPtsAtEntry
                    );
                    System.out.printf(Locale.US,
                            "保持持仓：不给出场提示，不响铃，不自动下单出场，不更新持仓状态/最近一次/最近十次机会%n"
                    );
                    System.out.println();
                    return null;  // 不发出场事件 → 无出场提醒、无闹钟、无自动下单
                }
            }


            if (stop || tp2 || okxSrTp || volRollExit || exitCombo7 || exitCombo2 || macdRev) {

                // ===================== AUTO-TRADE 出场（vm=makerOffset，post-only 轮询30s，超时市价吃单） =====================
                if (AUTO_TRADE_ENABLED) {
                    long nowMs = System.currentTimeMillis();
                    boolean autoCanSubmitClose = true;
                    if (autoClosePending) {
                        System.out.println("【OKX-AUTO】平仓任务进行中... 本轮仍会提示/更新策略，但不重复提交平仓单");
                        autoCanSubmitClose = false;
                    }

                    // vm 参数：你现在用 -Dokx.makerOffset 控制
                    double vm = sysDouble("okx.makerOffset", 0.03);
                    long ttlMs = sysLong("okx.makerTtlMs", 30_000L);

                    // 打印出场原因（便于你观察）
                    String reason = stop ? "止损" : (tp2 ? "TP2止盈" : (okxSrTp ? "OKX撑压止盈" : (volRollExit ? "放量滚动防守" : (exitCombo7 ? "EXIT_COMBO7" : (exitCombo2 ? "EXIT_COMBO2" : "MACD反转")))));
                    System.out.printf(Locale.US,
                            "【AUTO-EXIT信号】time=%s | reason=%s | side=%s | vm(makerOffset)=%.6f | ttl=%dms%n",
                            FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                            reason,
                            (LIVE_ENGINE.pos.side == Side.LONG ? "做多" : "做空"),
                            vm, ttlMs
                    );


                    // ===================== 修复：AUTO 模式也必须响铃 + 文字提示（出场） =====================
                    // 同时同步 LIVE_ENGINE.lastExitOpp，避免出现“响了但最近一次机会没更新”的错觉。
                    if (stop) {
                        LIVE_ENGINE.lastStopSigTs = c0.ts; //  记录止损信号K
                        long stopExitTs = c0.ts + BAR30_MS;
                        LIVE_ENGINE.stopCooldownUntilEntryTs = calcCooldownUntilEntryTsAfterStop(stopExitTs);
                    }
                    if (!stop && (tp2 || okxSrTp || volRollExit || exitCombo7 || exitCombo2 || macdRev)) {
                        LIVE_ENGINE.lastTpSigTs = c0.ts; //  记录止盈信号K
                        long tpExitTs = c0.ts + BAR30_MS;
                        LIVE_ENGINE.tpCooldownUntilEntryTs = calcCooldownUntilEntryTsAfterTakeProfit(tpExitTs);
                    }

                    double exitRef;
                    String reasonDetail;

                    if (stop) {
                        exitRef = stopFillPrice(LIVE_ENGINE.pos);
                        reasonDetail = "止损触发（SL=" + (int)Math.round(LIVE_ENGINE.pos.slPoints) + "点）";
                    } else if (tp2 || okxSrTp) {
                        double tp2Px = tp2FillPrice(LIVE_ENGINE.pos);
                        double srPx  = okxTpPx;

                        if (tp2 && okxSrTp) {
                            boolean chooseTp2;
                            if (LIVE_ENGINE.pos.side == Side.LONG) chooseTp2 = (tp2Px <= srPx);
                            else                           chooseTp2 = (tp2Px >= srPx);

                            if (chooseTp2) {
                                exitRef = tp2Px;
                                reasonDetail = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                            } else {
                                exitRef = c0.c;
                                reasonDetail = (LIVE_ENGINE.pos.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                            }
                        } else if (okxSrTp) {
                            exitRef = c0.c;
                            reasonDetail = (LIVE_ENGINE.pos.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                        } else {
                            exitRef = tp2Px;
                            reasonDetail = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                        }
                    } else if (volRollExit) {
                        exitRef = c0.c;
                        reasonDetail = "放量滚动防守出场（N=" + VOLROLL_N + ", Vmult=" + VOLROLL_VMULT + ", lossGate=" + (VOLROLL_LOSS_GATE?"是":"否") + "）";
                    } else if (exitCombo7) {
                        exitRef = c0.c;
                        reasonDetail = ExitCombo7Miner.REASON_DETAIL;
                    } else if (exitCombo2) {
                        exitRef = c0.c;
                        reasonDetail = ExitCombo2Miner.REASON_DETAIL;
                    } else {
                        exitRef = c0.c;
                        reasonDetail = "MACD颜色反转（strength比较）=> 当前收盘出";
                    }

                    double remainPnl = (LIVE_ENGINE.pos.side == Side.LONG)
                            ? (exitRef - LIVE_ENGINE.pos.entry)
                            : (LIVE_ENGINE.pos.entry - exitRef);
                    double feePts = okxFeePoints(LIVE_ENGINE.pos.entry, exitRef);
                    double finalPnl = remainPnl - feePts;

                    LIVE_ENGINE.lastExitOpp = new ExitOpportunity();
                    LIVE_ENGINE.lastExitOpp.ts = c0.ts + BAR30_MS;
                    LIVE_ENGINE.lastExitOpp.side = LIVE_ENGINE.pos.side;
                    LIVE_ENGINE.lastExitOpp.price = exitRef;
                    LIVE_ENGINE.lastExitOpp.reason = reasonDetail;

                    printExitAlertSummary(LIVE_ENGINE.pos, c0, exitRef, remainPnl, feePts, finalPnl, reasonDetail);
                    long exitAlarmKeyTs = c0.ts + BAR30_MS + 123L;
                    StringBuilder sbExit = new StringBuilder();
                    sbExit.append(String.format(Locale.US,
                            "【出场提醒】 时间=%s | 方向=%s | 参考出场=%.2f | 毛=%+.2f点 | 费=%.2f点 | 净=%+.2f点 | MFE=%.2f MAE=%.2f | 原因=%s%n",
                            FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                            (LIVE_ENGINE.pos.side == Side.LONG ? "做多" : "做空"),
                            exitRef, remainPnl, feePts, finalPnl,
                            LIVE_ENGINE.pos.mfe, LIVE_ENGINE.pos.mae,
                            reasonDetail
                    ));
                    EngineEvent exitEv = EngineEvent.exit(exitAlarmKeyTs, sbExit.toString());

                    // ===== AUTO-TRADE：仅执行，不影响策略状态/回测/提醒 =====
                    if (autoCanSubmitClose) {
                        autoClosePending = true;
                        Side closeSide = LIVE_ENGINE.pos.side;
                        boolean stopFinal = stop;
                        autoCloseFuture = AUTO_ES.submit(() -> {
                            try {
                                AUTO.tryCloseNetIsolated(
                                        INST_ID, closeSide, stopFinal,
                                        vm, ttlMs, 0, 0L, true
                                );
                                System.out.println("【OKX-AUTO】EXIT done ✅");
                            } catch (Exception e) {
                                System.out.println("【OKX-AUTO】EXIT exception: " + e.getMessage());
                                e.printStackTrace();
                            } finally {
                                autoClosePending = false;
                            }
                        });
                    }

                }


                if (stop) {
                    LIVE_ENGINE.lastStopSigTs = c0.ts; //  记录止损信号K
                    //  止损冷却：从本次止损的 exitTs 开始，等待 N 根30m K 再允许进场
                    long stopExitTs = c0.ts + BAR30_MS;
                    LIVE_ENGINE.stopCooldownUntilEntryTs = calcCooldownUntilEntryTsAfterStop(stopExitTs);
                }

                //  止盈冷却：TP 全平 / OKX 撑压止盈后，等待 N 根30m K 再允许进场（实盘/闹钟）
                if (!stop && (tp2 || okxSrTp || volRollExit || exitCombo7 || exitCombo2 || macdRev)) {
                    LIVE_ENGINE.lastTpSigTs = c0.ts; //  记录止盈信号K
                    long tpExitTs = c0.ts + BAR30_MS;
                    LIVE_ENGINE.tpCooldownUntilEntryTs = calcCooldownUntilEntryTsAfterTakeProfit(tpExitTs);
                }
                double exitRef;
                String reason;

                if (stop) {
                    exitRef = stopFillPrice(LIVE_ENGINE.pos);
                    reason = "止损触发（SL=" + (int)Math.round(LIVE_ENGINE.pos.slPoints) + "点）";
                } else if (tp2 || okxSrTp) {
                    //  多个止盈同时触发：LONG 取更小（更先触发）；SHORT 取更大（更先触发）
                    double tp2Px = tp2FillPrice(LIVE_ENGINE.pos);
                    double srPx  = okxTpPx;

                    if (tp2 && okxSrTp) {
                        //  多个止盈同时触发：仍按“更先触发”的近似规则选择“哪一个原因”，
                        // 但若选择的是 OKX 撑压线止盈，则【出场价/出场时间】按当前K线收盘确认（避免盘中/盘尾冲突）。
                        boolean chooseTp2;
                        if (LIVE_ENGINE.pos.side == Side.LONG) chooseTp2 = (tp2Px <= srPx);  // LONG：更小价更先触发
                        else                           chooseTp2 = (tp2Px >= srPx);  // SHORT：更大价更先触发

                        if (chooseTp2) {
                            exitRef = tp2Px;
                            reason = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                        } else {
                            exitRef = c0.c; //  SR 仍按盘中触价判定，但“以收盘价出”
                            reason = (LIVE_ENGINE.pos.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                        }
                    } else if (okxSrTp) {
                        exitRef = c0.c; //  SR 仍按盘中触价判定，但“以收盘价出”
                        reason = (LIVE_ENGINE.pos.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                    } else {
                        exitRef = tp2Px;
                        reason = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                    }
                } else if (volRollExit) {
                    exitRef = c0.c;
                    reason = "放量滚动防守出场（N=" + VOLROLL_N + ", Vmult=" + VOLROLL_VMULT + ", lossGate=" + (VOLROLL_LOSS_GATE?"是":"否") + ")";
                } else if (exitCombo7) {
                    exitRef = c0.c;
                    reason = ExitCombo7Miner.REASON_DETAIL;
                } else if (exitCombo2) {
                    exitRef = c0.c;
                    reason = ExitCombo2Miner.REASON_DETAIL;
                } else {
                    // macdRev
                    exitRef = c0.c;
                    reason = "MACD颜色反转（strength比较）=> 当前收盘出";
                }
                double remainPnl = (LIVE_ENGINE.pos.side == Side.LONG)
                        ? (exitRef - LIVE_ENGINE.pos.entry)
                        : (LIVE_ENGINE.pos.entry - exitRef) ;
                double feePts = okxFeePoints(LIVE_ENGINE.pos.entry, exitRef);
                double finalPnl = remainPnl - feePts;


                // 同步“最近一次出场机会”（避免出现响了但最近一次还停留在历史回看）
                LIVE_ENGINE.lastExitOpp = new ExitOpportunity();
                LIVE_ENGINE.lastExitOpp.ts = c0.ts + BAR30_MS;
                LIVE_ENGINE.lastExitOpp.side = LIVE_ENGINE.pos.side;
                LIVE_ENGINE.lastExitOpp.price = exitRef;
                LIVE_ENGINE.lastExitOpp.reason = reason;

                //  只新增摘要（不改你原输出）
                printExitAlertSummary(LIVE_ENGINE.pos, c0, exitRef, remainPnl, feePts, finalPnl, reason);
                //  exit 用不同key，避免同一根K内进/出场时闹钟被去重
                long exitAlarmKeyTs = c0.ts + BAR30_MS + 123L;
                StringBuilder sbExit = new StringBuilder();
                sbExit.append(String.format(Locale.US,
                        "【出场提醒】 时间=%s | 方向=%s | 参考出场=%.2f | 毛=%+.2f点 | 费=%.2f点 | 净=%+.2f点 | MFE=%.2f MAE=%.2f | 原因=%s%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                        (LIVE_ENGINE.pos.side == Side.LONG ? "做多" : "做空"),
                        exitRef, remainPnl, feePts, finalPnl,
                        LIVE_ENGINE.pos.mfe, LIVE_ENGINE.pos.mae,
                        reason
                ));
                EngineEvent exitEv = EngineEvent.exit(exitAlarmKeyTs, sbExit.toString());

                // 记录到“最近十次/最近一次机会”视图（避免：止损后这笔机会在历史里找不到）
                CompletedTrade rt = new CompletedTrade();
                rt.signalTs = LIVE_ENGINE.pos.signalTs;
                rt.enterTs = LIVE_ENGINE.pos.entryTs;
                rt.exitTs = c0.ts + BAR30_MS;
                rt.side = LIVE_ENGINE.pos.side;
                rt.entry = LIVE_ENGINE.pos.entry;
                rt.exit = exitRef;
                rt.grossPnlPoints = remainPnl;
                rt.feePoints = feePts;
                rt.pnlPoints = finalPnl;
                rt.isStop = stop;
                rt.reason = reason;
                rt.slPointsUsed = LIVE_ENGINE.pos.slPoints;
                rt.mult = LIVE_ENGINE.pos.entryMult;
                rt.regime = LIVE_ENGINE.pos.regimeAtEntry;
                // Bug3修复：补齐 mfe/mae/exitEquity/pnlUSDT/rMultiple，避免复盘视图数据为0
                rt.mfe = LIVE_ENGINE.pos.mfe;
                rt.mae = LIVE_ENGINE.pos.mae;
                rt.entryEquity = LIVE_ENGINE.pos.entryEquity;
                double rt_riskAmt = LIVE_ENGINE.pos.entryEquity * RISK_PCT * LIVE_ENGINE.pos.entryMult;
                rt.pnlUSDT = finalPnl * rt_riskAmt / Math.max(1e-9, LIVE_ENGINE.pos.slPointsAtEntry);
                LIVE_ENGINE.equity += rt.pnlUSDT;
                rt.exitEquity = LIVE_ENGINE.equity;
                rt.rMultiple = finalPnl / Math.max(1e-9, LIVE_ENGINE.pos.slPointsAtEntry);
                recordClosedTradeForView(rt);

                LIVE_ENGINE.pos = null;
                persistLiveState("LIVE_ENGINE.pos=null");
                return exitEv;
            }
        }


        return null;
    }

    // ===================== 输出：启动状态 =====================
    static void printBootStatusOnce() {
        if (printedBoot) return;
        printedBoot = true;

        System.out.println("=== OKX 指标闹钟（JST）===");
        System.out.println("交易对：" + INST_ID);
        System.out.println("周期：30m（主）" + (USE_1H_FILTER ? " + 1H（方向过滤）" : "（1H过滤已关闭）"));
        System.out.println("入场：dif/dea >=2（仅两种触发） | 进场价=下一根开盘(next-open)");
        System.out.println("固定止损：" + (int)SL_POINTS + " 点");
        System.out.println("止损冷却：" + (USE_STOPLOSS_COOLDOWN ? ("ON（" + STOPLOSS_COOLDOWN_BARS + " 根30m）") : "OFF"));
        System.out.println("止盈冷却：" + (USE_TAKEPROFIT_COOLDOWN ? ("ON（" + TAKEPROFIT_COOLDOWN_BARS + " 根30m）") : "OFF"));
        System.out.println("出场优先级：SL > TP全平 > OKX撑压止盈/MACD反转");
        System.out.println("MACD颜色反转：strength=2*(|dif|+|dea|)，多:cur<prev / 空:cur>prev，触发按当前收盘出");
        System.out.println("日志增强：每笔 trade 打印 MFE/MAE");
        System.out.printf(Locale.US, "风险：%.1f%%（R=本金×%.1f%%）%n", RISK_PCT*100, RISK_PCT*100);
        System.out.printf(Locale.US, "本金(capital)：%.2f%n", capital);
        if (APPLY_OKX_FEE) {
            System.out.printf(Locale.US, "手续费：OKX 合约双边=%.4f%%（单边=%.4f%%，以点数扣：feePts=rate*(|entry|+|exit|)）%n", OKX_FEE_RATE_PER_SIDE*2*100, OKX_FEE_RATE_PER_SIDE*100);
        } else {
            System.out.println("手续费：OFF");
        }
        System.out.printf("刷新时间：每小时 :00:%02d 和 :30:%02d（收盘后确认）%n", CHECK_SECOND, CHECK_SECOND);
        System.out.println("模式：" + (AUTO_TRADE_ENABLED ? "AUTO下单(挂单/平仓)" : "只提醒，不下单"));
        System.out.println("进场过滤：亚洲盘前半=" + FILTER_ASIA_LOW_LIQ + "（JST " + NO_TRADE_1_START + ":00-" + NO_TRADE_1_END + ":00）"
                +"（JST " + NO_TRADE_2_START + ":00-" + NO_TRADE_2_END + ":00）"+ "；1H中性第一根=" + FILTER_1H_NEUTRAL_FIRST_BAR + "（X=" + NEUTRAL_ABS_HIST_X + ", N=" + NEUTRAL_OSC_N + "）");
        System.out.println("进场过滤：仅美股开盘时段=" + US_SESSION_ONLY + "（09:30-16:00 美东，周一~周五；-Dokx.usSessionOnly=true 开启）");
        System.out.println("--------------------------------------");
    }

    static void printRefreshHeader(long nowMs, Candle last30, Position pos) {
        // 往 SRMF 系统里注入参数（收盘确认：这里的 last30 必须是 confirm=1 的已收K）
        if (last30 != null) lastPriceCache = last30.c;
        System.out.println();
        System.out.println("---------------- 刷新 ----------------");
        System.out.println("本地时间(JST)：" + FMT_JST.format(Instant.ofEpochMilli(nowMs)));

        if (last30 != null) {
            long closeTs = last30.ts + BAR30_MS;

            //  统一用“收盘时间”对齐展示（now=12:07 → 对应K线显示12:00）
            System.out.println("对应K线(30m-收盘JST)：" + FMT_JST.format(Instant.ofEpochMilli(closeTs)) + " confirm=" + last30.confirm);
            System.out.println("对应K线(30m-开盘JST)：" + FMT_JST.format(Instant.ofEpochMilli(last30.ts)));
            System.out.printf(Locale.US, "当前价格(last)：%.2f%n", last30.c);
        } else {
            System.out.println("对应K线(30m)：(暂无)");
        }

        // ====== 定时刷新显示：BT/RT 使用的最新 30m/1H K线（开盘/收盘时间）======
        if (PRINT_BT_RT_K_EACH_REFRESH && LAST_BT_RT_BOUNDARY_CLOSE_FOR_PRINT > 0) {
            System.out.println("【K线对齐】boundaryClose(30m收盘JST)=" + FMT_JST.format(Instant.ofEpochMilli(LAST_BT_RT_BOUNDARY_CLOSE_FOR_PRINT)));
            System.out.println("回测推演(BT) 最新30m: " + briefCandle(LAST_BT_30M_FOR_PRINT, BAR30_MS));
            System.out.println("回测推演(BT) 最新1H : " + briefCandle(LAST_BT_1H_FOR_PRINT, BAR1H_MS));
            System.out.println("实盘触发(RT) 最新30m: " + briefCandle(LAST_RT_30M_FOR_PRINT, BAR30_MS));
            System.out.println("实盘触发(RT) 最新1H : " + briefCandle(LAST_RT_1H_FOR_PRINT, BAR1H_MS));
        }

        //  放这里：每次刷新都能看到 SRMF 当前状态（最准确）
        System.out.println("SRMF状态 = regime=" + SRMF.getRegimeName(LIVE_ENGINE.srmf)
                + " trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts(LIVE_ENGINE.srmf))
                + " mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult(LIVE_ENGINE.srmf))
                + (APPLY_OKX_FEE ? (" | fee双边=" + String.format(Locale.US, "%.4f%%", OKX_FEE_RATE_PER_SIDE*2*100)) : " | fee=OFF"));

//  快速观察：稳健4档 / 进攻5档 的“参考名义”（按 A：已收K收盘价计算）
        double obsPrice = (last30 != null ? last30.c : lastPriceCache);
        double obsSl = Math.max(1e-9, lastSlPointsCache);
        if (obsPrice > 0) {
            double obsBaseRisk = capital * RISK_PCT;
            double tp = SRMF.getTrendPts(LIVE_ENGINE.srmf);
            double m4 = stable4MultByTrendPts(tp);
            double m5 = attack5MultByTrendPts(tp);
            double n4 = (obsBaseRisk * m4) * obsPrice / obsSl;
            double n5 = (obsBaseRisk * m5) * obsPrice / obsSl;
            System.out.printf(Locale.US, "名义参考(按已收K收盘价=%.2f, SL=%.0f点)：稳健4档(mult=%.2f)=%.0fU | 进攻5档(mult=%.2f)=%.0fU%n",
                    obsPrice, obsSl, m4, n4, m5, n5);
        }

        String st;
        if (pos == null) st = "无仓";
        else st = (pos.side == Side.LONG ? "持多" : "持空");
        System.out.println("当前持仓状态：" + st);
        debugPrintPosCore("RT", LIVE_ENGINE);

        if (last30 != null) {
            System.out.println("亚洲盘前半过滤=" + (isAsiaLowLiquidityTime(last30.ts + BAR30_MS) ? "命中（不进场）" : "未命中"));
        }

        System.out.println("--------------------------------------");
    }

    static void printIndicatorSnapshotLite(List<Candle> m30, List<MacdPoint> macd30,
                                           List<Candle> h2, List<MacdPoint> macd1h) {

        if (m30 == null || macd30 == null || m30.size() < 3 || macd30.size() < 3) {
            System.out.println("【30m 入场监控】数据不足（需要至少3根已收K）");
            System.out.println();
            return;
        }
        int i = m30.size() - 1;
        MacdPoint k1 = macd30.get(i - 1);
        MacdPoint k2 = macd30.get(i - 2);

        int dDif = (int)(k1.dif - k2.dif);
        int dDea = (int)(k1.dea - k2.dea);

        System.out.println("【30m 入场监控（看K1/K2）】");
        System.out.printf(Locale.US,
                "K1 dif=%.6f dea=%.6f | K2 dif=%.6f dea=%.6f | int(dif1-dif2)=%d | int(dea1-dea2)=%d%n",
                k1.dif, k1.dea, k2.dif, k2.dea, dDif, dDea
        );

        double s0 = macdStrength(macd30.get(i));
        double s1 = macdStrength(macd30.get(i - 1));
        System.out.printf(Locale.US, "MACD strength: cur=%.6f prev=%.6f%n", s0, s1);

        if (USE_1H_FILTER && !h2.isEmpty() && !macd1h.isEmpty()) {
            Trend1HInfo t2 = get1HTrendInfoAt(h2, macd1h, m30.get(i).ts);
            // 先在 printf 上面准备这两个变量（你已经有的话就不用重复写）
            boolean rule2_allSmall = (t2.oscCount >= NEUTRAL_OSC_N); // 最近N根都满足小柱(<=T)才会被置成N
            boolean rule2_mixSign  = ((t2.posCnt > 0 && t2.negCnt > 0) || t2.zeroCnt >= 2);

            System.out.printf(
                    "【1H 趋势过滤（升级版）】%n" +
                            "histNow=%.4f | histPrev=%.4f%n" +
                            "规则1(|hist|<X)：hist=%.4f → %s | X=%.1f%n" +
                            "规则2(震荡窗口N根)：混杂条件=%s | N=%d | 已检查=%d | 小柱阈值T=%.1f | (pos=%d neg=%d zero=%d)%n" +
                            "规则3(翻转第一根)：%s%n" +
                            "结论：%s%n",
                    // 1) histNow / histPrev
                    t2.hist, t2.prevHist,
                    // 2) 规则1
                    t2.hist, (t2.neutralByAbs ? "<未达标" : "≥(1/3)达标"), NEUTRAL_ABS_HIST_X,
                    // 3) 规则2（你要的一眼能看懂）
                    (rule2_mixSign  ? "混杂" : "(1/3)未混杂"),
                    NEUTRAL_OSC_N, t2.oscCount, NEUTRAL_OSC_ABS_HIST_MAX,
                    t2.posCnt, t2.negCnt, t2.zeroCnt,
                    // 4) 规则3
                    (t2.neutralByFlip ? "是" : "(1/3)否"),
                    // 5) 结论
                    (t2.neutral ? "中性（不进场）" : "趋势（允许进场）")
            );
        }else {
            System.out.println("1H趋势过滤=OFF");
        }
        System.out.println();
    }

    static void printLastOppTemplate() {
        System.out.println("------ 最近一次机会（回看）------");

        // 推演展示引擎：VIEW_USE_REPLAY=true 时，推演结果在 RESEARCH_ENGINE；
        // 否则使用 VIEW_ENGINE（SSOT 回看）。
        EngineState scanSt = (VIEW_USE_REPLAY ? RESEARCH_ENGINE : VIEW_ENGINE);



        // === 实盘触发事实（rt_trigger）优先显示：用 entryTs(K0收盘=K1开盘) + entryRef(K0收盘价) ===
        RtTrigger rt = AuditDb.readLastRtTrigger();
        if (rt != null && rt.entryTs > 0 && rt.signalTs > 0
                && !"SELFTEST".equalsIgnoreCase(rt.tf)
                && (rt.note == null || !rt.note.contains("SELFTEST"))) {
            System.out.println("【实盘触发（rt_trigger）】");
            System.out.println("触发/入场(entryTs=K0收盘=K1开盘)：" + fmtOpen(rt.entryTs));
            if (SHOW_SIGNAL_TS) {
                System.out.println("（参考）信号K0区间：" + fmtOpen(rt.signalTs) + "(开)/" + fmtClose(rt.signalTs, BAR30_MS) + "(收=entryTs)");
            }
            String rtSide = (rt.side == Side.LONG ? "做多" : (rt.side == Side.SHORT ? "做空" : "UNKNOWN"));
            System.out.println("方向：" + rtSide);
            System.out.printf(Locale.US, "参考入场价(K0收盘)：%.2f%n", rt.entryRef);
            System.out.printf(Locale.US, "dif=%.6f dea=%.6f prevDif=%.6f prevDea=%.6f diffInt=%d 1H=%s%n",
                    rt.dif, rt.dea, rt.prevDif, rt.prevDea, rt.diffInt, rt.t1h);
            if (rt.note != null && !rt.note.isEmpty()) System.out.println("备注：" + rt.note);

            // 与回看推演对齐性提示（避免“闹钟响了但回看显示另一根K”的错觉）
            if (scanSt.lastEnterOpp != null && scanSt.lastEnterOpp.ts > 0) {
                long dMs = Math.abs(scanSt.lastEnterOpp.ts - rt.entryTs);
                if (dMs >= BAR30_MS) {
                    System.out.printf(Locale.US,
                            "[ALIGN-CHECK][WARN] rt_trigger.entryTs != scanSt.lastEnterOpp.ts diff=%.1fm (rt=%s | view=%s)%n",
                            dMs / 60000.0, fmtOpen(rt.entryTs), fmtOpen(scanSt.lastEnterOpp.ts));
                } else {
                    System.out.printf(Locale.US,
                            "[ALIGN-CHECK][OK] rt_trigger.entryTs == scanSt.lastEnterOpp.ts (%s)%n",
                            fmtOpen(rt.entryTs));
                }
            }
            System.out.println();
        } else {
            if (RT_TRIGGER_SELFTEST) {
                System.out.println("【实盘触发（rt_trigger）】（自检模式已开：okx.rtTriggerSelfTest=true；表写入已验证）");
            } else {
                System.out.println("【实盘触发（rt_trigger）】（暂无记录，等待下一次进场提醒写入）");
            }
            System.out.println();
        }

        System.out.println("【回看推演（scanHistory/回测视图）】");




        // 持仓态下：避免“LAST ENTER(最新一次机会)”随刷新滚动，造成你看到的“入场时间在 9:30/10:00 变化”的错觉。
        // 规则：只要任一引擎处于持仓，就以持仓入场为准；LAST ENTER 仅在无仓时展示。
        EngineState holdSt = null;
        if (LIVE_ENGINE != null && LIVE_ENGINE.pos != null) holdSt = LIVE_ENGINE;
        else if (scanSt != null && scanSt.pos != null) holdSt = scanSt;
        if (holdSt != null) {
            Position hp = holdSt.pos;
            System.out.println("（持仓中：以持仓入场为准）");
            System.out.println("持仓入场：" + fmtOpen(hp.entryTs));
            System.out.println("方向：" + (hp.side == Side.LONG ? "做多" : (hp.side == Side.SHORT ? "做空" : "UNKNOWN")));
            System.out.printf(Locale.US, "入场价：%.2f%n", hp.entry);
            System.out.println();
        }
        boolean showLastEnterWhileHolding = sysBool("okx.btShowLastEnterWhileHolding", true);
        if (holdSt == null || showLastEnterWhileHolding)
        {
            // LAST ENTER：统一为“最后一次执行入场”（必须与 [POS-CORE][RT] entryTs 口径一致）
            // - 持仓中：直接以实时持仓核心 LIVE_ENGINE.pos 为准（entryTs/entryPx/side）
            // - 空仓：退回到回看推演的 lastEnterOpp（研究用），但仍按 entryTs(K0收盘=K1开盘) 口径打印
            Position rtPos = (LIVE_ENGINE == null ? null : LIVE_ENGINE.pos);
            boolean holdingRtPos = (rtPos != null && rtPos.side != null);

            System.out.println(holdingRtPos ? "LAST ENTER（执行口径=RT持仓）："
                    : "LAST ENTER：");

            if (holdingRtPos) {
                long entryTs = rtPos.entryTs;
                long sigOpenTs = entryTs - BAR30_MS;
                System.out.println("触发/入场(entryTs=K0收盘=K1开盘)：" + fmtOpen(entryTs));
                System.out.println("入场K1收盘：" + fmtClose(entryTs, BAR30_MS));
                if (SHOW_SIGNAL_TS) {
                    System.out.println("（参考）信号K0区间：" + fmtOpen(sigOpenTs) + "(开)/" + fmtClose(sigOpenTs, BAR30_MS) + "(收)");
                }
                System.out.println("方向：" + (rtPos.side == Side.LONG ? "做多" : "做空"));
                System.out.printf(Locale.US, "入场价：%.2f%n", rtPos.entry);
            } else {
                if (scanSt.lastEnterOpp == null) {
                    System.out.println("（暂无）");
                } else {
                    EnterOpportunity o = scanSt.lastEnterOpp;
                    long entryTs = o.ts;
                    long sigOpenTs = entryTs - BAR30_MS;
                    System.out.println("触发/入场(entryTs=K0收盘=K1开盘)：" + fmtOpen(entryTs));
                    System.out.println("入场K1收盘：" + fmtClose(entryTs, BAR30_MS));
                    if (SHOW_SIGNAL_TS) {
                        System.out.println("（参考）信号K0区间：" + fmtOpen(sigOpenTs) + "(开)/" + fmtClose(sigOpenTs, BAR30_MS) + "(收)");
                    }
                    System.out.println("方向：" + (o.side == Side.LONG ? "做多" : "做空"));
                    System.out.printf(Locale.US, "入场价：%.2f%n", o.entryRef);
                }
            }
        }


        System.out.println("LAST EXIT：");
        if (scanSt.lastExitOpp == null) {
            System.out.println("（暂无）");
        } else {
            ExitOpportunity x = scanSt.lastExitOpp;
            System.out.println("时间：" + FMT_JST_SHORT.format(Instant.ofEpochMilli(x.ts)));
            System.out.printf(Locale.US, "参考出场价：%.2f%n", x.price);
            System.out.println("出场原因：" + x.reason);
        }
        System.out.println("----------------------------------");
        System.out.println();
    }

    static void printLastTradesTemplate() {
        // === 实盘触发（rt_trigger）：默认按 entryTs 显示，signalTs 只做附注 ===
        if (AUDIT_DB_ENABLED) {
            List<RtTrigger> rts = AuditDb.readRecentRtTriggers(10);
            if (rts != null) {
                rts.removeIf(x -> x == null || x.entryTs <= 0
                        || "SELFTEST".equalsIgnoreCase(x.tf)
                        || (x.note != null && x.note.contains("SELFTEST")));
            }
            if (rts == null || rts.isEmpty()) {
                System.out.println("最近10次触发（实盘 rt_trigger）：无");
            } else {
                System.out.println("最近10次触发（实盘 rt_trigger）：");
                int ridx = 1;
                for (RtTrigger x : rts) {
                    String side = (x.side == Side.LONG ? "做多" : (x.side == Side.SHORT ? "做空" : "UNKNOWN"));
                    if (SHOW_SIGNAL_TS) {
                        System.out.printf(Locale.US,
                                "%2d) entryTs=%s | %s | entryRef=%.2f | 信号K0=%s(开)/%s(收=entryTs)%s%n",
                                ridx++,
                                fmtOpen(x.entryTs),
                                side,
                                x.entryRef,
                                fmtOpen(x.signalTs),
                                fmtClose(x.signalTs, BAR30_MS),
                                (x.note == null || x.note.isEmpty() ? "" : (" | " + x.note))
                        );
                    } else {
                        System.out.printf(Locale.US,
                                "%2d) entryTs=%s | %s | entryRef=%.2f%s%n",
                                ridx++,
                                fmtOpen(x.entryTs),
                                side,
                                x.entryRef,
                                (x.note == null || x.note.isEmpty() ? "" : (" | " + x.note))
                        );
                    }
                }
            }
            System.out.println();
        }

        // === 成交回看（回测/scanHistory） ===
        EngineState scanSt = (VIEW_USE_REPLAY ? RESEARCH_ENGINE : VIEW_ENGINE);

        if (scanSt == null || scanSt.lastTrades.isEmpty()) {
            System.out.println("最近10笔成交（回测/回看）：无");
            return;
        }
        System.out.println("最近10笔成交（回测/回看）：");
        int idx = 1;
        for (CompletedTrade t : scanSt.lastTrades) {
            long sigTs = (t.signalTs != 0 ? t.signalTs : (t.enterTs - BAR30_MS)); // 兼容旧记录
            String sigO  = fmtOpen(sigTs);
            String sigC  = fmtClose(sigTs, BAR30_MS);       // 信号K0收盘（=入场K1开盘）
            String entO  = fmtOpen(t.enterTs);              // 入场K1开盘
            String exitO = fmtOpen(t.exitTs);
            String exitC = fmtClose(t.exitTs, BAR30_MS);
            String side  = (t.side==Side.LONG?"做多":"做空");

            System.out.printf(Locale.US,
                    "%2d) 入场=%s(entryTs) → 出场=%s(开)/%s(收) | 信号K0=%s(开)/%s(收=entryTs) | %s%n" +
                            "    entry=%.2f exit=%.2f 毛=%+.2f点 费=%.2f点 净=%+.2f点 | MFE=%.2f MAE=%.2f%n" +
                            "    原因：%s%n%n",
                    idx++,
                    entO, exitO, exitC, sigO, sigC, side,
                    t.entry, t.exit, t.grossPnlPoints, t.feePoints, t.pnlPoints, t.mfe, t.mae,
                    t.reason
            );
        }
    }
    // ===================== SRMF 下单量打印（USDT名义价值） =====================

    // ===================== 档位映射（用于快速观察名义） =====================
    static double stable4MultByTrendPts(double trendPts) {
        // 稳健4档（Calmar）【优化】：<=30=1.5 | <=45=1.2 | >45=1.2
        // 依据本次回测统计：30-45 桶期望较差，避免加倍；<=30 桶期望最好，给更高倍数
        // if (trendPts <= 30) return 1.5;
        //if (trendPts <= 45) return 1.2;
        return 1;
    }

    static double attack5MultByTrendPts(double trendPts) {
        // 进攻5档（Attack）【优化】：<=30=1.5 | <=45=1.2 | >45=1.2
        // 若你想让 ATTACK 更保守，可把 <=30 也改回 1.2（建议后续 OOS 再定）
        //if (trendPts <= 30) return 1.5;
        //if (trendPts <= 45) return 1.2;
        return 1;
    }

    static void printSrmfBlockByPrice(String tag, double price, int confirm, double slPointsUsed) {
        double p = price;

        System.out.println("----- SRMF 下单参数（USDT 本位，仅打印） -----");
        System.out.println("来源：" + tag + " | confirm=" + confirm);

        // 防误读提示：尾盘应为 confirm=0；收盘确认应为 confirm=1
        if (tag != null && tag.contains("尾盘") && confirm != 0) {
            System.out.println("【注意】尾盘期望未收K(confirm=0)，但当前 confirm=" + confirm + "（请检查 fetchLatestCandles 排序/接口）。");
        }
        if (tag != null && tag.contains("收盘") && confirm != 1) {
            System.out.println("【注意】收盘确认期望已收K(confirm=1)，但当前 confirm=" + confirm + "（请检查 history-candles confirm 过滤）。");
        }

        if (p <= 0) {
            System.out.println("【WARN】price<=0，无法计算下单量");
            System.out.println("---------------------------------------------");
            System.out.println();
            return;
        }

        double baseRisk = capital * RISK_PCT;

        //  防御式刷新动态止损缓存：确保 ATR/k×ATR/clamp 不会打印为 0（即便调用顺序/定时线程变化）
        if (USE_DYNAMIC_SL) {
            ensureDynamicSlCacheForPrint();
        }

        System.out.printf(Locale.US, "本金(capital)：%.2f%n", capital);
        if (APPLY_OKX_FEE) {
            System.out.printf(Locale.US, "手续费：OKX 合约双边=%.4f%%（单边=%.4f%%，以点数扣：feePts=rate*(|entry|+|exit|)）%n", OKX_FEE_RATE_PER_SIDE*2*100, OKX_FEE_RATE_PER_SIDE*100);
        } else {
            System.out.println("手续费：OFF");
        }
        System.out.printf(Locale.US, "参考价格(last)：%.2f%n", p);        //  打印 OKX 撑压线（便于手动下单/设置止盈/判断差值过滤）
        if (lastOkxSrValidForPrint && Double.isFinite(lastOkxResistanceForPrint) && Double.isFinite(lastOkxSupportForPrint)) {
            double gapToRes = lastOkxResistanceForPrint - p;
            double gapToSup = p - lastOkxSupportForPrint;
            System.out.printf(Locale.US,
                    "OKX 撑压线：Resistance=%.2f | Support=%.2f | window=%d根30m | 距阻力=%.2f | 距支撑=%.2f%n",
                    lastOkxResistanceForPrint, lastOkxSupportForPrint, lastOkxSrUsedBarsForPrint, gapToRes, gapToSup);
        } else {
            System.out.printf(Locale.US,
                    "OKX 撑压线：窗口不足（目标=%d根30m，当前=%d）%n", OKX_SR_N, lastOkxSrUsedBarsForPrint);
        }
        System.out.printf(Locale.US, "风险R=本金×%.2f%%：%.2f USDT%n", RISK_PCT * 100, baseRisk);
        if (USE_DYNAMIC_SL) {
            int m30Used = (lastM30ClosedForPrint == null ? 0 : lastM30ClosedForPrint.size());
            if (m30Used < 2) {
                System.out.printf(Locale.US,
                        "止损点数：固定底线=%.0f | ATR%d=N/A | k×ATR=N/A | clamp(%.0f-%.0f)=N/A |  最终生效=%.2f%n",
                        SL_POINTS, ATR_N, SL_MIN, SL_MAX, slPointsUsed);
            } else {
                System.out.printf(Locale.US,
                        "止损点数：固定底线=%.0f | ATR%d=%.2f | k×ATR=%.2f | clamp(%.0f-%.0f)=%.2f | 最终生效=%.2f%n",
                        SL_POINTS, ATR_N, lastAtrCache, lastDynSlRawCache, SL_MIN, SL_MAX, lastDynSlClampedCache, slPointsUsed
                );
            }
        } else {
            System.out.printf(Locale.US, "固定止损点数：%.0f 点%n", slPointsUsed);
        }
        System.out.println();
        System.out.println("当前 SRMF 状态：regime=" + SRMF.regimeCn(SRMF.getRegimeName(LIVE_ENGINE.srmf)) +
                " | trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts(LIVE_ENGINE.srmf)) +
                " | mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult(LIVE_ENGINE.srmf)));


//  额外观察：把“投票倍率”叠加到当前 SRMF mult 上，快速看到 4票/5票 名义
        double tierMultObs = SRMF.getCurrentMult(LIVE_ENGINE.srmf); // 当前 SRMF 档位倍率（不含投票）
        double vote4ScaleObs = 0.9;
        double vote5ScaleObs = 1.7;
        System.out.println(String.format(Locale.US,
                "投票数4票名义(USDT)：trendPts=%.2f mult=%.2f*%.2f => %.0f",
                SRMF.getTrendPts(LIVE_ENGINE.srmf),
                tierMultObs,
                vote4ScaleObs,
                baseRisk * (tierMultObs * vote4ScaleObs) * p / slPointsUsed
        ));
        System.out.println(String.format(Locale.US,
                "投票数5票名义(USDT)：trendPts=%.2f mult=%.2f*%.2f => %.0f",
                SRMF.getTrendPts(LIVE_ENGINE.srmf),
                tierMultObs,
                vote5ScaleObs,
                baseRisk * (tierMultObs * vote5ScaleObs) * p / slPointsUsed
        ));

        //  方便观察：把稳健4档/进攻5档的“下单名义(USDT)”直接打印在 SRMF 状态下面（基于同一价格/止损点数）
        //  方便观察：把稳健4档/进攻5档的“下单名义(USDT)”直接打印在 SRMF 状态下面（基于同一价格/止损点数）
        double tpObs = SRMF.getTrendPts(LIVE_ENGINE.srmf);
        double mStableObs = stable4MultByTrendPts(tpObs);
        double mAttackObs = attack5MultByTrendPts(tpObs);

        System.out.println(String.format(Locale.US,
                "稳健4档名义(USDT)：trendPts=%.2f mult=%.2f => %.0f",
                tpObs,
                mStableObs,
                baseRisk * mStableObs * p / slPointsUsed
        ));
        System.out.println(String.format(Locale.US,
                "进攻5档名义(USDT)：trendPts=%.2f mult=%.2f => %.0f",
                tpObs,
                mAttackObs,
                baseRisk * mAttackObs * p / slPointsUsed
        ));
        System.out.println();
        System.out.println("【稳健4档（Calmar）】 trendPts→mult：<=30=1.5 | <=45=1.2 | >45=1.2");
        System.out.println("【进攻5档（Attack）】 trendPts→mult：<=30=1.5 | <=45=1.2 | >45=1.2");
        System.out.println();
        System.out.println("以当前 mult 计算（仅参考）：");
        double curMult = SRMF.getCurrentMult(LIVE_ENGINE.srmf);
        double curRisk = baseRisk * curMult;
        double curNotional = curRisk * p / slPointsUsed;
        System.out.printf(Locale.US, "mult=%.2f | 风险=%.2f USDT | 下单名义=%.0f USDT%n", curMult, curRisk, curNotional);
        System.out.println();


        // ===== 名义核对：本轮预览（无入场方向，避免错乱）=====
        System.out.println("【名义核对-本轮预览（无入场方向）】");
        double fixedBaseNotional = sysDouble("okx.testNotional", 0.0);
        if (fixedBaseNotional > 0) {
            System.out.printf(Locale.US, "当前模式=FIXED（-Dokx.testNotional=%.2f），名义公式：notional = baseNotional × (tier×vote×cut)%n", fixedBaseNotional);
        } else {
            System.out.printf(Locale.US, "当前模式=SRMF（默认），名义公式：notional = baseRisk × (tier×vote×cut) × entryRef / slPts（此处 baseRisk=capital×RISK%% 仅做预览）%n");
        }
        System.out.printf(Locale.US, "当前trendPts=%.2f | tierMult(档位)=%.2f%n", tpObs, tierMultObs);
        System.out.printf(Locale.US, "票数倍率(投票)=票4→%.2f | 票5→%.2f（票>=5按票5口径）%n", vote4ScaleObs, vote5ScaleObs);
        System.out.printf(Locale.US, "riskVote降档系数=RV=-1→%.2f | RV<=-2→%.2f | 其它→1.00%n", RV_CUT_1, RV_CUT_2);

        System.out.println("预览名义（仅参考：无方向/不提交AUTO；真实入场会以当时的票数/风控票/止损点/参考价为准）：");
        if (fixedBaseNotional > 0) {
            double pv4 = fixedBaseNotional * (tierMultObs * vote4ScaleObs);
            double pv5 = fixedBaseNotional * (tierMultObs * vote5ScaleObs);
            double pv5c1 = fixedBaseNotional * (tierMultObs * vote5ScaleObs * RV_CUT_1);
            double pv5c2 = fixedBaseNotional * (tierMultObs * vote5ScaleObs * RV_CUT_2);
            System.out.printf(Locale.US, "  票4/RV>=0: mult=%.4f => %.2f USDT%n", (tierMultObs*vote4ScaleObs), pv4);
            System.out.printf(Locale.US, "  票5/RV>=0: mult=%.4f => %.2f USDT%n", (tierMultObs*vote5ScaleObs), pv5);
            System.out.printf(Locale.US, "  票5/RV=-1 : mult=%.4f => %.2f USDT%n", (tierMultObs*vote5ScaleObs*RV_CUT_1), pv5c1);
            System.out.printf(Locale.US, "  票5/RV<=-2: mult=%.4f => %.2f USDT%n", (tierMultObs*vote5ScaleObs*RV_CUT_2), pv5c2);
        } else {
            double pv4 = baseRisk * (tierMultObs * vote4ScaleObs) * p / slPointsUsed;
            double pv5 = baseRisk * (tierMultObs * vote5ScaleObs) * p / slPointsUsed;
            double pv5c1 = baseRisk * (tierMultObs * vote5ScaleObs * RV_CUT_1) * p / slPointsUsed;
            double pv5c2 = baseRisk * (tierMultObs * vote5ScaleObs * RV_CUT_2) * p / slPointsUsed;
            System.out.printf(Locale.US, "  票4/RV>=0: mult=%.4f => %.0f USDT%n", (tierMultObs*vote4ScaleObs), pv4);
            System.out.printf(Locale.US, "  票5/RV>=0: mult=%.4f => %.0f USDT%n", (tierMultObs*vote5ScaleObs), pv5);
            System.out.printf(Locale.US, "  票5/RV=-1 : mult=%.4f => %.0f USDT%n", (tierMultObs*vote5ScaleObs*RV_CUT_1), pv5c1);
            System.out.printf(Locale.US, "  票5/RV<=-2: mult=%.4f => %.0f USDT%n", (tierMultObs*vote5ScaleObs*RV_CUT_2), pv5c2);
        }
        System.out.println("  最终传给AUTO名义：N/A（需触发入场提醒/自动下单计划时才会产生）");
        System.out.println();

        // ===== 名义核对：最近一次入场（档位×票数×风控票）以及最终传给自动下单的名义 =====
        System.out.println("【名义核对（最近一次入场）】");
        if (LAST_NOTIONAL_DEBUG != null && LAST_NOTIONAL_DEBUG.entryTs > 0) {
            LastNotionalDebug d = LAST_NOTIONAL_DEBUG;

            String sideCn = (d.side == Side.LONG ? "做多" : (d.side == Side.SHORT ? "做空" : "UNKNOWN"));
            String srcCn = (d.src == null ? "" : d.src);

            System.out.println("来源=" + srcCn + " | entryTs=" + fmtOpen(d.entryTs) + " | 方向=" + sideCn);
            if (d.detail != null && !d.detail.isEmpty()) {
                System.out.println("riskVote明细：" + d.detail);
            }

            double multFromParts = d.tierMult * d.voteScale * d.riskCut;
            System.out.printf(Locale.US,
                    "当前最终mult = (trendPts档位=%.2f) × (票数scale=%.2f,票=%d) × (riskVote系数=%.2f,RV=%d) = %.4f%n",
                    d.tierMult, d.voteScale, d.voteScore, d.riskCut, d.riskVote, d.entryMultFinal
            );
            // 若开启 capAtTierMult，entryMultFinal 可能不等于 tier*vote*cut；这里顺手提示
            if (Math.abs(multFromParts - d.entryMultFinal) > 1e-6) {
                System.out.printf(Locale.US,
                        "（提示）mult分解值=%.4f 与 实际entryMult=%.4f 不一致，可能启用了 capAtTierMult 或其它上限逻辑%n",
                        multFromParts, d.entryMultFinal);
            }

            if ("FIXED".equalsIgnoreCase(d.ndMode)) {
                System.out.printf(Locale.US,
                        "当前最终名义 = baseNotional(%.2f) × 最终mult(%.4f) = %.2f USDT%n",
                        d.baseNotional, d.entryMultFinal, d.computedNotional);
            } else {
                // SRMF 风险反推
                System.out.printf(Locale.US,
                        "当前最终名义 = baseRisk(%.2f=equity(%.2f)×%.2f%%) × 最终mult(%.4f) × entryRef(%.2f) / slPts(%.2f) = %.2f USDT%n",
                        d.baseRisk, d.equityUsed, RISK_PCT * 100.0, d.entryMultFinal, d.entryPx, d.slPts, d.computedNotional
                );
            }
            System.out.printf(Locale.US,
                    "最终传给自动下单的名义 = %.2f USDT%n",
                    d.sentToAutoNotional
            );
        } else {
            System.out.println("（暂无：本次运行尚未触发入场提醒/自动下单计划，因此没有“票数×风控票×档位”的最终名义快照）");
        }
        System.out.println();
        System.out.println("各档位开仓名义（按参考价格/同一止损点数，仅打印）：");

        // 稳健 4档（Calmar）
        System.out.println("【稳健4档（Calmar）】");
        double[] calmarMults = new double[]{1.20, 1.50};
        for (double mm : calmarMults) {
            double r = baseRisk * mm;
            double notional = r * p / slPointsUsed;
            System.out.printf(Locale.US, "  mult=%.2f | 风险=%.2f USDT | 名义=%.0f USDT%n", mm, r, notional);
        }
        System.out.println();
        System.out.println("【进攻5档（Attack）】");
        double[] attackMults = new double[]{1.20, 1.50};
        for (double mm : attackMults) {
            double r = baseRisk * mm;
            double notional = r * p / slPointsUsed;
            System.out.printf(Locale.US, "  mult=%.2f | 风险=%.2f USDT | 名义=%.0f USDT%n", mm, r, notional);
        }

        System.out.println("---------------------------------------------");
        System.out.println();
    }

    static void printSrmfBlock() {
        //  防御式更新：无论调用顺序怎么变，打印前都确保有“已收盘30m列表”，并刷新一次动态止损缓存（避免显示0）
        long nowMs = System.currentTimeMillis();
        if (lastM30ClosedForPrint == null || lastM30ClosedForPrint.size() < 2) {
            try {
                // 尝试从缓存补齐（不影响你原有 schedule/runOnce 逻辑）
                syncCache(nowMs);
                List<Candle> tmp = snapshotCache(CACHE_M30);
                tmp = filterClosedByTime(tmp, BAR30_MS, nowMs, SAFE_CLOSE_MS);
                lastM30ClosedForPrint = tmp;
                lastM30ClosedEndIdxForPrint = (tmp == null ? -1 : tmp.size() - 1);
            } catch (Exception ignore) {
                // ignore
            }
        }
        if (lastM30ClosedForPrint != null && lastM30ClosedForPrint.size() >= 2) {
            int endIdx = (lastM30ClosedEndIdxForPrint >= 0 ? lastM30ClosedEndIdxForPrint : lastM30ClosedForPrint.size() - 1);
            updateDynamicSlCache(lastM30ClosedForPrint, endIdx);
        }

        //  收盘确认(30:10)：只用“刚收盘K(confirm=1)”的价格 lastPriceCache
        printSrmfBlockByPrice("收盘确认(30:10)-已收K", lastPriceCache, 1, lastSlPointsCache);
    }

    // =====================  告警摘要（只新增，不改你原输出） =====================

    static void printEnterAlertSummary(Position pos, Candle k0Candle,
                                       Trend1HInfo t1hInfo, MacdPoint m0, MacdPoint m1, double minRangeNeed) {

        double range = k0Candle.h - k0Candle.l;
        double body  = Math.abs(k0Candle.c - k0Candle.o);
        double bodyRatio = (range <= 0 ? 0 : body / range);

        double sl = (pos.side == Side.LONG) ? (pos.entry - pos.slPoints) : (pos.entry + pos.slPoints);
        double tp2 = (pos.side == Side.LONG) ? (pos.entry + TP2_TRIGGER) : (pos.entry - TP2_TRIGGER);

        int diffInt = (pos.side == Side.LONG) ? (int)(m0.dif - m1.dif) : (int)(m0.dea - m1.dea);

        System.out.println("【进场摘要】");
        System.out.println("时间：" + FMT_JST.format(Instant.ofEpochMilli(k0Candle.ts + BAR30_MS)));
        System.out.println("方向：" + (pos.side == Side.LONG ? "做多" : "做空"));
        System.out.printf(Locale.US, "参考入场：%.2f（下一根开盘） | 动量diffInt=%d%n", pos.entry, diffInt);
        System.out.printf(Locale.US, "投票：score=%d | 额外倍率=%.1f | mult=档位%.2f*%.1f=%.2f%n",
                pos.entryVoteScore,
                pos.entryVoteScale,
                pos.entryTierMult,
                pos.entryVoteScale,
                pos.entryMult
        );

        if (USE_1H_FILTER) {
            System.out.printf(Locale.US,
                    "1H：%s | hist=%.4f prev=%.4f | neutral=%s firstNeutral=%s secondTrendBlock=%s%n",
                    t1hInfo.trend,
                    t1hInfo.hist, t1hInfo.prevHist,
                    (t1hInfo.neutral ? "YES(不进场)" : "NO"),
                    (t1hInfo.isFirstNeutralBar ? "YES(不进场)" : "NO"),
                    (t1hInfo.isSecondTrendBarAfterNeutral ? "YES(不进场)" : "NO")
            );
        } else {
            System.out.println("1H：OFF");
        }

        System.out.printf(Locale.US,
                "质量：range=%.2f（>=%.2f） | bodyRatio=%.2f（>=%.2f）%n",
                range, minRangeNeed, bodyRatio, MIN_BODY_RATIO
        );

        System.out.printf(Locale.US,
                "关键价位：SL=%.2f | TP=%.2f（触发%.0f点 全平）%n",
                sl,
                tp2, TP2_TRIGGER
        );
        if (!Double.isNaN(pos.okxResistanceAtEntry) && !Double.isNaN(pos.okxSupportAtEntry)) {
            System.out.printf(Locale.US,
                    "OKX撑压：Resistance=%.2f | Support=%.2f | 入场到目标%s差=%.2f（>=%.2f 才允许进场）%n",
                    pos.okxResistanceAtEntry,
                    pos.okxSupportAtEntry,
                    (pos.side == Side.LONG ? "压力" : "支撑"),
                    pos.okxGapAtEntry,
                    okxSrEntryMinGapPts(pos.slPointsAtEntry)
            );
            if (USE_OKX_SR_TP) {
                double okxTp = (pos.side == Side.LONG) ? pos.okxResistanceAtEntry : pos.okxSupportAtEntry;
                System.out.printf(Locale.US, "OKX撑压止盈：TP=%.2f（触达即止盈，且必须在K线高低点范围内）%n", okxTp);
            }
        } else {
            System.out.printf(Locale.US, "OKX撑压：窗口不足（OKX_SR_N=%d），本次不启用撑压过滤/止盈%n", OKX_SR_N);
        }
        System.out.println();
    }

    static void printExitAlertSummary(Position pos, Candle c0, double exitRef, double grossPnl, double feePts, double netPnl, String reason) {
        double sl = (pos.side == Side.LONG) ? (pos.entry - pos.slPoints) : (pos.entry + pos.slPoints);
        double tp2 = (pos.side == Side.LONG) ? (pos.entry + TP2_TRIGGER) : (pos.entry - TP2_TRIGGER);

        System.out.println("【出场摘要】");
        System.out.println("时间：" + FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)));
        System.out.println("方向：" + (pos.side == Side.LONG ? "做多" : "做空"));
        System.out.printf(Locale.US,
                "入场=%.2f | 出场参考=%.2f | 毛=%+.2f点 | 费=%.2f点 | 净=%+.2f点%n",
                pos.entry, exitRef, grossPnl, feePts, netPnl
        );
        System.out.printf(Locale.US, "MFE=%.2f | MAE=%.2f%n", pos.mfe, pos.mae);
        System.out.println("原因：" + reason);
        System.out.printf(Locale.US, "参考价位：SL=%.2f | TP=%.2f%n", sl, tp2);
        System.out.println();
    }

    // ===================== 出场判定函数（结构保持） =====================
    static boolean isBull(Candle c) { return c.c > c.o; }
    static boolean isBear(Candle c) { return c.c < c.o; }

    // =====================  动态止损点数计算（30m ATR） =====================
    static double clamp(double x, double lo, double hi) { return Math.max(lo, Math.min(hi, x)); }

    // 计算 ATR（简单平均 TR），endIdx 为包含在内的最后一根K索引
    static double calcAtr(List<Candle> cs, int endIdx, int n) {
        if (cs == null || cs.isEmpty()) return 0.0;
        endIdx = Math.min(endIdx, cs.size() - 1);
        int start = Math.max(1, endIdx - n + 1); // TR 需要 prev close，所以最小从1开始
        int count = 0;
        double sumTr = 0.0;

        for (int i = start; i <= endIdx; i++) {
            Candle cur = cs.get(i);
            Candle prev = cs.get(i - 1);
            double tr1 = cur.h - cur.l;
            double tr2 = Math.abs(cur.h - prev.c);
            double tr3 = Math.abs(cur.l - prev.c);
            double tr = Math.max(tr1, Math.max(tr2, tr3));
            sumTr += tr;
            count++;
        }
        return (count == 0 ? 0.0 : sumTr / count);
    }

    // 返回本次信号/持仓应使用的止损点数（用于回测/实时/尾盘一致）
    static double calcSlPointsForIndex(List<Candle> m30Like, int endIdx) {
        if (!USE_DYNAMIC_SL) return SL_POINTS;

        double atr = calcAtr(m30Like, endIdx, ATR_N);
        double dyn = SL_ATR_K * atr;
        double px = (endIdx >= 0 && endIdx < m30Like.size()) ? m30Like.get(endIdx).c : 0.0;
        dyn = clampSl(dyn, px);

        // 固定底线：不小于 SL_POINTS（绝对模式生效；百分比模式下 SL_POINTS 远小于价格%，floor 自然失效，不影响换币）
        return Math.max(SL_POINTS, dyn);
    }

    // =====================  风控票（R1~R4）实现：无未来函数（只用信号K0及历史已收盘K线） =====================
    static class RiskVoteResult {
        int riskVote;          // -2~-1/0/+1
        boolean r1Veto;        // true=直接否决（不进场）
        double multBefore;     // tierMult * voteScale（原始）
        double multAfter;      // 风控修正后的 mult
        String detail;         // 便于写入日志/复盘
    }

    static RiskVoteResult evalRiskVote(List<Candle> m30, List<Candle> h2, int sigIdx, Side side,
                                       double refPx, double tierMult, double voteScale,
                                       boolean silent, String tag) {
        RiskVoteResult r = new RiskVoteResult();
        r.multBefore = tierMult * voteScale;
        r.multAfter = r.multBefore;
        r.detail = "RV=0";

        if (m30 == null || m30.isEmpty() || sigIdx < 0 || sigIdx >= m30.size()) return r;
        Candle c0 = m30.get(sigIdx);

        boolean veto = (RV_R1_VETO && isReverseLongWick(c0, side));
        r.r1Veto = veto;

        int vote = 0;
        boolean r2 = false;
        boolean r3fail = false;
        boolean r4fail = false;

        if (RV_R2_STRONGK && isStrongK(c0, side)) {
            vote += 1;
            r2 = true;
        }
        if (RV_R3_VOLNOTLOW && !passVolNotLow(m30, sigIdx, refPx)) {
            vote -= 1;
            r3fail = true;
        }
        if (RV_R4_FAR_EMA400 && !passFarEma400(m30, h2, sigIdx, refPx)) {
            vote -= 1;
            r4fail = true;
        }

        r.riskVote = vote;

        // riskVote>=+1：默认不收缩（保持原 tierMult*voteScale 不变）。
        // 如确实想“禁止票数加仓”，可开启 okx.riskVote.capAtTierMult=true，把 mult 上限压到 tierMult。
        if (vote >= 1 && RV_CAP_AT_TIER_MULT) {
            r.multAfter = Math.min(r.multAfter, tierMult);
        }
        // riskVote<=-1：降仓
        if (vote <= -2) {
            r.multAfter *= RV_CUT_2;
        } else if (vote <= -1) {
            r.multAfter *= RV_CUT_1;
        }

        r.detail = buildRiskDetail(vote, r2, r3fail, r4fail);

        if (!silent && RV_PRINT_BLOCK && (veto || vote != 0 || Math.abs(r.multAfter - r.multBefore) > 1e-12)) {
            System.out.printf(Locale.US,
                    "【风控票】%s idx=%d ts=%s | side=%s | vote=%d | r1Veto=%s | mult %.3f -> %.3f | %s%n",
                    tag, sigIdx, FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                    side, vote, veto, r.multBefore, r.multAfter, r.detail
            );
        }

        return r;
    }

    static String buildRiskDetail(int vote, boolean r2, boolean r3fail, boolean r4fail) {
        StringBuilder sb = new StringBuilder();
        sb.append("RV=").append(vote);
        if (RV_R2_STRONGK) sb.append("|R2=").append(r2 ? "+1" : "0");
        if (RV_R3_VOLNOTLOW) sb.append("|R3=").append(r3fail ? "-1" : "0");
        if (RV_R4_FAR_EMA400) sb.append("|R4=").append(r4fail ? "-1" : "0");
        return sb.toString();
    }

    // R1：反向长影线（与方向相反的一侧影线显著 => 反转/拒绝信号）
    static boolean isReverseLongWick(Candle c, Side side) {
        double o = c.o, h = c.h, l = c.l, cl = c.c;
        double range = h - l;
        if (range <= 1e-9) return false;

        double body = Math.abs(cl - o);
        double upper = h - Math.max(o, cl);
        double lower = Math.min(o, cl) - l;

        double opp = (side == Side.LONG ? upper : lower); // 反向影线
        double oppRatio = opp / range;
        double bodyRatio = body / range;

        boolean doji = bodyRatio <= RV_R1_DOJI_BODY_RATIO;
        boolean wickBig = oppRatio >= RV_R1_WICK_RANGE_RATIO;
        boolean vsBody = opp >= body * RV_R1_WICK_BODY_MULT;

        return wickBig && (vsBody || doji);
    }

    // R2：强势K（顺势、实体占比高、收盘靠近极值）
    static boolean isStrongK(Candle c, Side side) {
        double o = c.o, h = c.h, l = c.l, cl = c.c;
        double range = h - l;
        if (range <= 1e-9) return false;

        double body = Math.abs(cl - o);
        double bodyRatio = body / range;
        double closePos = (cl - l) / range; // 0=收在最低，1=收在最高

        if (side == Side.LONG) {
            return (cl > o) && bodyRatio >= RV_R2_BODY_RANGE_MIN && closePos >= RV_R2_CLOSE_POS_MIN;
        } else {
            return (cl < o) && bodyRatio >= RV_R2_BODY_RANGE_MIN && closePos <= (1.0 - RV_R2_CLOSE_POS_MIN);
        }
    }

    // R3：波动不低（不满足 -1）。用 ATR 短/长相对 + 最小百分比，避免不同价格区间失效
    static boolean passVolNotLow(List<Candle> m30, int sigIdx, double refPx) {
        if (m30 == null || m30.isEmpty()) return true;
        double px = Math.max(1e-9, refPx);

        double atrS = calcAtr(m30, sigIdx, RV_R3_ATR_N);
        double atrL = calcAtr(m30, sigIdx, RV_R3_ATR_LONG_N);

        boolean passPct = (atrS / px) >= RV_R3_MIN_ATR_PCT;
        boolean passRel = (atrL <= 1e-9) || (atrS >= atrL * RV_R3_MIN_ATR_RATIO);
        return passPct && passRel;
    }

    // R4：远离 EMA400（不满足 -1）
    static boolean passFarEma400(List<Candle> m30, List<Candle> h2, int sigIdx, double refPx) {
        if (m30 == null || m30.isEmpty()) return true;
        double px = Math.max(1e-9, refPx);

        // 优先复用 EMA 投票上下文（无未来函数：只用已收盘K）
        EmaVoteContext ctx = null;
        if (EMA_VOTE_CTX_REALTIME != null && EMA_VOTE_CTX_REALTIME.m30 == m30) {
            ctx = EMA_VOTE_CTX_REALTIME;
        } else if (EMA_VOTE_CTX_BACKTEST != null && EMA_VOTE_CTX_BACKTEST.m30 == m30) {
            ctx = EMA_VOTE_CTX_BACKTEST;
        } else if (h2 != null && h2.size() >= 5) {
            ctx = EmaVoteContext.build(m30, h2);
        }

        double ema = Double.NaN;
        if (ctx != null && ctx.ema30 != null && sigIdx >= 0 && sigIdx < ctx.ema30.length) {
            ema = ctx.ema30[sigIdx];
        } else {
            // 兜底：直接算 EMA400（极少触发）
            double[] e = calcEmaArray(m30, EMA30_PERIOD);
            if (sigIdx >= 0 && sigIdx < e.length) ema = e[sigIdx];
        }

        if (!(ema > 0.0) || Double.isNaN(ema) || Double.isInfinite(ema)) return true; // 数据不足时不惩罚
        double distPct = Math.abs(px - ema) / px;
        return distPct >= RV_R4_MIN_EMA400_DIST_PCT;
    }

    //  每次刷新都更新“动态止损缓存”，避免打印为 0（尤其是 lastAtrCache/lastDynSlRawCache/lastDynSlClampedCache）
    static double updateDynamicSlCache(List<Candle> m30Like, int endIdx) {
        if (!USE_DYNAMIC_SL) {
            lastAtrCache = 0.0;
            lastDynSlRawCache = 0.0;
            lastDynSlClampedCache = 0.0;
            lastSlPointsCache = SL_POINTS;
            return lastSlPointsCache;
        }
        double atr = calcAtr(m30Like, endIdx, ATR_N);
        double raw = SL_ATR_K * atr;
        double pxC = (endIdx >= 0 && endIdx < m30Like.size()) ? m30Like.get(endIdx).c : 0.0;
        double clamped = clampSl(raw, pxC);
        double finalSl = Math.max(SL_POINTS, clamped);

        lastAtrCache = atr;
        lastDynSlRawCache = raw;
        lastDynSlClampedCache = clamped;
        lastSlPointsCache = finalSl;
        return finalSl;
    }

    //  给“打印用”的动态止损做一层兜底：即使 lastM30ClosedForPrint 为空/过短，也从缓存再取一次
    static void ensureDynamicSlCacheForPrint() {
        if (!USE_DYNAMIC_SL) return;

        List<Candle> src = lastM30ClosedForPrint;
        int endIdx = lastM30ClosedEndIdxForPrint;

        // 1) 优先用 runOnce 保存的“已收盘30m列表”
        boolean ok = (src != null && src.size() >= 2);

        // 2) 如果没有（或太短），就从缓存快照 + 时间过滤再取一次
        if (!ok) {
            try {
                List<Candle> snap = snapshotCache(CACHE_M30);
                snap = filterClosedByTime(snap, BAR30_MS, System.currentTimeMillis(), SAFE_CLOSE_MS);
                if (snap != null && snap.size() >= 2) {
                    src = snap;
                    endIdx = snap.size() - 1;
                    lastM30ClosedForPrint = src;
                    lastM30ClosedEndIdxForPrint = endIdx;
                    ok = true;
                }
            } catch (Exception ignore) {}
        }

        // 3) 有数据才刷新缓存；否则保持旧值（不要把缓存重置成0）
        if (ok) {
            if (endIdx < 0 || endIdx >= src.size()) endIdx = src.size() - 1;
            updateDynamicSlCache(src, endIdx);
            updateOkxSrCacheForPrint(src, endIdx);
        }
    }

    static boolean hitStopLoss(Position t, Candle c) {
        double sl = (t == null ? SL_POINTS : t.slPoints);
        if (t.side == Side.LONG) return c.l <= (t.entry - sl);
        else return c.h >= (t.entry + sl);
    }

    static double stopFillPrice(Position t) {
        double sl = (t == null ? SL_POINTS : t.slPoints);
        if (t.side == Side.LONG) return t.entry - sl;
        else return t.entry + sl;
    }

    // ===================== MACD 计算（hist = DIF-DEA，不乘2） =====================
    static List<MacdPoint> calcMacd(List<Candle> cs) {
        if (cs.isEmpty()) return Collections.emptyList();
        List<MacdPoint> out = new ArrayList<>(cs.size());

        double emaFast = cs.get(0).c;
        double emaSlow = cs.get(0).c;
        double dea = 0;

        double kFast = 2.0 / (FAST + 1);
        double kSlow = 2.0 / (SLOW + 1);
        double kSig  = 2.0 / (SIGNAL + 1);

        for (Candle c : cs) {
            emaFast = emaFast + kFast * (c.c - emaFast);
            emaSlow = emaSlow + kSlow * (c.c - emaSlow);

            double dif = emaFast - emaSlow;
            dea = dea + kSig * (dif - dea);

            MacdPoint p = new MacdPoint();
            p.dif = dif;
            p.dea = dea;
            p.hist = dif - dea;
            out.add(p);
        }
        return out;
    }


    // ===================== MACD 对齐（按 candle.ts 对齐，保证 scanHistory 与实时一致） =====================
    static List<MacdPoint> alignMacdByTs(List<Candle> srcCandles, List<MacdPoint> srcMacd,
                                         List<Candle> dstCandles, String label) {
        if (dstCandles == null || dstCandles.isEmpty()) return Collections.emptyList();

        if (srcCandles == null || srcMacd == null || srcCandles.isEmpty() || srcMacd.isEmpty()) {
            if (DEBUG_ALIGN) {
                System.out.println("[ALIGN-MACD][WARN] " + label + " src empty -> recalc");
            }
            return calcMacd(dstCandles);
        }

        int n = Math.min(srcCandles.size(), srcMacd.size());
        HashMap<Long, MacdPoint> map = new HashMap<>(Math.max(16, n * 2));

        for (int i = 0; i < n; i++) {
            Candle c = srcCandles.get(i);
            MacdPoint p = srcMacd.get(i);
            if (c != null && p != null) {
                map.put(c.ts, p);
            }
        }

        ArrayList<MacdPoint> out = new ArrayList<>(dstCandles.size());
        int miss = 0;

        for (int i = 0; i < dstCandles.size(); i++) {
            Candle c = dstCandles.get(i);
            MacdPoint p = (c == null ? null : map.get(c.ts));

            if (p == null) {
                // next-open 占位K：不要用“真实未收盘K”的 MACD（可能未来），用上一根的 MACD 安全对齐即可
                if (c != null && c.confirm == 0 && i > 0 && out.get(i - 1) != null) {
                    out.add(out.get(i - 1));
                    continue;
                }
                miss++;
                out.add(null);
            } else {
                out.add(p);
            }
        }

        if (miss > 0) {
            if (DEBUG_ALIGN) {
                System.out.println("[ALIGN-MACD][WARN] " + label + " missing=" + miss + " -> recalc");
            }
            return calcMacd(dstCandles);
        }

        for (MacdPoint p : out) {
            if (p == null) {
                if (DEBUG_ALIGN) {
                    System.out.println("[ALIGN-MACD][WARN] " + label + " has null -> recalc");
                }
                return calcMacd(dstCandles);
            }
        }

        if (DEBUG_ALIGN) {
            System.out.println("[ALIGN-MACD][OK] " + label + " aligned by ts size=" + out.size());
        }
        return out;
    }

    // ===================== 拉K线（按数量分页） =====================
    static List<Candle> fetchRecentByCount(String bar, int needBars) throws Exception {
        if (!USE_DB_CACHE) return fetchRecentByCountNet(bar, needBars);
        long nowMs = System.currentTimeMillis();
        // strictSsot=true 时：本进程必须只读 DB（由独立 Puller 负责写入）
        if (!STRICT_SSOT) {
            // 拉取器：先增量写 DB（SSOT）
            CandleDb.ingestRecent(INST_ID, bar, Math.max(needBars, 600), nowMs);
        }
        // 引擎：只读 DB
        return CandleDb.loadLatest(INST_ID, bar, needBars, false); // 引擎/回测输入只吃已收盘K(confirm=1)，未收K仅用于展示 nextOpen
    }

    static List<Candle> fetchRecentByCountNet(String bar, int needBars) throws Exception {
        TreeMap<Long, Candle> map = new TreeMap<>();

        Long after = null;
        int guard = 0;

        long lastOldest = -1, lastNewest = -1;

        while (map.size() < needBars) {
            String url = buildHistoryCandlesUrl(INST_ID, bar, after, LIMIT);
            String json = fetch(url);
            List<Candle> page = parseCandles(json);
            if (page.isEmpty()) break;

            page.sort(Comparator.comparingLong(c -> c.ts));
            long oldest = page.get(0).ts;
            long newest = page.get(page.size() - 1).ts;

            if (oldest == lastOldest && newest == lastNewest) break;
            lastOldest = oldest; lastNewest = newest;

            for (Candle c : page) map.put(c.ts, c);

            after = oldest - 1;

            Thread.sleep(Math.max(250, SLEEP_MS));
            if (++guard > GUARD_MAX) break;
        }

        return new ArrayList<>(map.values());
    }

    static String buildHistoryCandlesUrl(String instId, String bar, Long after, int limit) {
        StringBuilder sb = new StringBuilder();
        sb.append("https://www.okx.com/api/v5/market/history-candles")
                .append("?instId=").append(instId)
                .append("&bar=").append(bar)
                .append("&limit=").append(limit);
        if (after != null) sb.append("&after=").append(after);
        return sb.toString();
    }

    static List<Candle> fetchLatestCandles(String bar, int limit) throws Exception {
        if (!USE_DB_CACHE) return fetchLatestCandlesNet(bar, limit);
        if (STRICT_SSOT) {
            CandleDb.init();
            return CandleDb.loadLatest(INST_ID, bar, limit, true);
        }
        long nowMs = System.currentTimeMillis();
        // 拉取器：先用 latest endpoint 增量写 DB（SSOT）
        CandleDb.ingestLatest(INST_ID, bar, Math.max(10, limit), nowMs);
        // 引擎：再只读 DB
        return CandleDb.loadLatest(INST_ID, bar, limit, true);
    }

    static List<Candle> fetchLatestCandlesNet(String bar, int limit) throws Exception {
        String url = buildLatestCandlesUrl(INST_ID, bar, limit);
        String json = fetch(url);
        return parseCandles(json);
    }

    static String buildLatestCandlesUrl(String instId, String bar, int limit) {
        return "https://www.okx.com/api/v5/market/candles"
                + "?instId=" + instId
                + "&bar=" + bar
                + "&limit=" + limit;
    }

    static String fetch(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("User-Agent", "Mozilla/5.0")
                .GET()
                .build();

        int retries = 6;
        long sleep = 260;

        for (int i = 0; i <= retries; i++) {
            try {
                HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) return resp.body();

                if (resp.statusCode() == 429 || resp.statusCode() >= 500) {
                    Thread.sleep(sleep);
                    sleep = Math.min(3000, sleep * 2);
                    continue;
                }
                throw new IOException("HTTP " + resp.statusCode() + " body=" + resp.body());
            } catch (IOException ex) {
                if (i == retries) throw ex;
                Thread.sleep(sleep);
                sleep = Math.min(3000, sleep * 2);
            }
        }
        throw new IOException("fetch failed: " + url);
    }

    static List<Candle> parseCandles(String json) {
        JSONObject jo = new JSONObject(json);
        JSONArray data = jo.getJSONArray("data");
        List<Candle> out = new ArrayList<>(data.length());

        for (int i = 0; i < data.length(); i++) {
            JSONArray a = data.getJSONArray(i);

            long ts = Long.parseLong(a.getString(0));
            double o = Double.parseDouble(a.getString(1));
            double h = Double.parseDouble(a.getString(2));
            double l = Double.parseDouble(a.getString(3));
            double c = Double.parseDouble(a.getString(4));

            double vol = 0.0;
            if (a.length() >= 6) {
                try { vol = Double.parseDouble(a.getString(5)); } catch (Exception ignore) {}
            }

            int confirm = 1;
            if (a.length() >= 9) {
                try { confirm = Integer.parseInt(a.getString(8)); } catch (Exception ignore) {}
            }

            out.add(new Candle(ts, o, h, l, c, vol, confirm));
        }
        return out;
    }

    // ===================== 1H 工具：upperBound =====================
    // ===== 1H 中性判断（给“第二根趋势K”复用）=====
    static boolean isNeutral1H(int i, List<MacdPoint> macd1h) {
        if (i < 0 || i >= macd1h.size()) return false;
        double hist = macd1h.get(i).hist;
        double prev = (i - 1 >= 0) ? macd1h.get(i - 1).hist : 0.0;

        boolean absSmall = Math.abs(hist) < NEUTRAL_ABS_HIST_X;

        boolean signFlip = (i - 1 >= 0) && ((prev < 0 && hist > 0) || (prev > 0 && hist < 0));

        boolean oscNearZero = false;
        if (NEUTRAL_OSC_N > 1 && i - (NEUTRAL_OSC_N - 1) >= 0) {
            boolean allSmall = true;
            int posCnt = 0, negCnt = 0, zeroCnt = 0;
            for (int k = i - (NEUTRAL_OSC_N - 1); k <= i; k++) {
                double h = macd1h.get(k).hist;
                if (Math.abs(h) >= NEUTRAL_ABS_HIST_X) allSmall = false;
                if (h > 0) posCnt++;
                else if (h < 0) negCnt++;
                else zeroCnt++;
            }
            oscNearZero = allSmall && posCnt > 0 && negCnt > 0;
        }

        return absSmall || oscNearZero || signFlip;
    }

    static Trend1H trend1HAt(int i, List<MacdPoint> macd1h) {
        if (isNeutral1H(i, macd1h)) return Trend1H.中性;
        double hist = macd1h.get(i).hist;
        return (hist >= 0) ? Trend1H.多 : Trend1H.空;
    }

    static int upperBoundCandle(List<Candle> list, long ts) {
        int lo = 0, hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (list.get(mid).ts <= ts) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    // ======================================================================
    // Pool-2：投票机实现（与均匀盈利评估引擎保持一致的口径）
    // ======================================================================

    static Pool2VoteResult evalPool2Vote(List<Candle> m30, List<MacdPoint> macd30, int sigIdx, Side side,
                                         boolean silent, String phase) {
        if (!USE_POOL2_VOTE_MACHINE) return null;
        if (side == null && !USE_POOL2_ENTRY_FILTER) return null; // 不需要时不计算

        try {
            Pool2VoteResult r = POOL2.eval(m30, macd30, sigIdx);
            // Pool-2 诊断累加：仅在 side!=null（实际参与过滤决策）时记录，由开关控制
            if (PRINT_POOL2_DIAG && side != null) {
                Pool2DiagStats.record(r, m30.get(sigIdx).ts);
            }
            if (!silent && PRINT_POOL2_ON_SIGNAL && side != null) {
                System.out.printf("  [POOL2][%s] %s ts=%s score=%d dir=%s parts=%s\n",
                        phase, side, FMT_JST_SHORT.format(Instant.ofEpochMilli(m30.get(sigIdx).ts)), r.score, r.dirText(), r.partsText());
            }
            return r;
        } catch (Exception e) {
            if (!silent) {
                System.out.println("[POOL2] 计算异常：" + e.getMessage());
            }
            return null;
        }
    }

    static class Pool2VoteResult {
        int score;
        int dir; // 1=LONG, -1=SHORT, 0=FLAT

        int dSqueeze;
        int dCvd;
        int dEma200;
        int dFisher;
        int dMacd;
        int dStochRsi;
        int dRsiRev;

        // === EXTRA: TSRND_OVERALL_COMBO5: MA60+BOLL20_BREAK+ROC12_thr0.01+CCI20_thr100+BBWidth20_thr0.03 (bbWidth作为门槛，不参与方向)
        int dMa60;
        int dBollBreak;
        int dRoc12;
        int dCci20;
        int dBbWidth; // 1=宽度>=阈值, 0=不足


        boolean matchSide(Side side) {
            if (dir == 0) return false;
            return (side == Side.LONG && dir == 1) || (side == Side.SHORT && dir == -1);
        }

        String dirText() {
            return dir > 0 ? "LONG" : (dir < 0 ? "SHORT" : "FLAT");
        }

        String partsText() {
            return String.format("SQ=%+d CVD=%+d EMA200=%+d FISH=%+d MACD=%+d StochRSI=%+d RSIrev=%+d | MA60=%+d BOLLbr=%+d ROC12=%+d CCI=%+d BBW=%+d",
                    dSqueeze, dCvd, dEma200, dFisher, dMacd, dStochRsi, dRsiRev,
                    dMa60, dBollBreak, dRoc12, dCci20, dBbWidth);
        }

        String toOneLine() {
            return String.format("score=%d dir=%s [%s]", score, dirText(), partsText());
        }
    }

    // ========== Pool-2 诊断累加器（由 PRINT_POOL2_DIAG 控制是否使用）==========
    // 仅在 side!=null（Pool-2 实际参与过滤决策）时累加，回测结束打印。零行为影响。
    static final class Pool2DiagStats {
        // A) score 分布：key=score 值，value=出现次数
        static final java.util.TreeMap<Integer, Integer> scoreHist = new java.util.TreeMap<>();
        static long total = 0;

        // B) 成员投票行为：每个成员的 +1 / -1 / 0 次数，以及与最终 dir 一致的次数
        static final String[] MEMBERS = {
                "SQUEEZE", "CVD", "EMA200", "FISHER", "MACD", "StochRSI",
                "RSI_rev", "MA60", "BOLLbr", "ROC12", "CCI"
        };
        static final int M = MEMBERS.length;
        static final long[] cntPos = new long[M];
        static final long[] cntNeg = new long[M];
        static final long[] cntZero = new long[M];
        static final long[] cntAgreeDir = new long[M]; // 该成员投票方向与最终 dir 一致（且 dir!=0）的次数
        static long dirNonZero = 0;                     // 最终 dir!=0（即 Pool-2 给出明确方向）的次数

        static synchronized void record(Pool2VoteResult r) {
            record(r, 0L);
        }

        // 统计D：按月累加每个成员的投多/投空/投0次数。key=yyyy-MM
        static final java.util.TreeMap<String, long[][]> monthlyVotes = new java.util.TreeMap<>();
        // long[member][0]=投多 [1]=投空 [2]=投0
        static final DateTimeFormatter YM_D = DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZONE);

        static synchronized void record(Pool2VoteResult r, long tsMs) {
            if (r == null) return;
            total++;
            scoreHist.merge(r.score, 1, Integer::sum);

            int[] d = {
                    r.dSqueeze, r.dCvd, r.dEma200, r.dFisher, r.dMacd, r.dStochRsi,
                    r.dRsiRev, r.dMa60, r.dBollBreak, r.dRoc12, r.dCci20
            };
            boolean dirSet = (r.dir != 0);
            if (dirSet) dirNonZero++;
            for (int j = 0; j < M; j++) {
                if (d[j] > 0) cntPos[j]++;
                else if (d[j] < 0) cntNeg[j]++;
                else cntZero[j]++;
                if (dirSet && Integer.signum(d[j]) == r.dir) cntAgreeDir[j]++;
            }

            // 统计D：月度投票分布（仅当传入了有效时间戳）
            if (tsMs > 0) {
                String mon = YM_D.format(Instant.ofEpochMilli(tsMs));
                long[][] mv = monthlyVotes.computeIfAbsent(mon, k -> new long[M][3]);
                for (int j = 0; j < M; j++) {
                    if (d[j] > 0) mv[j][0]++;
                    else if (d[j] < 0) mv[j][1]++;
                    else mv[j][2]++;
                }
            }
        }

        static void print() {
            if (total == 0) {
                System.out.println("[POOL2-DIAG] 无样本（回测期间 side!=null 的 Pool-2 评估为 0 次）");
                return;
            }
            System.out.println();
            System.out.println("========== Pool-2 诊断（共 " + total + " 次参与过滤）==========");

            // ---- A) score 分布 ----
            System.out.println("--- A) score 分布（| 标记 ±" + POOL2_TH + " 阈值线）---");
            int loseCntBelow = 0; // 落在 (-TH, +TH) 被判 FLAT 拦截的次数
            for (java.util.Map.Entry<Integer, Integer> e : scoreHist.entrySet()) {
                int sc = e.getKey();
                int c = e.getValue();
                double pct = c * 100.0 / total;
                String gate = (sc >= POOL2_TH) ? " →LONG" : (sc <= -POOL2_TH ? " →SHORT" : " (FLAT拦截)");
                if (sc > -POOL2_TH && sc < POOL2_TH) loseCntBelow += c;
                int barLen = (int) Math.round(pct / 2.0); // 每 2% 一个 #
                StringBuilder bar = new StringBuilder();
                for (int b = 0; b < barLen; b++) bar.append('#');
                System.out.printf(Locale.US, "  score=%+3d  %6d  %5.1f%%  %s%s%n", sc, c, pct, bar.toString(), gate);
            }
            System.out.printf(Locale.US, "  → 被 FLAT 拦截(|score|<%d)合计: %d 次 (%.1f%%)%n",
                    POOL2_TH, loseCntBelow, loseCntBelow * 100.0 / total);

            // ---- B) 成员投票行为 ----
            System.out.println("--- B) 成员投票行为（+1/-1/0 次数 + 与最终dir一致率）---");
            System.out.printf(Locale.US, "%-10s %8s %8s %8s %8s %10s%n",
                    "成员", "+1", "-1", "投0", "投0占比", "一致率");
            for (int j = 0; j < M; j++) {
                double zeroPct = cntZero[j] * 100.0 / total;
                double agreePct = (dirNonZero == 0) ? 0.0 : cntAgreeDir[j] * 100.0 / dirNonZero;
                System.out.printf(Locale.US, "%-10s %8d %8d %8d %7.1f%% %9.1f%%%n",
                        MEMBERS[j], cntPos[j], cntNeg[j], cntZero[j], zeroPct, agreePct);
            }
            System.out.println("  注：投0占比高=划水(很少参与决策)；一致率高≠好(可能是应声虫)，低≠坏(可能是有用的否决者)。");

            // ---- D) 成员月度投票分布（专查"失衡是不是行情造成的"）----
            if (!monthlyVotes.isEmpty()) {
                System.out.println("--- D) 成员月度投票分布（每月 投多/投空 比例，看失衡随行情怎么变）---");
                System.out.println("    每格式：多%/空%（投0已省略）。某成员若某月偏多、某月平衡 → 失衡是行情造成，非指标缺陷。");
                // 表头
                StringBuilder h = new StringBuilder(String.format(Locale.US, "%-9s", "月份"));
                for (String m : MEMBERS) h.append(String.format(Locale.US, " %11s", m));
                System.out.println(h.toString());
                for (Map.Entry<String, long[][]> e : monthlyVotes.entrySet()) {
                    long[][] mv = e.getValue();
                    StringBuilder row = new StringBuilder(String.format(Locale.US, "%-9s", e.getKey()));
                    for (int j = 0; j < M; j++) {
                        long pos = mv[j][0], neg = mv[j][1], zero = mv[j][2];
                        long tot = pos + neg + zero;
                        if (tot == 0) { row.append(String.format(Locale.US, " %11s", "·")); continue; }
                        double pp = pos * 100.0 / tot, np = neg * 100.0 / tot;
                        row.append(String.format(Locale.US, " %5.0f/%-5.0f", pp, np));
                    }
                    System.out.println(row.toString());
                }
                System.out.println("    用法：重点看 StochRSI 列——若上涨月份多%高、震荡月份趋于平衡，则7:1失衡=行情真相。");
            }

            System.out.println("====================================================");
        }
    }

    static class Pool2VoteMachine {
        // 缓存：当 m30 序列变化时自动重建
        private int lastSize = -1;
        private long lastEndTs = Long.MIN_VALUE;

        private double[] ema200;
        private double[] ema20;
        private double[] rsi14;
        private double[] stochRsi14_14; // 注意：与评估引擎一致（0~100），但 ConditionFactory 用 0.8/0.2（这是原实现口径）
        private double[] fisher10;
        private double[] squeeze20_20; // 1=挤压中，0=非挤压

        // === EXTRA: TSRND_OVERALL_COMBO5 需要的预计算序列（全部基于闭合K的历史数据）
        private double[] ma60;
        private double[] bollMid20;
        private double[] bollUpper20;
        private double[] bollLower20;
        private double[] bbWidth20;
        private double[] roc12;
        private double[] cci20;


        // ✅ CVD：从数据库预计算的符号方向序列（替代原来的反射读取）
        private int[] cvdSign;

        //  CVD（可选）：按“数据库字段/对象字段 cvd”读取。
        // - 若你的 Candle / DB 不含 cvd：自动当作不可用（贡献=0），不报错。
        // - 这样也符合你说的：模拟系统拉不到 cvd，就直接不给。

        private List<Candle> ref;

        Pool2VoteResult eval(List<Candle> m30, List<MacdPoint> macd30, int idx) {
            ensure(m30);
            Pool2VoteResult r = new Pool2VoteResult();

            if (idx <= 0 || idx >= m30.size()) {
                r.dir = 0;
                r.score = 0;
                return r;
            }

            // 1) SQUEEZE20_20: 挤压中 -> 0；非挤压 -> close vs EMA20
            r.dSqueeze = 0;
            if (squeeze20_20 != null && idx < squeeze20_20.length && squeeze20_20[idx] > 0.5) {
                r.dSqueeze = 0;
            } else {
                double close = m30.get(idx).c;
                double e = (ema20 != null && idx < ema20.length) ? ema20[idx] : Double.NaN;
                if (!Double.isNaN(e)) {
                    r.dSqueeze = close >= e ? 1 : -1;
                }
            }

            // 2) CVD_sign: 从预计算的数据库序列中读取
            r.dCvd = 0;
            if (cvdSign != null && idx < cvdSign.length) {
                r.dCvd = cvdSign[idx];
            }

            // 3) EMA200: close vs EMA200
            r.dEma200 = 0;
            if (ema200 != null && idx < ema200.length) {
                double e200 = ema200[idx];
                if (!Double.isNaN(e200)) {
                    r.dEma200 = m30.get(idx).c >= e200 ? 1 : -1;
                }
            }

            // 4) FISHER_P10: fisher 值的符号（权重 x2）
            r.dFisher = 0;
            if (fisher10 != null && idx < fisher10.length) {
                double f = fisher10[idx];
                r.dFisher = f > 0 ? 1 : (f < 0 ? -1 : 0);
            }

            // 5) MACD12_26_9: dif vs dea
            r.dMacd = 0;
            if (macd30 != null && idx < macd30.size()) {
                MacdPoint p = macd30.get(idx);
                if (p != null) {
                    r.dMacd = p.dif > p.dea ? 1 : (p.dif < p.dea ? -1 : 0);
                }
            }

            // 6) StochRSI14_14: 与评估引擎保持一致：band(0.8/0.2)
            r.dStochRsi = 0;
            if (stochRsi14_14 != null && idx < stochRsi14_14.length) {
                double x = stochRsi14_14[idx];
                if (!Double.isNaN(x)) {
                    r.dStochRsi = x > 0.8 ? 1 : (x < 0.2 ? -1 : 0);
                }
            }

            // 7) RSI14_rev30_70: 反转（权重 x2）
            r.dRsiRev = 0;
            if (rsi14 != null && idx < rsi14.length) {
                double x = rsi14[idx];
                if (!Double.isNaN(x)) {
                    r.dRsiRev = x < 30 ? 1 : (x > 70 ? -1 : 0);
                }
            }

            // 8) PS_TSOUP_96 已删除（诊断统计证实：10697次评估中投票0次，纯死票）

            // 9) TSRND_OVERALL_COMBO5: MA60+BOLL20_BREAK+ROC12_thr0.01+CCI20_thr100+BBWidth20_thr0.03
            r.dBbWidth = 0;
            r.dMa60 = 0;
            r.dBollBreak = 0;
            r.dRoc12 = 0;
            r.dCci20 = 0;

            double bbw = 0.0;
            if (bbWidth20 != null && idx < bbWidth20.length) {
                bbw = bbWidth20[idx];
                r.dBbWidth = (bbw >= 0.03 ? 1 : 0);
            }

            // 只有当 BBWidth 满足阈值时，这组组合才开始贡献方向票（符合你挖掘时的组合语义）
            if (r.dBbWidth != 0) {
                Candle c = m30.get(idx);
                // MA60: close 在均线之上=多, 之下=空
                if (ma60 != null && idx < ma60.length) {
                    r.dMa60 = (c.c >= ma60[idx] ? 1 : -1);
                }
                // BOLL20_BREAK：突破上轨=多，跌破下轨=空，否则=0
                if (bollUpper20 != null && bollLower20 != null && idx < bollUpper20.length && idx < bollLower20.length) {
                    if (c.c > bollUpper20[idx]) r.dBollBreak = 1;
                    else if (c.c < bollLower20[idx]) r.dBollBreak = -1;
                    else r.dBollBreak = 0;
                }
                // ROC12_thr0.01：超过阈值=方向票
                if (roc12 != null && idx < roc12.length) {
                    double v = roc12[idx];
                    if (v > 0.01) r.dRoc12 = 1;
                    else if (v < -0.01) r.dRoc12 = -1;
                    else r.dRoc12 = 0;
                }
                // CCI20_thr100：超过阈值=方向票
                if (cci20 != null && idx < cci20.length) {
                    double v = cci20[idx];
                    if (v > 100.0) r.dCci20 = 1;
                    else if (v < -100.0) r.dCci20 = -1;
                    else r.dCci20 = 0;
                }
            }


            // EXTRA FAILM_COMBO8 已删除（诊断证实：10697次评估投票0次，'8指标全同向'条件从未满足，纯死票）

            int score = 0;
            score += POOL2_WT_SQUEEZE  * r.dSqueeze;
            score += POOL2_WT_CVD      * r.dCvd;
            score += POOL2_WT_EMA200   * r.dEma200;
            score += POOL2_WT_FISHER   * r.dFisher;
            score += POOL2_WT_MACD     * r.dMacd;
            score += POOL2_WT_STOCHRSI * r.dStochRsi;
            score += POOL2_WT_RSI_REV  * r.dRsiRev;

            // EXTRA COMBO5 votes
            score += POOL2_WT_MA60     * r.dMa60;
            score += POOL2_WT_BOLLBR   * r.dBollBreak;
            score += POOL2_WT_ROC12    * r.dRoc12;
            score += POOL2_WT_CCI      * r.dCci20;

            r.score = score;
            if (score >= POOL2_TH) r.dir = 1;
            else if (score <= -POOL2_TH) r.dir = -1;
            else r.dir = 0; // FLAT

            return r;
        }

        void ensure(List<Candle> m30) {
            if (m30 == null || m30.isEmpty()) return;
            long endTs = m30.get(m30.size() - 1).ts;
            if (m30.size() == lastSize && endTs == lastEndTs) return;

            this.ref = m30;
            this.lastSize = m30.size();
            this.lastEndTs = endTs;

            // 基础序列预计算（不引入未来数据：每个点只用 <= 当前 idx）
            this.ema200 = calcEmaArray(m30, 200);
            this.ema20 = calcEmaArray(m30, 20);
            this.rsi14 = p2_rsiWilder(m30, 14);
            this.stochRsi14_14 = p2_stochRsi(m30, 14, 14);
            this.fisher10 = p2_fisherPrice(m30, 10);
            this.squeeze20_20 = p2_squeeze(m30, 20, 2.0, 20, 1.5);


            //  不再从外部文件加载 CVD：只按 Candle/DB 的 cvd 字段读取。
            // ✅ 新增：从数据库读取 CVD（替代原来的反射读取）
            this.cvdSign = p2_cvdSignFromDb(m30, INST_ID, "30m");
            // === EXTRA: TSRND_OVERALL_COMBO5 序列（全部仅使用 <= 当前 idx 的闭合K）
            this.ma60 = calcSmaArray(m30, 60);
            double[][] boll = calcBollingerBands(m30, 20, 2.0);
            this.bollMid20 = boll[0];
            this.bollUpper20 = boll[1];
            this.bollLower20 = boll[2];
            this.bbWidth20 = boll[3];
            this.roc12 = calcRocArray(m30, 12);
            this.cci20 = calcCciArray(m30, 20);

            // EXTRA FAILM_COMBO8 构建段已删除（该票为纯死票，相关8个dir序列一并移除）
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private static int[] safeDir(PoolCondition pc, List evalCandles, int total) throws Exception {
            // 反射调用：避免 Candle 类型不匹配导致编译报错
            // 修复：ConditionFactory 常用匿名内部类（非 public 类）时，反射 invoke 可能触发 IllegalAccessException。
            Method m;
            try {
                m = pc.getClass().getMethod("dir", List.class, int.class, int.class);
            } catch (NoSuchMethodException e) {
                m = pc.getClass().getDeclaredMethod("dir", List.class, int.class, int.class);
            }
            try { m.setAccessible(true); } catch (Throwable ignore) {}
            Object arr = m.invoke(pc, evalCandles, 0, total);
            if (arr instanceof int[]) return (int[]) arr;
            // 兜底：如果返回不是 int[]，直接当全0
            return new int[total];
        }


    }

    /**
     * Pool-2：安全读取 Candle 的 cvd 字段（可选）。
     * - 不引用任何编译期字段名，避免“没有 cvd 字段就编译失败”。
     * - 若不存在/取不到：返回 NaN（上层会当作贡献=0）。
     */
    /**
     * @deprecated 已废弃：CVD 现在从数据库 okx_ext_features 表读取
     * Pool-2：安全读取 Candle 的 cvd 字段（可选）。
     * - 保留此函数仅作备用，不再主动调用
     */
    @Deprecated
    static double p2_readCvdSafe(Object candle) {
        if (candle == null) return Double.NaN;
        try {
            Field f = candle.getClass().getDeclaredField("cvd");
            f.setAccessible(true);
            Object v = f.get(candle);
            if (v instanceof Number) return ((Number) v).doubleValue();
        } catch (Exception ignore) {
        }
        try {
            Method m = candle.getClass().getMethod("getCvd");
            Object v = m.invoke(candle);
            if (v instanceof Number) return ((Number) v).doubleValue();
        } catch (Exception ignore) {
        }
        return Double.NaN;
    }

    // =====================
    // Pool-2：直接从 SQLite(okx_candles.db) 读取 funding，生成 fundingExtreme 的方向数组
    // - 表：okx_ext_features(inst_id, bar, ts, funding, ...)
    // - 对齐：按 Candle.ts（openTs）精确对齐同一根 30m K
    // - 口径：与 ConditionFactory.fundingExtreme 保持一致：
    //         funding > thrAbs => -1（偏空），funding < -thrAbs => +1（偏多），否则 => 0；缺失 => 2
    // - 无未来函数：对每根K只读取该K自己的 funding 值；不引用 i+1
    // =====================
    static int[] p2_fundingExtremeDirFromDb(List<Candle> candles, String instId, String bar, double thrAbs) {
        int n = (candles == null ? 0 : candles.size());
        int[] out = new int[n];
        Arrays.fill(out, 2); // 2 表示 MISSING（缺失）；0 表示有数据但未达阈值
        if (n == 0) return out;

        // 只拉本次 candles 覆盖的 ts 区间，批量查询一次
        long tsFrom = candles.get(0).ts;
        long tsTo = candles.get(n - 1).ts;

        HashMap<Long, Double> map = new HashMap<>(n * 2);

        // 允许用户通过参数覆盖外部特征库路径；默认就是 candles DB（okx_candles.db）
        String extDb = sysStr("okx.extDb", DB_FILE_RAW);
        String extDbCanon = canonicalPath(extDb);

        String sql = "select ts, funding from okx_ext_features where inst_id=? and bar=? and ts>=? and ts<=?";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + extDbCanon);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, instId);
            ps.setString(2, bar);
            ps.setLong(3, tsFrom);
            ps.setLong(4, tsTo);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long ts = rs.getLong(1);
                    double v = rs.getDouble(2);
                    if (rs.wasNull()) continue;
                    if (Double.isFinite(v)) map.put(ts, v);
                }
            }
        } catch (Exception e) {
            // 查询失败：全部当缺失，不影响主逻辑；可选打印
            if (DEBUG_FILTER_TRACE) {
                System.out.println("[POOL2][FAILM8][WARN] funding query failed: " + e.getMessage());
            }
            return out;
        }

        for (int i = 0; i < n; i++) {
            Double v = map.get(candles.get(i).ts);
            if (v == null || !Double.isFinite(v)) { out[i] = 2; continue; }
            if (v > thrAbs) out[i] = -1;
            else if (v < -thrAbs) out[i] = 1;
            else out[i] = 0;
        }
        return out;
    }

    // =====================
    // EXIT_COMBO7（严格按 Miner/ConditionFactory 口径）
    // 组合：ENV_SMA_20_0.02_BREAK + RSI14_rev30_70 + StochRSI14_14 + VWAP_D + BRAR_26 + PS_PIVOT_6_6 + PS_RANGE_96_0.015
    // - 只读 confirm=1 的已收盘 K（上层已保证 m30 来源）
    // - 无未来函数：只用 [0..i] 的历史（dir 计算到 size）
    // - 触发规则完全复刻 ExitDiscriminativeIndicatorsMain：
    //   strength = Σ condStrengthAt(i, lookback=12)，其中 condStrengthAt 为最近 12 根 dir 的累加
    //   L = lookback * k (=12*7=84)
    //   bin = to5Bin(strength, L)
    //   LONG 持仓：bin<=1 触发出场；SHORT 持仓：bin>=3 触发出场
    // =====================
    static final class ExitCombo7Miner {

        static final int LOOKBACK = 12;
        static final int K = 7; // combo7
        static final int L = LOOKBACK * K;

        static final String REASON =
                "EXIT_COMBO7(miner):ENV_SMA_20_0.02_BREAK+RSI14_rev30_70+StochRSI14_14+VWAP_D+BRAR_26+PS_PIVOT_6_6+PS_RANGE_96_0.015";
        static final String REASON_DETAIL =
                "EXIT_COMBO7（ENV_BREAK+RSI_rev+StochRSI+VWAP_D+BRAR+Pivot+Range）=> 当前收盘出";

        // 按你的习惯做一个“轻量缓存”：同一根 m30 列表在回测内会被反复查询，避免每根 K 重新算全量 dir。
        // 实盘 runOnce 里 m30 可能每次都重建 List，这个缓存命中率低，但不影响正确性。
        static volatile SeriesCache LAST = null;

        static boolean shouldExit(List<Candle> m30, int i, Side posSide, String tag) {
            try {
                if (m30 == null || m30.isEmpty()) return false;
                if (i < 0 || i >= m30.size()) return false;
                if (posSide == null) return false;

                SeriesCache sc = ensureSeries(m30, tag);
                if (sc == null || sc.size != m30.size()) return false;
                int bin = sc.binAt(i);
                if (posSide == Side.LONG) return bin <= 1;
                else return bin >= 3;
            } catch (Throwable t) {
                // 出场兜底：宁可不触发，也不要影响主流程
                return false;
            }
        }

        static SeriesCache ensureSeries(List<Candle> m30, String tag) {
            long lastTs = m30.get(m30.size() - 1).ts;
            int sz = m30.size();
            SeriesCache sc = LAST;
            if (sc != null && sc.key == m30 && sc.size == sz && sc.lastTs == lastTs) return sc;

            synchronized (ExitCombo7Miner.class) {
                sc = LAST;
                if (sc != null && sc.key == m30 && sc.size == sz && sc.lastTs == lastTs) return sc;

                SeriesCache built = buildSeries(m30, tag);
                LAST = built;
                return built;
            }
        }

        static SeriesCache buildSeries(List<Candle> m30, String tag) {
            try {
                // 这里用你文件里已经存在的 eval 适配器 + ConditionFactory，保证与 miner 100% 同源
                List<Object> evalCandles = EvalCandleAdapter.toEvalCandles(m30);
                if (evalCandles == null || evalCandles.isEmpty()) return null;

                @SuppressWarnings({"rawtypes", "unchecked"})
                List evalList = (List) evalCandles;

                PoolCondition pcEnv   = ConditionFactory.envelopeBandBreakSma(20, 0.02);
                PoolCondition pcRsi   = ConditionFactory.rsiRevert(14, 30, 70);
                PoolCondition pcStoch = ConditionFactory.stochRsiBand(14, 14, 0.8, 0.2);
                PoolCondition pcVwap  = ConditionFactory.vwapDaily();
                PoolCondition pcBrar  = ConditionFactory.brarArBr(26);
                PoolCondition pcPivot = ConditionFactory.psPivot(6, 6);
                PoolCondition pcRange = ConditionFactory.psRange(96, 0.015);

                int total = evalList.size();

                int[] dEnv   = safeDir(pcEnv,   evalList, total);
                int[] dRsi   = safeDir(pcRsi,   evalList, total);
                int[] dStoch = safeDir(pcStoch, evalList, total);
                int[] dVwap  = safeDir(pcVwap,  evalList, total);
                int[] dBrar  = safeDir(pcBrar,  evalList, total);
                int[] dPivot = safeDir(pcPivot, evalList, total);
                int[] dRange = safeDir(pcRange, evalList, total);

                return new SeriesCache(m30, total, m30.get(total - 1).ts,
                        dEnv, dRsi, dStoch, dVwap, dBrar, dPivot, dRange
                );
            } catch (Throwable t) {
                System.out.println("【EXIT_COMBO7】构建指标序列失败(" + tag + "): " + t.getMessage());
                return null;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        static int[] safeDir(PoolCondition pc, List evalCandles, int total) throws Exception {
            int[] d = pc.dir(evalCandles, 0, total);
            if (d == null || d.length != total) {
                int[] z = new int[total];
                return z;
            }
            return d;
        }

        static final class SeriesCache {
            final List<Candle> key;
            final int size;
            final long lastTs;
            final int[] env, rsi, stoch, vwap, brar, pivot, range;

            SeriesCache(List<Candle> key, int size, long lastTs,
                        int[] env, int[] rsi, int[] stoch, int[] vwap, int[] brar, int[] pivot, int[] range) {
                this.key = key; this.size = size; this.lastTs = lastTs;
                this.env = env; this.rsi = rsi; this.stoch = stoch; this.vwap = vwap; this.brar = brar; this.pivot = pivot; this.range = range;
            }

            int binAt(int idx) {
                int strength = strengthAt(env, idx) + strengthAt(rsi, idx) + strengthAt(stoch, idx)
                        + strengthAt(vwap, idx) + strengthAt(brar, idx) + strengthAt(pivot, idx) + strengthAt(range, idx);
                return to5Bin(strength, L);
            }

            int strengthAt(int[] d, int idx) {
                int l = Math.max(0, idx - LOOKBACK + 1);
                int s = 0;
                for (int i = l; i <= idx; i++) s += d[i];
                return s;
            }
        }

        static int to5Bin(int strength, int L) {
            if (L <= 0) return 2;
            double x = strength * 1.0 / L;
            if (x <= -0.6) return 0;
            if (x <= -0.2) return 1;
            if (x < 0.2) return 2;
            if (x < 0.6) return 3;
            return 4;
        }
    }
    // =====================
    // EXIT_COMBO2（严格按 Miner/ConditionFactory 口径）
    // 组合：StochRSI14_14 + PS_DON_96
    // - 只读 confirm=1 的已收盘 K（上层已保证 m30 来源）
    // - 无未来函数：只用 [0..i] 的历史
    // - 触发规则完全复刻 ExitDiscriminativeIndicatorsMain：
    //   strength = Σ condStrengthAt(i, lookback=12)，其中 condStrengthAt 为最近 12 根 dir 的累加
    //   L = lookback * k (=12*2=24)
    //   bin = to5Bin(strength, L)
    //   LONG 持仓：bin<=1 触发出场；SHORT 持仓：bin>=3 触发出场
    //
    // 备注：该组合为 LG0（盈利也允许触发），因此这里不做“浮亏门禁”。
    // =====================
    static final class ExitCombo2Miner {

        static final int LOOKBACK = 12;
        static final int K = 2; // combo2
        static final int L = LOOKBACK * K;

        static final String REASON =
                "EXIT_COMBO2(miner):StochRSI14_14+PS_DON_96";
        static final String REASON_DETAIL =
                "EXIT_COMBO2（StochRSI+Donchian）=> 当前收盘出";

        static volatile SeriesCache LAST = null;

        static boolean shouldExit(List<Candle> m30, int i, Side posSide, String tag) {
            try {
                if (m30 == null || m30.isEmpty()) return false;
                if (i < 0 || i >= m30.size()) return false;
                if (posSide == null) return false;

                SeriesCache sc = ensureSeries(m30, tag);
                if (sc == null || sc.size != m30.size()) return false;
                int bin = sc.binAt(i);
                if (posSide == Side.LONG) return bin <= 1;
                else return bin >= 3;
            } catch (Throwable t) {
                return false;
            }
        }

        static SeriesCache ensureSeries(List<Candle> m30, String tag) {
            long lastTs = m30.get(m30.size() - 1).ts;
            int sz = m30.size();
            SeriesCache sc = LAST;
            if (sc != null && sc.key == m30 && sc.size == sz && sc.lastTs == lastTs) return sc;

            synchronized (ExitCombo2Miner.class) {
                sc = LAST;
                if (sc != null && sc.key == m30 && sc.size == sz && sc.lastTs == lastTs) return sc;

                SeriesCache built = buildSeries(m30, tag);
                LAST = built;
                return built;
            }
        }

        static SeriesCache buildSeries(List<Candle> m30, String tag) {
            try {
                List<Object> evalCandles = EvalCandleAdapter.toEvalCandles(m30);
                if (evalCandles == null || evalCandles.isEmpty()) return null;

                @SuppressWarnings({"rawtypes", "unchecked"})
                List evalList = (List) evalCandles;

                PoolCondition pcStoch = ConditionFactory.stochRsiBand(14, 14, 0.8, 0.2);
                PoolCondition pcDon   = ConditionFactory.psDonchian(96);

                int total = evalList.size();

                int[] dStoch = safeDir(pcStoch, evalList, total);
                int[] dDon   = safeDir(pcDon,   evalList, total);

                return new SeriesCache(m30, total, m30.get(total - 1).ts, dStoch, dDon);
            } catch (Throwable t) {
                System.out.println("【EXIT_COMBO2】构建指标序列失败(" + tag + "): " + t.getMessage());
                return null;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        static int[] safeDir(PoolCondition pc, List evalCandles, int total) throws Exception {
            int[] d = pc.dir(evalCandles, 0, total);
            if (d == null || d.length != total) {
                return new int[total];
            }
            return d;
        }

        static final class SeriesCache {
            final List<Candle> key;
            final int size;
            final long lastTs;
            final int[] stoch, don;

            SeriesCache(List<Candle> key, int size, long lastTs, int[] stoch, int[] don) {
                this.key = key; this.size = size; this.lastTs = lastTs;
                this.stoch = stoch; this.don = don;
            }

            int binAt(int idx) {
                int strength = strengthAt(stoch, idx) + strengthAt(don, idx);
                return to5Bin(strength, L);
            }

            int strengthAt(int[] d, int idx) {
                int l = Math.max(0, idx - LOOKBACK + 1);
                int s = 0;
                for (int i = l; i <= idx; i++) s += d[i];
                return s;
            }
        }

        static int to5Bin(int strength, int L) {
            if (L <= 0) return 2;
            double x = strength * 1.0 / L;
            if (x <= -0.6) return 0;
            if (x <= -0.2) return 1;
            if (x < 0.2) return 2;
            if (x < 0.6) return 3;
            return 4;
        }
    }




    // =====================
    // Pool-2 指标实现（与评估引擎口径对齐）
    // =====================

    // RSI (Wilder)
    static double[] p2_rsiWilder(List<Candle> cs, int n) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        if (len <= n) return out;

        double gain = 0, loss = 0;
        for (int i = 1; i <= n; i++) {
            double ch = cs.get(i).c - cs.get(i - 1).c;
            if (ch >= 0) gain += ch; else loss -= ch;
        }
        gain /= n;
        loss /= n;

        double rs = (loss == 0) ? Double.POSITIVE_INFINITY : (gain / loss);
        out[n] = 100.0 - (100.0 / (1.0 + rs));

        for (int i = n + 1; i < len; i++) {
            double ch = cs.get(i).c - cs.get(i - 1).c;
            double g = ch > 0 ? ch : 0;
            double l = ch < 0 ? -ch : 0;
            gain = (gain * (n - 1) + g) / n;
            loss = (loss * (n - 1) + l) / n;
            rs = (loss == 0) ? Double.POSITIVE_INFINITY : (gain / loss);
            out[i] = 100.0 - (100.0 / (1.0 + rs));
        }
        return out;
    }

    // StochRSI：与评估引擎一致（输出 0~100）
    static double[] p2_stochRsi(List<Candle> cs, int rsiN, int stochN) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        double[] rsi = p2_rsiWilder(cs, rsiN);
        if (len <= rsiN + stochN) return out;

        for (int i = 0; i < len; i++) {
            if (Double.isNaN(rsi[i])) continue;
            int start = i - stochN + 1;
            if (start < 0) continue;
            double lo = Double.POSITIVE_INFINITY;
            double hi = Double.NEGATIVE_INFINITY;
            for (int j = start; j <= i; j++) {
                double v = rsi[j];
                if (Double.isNaN(v)) { lo = Double.NaN; break; }
                if (v < lo) lo = v;
                if (v > hi) hi = v;
            }
            if (Double.isNaN(lo)) continue;
            double denom = (hi - lo);
            out[i] = denom == 0 ? 0 : ((rsi[i] - lo) / denom) * 100.0;
        }
        return out;
    }

    // Fisher Transform（基于 close）
    static double[] p2_fisherPrice(List<Candle> cs, int n) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        if (len <= n) return out;

        double prevV = 0;
        double prevF = 0;
        for (int i = 0; i < len; i++) {
            if (i < n) continue;
            double lo = Double.POSITIVE_INFINITY;
            double hi = Double.NEGATIVE_INFINITY;
            for (int j = i - n + 1; j <= i; j++) {
                double x = cs.get(j).c;
                if (x < lo) lo = x;
                if (x > hi) hi = x;
            }
            double x = cs.get(i).c;
            double v = 0;
            if (hi != lo) {
                v = 0.33 * (2.0 * ((x - lo) / (hi - lo)) - 1.0) + 0.67 * prevV;
            } else {
                v = 0.67 * prevV;
            }
            if (v > 0.999) v = 0.999;
            if (v < -0.999) v = -0.999;
            double f = 0.5 * Math.log((1.0 + v) / (1.0 - v)) + 0.5 * prevF;
            out[i] = f;
            prevV = v;
            prevF = f;
        }
        return out;
    }

    // Squeeze（BB vs KC）输出：1=挤压中，0=非挤压
    static double[] p2_squeeze(List<Candle> cs, int bbN, double bbK, int kcN, double kcK) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        if (len <= Math.max(bbN, kcN)) return out;

        double[] sma = p2_smaClose(cs, bbN);
        double[] std = p2_stdClose(cs, bbN);
        double[] atr = p2_atr(cs, kcN);

        for (int i = 0; i < len; i++) {
            if (i < Math.max(bbN, kcN)) continue;
            double basis = sma[i];
            double dev = std[i] * bbK;
            double bbUpper = basis + dev;
            double bbLower = basis - dev;

            double range = atr[i] * kcK;
            double kcUpper = basis + range;
            double kcLower = basis - range;

            boolean squeezeOn = (bbUpper <= kcUpper) && (bbLower >= kcLower);
            out[i] = squeezeOn ? 1.0 : 0.0;
        }
        return out;
    }

    static double[] p2_smaClose(List<Candle> cs, int n) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        if (len < n) return out;

        double sum = 0;
        for (int i = 0; i < len; i++) {
            double x = cs.get(i).c;
            sum += x;
            if (i >= n) sum -= cs.get(i - n).c;
            if (i >= n - 1) out[i] = sum / n;
        }
        return out;
    }

    static double[] p2_stdClose(List<Candle> cs, int n) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        if (len < n) return out;

        double sum = 0, sumSq = 0;
        for (int i = 0; i < len; i++) {
            double x = cs.get(i).c;
            sum += x;
            sumSq += x * x;
            if (i >= n) {
                double y = cs.get(i - n).c;
                sum -= y;
                sumSq -= y * y;
            }
            if (i >= n - 1) {
                double mean = sum / n;
                double var = (sumSq / n) - (mean * mean);
                if (var < 0) var = 0;
                out[i] = Math.sqrt(var);
            }
        }
        return out;
    }

    static double[] p2_atr(List<Candle> cs, int n) {
        int len = cs.size();
        double[] out = new double[len];
        for (int i = 0; i < len; i++) out[i] = Double.NaN;
        if (len == 0) return out;

        double prevAtr = Double.NaN;
        for (int i = 0; i < len; i++) {
            double tr;
            if (i == 0) {
                tr = cs.get(i).h - cs.get(i).l;
            } else {
                double h = cs.get(i).h;
                double l = cs.get(i).l;
                double pc = cs.get(i - 1).c;
                tr = Math.max(h - l, Math.max(Math.abs(h - pc), Math.abs(l - pc)));
            }
            if (i == 0) {
                prevAtr = tr;
                out[i] = tr;
            } else {
                if (Double.isNaN(prevAtr)) prevAtr = tr;
                prevAtr = (prevAtr * (n - 1) + tr) / n;
                out[i] = prevAtr;
            }
        }
        return out;
    }

    // Turtle Soup False Breakout（与评估引擎口径一致）
    static int[] p2_turtleSoupFalseBreakout(List<Candle> cs, int lookback, double reentryFrac) {
        int len = cs.size();
        int[] pos = new int[len];
        for (int i = 0; i < len; i++) pos[i] = 0;
        if (len <= lookback + 1) return pos;

        for (int i = 1; i < len; i++) {
            int start = i - lookback;
            if (start < 0) continue;

            double prevClose = cs.get(i - 1).c;
            double curClose = cs.get(i).c;

            double prevHighN = Double.NEGATIVE_INFINITY;
            double prevLowN = Double.POSITIVE_INFINITY;
            for (int j = start; j <= i - 1; j++) {
                Candle cj = cs.get(j);
                if (cj.h > prevHighN) prevHighN = cj.h;
                if (cj.l < prevLowN) prevLowN = cj.l;
            }

            boolean prevBreakAbove = prevClose > prevHighN;
            boolean prevBreakBelow = prevClose < prevLowN;

            if (prevBreakAbove) {
                double band = prevHighN * reentryFrac;
                if (curClose < prevHighN - band) pos[i] = -1;
            } else if (prevBreakBelow) {
                double band = prevLowN * reentryFrac;
                if (curClose > prevLowN + band) pos[i] = 1;
            }
        }
        return pos;
    }

    /**
     * @deprecated 已废弃：CVD 现在从数据库 okx_ext_features 表读取
     * 外部特征：Long->Double 序列（CVD）
     * - 保留此类仅作备用，不再主动调用
     */
    @Deprecated
    static class LongDoubleSeries {
        final HashMap<Long, Double> map = new HashMap<>();

        Double get(long ts) {
            return map.get(ts);
        }

        static LongDoubleSeries tryLoadCvdDefault() {
            // 你可以把 CVD 数据放在以下任一位置：
            // 1) ./ext/cvd_30m.csv
            // 2) ./ext/ETH-USDT-SWAP_30m_cvd.csv
            // CSV 格式：ts,value（可带表头）
            String[] candidates = new String[]{
                    "./ext/cvd_30m.csv",
                    "./ext/ETH-USDT-SWAP_30m_cvd.csv",
                    "./ext/ETH-USDT-SWAP_30m_CVD.csv"
            };
            for (String p : candidates) {
                LongDoubleSeries s = tryLoadCsv(p);
                if (s != null) return s;
            }
            return null;
        }

        static LongDoubleSeries tryLoadCsv(String path) {
            try {
                Path p = Path.of(path);
                if (!Files.exists(p)) return null;
                LongDoubleSeries s = new LongDoubleSeries();
                try (BufferedReader br = Files.newBufferedReader(p)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        if (line.startsWith("#")) continue;
                        // 跳过表头
                        if (line.toLowerCase().contains("ts") && line.toLowerCase().contains("value")) continue;
                        String[] a = line.split(",");
                        if (a.length < 2) continue;
                        long ts = Long.parseLong(a[0].trim());
                        double v = Double.parseDouble(a[1].trim());
                        s.map.put(ts, v);
                    }
                }
                System.out.println("[POOL2] 已加载外部特征 CVD: " + path + " rows=" + s.map.size());
                return s;
            } catch (Exception e) {
                System.out.println("[POOL2] 加载外部特征失败: " + path + " err=" + e.getMessage());
                return null;
            }
        }
    }

    // =====================
    // Pool-2：直接从 SQLite(okx_candles.db) 读取 CVD，生成符号方向数组
    // - 表：okx_ext_features(inst_id, bar, ts, cvd, ...)
    // - 对齐：按 Candle.ts（openTs）精确对齐同一根 30m K
    // - 口径：cvd[i] - cvd[i-1] 的符号：> 0 => +1（偏多），< 0 => -1（偏空），= 0 => 0；缺失 => 0
    // - 无未来函数：对每根K只读取该K自己的 cvd 值；不引用 i+1
    // =====================
    static int[] p2_cvdSignFromDb(List<Candle> candles, String instId, String bar) {
        int n = (candles == null ? 0 : candles.size());
        int[] out = new int[n];
        Arrays.fill(out, 0); // 0 表示缺失或无变化
        if (n == 0) return out;

        // 只拉本次 candles 覆盖的 ts 区间，批量查询一次
        long tsFrom = candles.get(0).ts;
        long tsTo = candles.get(n - 1).ts;

        HashMap<Long, Double> map = new HashMap<>(n * 2);

        // 使用与 funding 相同的数据库路径配置
        String extDb = sysStr("okx.extDb", DB_FILE_RAW);
        String extDbCanon = canonicalPath(extDb);

        String sql = "select ts, cvd from okx_ext_features where inst_id=? and bar=? and ts>=? and ts<=?";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + extDbCanon);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, instId);
            ps.setString(2, bar);
            ps.setLong(3, tsFrom);
            ps.setLong(4, tsTo);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long ts = rs.getLong(1);
                    double v = rs.getDouble(2);
                    if (rs.wasNull()) continue;
                    if (Double.isFinite(v)) map.put(ts, v);
                }
            }
        } catch (Exception e) {
            // 查询失败：全部当缺失，不影响主逻辑；可选打印
            if (DEBUG_FILTER_TRACE) {
                System.out.println("[POOL2][CVD][WARN] cvd query failed: " + e.getMessage());
            }
            return out;
        }

        // 计算符号：cvd[i] - cvd[i-1]
        for (int i = 0; i < n; i++) {
            Double v = map.get(candles.get(i).ts);
            if (v == null || !Double.isFinite(v)) {
                out[i] = 0;
                continue;
            }

            if (i == 0) {
                // 第一根K无法计算差值，默认为0
                out[i] = 0;
            } else {
                Double vPrev = map.get(candles.get(i - 1).ts);
                if (vPrev == null || !Double.isFinite(vPrev)) {
                    out[i] = 0;
                } else {
                    double diff = v - vPrev;
                    if (diff > 0) out[i] = 1;
                    else if (diff < 0) out[i] = -1;
                    else out[i] = 0;
                }
            }
        }

        return out;
    }

}