package eval;

import eval.engine.candidates.ConditionFactory;
import eval.engine.candidates.PoolCondition;
import eval.model.Candle;
import eval.okx.OkxCandleCacheEthOsc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ExitDiscriminativeIndicatorsMain_Enhanced (增强版 v4.0)
 *
 * ✅ 新增功能：
 * 1) 支持延后出场：新增 --exitMode 参数控制（EARLIER_ONLY/LATER_ONLY/BOTH）
 * 2) 修复中文控制台输出：强制 UTF-8 编码，System.setOut() 确保正确显示
 * 3) 代码优化：修复潜在的 NPE、边界检查、更清晰的注释
 * 4) 增强统计：增加延后出场时的额外持仓时间统计
 *
 * ✅ 保留原有功能：
 * 1) 手续费按费率：fee = notional * (openFeeRate + closeFeeRate)
 * 2) 允许"同K瞬间止损"，最保守 intrabar 假设
 * 3) notional 使用日志名义（实盘口径）
 * 4) 破产保护(BLOWUP)、去重、minEq/peakEq 统计
 *
 * ✅ 用途：
 * - 基于日志交易 + SQLite K线，评估出场指标组合对资金/回撤/胜率/PF 的影响
 * - 现在可以发现"延后出场"的机会（如在原出场后继续持有若干K线的优势）
 */
public class ExitDiscriminativeIndicatorsMain {

    // ==================== 命令行参数 ====================
    private static final String ARG_TRADE_LOG = "--tradeLog=";
    private static final String ARG_TOPN = "--topN=";
    private static final String ARG_LOOKBACK = "--lookback=";
    private static final String ARG_MAX_EVENTS = "--maxEvents=";
    private static final String ARG_RAND_COMBOS = "--randCombos=";
    private static final String ARG_COMBO_MIN_K = "--comboMinK=";
    private static final String ARG_COMBO_MAX_K = "--comboMaxK=";
    private static final String ARG_RAND_SEED = "--randSeed=";
    private static final String ARG_TOPM = "--topM=";
    private static final String ARG_SAFE_CLOSE_MS = "--safeCloseMs=";
    private static final String ARG_OPEN_FEE = "--openFeeRate=";
    private static final String ARG_CLOSE_FEE = "--closeFeeRate=";

    // 新增：出场模式控制
    private static final String ARG_EXIT_MODE = "--exitMode=";

    // ==================== 日志解析正则 ====================
    private static final Pattern P_SRMF_ENTRY = Pattern.compile("SRMF-进场.*?时间=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_ENTRY_DIR = Pattern.compile("方向=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_ENTRY_PRICE = Pattern.compile("入场开盘=([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_NOTIONAL = Pattern.compile("名义=([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SL_PTS = Pattern.compile("止损点=([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_CAPITAL = Pattern.compile("资金=([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT = Pattern.compile("SRMF-出场.*?时间=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_DIR = Pattern.compile("SRMF-出场.*?方向=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_GROSS_PTS = Pattern.compile("毛=([-+]?\\d+(?:\\.\\d+)?)点", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_FEE_PTS   = Pattern.compile("费=([-+]?\\d+(?:\\.\\d+)?)点", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_NET_U = Pattern.compile("净=\\s*([-+]?\\d+(?:\\.\\d+)?)点\\(([-+]?\\d+(?:\\.\\d+)?)U\\)", Pattern.CASE_INSENSITIVE);

    private static final ZoneId TOKYO = ZoneId.of("Asia/Tokyo");
    private static final DateTimeFormatter[] DT_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    // ==================== 枚举和数据结构 ====================
    enum Dir { LONG, SHORT, UNKNOWN }

    enum ExitMode {
        EARLIER_ONLY,  // 仅允许提前出场（原逻辑）
        LATER_ONLY,    // 仅允许延后出场（新增）
        BOTH          // 允许提前或延后（新增）
    }

    static final class TradeEvent {
        long entryTsMs;
        long exitTsMs;
        Dir dir;

        double entryPrice;
        double notional;
        double slPts;

        Double netU_log;      // 日志净盈亏(U)
        Double grossPts_log;  // 日志毛盈亏(点)
        Double feePts_log;    // 日志手续费(点)
        Double equityBefore;

        int entryIdx = -1;
        int exitIdx = -1;
    }

    static final class CondSeries {
        final PoolCondition cond;
        final int[] dir;
        final int[] pref;
        CondSeries(PoolCondition cond, int[] dir) {
            this.cond = cond;
            this.dir = dir;
            this.pref = new int[dir.length + 1];
            for (int i = 0; i < dir.length; i++) pref[i + 1] = pref[i] + dir[i];
        }
        int strengthAt(int idx, int lookback) {
            if (idx < 0) return 0;
            int r = idx + 1;
            int l = Math.max(0, idx - lookback + 1);
            return pref[r] - pref[l];
        }
    }

    static final class ExitComboSpec {
        final int k;
        final int[] idx;
        final boolean lossGate;
        final String id;
        final String group;
        ExitComboSpec(int k, int[] idx, boolean lossGate, String id, String group) {
            this.k = k; this.idx = idx; this.lossGate = lossGate; this.id = id; this.group = group;
        }
    }

    static final class FixedStats {
        double finalEq;
        double maxDD;
        double winRate;
        double pf;
        int wins;
        int losses;
        double sumWin;
        double sumLossAbs;

        int blowup;
        double minEquity;
        double peakEquity;

        int sameBarSLCount;
    }

    static final class ReplayResult {
        final FixedStats stats;
        final int triggers;
        final double avgDeltaBars;  // 正数=提前，负数=延后
        ReplayResult(FixedStats stats, int triggers, double avgDeltaBars) {
            this.stats = stats; this.triggers = triggers; this.avgDeltaBars = avgDeltaBars;
        }
    }

    static final class ExitImpactRow {
        final String id;
        final String group;
        final int k;
        final boolean lossGate;

        final int totalN;
        final int triggers;
        final double avgDeltaBars;  // 正数=提前，负数=延后

        final double baseFinalEquity;
        final double newFinalEquity;
        final double profitImpactPct;

        final double baseMaxDD;
        final double newMaxDD;
        final double ddImprove;

        final double baseWinRate;
        final double newWinRate;
        final double basePF;
        final double newPF;

        final int blowup;
        final double minEquity;
        final double peakEquity;

        final int sameBarSLCount;

        ExitImpactRow(String id, String group, int k, boolean lossGate,
                      int totalN, int triggers, double avgDeltaBars,
                      double baseFinalEquity, double newFinalEquity, double profitImpactPct,
                      double baseMaxDD, double newMaxDD, double ddImprove,
                      double baseWinRate, double newWinRate, double basePF, double newPF,
                      int blowup, double minEquity, double peakEquity,
                      int sameBarSLCount) {
            this.id = id; this.group = group; this.k = k; this.lossGate = lossGate;
            this.totalN = totalN; this.triggers = triggers; this.avgDeltaBars = avgDeltaBars;
            this.baseFinalEquity = baseFinalEquity; this.newFinalEquity = newFinalEquity; this.profitImpactPct = profitImpactPct;
            this.baseMaxDD = baseMaxDD; this.newMaxDD = newMaxDD; this.ddImprove = ddImprove;
            this.baseWinRate = baseWinRate; this.newWinRate = newWinRate; this.basePF = basePF; this.newPF = newPF;
            this.blowup = blowup; this.minEquity = minEquity; this.peakEquity = peakEquity;
            this.sameBarSLCount = sameBarSLCount;
        }
    }

    static final class ExitDecision {
        final int exitIdx;
        final double exitPrice;
        final boolean sameBarSL;
        ExitDecision(int exitIdx, double exitPrice, boolean sameBarSL) {
            this.exitIdx = exitIdx; this.exitPrice = exitPrice; this.sameBarSL = sameBarSL;
        }
    }

    interface ExitProvider {
        ExitDecision decide(TradeEvent e);
    }

    // ==================== MAIN ====================
    public static void main(String[] args) throws Exception {
        // ✅ 修复中文输出：强制 System.out 使用 UTF-8
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        String tradeLog = getArg(args, ARG_TRADE_LOG, "C:\\Users\\baiyu\\TimeOEX\\out\\trade_log.txt");
        int topN = getArgInt(args, ARG_TOPN, 20);
        int lookback = getArgInt(args, ARG_LOOKBACK, 12);
        int maxEvents = getArgInt(args, ARG_MAX_EVENTS, 999999);
        int randCombos = getArgInt(args, ARG_RAND_COMBOS, 500);
        int comboMinK = getArgInt(args, ARG_COMBO_MIN_K, 2);
        int comboMaxK = getArgInt(args, ARG_COMBO_MAX_K, 4);
        long randSeed = getArgLong(args, ARG_RAND_SEED, 42L);
        int topM = getArgInt(args, ARG_TOPM, 200);
        int safeCloseMs  = getArgInt(args, ARG_SAFE_CLOSE_MS, 3000);
        // 延后出场最多往后扫多少根K（默认16根=8小时@30m）
        int maxLaterBars = getArgInt(args, "--maxLaterBars=", 16);

        double openFeeRate = getArgDouble(args, ARG_OPEN_FEE, 0.00045);
        double closeFeeRate = getArgDouble(args, ARG_CLOSE_FEE, 0.00045);

        // ✅ 新增：出场模式参数
        String exitModeStr = getArg(args, ARG_EXIT_MODE, "BOTH");
        ExitMode exitMode;
        try {
            exitMode = ExitMode.valueOf(exitModeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("❌ 错误：未知的 exitMode=" + exitModeStr + "，支持：EARLIER_ONLY / LATER_ONLY / BOTH");
            exitMode = ExitMode.BOTH;
        }

        System.out.println("================ 出场指标挖掘 v4.0（增强版）================");
        System.out.println("交易日志: " + tradeLog);
        System.out.println("Top N: " + topN + " | Lookback: " + lookback + " | 最大事件: " + maxEvents);
        System.out.println("随机组合: " + randCombos + " | 组合K范围: [" + comboMinK + ", " + comboMaxK + "] | 随机种子: " + randSeed);
        System.out.println("开仓费率: " + (openFeeRate * 100) + "% | 平仓费率: " + (closeFeeRate * 100) + "%");
        System.out.println("✅ 出场模式: " + exitMode + " | safeCloseMs: " + safeCloseMs);
        System.out.println("========================================================\n");

        // 1) 解析交易日志
        List<TradeEvent> allEvents = parseTradeLog(tradeLog);
        System.out.println("✅ 成功解析交易日志：共 " + allEvents.size() + " 笔");
        if (allEvents.isEmpty()) {
            System.err.println("❌ 错误：交易日志为空，退出");
            return;
        }

        if (allEvents.size() > maxEvents) {
            allEvents = allEvents.subList(0, maxEvents);
            System.out.println("⚠️  限制事件数为前 " + maxEvents + " 笔");
        }

        // 2) 加载K线
        System.out.println("正在加载 K线数据（30m）...");
        final String INST_ID  = "ETH-USDT-SWAP";
        final String BAR      = "30m";
        final long   BAR_MS   = 30L * 60 * 1000;
        final long   NOW_MS   = System.currentTimeMillis();
        final int    NEED_BARS = 20000;
        List<Candle> candles = OkxCandleCacheEthOsc.get().getRecentByCount(
                INST_ID, BAR, NEED_BARS, NOW_MS, BAR_MS, (long) safeCloseMs);
        if (candles == null || candles.isEmpty()) {
            System.err.println("❌ 错误：无法加载K线数据");
            return;
        }
        System.out.println("✅ K线加载完成：共 " + candles.size() + " 根\n");

        // 3) 映射事件到K线索引
        int mapped = mapEventsToCandles(allEvents, candles);
        System.out.println("✅ 事件映射完成：" + mapped + " / " + allEvents.size() + " 笔成功映射到K线\n");

        List<TradeEvent> validEvents = new ArrayList<>();
        for (TradeEvent e : allEvents) {
            if (e.entryIdx >= 0 && e.exitIdx >= 0) validEvents.add(e);
        }
        if (validEvents.isEmpty()) {
            System.err.println("❌ 错误：无有效交易事件，退出");
            return;
        }

        double initEq = validEvents.get(0).equityBefore != null ? validEvents.get(0).equityBefore : 5043.0;
        System.out.println("初始资金: " + initEq + " USDT\n");

        // 4) 构建指标池
        System.out.println("正在构建指标池...");
        List<CondSeries> allSeries = buildExitCondSeries(candles, lookback);
        System.out.println("✅ 指标池构建完成：共 " + allSeries.size() + " 个指标\n");

        // 5) 基准回放（原出场）
        ReplayResult baseRes = replayFixed(validEvents, candles, initEq, openFeeRate, closeFeeRate, null, exitMode);
        FixedStats baseSt = baseRes.stats;

        System.out.println("========== 基准（原出场）==========");
        System.out.printf(Locale.ROOT, "最终资金: %.6f | 最大回撤: %.2f%% | 胜率: %.2f%% | PF: %.4f%n",
                baseSt.finalEq, baseSt.maxDD * 100, baseSt.winRate * 100, baseSt.pf);
        System.out.printf(Locale.ROOT, "最低资金: %.6f | 峰值资金: %.6f | 破产: %d | 同K止损: %d%n",
                baseSt.minEquity, baseSt.peakEquity, baseSt.blowup, baseSt.sameBarSLCount);
        System.out.println("====================================\n");

        if (baseSt.blowup > 0) {
            System.out.println("⚠️  警告：基准已破产，无法继续挖掘");
            return;
        }

        // 6) 生成组合并测试
        List<ExitComboSpec> combos = genRandomExitCombosFromPool("随机组合", allSeries,
                range(0, allSeries.size()), randCombos, comboMinK, comboMaxK, randSeed);

        if (combos.isEmpty()) {
            System.out.println("⚠️  警告：未生成任何组合，退出");
            return;
        }

        System.out.println("\n开始测试 " + combos.size() + " 个组合...\n");
        List<ExitImpactRow> rows = new ArrayList<>();

        for (int ci = 0; ci < combos.size(); ci++) {
            ExitComboSpec spec = combos.get(ci);
            if ((ci + 1) % 100 == 0) {
                System.out.println("进度: " + (ci + 1) + " / " + combos.size());
            }

            ExitProvider provider = buildExitProvider(spec, allSeries, validEvents, candles, lookback, exitMode, maxLaterBars);
            ReplayResult res = replayFixed(validEvents, candles, initEq, openFeeRate, closeFeeRate, provider, exitMode);

            if (res.stats.blowup > 0) continue;

            double profitImpact = (res.stats.finalEq - baseSt.finalEq) / baseSt.finalEq * 100.0;
            double ddImprove = baseSt.maxDD - res.stats.maxDD;

            ExitImpactRow row = new ExitImpactRow(
                    spec.id, spec.group, spec.k, spec.lossGate,
                    validEvents.size(), res.triggers, res.avgDeltaBars,
                    baseSt.finalEq, res.stats.finalEq, profitImpact,
                    baseSt.maxDD, res.stats.maxDD, ddImprove,
                    baseSt.winRate, res.stats.winRate, baseSt.pf, res.stats.pf,
                    res.stats.blowup, res.stats.minEquity, res.stats.peakEquity,
                    res.stats.sameBarSLCount
            );
            rows.add(row);
        }

        System.out.println("\n✅ 测试完成，共 " + rows.size() + " 个有效结果\n");

        if (rows.isEmpty()) {
            System.out.println("⚠️  警告：无有效结果，退出");
            return;
        }

        // 7) 去重和排序
        rows = dedupById(rows);
        rows.sort((a, b) -> {
            int c = Double.compare(b.newFinalEquity, a.newFinalEquity);
            if (c != 0) return c;
            c = Double.compare(b.ddImprove, a.ddImprove);
            if (c != 0) return c;
            return Integer.compare(a.blowup, b.blowup);
        });

        // 8) 打印和导出
        printTop("最终排名（按最终资金降序）", baseSt, rows, topN);

        String outPath = "./exit_mining_results_v4.csv";
        writeExitCsv(outPath, "出场挖掘 v4.0", rows, topM);
        System.out.println("\n✅ 结果已导出到: " + outPath);
    }

    // ==================== 核心回放逻辑（修改版，支持延后）====================
    private static ReplayResult replayFixed(List<TradeEvent> events,
                                            List<Candle> candles,
                                            double initEq,
                                            double openFeeRate,
                                            double closeFeeRate,
                                            ExitProvider exitProvider,
                                            ExitMode exitMode) {
        FixedStats st = new FixedStats();
        double equity = initEq;
        double peak = initEq;
        double minEq = initEq;
        double maxDD = 0.0;

        int wins = 0, losses = 0;
        double sumWin = 0.0, sumLossAbs = 0.0;

        int triggers = 0;
        long deltaSum = 0;  // 正数=提前，负数=延后
        int sameBarSLCount = 0;

        for (TradeEvent e : events) {
            if (e.entryIdx < 0 || e.exitIdx < 0) continue;

            ExitDecision dec;
            if (exitProvider == null) {
                dec = new ExitDecision(e.exitIdx, candles.get(e.exitIdx).c, false);
            } else {
                dec = exitProvider.decide(e);
            }

            // ✅ 关键修改：根据 exitMode 决定如何约束出场时机
            int finalExitIdx;
            switch (exitMode) {
                case EARLIER_ONLY:
                    // 只允许提前：min(指标出场, 原出场)
                    finalExitIdx = Math.min(dec.exitIdx, e.exitIdx);
                    break;
                case LATER_ONLY:
                    // 只允许延后：max(指标出场, 原出场)
                    finalExitIdx = Math.max(dec.exitIdx, e.exitIdx);
                    break;
                case BOTH:
                default:
                    // 允许提前或延后：直接使用指标出场
                    finalExitIdx = dec.exitIdx;
                    break;
            }

            // 确保索引有效
            finalExitIdx = Math.max(e.entryIdx, Math.min(finalExitIdx, candles.size() - 1));

            if (exitProvider != null && finalExitIdx != e.exitIdx) {
                triggers++;
                deltaSum += (e.exitIdx - finalExitIdx);  // 正=提前，负=延后
            }
            if (dec.sameBarSL) sameBarSLCount++;

            double net;
            if (exitProvider == null || finalExitIdx == e.exitIdx) {
                // 基准 或 指标未触发：直接用日志净值（最准确）
                net = (e.netU_log != null) ? e.netU_log : 0.0;
            } else {
                // 指标触发了不同出场：用K线价格重新计算盈亏，手续费从日志反推
                double entryPx = e.entryPrice;
                double exitPx  = candles.get(finalExitIdx).c;
                // qty = notional / entryPrice（合约张数×面值）
                double qty   = e.notional / entryPx;
                double gross = (e.dir == Dir.LONG) ? qty * (exitPx - entryPx) : qty * (entryPx - exitPx);
                // 手续费 = 日志里费用点数 × qty（点数→U）；无日志数据则用费率估算
                double feeU;
                if (e.feePts_log != null && e.feePts_log > 0 && e.grossPts_log != null && Math.abs(e.grossPts_log) > 0) {
                    // 按日志费率反推：feeU = feePts_log * qty
                    feeU = e.feePts_log * qty;
                } else {
                    feeU = e.notional * (openFeeRate + closeFeeRate);
                }
                net = gross - feeU;
            }

            equity += net;

            // 破产检查
            if (equity <= 0) {
                st.blowup = 1;
                st.finalEq = 0;
                st.maxDD = -1.0;
                st.minEquity = Math.min(minEq, equity);
                st.peakEquity = Math.max(peak, initEq);
                st.winRate = (events.isEmpty() ? 0 : (wins * 1.0 / events.size()));
                st.pf = (sumLossAbs <= 0 ? (sumWin > 0 ? 999.0 : 0.0) : (sumWin / sumLossAbs));
                st.sameBarSLCount = sameBarSLCount;
                double avgDelta = triggers == 0 ? 0.0 : (deltaSum * 1.0 / triggers);
                return new ReplayResult(st, triggers, avgDelta);
            }

            if (equity > peak) peak = equity;
            if (equity < minEq) minEq = equity;

            double dd = equity / peak - 1.0;
            if (dd < maxDD) maxDD = dd;

            if (net > 0) { wins++; sumWin += net; }
            else if (net < 0) { losses++; sumLossAbs += -net; }
        }

        int n = events.size();
        st.finalEq = equity;
        st.maxDD = maxDD;
        st.winRate = (n == 0 ? 0 : wins * 1.0 / n);
        st.pf = (sumLossAbs <= 0 ? (sumWin > 0 ? 999.0 : 0.0) : (sumWin / sumLossAbs));
        st.blowup = 0;
        st.minEquity = minEq;
        st.peakEquity = peak;
        st.sameBarSLCount = sameBarSLCount;

        double avgDelta = triggers == 0 ? 0.0 : (deltaSum * 1.0 / triggers);
        return new ReplayResult(st, triggers, avgDelta);
    }

    // ==================== 构建出场提供者 ====================
    private static ExitProvider buildExitProvider(final ExitComboSpec spec,
                                                  final List<CondSeries> allSeries,
                                                  final List<TradeEvent> events,
                                                  final List<Candle> candles,
                                                  final int lookback,
                                                  final ExitMode exitMode,
                                                  final int maxLaterBars) {
        return new ExitProvider() {
            @Override
            public ExitDecision decide(TradeEvent e) {
                return decideExit(e, spec, allSeries, candles, lookback, exitMode, maxLaterBars);
            }
        };
    }

    private static ExitDecision decideExit(TradeEvent e,
                                           ExitComboSpec spec,
                                           List<CondSeries> allSeries,
                                           List<Candle> candles,
                                           int lookback,
                                           ExitMode exitMode,
                                           int maxLaterBars) {
        if (e.entryIdx < 0 || e.exitIdx < 0) {
            return new ExitDecision(e.exitIdx, candles.get(Math.max(0, e.exitIdx)).c, false);
        }

        int start = e.entryIdx;
        int end   = e.exitIdx;

        // 扫描上限：EARLIER_ONLY 只扫到原出场；LATER_ONLY/BOTH 继续往后 maxLaterBars 根
        int scanEnd = (exitMode == ExitMode.EARLIER_ONLY)
                ? end
                : Math.min(end + maxLaterBars, candles.size() - 1);

        // 检查同K止损（入场K内触发止损）—— 仅对提前出场有意义
        if (exitMode != ExitMode.LATER_ONLY && start < candles.size()) {
            Candle entryBar = candles.get(start);
            double slPrice = (e.dir == Dir.LONG)
                    ? e.entryPrice - e.slPts
                    : e.entryPrice + e.slPts;
            boolean sameBarSL = (e.dir == Dir.LONG && entryBar.l <= slPrice)
                    || (e.dir == Dir.SHORT && entryBar.h >= slPrice);
            if (sameBarSL) {
                return new ExitDecision(start, slPrice, true);
            }
        }

        // ---- 阶段一：原出场之前扫描（提前触发）----
        if (exitMode != ExitMode.LATER_ONLY) {
            for (int i = start + 1; i < end && i < candles.size(); i++) {
                if (votes(i, e, spec, allSeries, candles, lookback) >= spec.k) {
                    if (checkLossGate(spec, e, i, candles)) continue;
                    return new ExitDecision(i, candles.get(i).c, false);
                }
            }
        }

        // ---- 阶段二：原出场之后扫描（延后触发）——仅 LATER_ONLY / BOTH ----
        if (exitMode != ExitMode.EARLIER_ONLY) {
            // 在原出场之后，找第一个"反向信号消失/转正向"的时机出场
            // 逻辑：继续持仓，直到指标组合票数 < k（即信号减弱），再出场
            for (int i = end + 1; i <= scanEnd; i++) {
                int v = votes(i, e, spec, allSeries, candles, lookback);
                // 延后：票数 < k 表示持仓理由充足（信号未反转），继续持；
                // 票数 >= k 表示信号已反转，此时出场
                if (v >= spec.k) {
                    if (checkLossGate(spec, e, i, candles)) continue;
                    return new ExitDecision(i, candles.get(i).c, false);
                }
            }
        }

        // 未触发：返回原出场
        return new ExitDecision(end, candles.get(end).c, false);
    }

    /** 计算某根K的出场方向票数。 */
    private static int votes(int i, TradeEvent e, ExitComboSpec spec,
                             List<CondSeries> allSeries, List<Candle> candles, int lookback) {
        int voteCount = 0;
        for (int si : spec.idx) {
            if (si < 0 || si >= allSeries.size()) continue;
            CondSeries cs = allSeries.get(si);
            if (i >= cs.dir.length) continue;
            int str = cs.strengthAt(i, lookback);
            int bin = to5Bin(str, lookback);
            int dir = cs.dir[i];
            boolean vote = (e.dir == Dir.LONG  && (dir < 0 || bin <= 1))
                    || (e.dir == Dir.SHORT && (dir > 0 || bin >= 3));
            if (vote) voteCount++;
        }
        return voteCount;
    }

    /** LossGate 检查：返回 true 表示应跳过（持仓中）。 */
    private static boolean checkLossGate(ExitComboSpec spec, TradeEvent e, int i, List<Candle> candles) {
        if (!spec.lossGate) return false;
        double px  = candles.get(i).c;
        double qty = e.notional / e.entryPrice;
        double floatPnl = (e.dir == Dir.LONG)
                ? qty * (px - e.entryPrice)
                : qty * (e.entryPrice - px);
        return floatPnl >= 0;  // 浮盈时跳过（只在浮亏时出场）
    }

    // ==================== 辅助方法 ====================
    private static List<TradeEvent> parseTradeLog(String tradeLog) throws IOException {
        ArrayList<TradeEvent> out = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(tradeLog), StandardCharsets.UTF_8))) {
            String line;
            TradeEvent pending = null;
            int lineNum = 0;
            while ((line = br.readLine()) != null) {
                lineNum++;
                if (pending == null) {
                    Matcher me = P_SRMF_ENTRY.matcher(line);
                    if (me.find()) {
                        try {
                            TradeEvent e = new TradeEvent();
                            e.entryTsMs = parseTsMsTokyo(me.group(1).trim());
                            e.entryPrice = findDouble(line, P_ENTRY_PRICE, 0);
                            e.notional = findDouble(line, P_NOTIONAL, 0);
                            e.slPts = findDouble(line, P_SL_PTS, 0);
                            e.equityBefore = findDoubleObj(line, P_CAPITAL);
                            e.dir = parseDir(findStr(line, P_ENTRY_DIR));
                            pending = e;
                        } catch (Exception ex) {
                            System.err.println("⚠️  警告：第" + lineNum + "行解析进场失败: " + ex.getMessage());
                        }
                    }
                } else {
                    Matcher mx = P_SRMF_EXIT.matcher(line);
                    if (mx.find()) {
                        try {
                            pending.exitTsMs = parseTsMsTokyo(mx.group(1).trim());
                            Dir d2 = parseDir(findStr(line, P_SRMF_EXIT_DIR));
                            if (d2 != Dir.UNKNOWN) pending.dir = d2;
                            // 解析毛点数、费点数、净U（正则 group 索引已更新）
                            pending.grossPts_log = findDoubleObj(line, P_SRMF_EXIT_GROSS_PTS);
                            pending.feePts_log   = findDoubleObj(line, P_SRMF_EXIT_FEE_PTS);
                            pending.netU_log     = findDouble2ndGroup(line, P_SRMF_EXIT_NET_U);

                            // 数据验证
                            if (pending.entryPrice <= 0 || pending.notional <= 0 || pending.slPts <= 0) {
                                System.err.println("⚠️  警告：第" + lineNum + "行交易数据不完整，跳过");
                                pending = null;
                                continue;
                            }
                            if (pending.dir == Dir.UNKNOWN) {
                                System.err.println("⚠️  警告：第" + lineNum + "行方向未知，跳过");
                                pending = null;
                                continue;
                            }

                            out.add(pending);
                            pending = null;
                        } catch (Exception ex) {
                            System.err.println("⚠️  警告：第" + lineNum + "行解析出场失败: " + ex.getMessage());
                            pending = null;
                        }
                    }
                }
            }
        }
        return out;
    }

    private static int mapEventsToCandles(List<TradeEvent> events, List<Candle> candles) {
        int ok = 0;
        for (TradeEvent e : events) {
            int ei = findClosestIdx(candles, e.entryTsMs);
            int xi = findClosestIdx(candles, e.exitTsMs);
            if (ei >= 0 && xi >= 0 && ei <= xi) {
                e.entryIdx = ei;
                e.exitIdx = xi;
                ok++;
            }
        }
        return ok;
    }

    private static int findClosestIdx(List<Candle> candles, long tsMs) {
        if (candles == null || candles.isEmpty()) return -1;
        int best = -1;
        long minDiff = Long.MAX_VALUE;
        for (int i = 0; i < candles.size(); i++) {
            long diff = Math.abs(candles.get(i).ts - tsMs);  // ⚠️ adapt field name: ts / openTime / open_time
            if (diff < minDiff) {
                minDiff = diff;
                best = i;
            }
        }
        return best;
    }

    private static List<CondSeries> buildExitCondSeries(List<Candle> candles, int lookback) {
        List<CondSeries> out = new ArrayList<>();
        List<PoolCondition> pool = ConditionFactory.defaultConditions();
        int n = candles.size();
        for (PoolCondition c : pool) {
            try {
                // dir(candles, fromIdx, toIdx) 返回长度为 (toIdx-fromIdx) 的数组
                int[] dir = c.dir(candles, 0, n);
                out.add(new CondSeries(c, dir));
            } catch (Exception e) {
                // 某个指标计算失败时跳过，不中断整体
                System.err.println("⚠️  指标计算失败，跳过 [" + c.id() + "]: " + e.getMessage());
            }
        }
        return out;
    }

    private static List<ExitComboSpec> genRandomExitCombosFromPool(String title,
                                                                   List<CondSeries> series,
                                                                   int[] pool,
                                                                   int want,
                                                                   int minK,
                                                                   int maxK,
                                                                   long seed) {
        int n = pool.length;
        if (n < 2 || want <= 0) return new ArrayList<>();
        minK = Math.max(2, minK);
        maxK = Math.min(maxK, n);
        if (minK > maxK) { int t = minK; minK = maxK; maxK = t; }

        Random rnd = new Random(seed);
        HashSet<String> seen = new HashSet<>();
        ArrayList<ExitComboSpec> out = new ArrayList<>(want * 2);

        int maxAttempts = Math.max(20000, want * 80);
        for (int attempts = 0; out.size() < want * 2 && attempts < maxAttempts; attempts++) {
            int k = minK + rnd.nextInt(maxK - minK + 1);
            int[] idx = pickDistinctFromPool(pool, k, rnd);
            Arrays.sort(idx);
            String key = Arrays.toString(idx);
            if (!seen.add(key)) continue;

            String baseId = buildComboId(idx, series);
            String group = "EXIT_RND_K" + k;

            out.add(new ExitComboSpec(k, idx, false, baseId + "|LG0", group));
            out.add(new ExitComboSpec(k, idx, true, baseId + "|LG1", group));
        }

        System.out.println("随机组合[" + title + "]：pool=" + n + " | 组合数(base)=" + (out.size() / 2) + " | 扩展后=" + out.size());
        return out;
    }

    private static int[] pickDistinctFromPool(int[] pool, int k, Random rnd) {
        int n = pool.length;
        if (k >= n) return Arrays.copyOf(pool, pool.length);
        int[] pick = new int[k];
        HashSet<Integer> used = new HashSet<>();
        int p = 0;
        while (p < k) {
            int idx = pool[rnd.nextInt(n)];
            if (used.add(idx)) pick[p++] = idx;
        }
        return pick;
    }

    private static String buildComboId(int[] idx, List<CondSeries> series) {
        StringBuilder sb = new StringBuilder();
        sb.append("EXIT_COMBO").append(idx.length).append(":");
        for (int i = 0; i < idx.length; i++) {
            if (i > 0) sb.append("+");
            if (idx[i] >= 0 && idx[i] < series.size()) {
                sb.append(series.get(idx[i]).cond.id());
            } else {
                sb.append("INVALID_").append(idx[i]);
            }
        }
        return sb.toString();
    }

    private static List<ExitImpactRow> dedupById(List<ExitImpactRow> rows) {
        HashMap<String, ExitImpactRow> best = new HashMap<>();
        for (ExitImpactRow r : rows) {
            ExitImpactRow cur = best.get(r.id);
            if (cur == null || isBetter(r, cur)) best.put(r.id, r);
        }
        return new ArrayList<>(best.values());
    }

    private static boolean isBetter(ExitImpactRow a, ExitImpactRow b) {
        int c = Double.compare(a.newFinalEquity, b.newFinalEquity);
        if (c != 0) return c > 0;
        c = Double.compare(a.ddImprove, b.ddImprove);
        if (c != 0) return c > 0;
        return a.blowup < b.blowup;
    }

    private static int[] range(int start, int end) {
        int[] arr = new int[end - start];
        for (int i = 0; i < arr.length; i++) arr[i] = start + i;
        return arr;
    }

    private static int to5Bin(int strength, int L) {
        if (L <= 0) return 2;
        double x = strength * 1.0 / L;
        if (x <= -0.6) return 0;
        if (x <= -0.2) return 1;
        if (x < 0.2) return 2;
        if (x < 0.6) return 3;
        return 4;
    }

    private static void writeExitCsv(String path, String title, List<ExitImpactRow> rows, int topN) throws IOException {
        new File(new File(path).getParent()).mkdirs();
        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title + " Exit Mining v4.0 (支持延后出场)");
            pw.println("id,group,k,lossGate,totalN,triggers,avgDeltaBars,baseFinalEq,newFinalEq,profitImpactPct,baseMaxDD,newMaxDD,ddImprove,baseWinRate,newWinRate,basePF,newPF,blowup,minEquity,peakEquity,sameBarSL");
            int lim = Math.min(topN, rows.size());
            for (int i = 0; i < lim; i++) {
                ExitImpactRow r = rows.get(i);
                pw.printf(Locale.ROOT,
                        "%s,%s,%d,%s,%d,%d,%.4f,%.8f,%.8f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d,%.8f,%.8f,%d%n",
                        esc(r.id), esc(r.group), r.k, r.lossGate ? "LG1" : "LG0",
                        r.totalN, r.triggers, r.avgDeltaBars,
                        r.baseFinalEquity, r.newFinalEquity, r.profitImpactPct,
                        r.baseMaxDD, r.newMaxDD, r.ddImprove,
                        r.baseWinRate, r.newWinRate,
                        r.basePF, r.newPF,
                        r.blowup, r.minEquity, r.peakEquity,
                        r.sameBarSLCount);
            }
        }
    }

    private static void printTop(String title, FixedStats base, List<ExitImpactRow> rows, int topN) {
        System.out.println("\n================ " + title + "（Top" + topN + "） ================");
        System.out.printf(Locale.ROOT,
                "【基准-原出场】最终资金=%.6f | 最大回撤=%.2f%% | 胜率=%.2f%% | PF=%.4f | minEq=%.6f | peakEq=%.6f | blowup=%d | 同K止损=%d%n",
                base.finalEq, base.maxDD * 100.0, base.winRate * 100.0, base.pf,
                base.minEquity, base.peakEquity, base.blowup, base.sameBarSLCount);

        int lim = Math.min(topN, rows.size());
        for (int i = 0; i < lim; i++) {
            ExitImpactRow r = rows.get(i);
            String deltaDesc = r.avgDeltaBars >= 0
                    ? String.format("提前%.2f根", r.avgDeltaBars)
                    : String.format("延后%.2f根", -r.avgDeltaBars);

            System.out.printf(Locale.ROOT,
                    "#%02d [%s] trig=%d %s | 最终=%.6f (相对=%.2f%%) | 回撤=%.2f%% | 胜率=%.2f%% | PF=%.4f | minEq=%.6f | peakEq=%.6f | blowup=%d | 同K止损=%d | %s%n",
                    i + 1,
                    r.lossGate ? "LG1(仅浮亏触发)" : "LG0(盈利也触发)",
                    r.triggers,
                    deltaDesc,
                    r.newFinalEquity,
                    r.profitImpactPct,
                    r.newMaxDD * 100.0,
                    r.newWinRate * 100.0,
                    r.newPF,
                    r.minEquity,
                    r.peakEquity,
                    r.blowup,
                    r.sameBarSLCount,
                    r.id);
        }
    }

    private static Dir parseDir(String s) {
        if (s == null) return Dir.UNKNOWN;
        String t = s.trim();
        if (t.contains("做多") || t.equalsIgnoreCase("LONG") || t.contains("long")
                || t.equalsIgnoreCase("BUY") || "多".equals(t)) return Dir.LONG;
        if (t.contains("做空") || t.equalsIgnoreCase("SHORT") || t.contains("short")
                || t.equalsIgnoreCase("SELL") || "空".equals(t)) return Dir.SHORT;
        return Dir.UNKNOWN;
    }

    private static long parseTsMsTokyo(String s) {
        LocalDateTime ldt = null;
        for (DateTimeFormatter f : DT_FORMATS) {
            try { ldt = LocalDateTime.parse(s.trim(), f); break; }
            catch (DateTimeParseException ignore) {}
        }
        if (ldt == null) throw new IllegalArgumentException("无法解析时间：" + s);
        return ldt.atZone(TOKYO).toInstant().toEpochMilli();
    }

    private static String fmtTs(long tsMs) {
        return Instant.ofEpochMilli(tsMs).atZone(TOKYO).toLocalDateTime().toString().replace('T',' ');
    }

    private static String findStr(String line, Pattern p) {
        if (line == null) return null;
        Matcher m = p.matcher(line);
        if (m.find()) return m.group(1);
        return null;
    }

    private static double findDouble(String line, Pattern p, double def) {
        Double v = findDoubleObj(line, p);
        return v == null ? def : v;
    }

    private static Double findDoubleObj(String line, Pattern p) {
        if (line == null) return null;
        Matcher m = p.matcher(line);
        if (m.find()) {
            try { return Double.parseDouble(m.group(1)); }
            catch (Exception ignore) { return null; }
        }
        return null;
    }

    /** 取正则第2个捕获组（用于 P_SRMF_EXIT_NET_U 等双组正则）。 */
    private static Double findDouble2ndGroup(String line, Pattern p) {
        if (line == null) return null;
        Matcher m = p.matcher(line);
        if (m.find()) {
            try { return Double.parseDouble(m.group(2)); }
            catch (Exception ignore) { return null; }
        }
        return null;
    }

    private static String esc(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"") || s.contains("\n"))
            return '"' + s.replace("\"","\"\"") + '"';
        return s;
    }

    // ==================== 内联命令行参数解析（替代 eval.engine.Args）====================
    private static String getArg(String[] args, String prefix, String def) {
        for (String a : args) if (a.startsWith(prefix)) return a.substring(prefix.length());
        return def;
    }
    private static int getArgInt(String[] args, String prefix, int def) {
        String v = getArg(args, prefix, null);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
    private static long getArgLong(String[] args, String prefix, long def) {
        String v = getArg(args, prefix, null);
        if (v == null) return def;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return def; }
    }
    private static double getArgDouble(String[] args, String prefix, double def) {
        String v = getArg(args, prefix, null);
        if (v == null) return def;
        try { return Double.parseDouble(v); } catch (NumberFormatException e) { return def; }
    }

}