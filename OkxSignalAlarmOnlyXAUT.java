package com.xl.XAUT趋势系统;

import com.xl.SRMF.SRMFExecutor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public class OkxSignalAlarmOnlyXAUT {

    // ===================== 基础参数 =====================
    static final String INST_ID = "XAUT-USDT-SWAP";

    // =====================  SQLite K线数据库缓存（4年） =====================
    // 目标：启动/回测/定时不再频繁全量拉K线；优先从DB读，不够再分页补齐，并且只入库已收盘(confirm=1)。
    static final boolean USE_DB_CACHE = true;
    static final String DB_FILE = "C:\\\\Users\\\\baiyu\\\\TimeOEX\\\\okx_candles.db";
    static final int DB_KEEP_YEARS = 4;

    // =====================  OKX 合约手续费（双边） =====================
    // 说明：以“点数”方式扣减手续费：feePts = feeRatePerSide * (|entry| + |exit|)
    // 你只需要改 OKX_FEE_RATE_PER_SIDE 即可（例如 0.0002=0.02% 单边；双边=0.04%）
    static final boolean APPLY_OKX_FEE = true;
    static final double OKX_FEE_RATE_PER_SIDE = 0.00045; // 单边费率（按你的实际等级/费率修改）

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
    static boolean USE_SL_PCT_Q_GATE = true;
    static int SL_PCT_Q_LOOKBACK_BARS = 2000; // 只用过去 N 根30m 的 slPct（每根用“下一根开盘价”作为当时的入场价）
    static double SL_PCT_Q = 0.90;            // 0.90=过滤最“贵”的10%止损环境
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
    static final int ATR_N = 100;                    // ATR 周期（30m）
    static final double SL_ATR_K = 1.0;             // k * ATR
    static final double SL_MIN = 8.0;              // 动态止损最小点数（clamp下限）根据最低1000价格更改为8
    static final double SL_MAX = 45.0;              // 动态止损最大点数（clamp上限）根据最新价格超过4800更改为35
    //双动态止损 ->根据最新价格设定动态止损的最大/最小值目前区间 1000(8)->4800(35)

    //  TP2：达到 TP2_TRIGGER 点后，全平（剩余 size 全部平掉）
    static final double TP2_TRIGGER = 5555.0; // 例：60
    // =====================  OKX 撑压线（Support/Resistance）过滤 & 止盈 =====================
    // 说明：OKX APP 图上的 "Resistance / Support" 你无法从行情接口直接拿到（除非OKX单独提供指标接口），
    // 所以这里用“最近N根已收盘K”的最高/最低（类似 Donchian 通道）来生成一组撑压线，效果最接近也最稳定。
    static final boolean USE_OKX_SR_ENTRY_FILTER = true;

    // =====================  TSRND 外层投票 + 仓位加成（最外层） =====================
    // 规则：方向票通过=2分即可进场（名义 *1.0），方向票+总体票通过=3分更强（名义 *1.2）
    static final boolean USE_TSRND_OUTER_VOTE = true;
    static final int TSRND_LOOKBACK_BARS = 12;       // 投票强度累积窗口（避免单根噪声）
    static final int TSRND_SCORE_THRESHOLD = 3;      // 至少 2 分（方向票）才允许进场
    static final double TSRND_SCALE_SCORE2 = 1.0;    // 2分：名义*1.0
    static final double TSRND_SCALE_SCORE3 = 1.0;    // 3分：名义*1.2
    static final double TSRND_SCALE_SCORE5 = 1.4;    // 5分：名义*1.4
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
    static final boolean USE_OKX_SR_TP = true;           // 止盈：触达撑/压线即止盈
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
    static final boolean USE_MACD_EXIT = true;
    static final boolean MACD_EXIT_2BAR_CONFIRM = true;
    static final int MACD_EXIT_CONFIRM_BARS = 2;
    static final boolean MACD_EXIT_MIN_HOLD_ENABLE = true;
    static final int MACD_EXIT_MIN_HOLD_BARS = 2;
    static final boolean MACD_EXIT_MIN_PROFIT_ENABLE = false;
    static final double MACD_EXIT_MIN_PROFIT_FEE_K = 10.0;
    static final boolean PRINT_MACD_EXIT_BLOCK = true;

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
    static final boolean USE_3K_ENTRY_CONFIRM = false;            //  开关：三连K入场确认（true=3根同号；false=2根同号）
    static final boolean ENABLE_PULLBACK_ENTRY_BACKTEST = true;  //  开关：回测是否启用回撤限价入场
    static final boolean ENABLE_PULLBACK_ENTRY_LIVE_PRINT = true; //  开关：实盘打印“回撤入场参考”（不改变原入场提醒逻辑）
    static final PullbackMode PULLBACK_MODE = PullbackMode.FIXED;   // ATR 或 FIXED
    static final double PULLBACK_ATR_MULT = 0.5;                 // x = ATR * mult原0.25
    static final double PULLBACK_FIXED_POINTS = 2;              // x = 固定点数（当模式=FIXED时生效）原8
    static final int PULLBACK_VALID_BARS = 1;                      // 订单有效K数：1=只看下一根K；>1 可后续扩展

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
    static double capital = 4653.0;            // 你已设好 初始4244+239-140+213
    //  SRMF 执行器（4档稳健 + 环境判别器切5档）
    static final SRMFExecutor SRMF = new SRMFExecutor();

    // 仅用于回测/scanHistory 的“资金曲线模拟”（不下单时也能得到最新 trendPts/环境状态）
    static double equitySim = capital;

    // MACD 参数（12/26/9）
    static final int FAST = 12;
    static final int SLOW = 26;
    static final int SIGNAL = 9;

    // 最近交易展示条数
    static final int LAST_N = 10;

    // 定时（每半小时收盘前约 :00:10 / :30:10）
    static final int CHECK_SECOND = 6;

    //  K线收盘判定（避免 confirm 延迟导致 K 线滞后一根）
    static final long BAR30_MS = 30L * 60 * 1000;

    static final long BAR1H_MS = 1L * 60 * 60 * 1000;

    static final long BAR4H_MS = 4L * 60 * 60 * 1000;

    static boolean DEBUG_REALTIME = true;
    static final long SAFE_CLOSE_MS = 3_000L; // 收盘后留 2s 安全窗口（配合 :00:07/:30:07）
    static final int WAIT_OKX_REFRESH_TRIES = 18;  //  若 OKX 收盘后延迟更新，最多等待次数
    static final long WAIT_OKX_REFRESH_MS = 800L; //  每次等待间隔(ms)，配合 :00:07/:30:07

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
    static final boolean FILTER_ASIA_LOW_LIQ = true;
    static final int NO_TRADE_1_START = 0;
    static final int NO_TRADE_1_END = 0;

    static final int NO_TRADE_2_START = 0;
    static final int NO_TRADE_2_END = 0;

    // B)  大级别中性期第一根不进场（1H趋势升级）
    static final boolean FILTER_1H_NEUTRAL_FIRST_BAR = false;
    static final boolean FILTER_1H_SECOND_TREND_BAR = false;  // 新增：中性→趋势后的「第二根趋势K」禁止进场

    // C)  12月禁止开仓（只影响新开仓，不影响已有持仓的止盈/止损/管理）
    //     月份判断：只用 30m 的 K1（sigIdx-1）的时间，避免未来函数/边界误判
    static final boolean DISABLE_DECEMBER_NEW_ENTRY = true;

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

    // =====================  EMA 多时间投票过滤（30m EMA400 / 1H EMA200 / 4H EMA50） =====================
    // 说明：
    // - 只用【已收盘K线】计算，避免未来函数
    // - 每周期票：基础 0.5；若 |diff01| >= 1.5 * avgDiff，则追加强度票 0.3
    // - diff/avgDiff 默认用“百分比归一”（可切换），更抗价格区间切换
    // - avgDiff 窗口不包含当前 diff01（用历史 k1-k2..kN-kN+1 的均值），避免“自己抬高阈值”
    static final boolean USE_EMA_VOTE_FILTER = true;
    static final boolean EMA_VOTE_USE_PCT_NORM = true;      // diffPct=(ema0-ema1)/ema1
    static final boolean PRINT_EMA_VOTE_BLOCK = false;       // 仅在拦截时打印

    static final double EMA_VOTE_BASE = 0.5;
    static final double EMA_VOTE_STRONG_EXTRA = 0.3;
    static final double EMA_VOTE_STRONG_MULT = 1.5;

    // 总分=3周期各自最大(0.8)之和=2.4；阈值=35% => 0.84
    static final double EMA_VOTE_SCORE_PCT = 0.35;

    // 周期参数（你指定）
    static final int EMA30_PERIOD = 400;
    static final int EMA1H_PERIOD = 200;
    static final int EMA4H_PERIOD = 50;

    // 强度阈值窗口（你指定：30m=400, 1H=200, 4H=50）
    static final int EMA30_AVG_N = 400;
    static final int EMA1H_AVG_N = 200;
    static final int EMA4H_AVG_N = 50;

    // warmup（你指定）
    static final int EMA30_MIN_BARS = 400 + 300; // 700
    static final int EMA1H_MIN_BARS = 200 + 200; // 400
    static final int EMA4H_MIN_BARS = 50 + 100; // 150

    // 实盘/回测：投票上下文（预先算好 EMA 与 rolling avg，避免 O(n^2)）
    static volatile EmaVoteContext EMA_VOTE_CTX_REALTIME = null;
    static EmaVoteContext EMA_VOTE_CTX_BACKTEST = null;

    // =========================
    // 第二投票机（Pool-2）：把你指定的组合嵌入到趋势系。
    // 重要：默认不改变原逻辑，只做“并行计算 + 打印/闹钟”。
    // 如需把它变成硬过滤，只需把 USE_POOL2_ENTRY_FILTER 改为 true。
    // 组合：POOLM_SQUEEZE20_20_CVD_sign_EMA200_FISHER_P10x2_MACD12_26_9_StochRSI14_14_RSI14_rev30_70x2_PS_TSOUP_96_TH6_FLAT
    // =========================
    static final boolean USE_POOL2_VOTE_MACHINE = false;
    static final boolean PRINT_POOL2_ON_SIGNAL = true;   // 仅在“出现入场候选side”时打印
    //  方案2：作为硬过滤器（投票必须一致才能进）。
    // - true：Pool-2 方向必须与原本 entrySideByDifDea 的方向一致，才允许进场。
    // - false：只打印/报警，不影响原本逻辑。
    static final boolean USE_POOL2_ENTRY_FILTER = true;

    static final String POOL2_NAME = "POOLM_SQUEEZE20_20_CVD_sign_EMA200_FISHER_P10x2_MACD12_26_9_StochRSI14_14_RSI14_rev30_70x2_PS_TSOUP_96_TH6_FLAT";

    static final int POOL2_TH = 4;
    static final int POOL2_W_FISHER = 2;
    static final int POOL2_W_RSI_REV = 2;

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

            EmaVoteSnapshot s = new EmaVoteSnapshot();

            s.t30 = voteOne("30m", ema30, absUnit30, ps30, idx30, EMA30_AVG_N, EMA30_MIN_BARS, m30);
            s.t1h = voteOne("1h", ema1h, absUnit1h, ps1h, idx1h, EMA1H_AVG_N, EMA1H_MIN_BARS, h1);
            s.t4h = voteOne("4h", ema4h, absUnit4h, ps4h, idx4h, EMA4H_AVG_N, EMA4H_MIN_BARS, h4);

            s.v30 = s.t30.vote;
            s.v1h = s.t1h.vote;
            s.v4h = s.t4h.vote;

            s.score = s.v30 + s.v1h + s.v4h;
            s.posCnt = (s.v30 > 0 ? 1 : 0) + (s.v1h > 0 ? 1 : 0) + (s.v4h > 0 ? 1 : 0);
            s.negCnt = (s.v30 < 0 ? 1 : 0) + (s.v1h < 0 ? 1 : 0) + (s.v4h < 0 ? 1 : 0);

            double maxScore = 3.0 * (EMA_VOTE_BASE + EMA_VOTE_STRONG_EXTRA); // 2.4
            double thr = maxScore * EMA_VOTE_SCORE_PCT;

            s.allowLong = (s.posCnt >= 2) && (s.score >= thr);
            s.allowShort = (s.negCnt >= 2) && (s.score <= -thr);
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

    // ===================== 运行状态（只用于“实时提醒”） =====================
    static Position livePos = null; // 当前是否“已提醒入场但未提醒出场”
    static volatile long lastStopSigTs = -1L; //  实盘：止损后禁止同一根信号K再进场（禁止一根K线交易）

    //  实盘：止盈后禁止同一根信号K再进场（防止重启/重复刷新导致同K又入）
    static volatile long lastTpSigTs = -1L;

    // =====================  止损冷却（Stop-loss Cooldown） =====================
    // 目的：在“震荡反复抽打”的阶段，止损后强制等待 N 根30m K线再允许重新开仓（可开关）。
    // 说明：
    // - 只影响【进场】；不影响持仓中的 SL/TP/形态/MACD 出场
    // - 与“禁止同一根信号K再进场(lastStopSigTs / skipEntrySignalTsAfterStop)”互补
    // - scanHistory(runBacktestAlarmLogic) 与 checkRealtime 共用同一规则，避免回测/实时不一致
    static final boolean USE_STOPLOSS_COOLDOWN = true;
    static final int STOPLOSS_COOLDOWN_BARS = 2;            // 推荐：6根30m=3小时（可调 2/4/6/8/12）
    static final boolean PRINT_STOPLOSS_COOLDOWN_BLOCK = false;

    //  实盘：止损后，下一次允许进场的“entryTs”（entryTs=下一根K开盘）
    static volatile long liveCooldownUntilEntryTs = -1L;

    //  实盘：止盈后，下一次允许进场的“entryTs”（entryTs=下一根K开盘）


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
    static volatile long liveTpCooldownUntilEntryTs = -1L;

    static long calcCooldownUntilEntryTsAfterTakeProfit(long tpExitTs) {
        if (!USE_TAKEPROFIT_COOLDOWN) return -1L;
        return tpExitTs + (long) TAKEPROFIT_COOLDOWN_BARS * BAR30_MS;
    }

    static boolean blockedByTakeprofitCooldown(long candidateEntryTs, long cooldownUntilEntryTs) {
        return USE_TAKEPROFIT_COOLDOWN && cooldownUntilEntryTs > 0 && candidateEntryTs < cooldownUntilEntryTs;
    }

    static boolean printedBoot = false;

    // =====================  K线缓存（防止定时刷新窗口变化导致 MACD/EMA 漂移，回看分叉） =====================
    // 说明：
    //  - 启动时你会拉 1 年回测（全量已收K），这里把它作为“EMA/MACD 的稳定起算点”
    //  - 定时刷新只增量合并最新K线，不再用短窗口重算，避免“同一段时间的MACD”因为起点不同而漂移
    static final long CACHE_KEEP_MS = Duration.ofDays(60).toMillis();//原370
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
            Instant end = Instant.ofEpochMilli(nowMs);
            Instant start = end.minus(Duration.ofDays(60));//原370
            List<Candle> m30 = fetchRangeCandles(INST_ID, "30m", start.toEpochMilli(), end.toEpochMilli());
            List<Candle> h2 = fetchRangeCandles(INST_ID, "1H", start.toEpochMilli(), end.toEpochMilli());
            m30 = m30.stream().filter(c -> c.confirm == 1).collect(java.util.stream.Collectors.toList());
            h2 = h2.stream().filter(c -> c.confirm == 1).collect(java.util.stream.Collectors.toList());
            initCacheFromBacktest(m30, h2, nowMs);
        }

        // 2) 增量：只拉最近一小段合并，避免每次短窗口起算导致EMA漂移
        //    30m 拉 500 根（约 10 天），1H 拉 300 根（约 25 天），足够覆盖未及时刷新/补洞
        List<Candle> m30Recent = fetchRecentByCount("30m", 100);//原500
        List<Candle> h2Recent = fetchRecentByCount("1H", 120);//原300

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

    // 用于展示最近交易
    static final Deque<CompletedTrade> lastTrades = new ArrayDeque<>();

    // 最近一次机会（用于 LAST ENTER / LAST EXIT）
    static EnterOpportunity lastEnterOpp = null;
    static ExitOpportunity lastExitOpp = null;

    //  统一记录：最近一次出场机会 + 最近N笔交易（回测/scanHistory/实盘复盘共用）
    static void recordClosedTradeForView(CompletedTrade ct) {
        if (ct == null) return;
        // 最近一次出场机会
        lastExitOpp = new ExitOpportunity();
        lastExitOpp.ts = ct.exitTs;
        lastExitOpp.side = ct.side;
        lastExitOpp.price = ct.exit;
        lastExitOpp.reason = ct.reason;

        // 最近N笔
        while (lastTrades.size() >= LAST_N) lastTrades.removeFirst();
        lastTrades.addLast(ct);
    }

    // ===================== 数据结构 =====================
    static class Candle {
        long ts;
        double o, h, l, c;
        double vol;     //  新增
        int confirm;

        // 反射读库需要：无参构造（OkxCandleCache.readRange 会用 getDeclaredConstructor()）
        Candle() {}

        // 旧构造保持不变（兼容你所有旧代码）
        Candle(long ts, double o, double h, double l, double c, int confirm) {
            this(ts, o, h, l, c, 0.0, confirm);
        }

        // 新构造：带 vol
        Candle(long ts, double o, double h, double l, double c, double vol, int confirm) {
            this.ts = ts;
            this.o = o;
            this.h = h;
            this.l = l;
            this.c = c;
            this.vol = vol;
            this.confirm = confirm;
        }
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

        int overallMode;           // 0/1/2
        boolean strictBlocked;     // 是否被“总体严格门”拦截
        String detail;
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
        double[] tsi25_13;
        double[] stochD14_3;
        double[] lastPivotHigh6_6, lastPivotLow6_6;

        // FailMonth 票所需：BBWidth20
        double[] bbWidth20;

        // 新总体票（COMBO7）所需：ROC12 / SQUEEZE20_20
        double[] roc12;
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
        c.tsi25_13 = buildTsi(c.close, 25, 13);
        c.stochD14_3 = buildStochD(c.high, c.low, c.close, 14, 3);

        double[][] piv = buildPivotLast(c.high, c.low, 6, 6);
        c.lastPivotHigh6_6 = piv[0];
        c.lastPivotLow6_6 = piv[1];

        // FailMonth 票：BBWidth20
        c.bbWidth20 = buildBbWidth20(c.close, 20);

        // 新总体票（COMBO7）：ROC12 / SQUEEZE20_20
        c.roc12 = buildRoc(c.close, 12);
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
                // 旧总体 Top1：EMA20+EMA60+EMA100+EMA200+ADX14_DI+CCI20_thr100+TSI25_13
                sOverall1 += tsrndDirEma(c.close[i], c.ema20[i]);
                sOverall1 += tsrndDirEma(c.close[i], c.ema60[i]);
                sOverall1 += tsrndDirEma(c.close[i], c.ema100[i]);
                sOverall1 += tsrndDirEma(c.close[i], c.ema200[i]);
                sOverall1 += tsrndDirAdxDi(c.plusDI14[i], c.minusDI14[i]);
                sOverall1 += tsrndDirCci(c.cci20[i]);
                sOverall1 += tsrndDirTsi(c.tsi25_13[i]);
                overallStrength1 += sOverall1;
            }

            if (TSRND_OVERALL_MODE == 1 || TSRND_OVERALL_MODE == 2) {
                // 新总体 Top1：EMA60+EMA100+EMA200+ADX14_DI+ROC12_thr0.01+TSI25_13+SQUEEZE20_20
                sOverall2 += tsrndDirEma(c.close[i], c.ema60[i]);
                sOverall2 += tsrndDirEma(c.close[i], c.ema100[i]);
                sOverall2 += tsrndDirEma(c.close[i], c.ema200[i]);
                sOverall2 += tsrndDirAdxDi(c.plusDI14[i], c.minusDI14[i]);
                sOverall2 += tsrndDirRocThr((c.roc12 != null ? c.roc12[i] : Double.NaN), 0.01);
                sOverall2 += tsrndDirTsi(c.tsi25_13[i]);
                sOverall2 += tsrndDirSqueeze(c.close[i], c.ema20[i], (c.squeeze20_20 != null ? c.squeeze20_20[i] : Double.NaN));
                overallStrength2 += sOverall2;
            }

            // FailMonth 总体票（COMBO7）：EMA60+EMA100+EMA200+STOCH14_3_band+ROC12_thr0.01+TSI25_13+BBWidth20_thr0.03
            if (TSRND_FAILM_VOTER_ENABLE) {
                int sFailm = 0;
                sFailm += tsrndDirEma(c.close[i], c.ema60[i]);
                sFailm += tsrndDirEma(c.close[i], c.ema100[i]);
                sFailm += tsrndDirEma(c.close[i], c.ema200[i]);
                sFailm += tsrndDirStochBand(c.stochD14_3[i]);
                sFailm += tsrndDirRocThr((c.roc12 != null ? c.roc12[i] : Double.NaN), TSRND_FAILM_ROC_THR);
                sFailm += tsrndDirTsi(c.tsi25_13[i]);
                sFailm += tsrndDirBbWidthThr(c.close[i], c.ema20[i], (c.bbWidth20 != null ? c.bbWidth20[i] : Double.NaN), TSRND_FAILM_BBWIDTH_THR);
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
                // Short Top1：EMA100+EMA200+MACD12_26_9+CCI20_thr100+TSI25_13
                sDir += tsrndDirEma(c.close[i], c.ema100[i]);
                sDir += tsrndDirEma(c.close[i], c.ema200[i]);
                sDir += tsrndDirMacd(macd30.get(i));
                sDir += tsrndDirCci(c.cci20[i]);
                sDir += tsrndDirTsi(c.tsi25_13[i]);
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
            int L = lookback * 7; // 7 个子指标
            d.failmBin = tsrndTo5Bin(failmStrength, L);
            d.failmPass = tsrndIsAgreeBin(side, d.failmBin);
        } else {
            d.failmBin = 2;
            d.failmPass = false;
        }

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

        // 严格门：要求总体票也通过（否则当作无信号）
        if (TSRND_OVERALL_STRICT_GATE && d.dirPass && !(d.overallPass || d.failmPass)) {
            d.strictBlocked = true;
            d.score = 0;
        }
        if (d.score >= 5) d.voteScale = TSRND_SCALE_SCORE5;
        else if (d.score >= 3) d.voteScale = TSRND_SCALE_SCORE3;
        else d.voteScale = TSRND_SCALE_SCORE2;

        d.detail = String.format(Locale.US,
                "phase=%s side=%s mode=%d score=%d scale=%.1f dirStr=%d oStr1=%d oStr2=%d failmStr=%d failmBin=%d failmPass=%s dirPass=%s oPass=%s o1=%s o2=%s strict=%s",
                phase,
                (side == Side.LONG ? "LONG" : "SHORT"),
                d.overallMode,
                d.score, d.voteScale,
                d.dirStrength, d.overallStrength, d.overallStrength2,
                d.failmStrength, d.failmBin, String.valueOf(d.failmPass),
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
        Side side;
        double entry;
        long entryTs;
        double entryNotional = 0.0; //  入场时锁定的下单名义(USDT)

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
        // ===== TSRND 外层投票（入场时锁定，用于复盘/输出）=====
        int entryVoteScore = 0;        // 2 或 3
        double entryVoteScale = 1.0;   // 1.0 / 1.2 / 1.3
        boolean entryVoteDirPass = false;
        boolean entryVoteOverallPass = false;
        String entryVoteDetail = "";
        double slPointsAtEntry = SL_POINTS; // 入场时止损点数（用于R倍数与资金盈亏换算）
        String regimeAtEntry = "STABLE_4";
        double trendPtsAtEntry = 0.0;

        //  MFE / MAE（点数）
        double mfe = 0.0; // 最大浮盈点
        double mae = 0.0; // 最大浮亏点（这里用“点数幅度”，越大代表越不利）
    }

    static class CompletedTrade {
        long enterTs, exitTs;
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

        double mfe;
        double mae;
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

    public static void main(String[] args) throws Exception {
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        System.out.println("🔔🔔每年12月禁止进场，🔔🔔月亏点数大不证明亏损多只是ETH价格变化大");
        if (USE_DB_CACHE) OkxCandleCache.get(DB_FILE, DB_KEEP_YEARS).syncLastYears(INST_ID, "30m", DB_KEEP_YEARS, Candle.class, OkxSignalAlarmOnlyXAUT::fetchRangeCandlesNet);
        if (USE_DB_CACHE) OkxCandleCache.get(DB_FILE, DB_KEEP_YEARS).syncLastYears(INST_ID, "1H", DB_KEEP_YEARS, Candle.class, OkxSignalAlarmOnlyXAUT::fetchRangeCandlesNet);  // 如果你有1H过滤
        printBootStatusOnce();
        new Thread(() -> {
            try { Thread.sleep(200); } catch (Exception ignore) {}
            //alarmOnceInTick(  BAR30_MS);
        }).start();
        // 启动回测：一年（打印进/出场条件 + 月度胜率）
        if (RUN_COMPARE_ON_BOOT) {
            runCompareBacktestOnBoot();
            if (COMPARE_EXIT_AFTER) return;
        } else {
            runBacktestOneYearOnBoot();
        }

        schedule();
    }


    // =============================
    // 解耦版：本文件自带 alarmOnceInTick + 一年回测入口（不再依赖 ETH 包）
    // =============================
    static final Set<Long> ALARM_ONCE_KEYS = Collections.synchronizedSet(new HashSet<>());

    static void alarmOnceInTick(long key) {
        if (!ALARM_ONCE_KEYS.add(key)) return;
        try { java.awt.Toolkit.getDefaultToolkit().beep(); } catch (Throwable ignore) {}
        // 控制台响铃（部分终端有效）
        System.out.print("\007");
    }

    static void runBacktestOneYearOnBoot() throws Exception {
        long endMs = System.currentTimeMillis();
        long startMs = endMs - 365L * 24 * 3600 * 1000;

        // 给 MACD/均线 等一个预热窗口，避免刚开头初始化偏差过大
        long warmStartMs = startMs - 90L * 24 * 3600 * 1000;

        List<Candle> m30All = loadRangeCandles(INST_ID, "30m", warmStartMs, endMs);
        List<Candle> h1All  = loadRangeCandles(INST_ID, "1H",  warmStartMs, endMs);

        // 只用已收盘（confirm=1）
        List<Candle> m30 = new ArrayList<>(m30All.size());
        for (Candle c : m30All) if (c.confirm == 1) m30.add(c);
        List<Candle> h1  = new ArrayList<>(h1All.size());
        for (Candle c : h1All)  if (c.confirm == 1) h1.add(c);

        if (m30.isEmpty()) {
            System.out.println("⚠️ 回测跳过：30m K 为空");
            return;
        }

        List<MacdPoint> macd30 = calcMacd(m30);
        List<MacdPoint> macd1h = (h1.isEmpty() ? Collections.emptyList() : calcMacd(h1));

        int from30 = Math.max(0, upperBoundCandle(m30, startMs) - 1);
        int from1h = (h1.isEmpty() ? 0 : Math.max(0, upperBoundCandle(h1, startMs) - 1));

        List<Candle> m30_1y = m30.subList(from30, m30.size());
        List<MacdPoint> macd30_1y = macd30.subList(from30, macd30.size());

        List<Candle> h1_1y;
        List<MacdPoint> macd1h_1y;
        if (h1.isEmpty()) {
            h1_1y = Collections.emptyList();
            macd1h_1y = Collections.emptyList();
        } else {
            h1_1y = h1.subList(from1h, h1.size());
            macd1h_1y = macd1h.subList(from1h, macd1h.size());
        }

        List<CompletedTrade> trades = runBacktestAlarmLogic(m30_1y, macd30_1y, h1_1y, macd1h_1y);
        printBacktestReport(trades);
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
    static Side entrySideByDifDea(List<Candle> m30, List<MacdPoint> macd30, int i) {
        Side side = entrySideByDifDeaCore(macd30, i);
        if (side != null) return side;

        // 原逻辑未给方向 -> 进入 OKX 组合兜底
        return OkxComboEntryDecision.decide(m30, i);
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
            if (k2.dif > 0 && k1.dif > 1 && k0.dif > 1 &&
                    k2.dif < k1.dif
                    && k1.dif < k0.dif
                    && (k0.dif - k1.dif) >0.44) {

                return Side.LONG;
            }

            // ===== SHORT（保持你原来的逻辑，不动）=====
            //  如果你原文件里 SHORT 是别的写法，
            // 请把下面这段替换回你原本的 SHORT 判断即可

            // 空：K1.dea<0 && K2.dea<0 && int(K1.dea - K2.dea) >= 2
            if (k2.dea < 0 && k1.dea < -1 && k0.dea < -1&&
                    k2.dea > k1.dea &&
                    k1.dea > k0.dea &&
                    (k1.dea - k0.dea) > 0.44) {
                // if ((int)(k1.dea - k2.dea) >= 0.88)
                return Side.SHORT;
            }

            return null;

        } else {

            //  严谨：要算最近N根平均值，至少要有 N 根数据（包含当前 i）
            if (i < AVG_N - 1) return null;

            MacdPoint k0 = macd30.get(i);
            MacdPoint k1 = macd30.get(i - 1);

            // 用最近N根的平均值作为阈值（double）
            // 如果你想排除当前k0（例如k0可能是未收盘），把 endIdx 改成 i - 1
            double difAvgN = avgAbsDif(macd30, i, AVG_N);
            double deaAvgN = avgAbsDea(macd30, i, AVG_N);

            // LONG: 保留你当前阈值的后两根
            if (k1.dif > 2 && k0.dif > 2 &&
                    k1.dif < k0.dif &&
                    (k0.dif - k1.dif) >= difAvgN/8) {
                return Side.LONG;
            }

            // SHORT: 保留你当前阈值的后两根
            if (k1.dea < 2 && k0.dea < 2 &&
                    k1.dea > k0.dea &&
                    (k1.dea - k0.dea) >= deaAvgN/8) {
                return Side.SHORT;
            }

            return null;
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
        static final int EVAL_HISTORY_BARS = 3000;

        // OKX 组合（固定参数，与你挖掘结果一致）
        static final eval.engine.candidates.PoolCondition PC_SAR =
                eval.engine.candidates.ConditionFactory.psarTrend(0.02, 0.2);      // id= SAR_0.02_0.2
        static final eval.engine.candidates.PoolCondition PC_SUPER =
                eval.engine.candidates.ConditionFactory.superTrend(10, 3.0);       // id= SUPER_TREND_10_3.0
        static final eval.engine.candidates.PoolCondition PC_PSNX =
                eval.engine.candidates.ConditionFactory.psInsideNrx(8);            // id= PS_NRX_8  (完美复制)
        static final eval.engine.candidates.PoolCondition PC_STOCH_BAND =
                eval.engine.candidates.ConditionFactory.stochBand(14, 3, 80, 20);  // id= STOCH14_3_band
        static final eval.engine.candidates.PoolCondition PC_FISHER =
                eval.engine.candidates.ConditionFactory.fisherPrice(10);           // id= FISHER_P10

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
                if (mainDir > 0) return Side.LONG;
                if (mainDir < 0) return Side.SHORT;

                // ② 主力不允许 -> 空头专用(SHORT)
                int stochDir  = dirAt(PC_STOCH_BAND, evalCandles, idx);
                int fisherDir = dirAt(PC_FISHER, evalCandles, idx);
                if (stochDir < 0 && fisherDir < 0) return Side.SHORT;

                return null;
            } catch (Throwable t) {
                // 兜底：不让组合逻辑影响你原本的系统稳定性
                return null;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        static int dirAt(eval.engine.candidates.PoolCondition pc, List<Object> evalCandles, int idx) throws Exception {
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
    static final class EvalCandleAdapter {

        static volatile boolean inited = false;
        static Class<?> evalCandleCls;
        static java.lang.reflect.Constructor<?> ctor7;
        static java.lang.reflect.Constructor<?> ctor6;
        static java.lang.reflect.Constructor<?> ctor5;
        static java.lang.reflect.Constructor<?> ctor0;

        static java.lang.reflect.Field fTs, fO, fH, fL, fC, fVol, fConfirm;
        static java.lang.reflect.Field fOpenTs, fOpenTsMs;

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

        static java.lang.reflect.Constructor<?> findCtor(Class<?> cls, Class<?>... p) {
            try {
                java.lang.reflect.Constructor<?> c = cls.getDeclaredConstructor(p);
                c.setAccessible(true);
                return c;
            } catch (Throwable t) {
                return null;
            }
        }

        static java.lang.reflect.Field findField(Class<?> cls, String name) {
            try {
                java.lang.reflect.Field f = cls.getDeclaredField(name);
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
        if (entryIdx <= 0 || entryIdx >= m30.size()) return null;

        int sigIdx = entryIdx - 1; // 信号K0索引（上一根已收盘）
        if (sigIdx < 2) return null;

        Candle entryC = m30.get(entryIdx);

        // 亚洲盘过滤：按入场K时间对齐（与实盘一致）
        if (isAsiaLowLiquidityTime(entryC.ts)) return null;

        // 先给出方向（仅基于 DIF/DEA）
        Side side = entrySideByDifDea(m30, macd30, sigIdx);

        //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1）
        side = applyDecemberNoEntryGate(side, m30, sigIdx, m30.get(entryIdx).ts, "回看", silent);
        if (side == null) return null;
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
            else if (h2 != null && !h2.isEmpty()) h1Idx = upperBoundCandle(h2, entryC.ts) - 1;

            MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
            if (regime == MarketRegime.RANGE) {
                if (!silent) {
                    System.out.printf(Locale.US,
                            "【状态机过滤-回看】ts=%s | regime=RANGE → 趋势系统不进场%n",
                            FMT_JST.format(Instant.ofEpochMilli(entryC.ts))
                    );
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

        //  解法一：手续费 / 波动门槛（用信号K0及之前数据计算，避免未来函数）
        double atrSig = calcAtr(m30, sigIdx, FEE_VOL_ATR_N);
        if (!passFeeVolGuard(atrSig, entryC.o)) {
            if (!silent) {
                System.out.printf(Locale.US,
                        "【手续费/波动过滤-回看】ts=%s | %s → 不进场%n",
                        FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                        feeVolGuardReason(atrSig, entryC.o)
                );
            }
            return null;
        }




        // 解法一.5：slPts/entry 的分位数门槛（动态阈值，只用过去数据）
        if (USE_SL_PCT_Q_GATE) {
            SlPctGateResult g = evalSlPctQuantileGate(m30, sigIdx, entryC.o);
            if (!g.pass) {
                if (!silent) {
                    System.out.printf(Locale.US,
                            "【SL%%分位过滤-回看】ts=%s | slPts=%.2f | entry=%.2f | slPct=%.5f > q%.0f=%.5f (n=%d, lb=%d) → 不进场%n",
                            FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                            g.slPts, entryC.o, g.slPct,
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
        if (i < 1) return false;
        double cur = macdStrength(macd.get(i));
        double prev = macdStrength(macd.get(i - 1));
        if (side == Side.LONG) return cur < prev;
        return cur < prev;
    }

    // =====================  新增：亚洲盘前半过滤（只影响进场） =====================
    public static boolean isAsiaLowLiquidityTime(long tsMs) {
        if (!FILTER_ASIA_LOW_LIQ) return false;
        ZonedDateTime z = Instant.ofEpochMilli(tsMs).atZone(ZONE);
        int h = z.getHour();
        //  JST 不开仓窗口：04-08 & 13-20

        boolean block1 = (h >= NO_TRADE_1_START && h < NO_TRADE_1_END);
        boolean block2 = (h >= NO_TRADE_2_START && h < NO_TRADE_2_END);
        // 周五后半段等时间过滤
        DayOfWeek dow = z.getDayOfWeek();

        // 周五后半段不进场（JST 周五 18:00-24:00）
        boolean fridayLate = (dow == DayOfWeek.FRIDAY && h >= 18);

        // 周一早盘不进场（JST 周一 04:00-09:00）
        boolean mondayMorning = (dow == DayOfWeek.MONDAY && h >= 4 && h < 9);
        // 周六和周天的早8点到晚上9点不进场（JST 周六和周天 08:00-21:00）
        boolean weekendMorningToEvening = ((dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) && h >= 8 && h <= 21);

        return weekendMorningToEvening ;//block1 || block2 ||
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
    static List<CompletedTrade> runBacktestAlarmLogic(List<Candle> m30, List<MacdPoint> macd30,
                                                      List<Candle> h2,  List<MacdPoint> macd1h) {
        //  回测前清空“最近机会/最近10笔”缓存，保证回测后辅助输出不为空
        lastEnterOpp = null;
        lastExitOpp = null;
        lastTrades.clear();

        //  EMA 多时间投票上下文（回测）
        EMA_VOTE_CTX_BACKTEST = (USE_EMA_VOTE_FILTER ? EmaVoteContext.build(m30, h2) : null);

        List<CompletedTrade> trades = new ArrayList<>();
        Position cur = null;

        long skipEntrySignalTsAfterStop = -1L; //  止损后：禁止同一信号K再进场（禁止一根K线交易）
        long cooldownUntilEntryTsAfterStop = -1L; //  止损冷却：在此时间前不允许再入场（entryTs=下一根K开盘）

        long skipEntrySignalTsAfterTp = -1L; //  止盈后：禁止同一信号K再进场（禁止一根K线交易）
        long cooldownUntilEntryTsAfterTp = -1L; //  止盈冷却：在此时间前不允许再入场（entryTs=下一根K开盘）
        for (int i = 1; i < m30.size(); i++) {
            Candle c = m30.get(i); // 入场K（next-open）

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
                if (sigIdx < 2) {
                    // entrySideByDifDea 需要 i>=2（要访问 i-2）
                    continue;
                }

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
                    continue;
                }

                //  1H趋势信息：按【入场K(i)】时间对齐（修复 01:00 被 00:30 的1H neutral 错杀）
                if (USE_1H_FILTER && !h2.isEmpty() && !macd1h.isEmpty()) {
                    t1hInfo = get1HTrendInfoAt(h2, macd1h, c.ts) /* 按入场K时间取1H */;
                }

                Side side = entrySideByDifDea(m30, macd30, sigIdx);


                //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1）
                side = applyDecemberNoEntryGate(side, m30, sigIdx, c.ts, "回测", true);
                if (side == null) continue;
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
                //  解法三：市场状态机过滤（趋势态才允许趋势系统进场）
                if (side != null && USE_MARKET_REGIME_FILTER) {
                    int h1Idx = -1;
                    if (USE_1H_FILTER) h1Idx = t1hInfo.idx;
                    else if (h2 != null && !h2.isEmpty()) h1Idx = upperBoundCandle(h2, c.ts + BAR30_MS) - 1;

                    MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
                    if (regime == MarketRegime.RANGE) {
                        if (false) {
                            System.out.printf(Locale.US,
                                    "【状态机拦截-回测】ts=%s | %s → 不进场%n",
                                    FMT_JST.format(Instant.ofEpochMilli(c.ts + BAR30_MS)),
                                    marketRegimeReason(h2, h1Idx)
                            );
                        }
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
                }                //  EMA 投票过滤（入场时间=下一根30m开盘=c.ts）
                if (side != null && USE_EMA_VOTE_FILTER && EMA_VOTE_CTX_BACKTEST != null) {
                    side = applyEmaVoteFilter(side, c.ts, EMA_VOTE_CTX_BACKTEST, false, "回测");
                }
                if (side != null) {

                    // =====================  TSRND 外层投票：方向(2分) + 总体(+1) => 名义倍率 *1.0/*1.2 =====================
                    OuterVoteDecision ov = evalTsRndOuterVote(m30, macd30, sigIdx, side, "回测", true);
                    if (USE_TSRND_OUTER_VOTE) {
                        if (ov.score < TSRND_SCORE_THRESHOLD) {
                            // 方向票不过：直接不进场
                            continue;
                        }
                    }
                    int voteScore = ov.score;
                    double voteScale = ov.voteScale;
                    MacdPoint k0 = macd30.get(sigIdx);      // 信号K0（已收盘）
                    MacdPoint k1 = macd30.get(sigIdx - 1);  // 信号K1（已收盘）
                    int entryIdx = sigIdx + 1;                 //  next-open 的K索引
                    if (entryIdx >= m30.size()) continue;      //  没有下一根就跳过（或 break，看你逻辑）
                    Candle entryC = m30.get(entryIdx);         //  入场K = 信号K0 的下一根
                    double entryPx = entryC.o;                 //  next-open 入场参考价（原逻辑）
                    // =====================  OKX 撑压线入场限制：差 60 点以内不进（回测一致） =====================
                    SrLevels sr = calcOkxSrLevels(m30, sigIdx, OKX_SR_N);
                    if (USE_OKX_SR_ENTRY_FILTER && sr.valid) {
                        double dynSlPtsForGate = calcSlPointsForIndex(m30, sigIdx); // K0及之前（避免未来函数）
                        double minGapPts = okxSrEntryMinGapPts(dynSlPtsForGate);
                        double gap = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                        if (gap < minGapPts) {
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
//  回撤入场诊断打印：无论是否成交都打印一次，方便你确认逻辑是否真的启用
                    if (pbEnabled) {
                        System.out.printf(Locale.US,
                                "    【回撤挂单】entryRef=%.2f limit=%.2f | used=%.2f mode=%s | ATR=%.2f | entryK(H/L)=%.2f/%.2f%n",
                                entryPx, pbLimitPx, pbUsedPts, PULLBACK_MODE, atrSig, entryC.h, entryC.l
                        );
                    } else {
                        System.out.printf(Locale.US,
                                "    【回撤挂单】OFF（按原next-open进场）entryRef=%.2f | used(仅打印)=%.2f mode=%s | ATR=%.2f%n",
                                entryPx, pbUsedPts, PULLBACK_MODE, atrSig
                        );
                    }

                    int diffInt = (side == Side.LONG)
                            ? (int) (k0.dif - k1.dif)
                            : (int) (k0.dea - k1.dea);

                    System.out.printf(Locale.US,
                            "【回测-进场】 时间=%s | 方向=%s | 1H=%s | 入场(下一根开盘)=%.2f | dif(本根/前一)=%.6f/%.6f | dea(本根/前一)=%.6f/%.6f | 动量diffInt=%d%n",
                            FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                            (side == Side.LONG ? "做多" : "做空"),
                            (USE_1H_FILTER ? (t1hInfo.trend + (t1hInfo.isFirstNeutralBar ? "(NEUTRAL_FIRST)" : "")) : "OFF"),
                            entryPx,
                            k0.dif, k1.dif, k0.dea, k1.dea,
                            diffInt
                    );

                    //  更新“最近一次进场机会”（回测/实盘统一显示）
                    lastEnterOpp = new EnterOpportunity();
                    lastEnterOpp.ts = entryC.ts;
                    lastEnterOpp.side = side;
                    lastEnterOpp.entryRef = entryPx;
                    lastEnterOpp.pbEnabled = pbEnabled;
                    lastEnterOpp.pbMode = PULLBACK_MODE.name();
                    lastEnterOpp.atrAtSignal = atrSig;
                    lastEnterOpp.pbAtrPts = pbAtrPts;
                    lastEnterOpp.pbFixedPts = pbFixedPts;
                    lastEnterOpp.pbUsedPts = pbUsedPts;
                    lastEnterOpp.pbLimitPx = pbLimitPx;
                    lastEnterOpp.pbFilled = pbFilled;
                    lastEnterOpp.pbPending = false;
                    lastEnterOpp.entryFillPx = entryFillPx;

                    lastEnterOpp.dif1 = k0.dif;
                    lastEnterOpp.dif2 = k1.dif;
                    lastEnterOpp.dea1 = k0.dea;
                    lastEnterOpp.dea2 = k1.dea;
                    lastEnterOpp.diffInt = diffInt;
                    lastEnterOpp.t1h = t1hInfo.trend;

                    //  回撤入场：回测开关打开但未触达 limit → 本次不成交（避免未来函数）
                    if (pbEnabled && !pbFilled) {
                        System.out.printf(Locale.US,
                                "    【回撤未成交】entryRef=%.2f limit=%.2f | used=%.2f mode=%s | ATR=%.2f | 说明=下一根K未触达回撤限价，本次信号不成交%n",
                                entryPx, pbLimitPx, pbUsedPts, PULLBACK_MODE, atrSig
                        );
                        continue;
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
                    cur.entryTs = entryC.ts;
                    cur.slPoints = calcSlPointsForIndex(m30, sigIdx); //  用信号K0对齐动态SL
                    //  SRMF：锁定入场时的 mult / regime / trendPts / equity
                    cur.entryEquity = equitySim;
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
                    cur.entryVoteDetail = ov.detail;
                    cur.entryTierMult = SRMF.getCurrentMult();
                    cur.entryMult = cur.entryTierMult * voteScale;
                    cur.regimeAtEntry = SRMF.getRegime().name();
                    cur.trendPtsAtEntry = SRMF.getTrendPts();
                    double entryRiskAmt = cur.entryEquity * RISK_PCT * cur.entryMult;
                    double entryNotional = entryRiskAmt * entryFillPx / Math.max(1e-9, cur.slPointsAtEntry);
                    cur.entryNotional = entryNotional;
                    cur.entryNotional = entryNotional;
                    if (VERBOSE_BACKTEST) System.out.printf(Locale.US, "【SRMF-进场】 时间=%s | 入场开盘=%.2f | 模式=%s | trendPts=%.2f | 倍数=%.2f(*%.1f) | 票=%d | 止损点=%.0f | 风险R=%.2f | 名义=%.0f | 资金=%.2f%n",
                            FMT_JST.format(Instant.ofEpochMilli(cur.entryTs)), cur.entry,
                            cur.regimeAtEntry, cur.trendPtsAtEntry, cur.entryMult, cur.entryVoteScale, cur.entryVoteScore, cur.slPointsAtEntry,
                            entryRiskAmt, entryNotional, cur.entryEquity);
                    cur.mfe = 0.0;
                    cur.mae = 0.0;
                }
                //  允许“入场根”触发出场：如果本根没开仓就 continue；若已开仓则落入下面持仓段执行 SL/TP（SL 优先）。
                if (cur == null) continue;
            }

            // ========== 持仓：先更新 MFE/MAE ==========
            updateMfeMae(cur, c);

            //  出场优先级：SL -> TP全平/OKX撑压止盈 -> 原出场
            boolean stop = hitStopLoss(cur, c);
            boolean tp2  = (!stop) && hitTP2(cur, c);

            double okxTpPx = (cur.side == Side.LONG) ? cur.okxResistanceAtEntry : cur.okxSupportAtEntry;
            boolean okxSrTp = (!stop) && USE_OKX_SR_TP && hitOkxSrTP(cur, c, okxTpPx);

            if (stop || tp2 || okxSrTp) {
                CompletedTrade ct = new CompletedTrade();
                ct.enterTs = cur.entryTs;
                ct.side = cur.side;
                ct.entry = cur.entry;

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
                    long nextTs = (i + 1 < m30.size() ? m30.get(i + 1).ts : -1);
                    if (ct.exitTs == nextTs) {
                        int sigIdx2 = i;          // 下一根入场K(i+1) 的信号K0 = 当前K(i)
                        int entryIdx2 = i + 1;    // 入场K索引
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
                equitySim += pnlUSDT;

                ct.entryEquity = cur.entryEquity;
                ct.exitEquity = equitySim;
                ct.pnlUSDT = pnlUSDT;
                ct.mult = cur.entryMult;
                ct.slPointsUsed = cur.slPointsAtEntry;
                ct.regime = cur.regimeAtEntry;
                ct.rMultiple = ct.pnlPoints / Math.max(1e-9, cur.slPointsAtEntry);
                ct.trendPtsAfter = 0.0; // 下面 onTradeClosed 后回填

                //  用历史回放更新 SRMF（用于下一笔 mult / 环境状态）
                SRMF.onTradeClosed(ct.pnlPoints, equitySim);
                ct.trendPtsAfter = SRMF.getTrendPts();
                if (VERBOSE_BACKTEST) System.out.printf(Locale.US, "【SRMF-出场】 时间=%s | 方向=%s | 毛=%.2f点 | 费=%.2f点 | 净=%.2f点(%.2fU) | R倍=%.2f | 倍数=%.2f | 止损点=%.0f | 资金=%.2f | trendPts=%.2f | 原因=%s%n",
                        FMT_JST.format(Instant.ofEpochMilli(ct.exitTs)),
                        (ct.side==Side.LONG?"做多":"做空"),
                        ct.grossPnlPoints, ct.feePoints, ct.pnlPoints, ct.pnlUSDT, ct.rMultiple, ct.mult, ct.slPointsUsed, ct.exitEquity, ct.trendPtsAfter, ct.reason);

                ct.mfe = cur.mfe;
                ct.mae = cur.mae;

                //  统一记录：最近一次出场机会 + 最近10笔（止损/TP/OKX SR 都要计入）
                recordClosedTradeForView(ct);

                trades.add(ct);
                cur = null;
                continue;
            }
            boolean macdRev = shouldExitByMacdWithGuards(m30, macd30, i, cur, c.c, "回测", c.ts + BAR30_MS);

            if (macdRev) {
                CompletedTrade ct = new CompletedTrade();
                ct.enterTs = cur.entryTs;
                ct.side = cur.side;
                ct.entry = cur.entry;

                if (macdRev){
                    ct.isStop = false;
                    ct.reason = "MACD颜色反转出场";
                    ct.exitTs = c.ts + BAR30_MS;
                    ct.exit = c.c;
                }
                //  出场时间对齐到“下一根开盘”(K+1 open)。若同一时间也满足入场，则按你的要求：不出不进=继续持仓。
                if (ct.exitTs == ((i + 1) < m30.size() ? m30.get(i + 1).ts : Long.MIN_VALUE)) {
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
                equitySim += pnlUSDT;

                ct.entryEquity = cur.entryEquity;
                ct.exitEquity = equitySim;
                ct.pnlUSDT = pnlUSDT;
                ct.mult = cur.entryMult;
                ct.slPointsUsed = cur.slPointsAtEntry;
                ct.regime = cur.regimeAtEntry;
                ct.rMultiple = ct.pnlPoints / Math.max(1e-9, cur.slPointsAtEntry);
                ct.trendPtsAfter = 0.0; // 下面 onTradeClosed 后回填

                //  用历史回放更新 SRMF（用于下一笔 mult / 环境状态）
                SRMF.onTradeClosed(ct.pnlPoints, equitySim);
                ct.trendPtsAfter = SRMF.getTrendPts();
                if (VERBOSE_BACKTEST) System.out.printf(Locale.US, "【SRMF-出场】 时间=%s | 方向=%s | 毛=%.2f点 | 费=%.2f点 | 净=%.2f点(%.2fU) | R倍=%.2f | 倍数=%.2f | 止损点=%.0f | 资金=%.2f | trendPts=%.2f | 原因=%s%n",
                        FMT_JST.format(Instant.ofEpochMilli(ct.exitTs)),
                        (ct.side==Side.LONG?"做多":"做空"),
                        ct.grossPnlPoints, ct.feePoints, ct.pnlPoints, ct.pnlUSDT, ct.rMultiple, ct.mult, ct.slPointsUsed, ct.exitEquity, ct.trendPtsAfter, ct.reason);

                ct.mfe = cur.mfe;
                ct.mae = cur.mae;

                //  统一记录：最近一次出场机会 + 最近10笔（包含止损/TP/形态/MACD等所有出场）
                recordClosedTradeForView(ct);
                trades.add(ct);

                //  MACD 止盈也纳入“止盈冷却”：出场后等待 N 根K线才允许重新进场
                lastTpSigTs = c.ts;
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
            }
        }
        EMA_VOTE_CTX_BACKTEST = null;
        return trades;
    }

    static void printBacktestReport(List<CompletedTrade> trades) {
        System.out.println();
        System.out.println("========== 回测结果 ==========");

        int n = trades.size();
        long win = trades.stream().filter(t -> t.pnlPoints > 0).count();
        double total = trades.stream().mapToDouble(t -> t.pnlPoints).sum();
        double totalGross = trades.stream().mapToDouble(t -> t.grossPnlPoints).sum();
        double totalFee = trades.stream().mapToDouble(t -> t.feePoints).sum();
        double avg = (n == 0 ? 0 : total / n);

        double maxDd = calcMaxDrawdownPoints(trades);
        int maxLoseStreak = calcMaxLoseStreak(trades);

        System.out.println("交易次数 = " + n);
        System.out.println("胜率    = " + (n == 0 ? "0.00%" : String.format(Locale.US, "%.2f%%", (win * 100.0 / n))) + " (" + win + "/" + n + ")");
        System.out.println("总盈亏(点) = " + String.format(Locale.US, "净%.2f | 毛%.2f | 手续费%.2f", total, totalGross, totalFee));
        System.out.println("平均每笔(点) = " + String.format(Locale.US, "%.4f", avg));
        System.out.println("平均手续费(点) = " + String.format(Locale.US, "%.4f", (n==0?0:totalFee/n)));
        System.out.println("最大回撤(点) = " + String.format(Locale.US, "%.2f", maxDd));
        System.out.println("最大连亏(笔) = " + maxLoseStreak);

        // ===== 资金曲线（动态R=2% + mult）统计（如果 trades 已填充 pnlUSDT/exitEquity） =====
        boolean hasEquity = trades.stream().anyMatch(t -> t.exitEquity != 0.0);
        if (hasEquity) {
            double startEq = capital;
            double endEq = trades.get(trades.size()-1).exitEquity;
            double totalUsdt = trades.stream().mapToDouble(t -> t.pnlUSDT).sum();
            double maxDdAmt = calcMaxDrawdownUSDT(trades, startEq);
            double maxDdPct = (startEq <= 0 ? 0 : (maxDdAmt / Math.max(1e-9, calcPeakEquity(trades, startEq)) * 100.0));
            System.out.println();
            System.out.println("========== EQUITY（动态R=2%） ==========");
            System.out.println("初始资金 = " + String.format(Locale.US, "%.2f", startEq));
            System.out.println("最终资金 = " + String.format(Locale.US, "%.2f", endEq));
            System.out.println("总盈亏(USDT) = " + String.format(Locale.US, "%.2f", totalUsdt));
            System.out.println("最大回撤(USDT) = " + String.format(Locale.US, "%.2f", maxDdAmt));
            // maxDdPct 这里用全程 peak 近似（更稳可以按逐步峰值算），先给你一个可读指标
            System.out.println("最大回撤(%)  ≈ " + String.format(Locale.US, "%.2f", maxDdPct) + "%");
        }   System.out.println("SRMF状态 = regime=" + SRMF.getRegime()
                + " trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts())
                + " mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult())
                + (APPLY_OKX_FEE ? (" | fee双边=" + String.format(Locale.US, "%.4f%%", OKX_FEE_RATE_PER_SIDE*2*100)) : " | fee=OFF"));

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

        if (lastEnterOpp != null && lastEnterOpp.side != null) {
            double lim = (lastEnterOpp.side == Side.LONG)
                    ? (lastEnterOpp.entryRef - pbUsedPtsPrint)
                    : (lastEnterOpp.entryRef + pbUsedPtsPrint);
            System.out.printf(Locale.US,
                    "回撤入场参考价：%s limit=%.2f （参考进场价=%.2f %s %.2f）%n",
                    (lastEnterOpp.side == Side.LONG ? "做多" : "做空"),
                    lim,
                    lastEnterOpp.entryRef,
                    (lastEnterOpp.side == Side.LONG ? "-" : "+"),
                    pbUsedPtsPrint
            );
        }

//  快速观察：稳健4档 / 进攻5档 的“参考名义”（按 A：已收K收盘价=最后一根已收K close）
        double obsPrice = lastPriceCache;
        double obsSl = Math.max(1e-9, lastSlPointsCache);
        if (obsPrice > 0) {
            double obsBaseRisk = capital * RISK_PCT;
            double tp = SRMF.getTrendPts();
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

        for (CompletedTrade t : trades) {
            String key = ym.format(Instant.ofEpochMilli(t.enterTs));
            byMonth.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }

        if (byMonth.isEmpty()) {
            System.out.println("（全年无交易触发）");
        } else {
            for (Map.Entry<String, List<CompletedTrade>> e : byMonth.entrySet()) {
                List<CompletedTrade> list = e.getValue();
                int cnt = list.size();
                long w = list.stream().filter(x -> x.pnlPoints > 0).count();
                double sum = list.stream().mapToDouble(x -> x.pnlPoints).sum();
                double wr = (cnt == 0 ? 0 : (w * 100.0 / cnt));

                System.out.printf(Locale.US, "%s | 笔数=%d | 胜率=%.2f%% | 盈亏点=%.2f%n",
                        e.getKey(), cnt, wr, sum);
            }
        }

        System.out.println("=====================================");
        System.out.println();
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
            if (t.exitEquity != 0.0) {
                eq = t.exitEquity;
            } else {
                eq += t.pnlUSDT;
            }
            if (eq > peak) peak = eq;

            double dd = peak - eq;
            if (dd > maxDd) maxDd = dd;
        }
        return maxDd;
    }
    // ===================== DB优先：拉K线（按时间范围） =====================
    // - USE_DB_CACHE=true：优先读SQLite；不够再走网络分页补齐；只入库已收盘(confirm=1)
    // - USE_DB_CACHE=false：直接走网络分页
    static List<Candle> fetchRangeCandles(String instId, String bar, long startMs, long endMs) throws Exception {
        if (!USE_DB_CACHE) return fetchRangeCandlesNetAdapter(instId, bar, startMs, endMs);
        return OkxCandleCache.get(DB_FILE, DB_KEEP_YEARS).loadRange(instId, bar, startMs, endMs, Candle.class, OkxSignalAlarmOnlyXAUT::fetchRangeCandlesNet);
    }


    // ===================== 兼容包装：保持你旧版本调用不变 =====================
    // 之前某些版本在启动 warmup 时调用 loadRangeCandles(...)，这里做一层薄包装，避免“找不到方法”。
    static List<Candle> loadRangeCandles(String instId, String bar, long startMs, long endMs) throws Exception {
        return fetchRangeCandles(instId, bar, startMs, endMs);
    }

    // USE_DB_CACHE=false 时的适配器：旧代码里叫 fetchRangeCandlesNetAdapter，实际就是直接网络分页拉取
    static List<Candle> fetchRangeCandlesNetAdapter(String instId, String bar, long startMs, long endMs) throws Exception {
        return fetchRangeCandlesNet(instId, bar, startMs, endMs);
    }

// ===================== 网络：拉K线（按时间范围分页） =====================
    static List<Candle> fetchRangeCandlesNet(String instId, String bar, long startMs, long endMs) throws Exception {
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
            printRefreshHeader(now, last30, livePos);
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
        long expectedClose = (now / BAR30_MS) * BAR30_MS;
        long expectedOpen  = expectedClose - BAR30_MS;
        //  若 OKX 收盘后延迟更新（:00:07/:30:07 仍可能拿到上一根），这里短暂等待并重拉，避免“无进场/晚一根”
        int waitTry = 0;
        while ((last30 == null || last30.ts != expectedOpen) && waitTry < WAIT_OKX_REFRESH_TRIES) {
            if (waitTry == 0) {
                printRefreshHeader(now, last30, livePos);
                System.out.println("[WARN] OKX 30m K线还没刷新到最新收盘，开始等待刷新（防止无提示/晚一根）。");
                System.out.println("期望已收K(开盘)=" + FMT_JST.format(Instant.ofEpochMilli(expectedOpen))
                        + " | 实际已收K(开盘)=" + (last30 == null ? "(null)" : FMT_JST.format(Instant.ofEpochMilli(last30.ts))));
                System.out.println();
            }
            Thread.sleep(WAIT_OKX_REFRESH_MS);
            long now2 = System.currentTimeMillis();
            syncCache(now2);
            m30 = filterClosedByTime(snapshotCache(CACHE_M30), BAR30_MS, now2, SAFE_CLOSE_MS);
            h2  = filterClosedByTime(snapshotCache(CACHE_1H),  BAR1H_MS, now2, SAFE_CLOSE_MS);
            last30 = (m30.isEmpty() ? null : m30.get(m30.size() - 1));
            waitTry++;
        }
        if (last30 == null || last30.ts != expectedOpen) {
            printRefreshHeader(now, last30, livePos);
            System.out.println("[WARN] OKX 30m K线仍未刷新到最新收盘，跳过本次提醒（避免时间不一致造成误判）。");
            System.out.println("期望已收K(开盘)=" + FMT_JST.format(Instant.ofEpochMilli(expectedOpen))
                    + " | 实际已收K(开盘)=" + (last30 == null ? "(null)" : FMT_JST.format(Instant.ofEpochMilli(last30.ts))));
            System.out.println();
            return;
        }

        //  等待刷新后：m30/last30 可能已被重拉，这里同步刷新“打印/下单用”的缓存
        // （保证 SRMF 下单名义使用“本次定时刷新最终的动态止损点数”，且仅基于已收盘K0，无未来函数）
        lastM30ClosedForPrint = m30;
        lastM30ClosedEndIdxForPrint = m30.size() - 1;
        updateOkxSrCacheForPrint(m30, lastM30ClosedEndIdxForPrint);
        if (m30.size() >= 2) updateDynamicSlCache(m30, lastM30ClosedEndIdxForPrint);
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

        List<MacdPoint> macd30 = calcMacd(m30);
        List<MacdPoint> macd1h = (h2.isEmpty() ? Collections.emptyList() : calcMacd(h2));

        //  回看（最近10笔 / LAST ENTER/EXIT）
        scanHistory(m30, macd30, h2, macd1h);

        //  动态止损已在 runOnce 早期更新（lastM30ClosedForPrint），这里不重复计算
        List<Candle> m30Latest = fetchLatestCandles("30m", 10);

        //  先实时触发（会更新 livePos），再打印状态块，避免“响了但显示空仓”的错觉
        checkRealtime(m30, macd30, h2, macd1h, m30Latest);

        //  再打印（保证持仓状态、最近机会等和提示后的状态一致）
        printRefreshHeader(now, last30, livePos);

        printIndicatorSnapshotLite(m30, macd30, h2, macd1h);
        printLastOppTemplate();
        printLastTradesTemplate();
        printSrmfBlock();
    }

    // ===================== 扫历史（用于最近10笔/last enter/exit） =====================
    // ===================== 扫历史（用于：最近10笔 / 最近机会 / 与实盘一致） =====================
    // 说明：之前这里有一套“简化回放”逻辑，缺少【出场=下一根开盘 & 同一时刻又触发入场】的冲突保护，
    // 会导致你看到的“分叉”（同一时间先出再进）。现在直接复用 runBacktestAlarmLogic，保证与开局一年回测一致。
    static void scanHistory(List<Candle> m30, List<MacdPoint> macd30, List<Candle> h2, List<MacdPoint> macd1h) {

        // 关键修复：scanHistory 只能用“已收盘(confirm=1)”K线做回看。
        // 否则未收盘K线(high/low/close)会在下一次刷新被修正，导致回看结果漂移（重启后 4:00 → 4:30 等）。
        // 同时：MACD 序列长度必须与 candle 序列严格对齐（避免尾部 confirm=0 导致索引错位）。
        List<Candle> m30Closed = new ArrayList<>();
        for (Candle c : m30) {
            if (c != null && c.confirm == 1) m30Closed.add(c);
        }
        List<Candle> h2Closed = new ArrayList<>();
        for (Candle c : h2) {
            if (c != null && c.confirm == 1) h2Closed.add(c);
        }

        List<MacdPoint> macd30Closed;
        if (macd30 != null && macd30.size() >= m30Closed.size()) {
            macd30Closed = macd30.subList(0, m30Closed.size());
        } else {
            macd30Closed = calcMacd(m30Closed);
        }

        List<MacdPoint> macd1hClosed;
        if (macd1h != null && macd1h.size() >= h2Closed.size()) {
            macd1hClosed = macd1h.subList(0, h2Closed.size());
        } else {
            macd1hClosed = calcMacd(h2Closed);
        }

        // 复用回测引擎（内部会清 lastEnterOpp/lastExitOpp/lastTrades，并按 SRMF 逐笔更新 equitySim）
        runBacktestAlarmLogic(m30Closed, macd30Closed, h2Closed, macd1hClosed);

        // —— 按旧格式输出最近 N 笔（保持你原来控制台可读性）——
        if (!lastTrades.isEmpty()) {
            int k = 1;
            for (CompletedTrade ct : lastTrades) {
                System.out.printf(Locale.US,
                        "%d) %s 进 → %s 出 | %s%n" +
                                "    entry=%.2f exit=%.2f PnL=%+.2f点 | MFE=%.2f MAE=%.2f%n" +
                                "    原因：%s%n%n",
                        k++,
                        FMT_JST_SHORT.format(Instant.ofEpochMilli(ct.enterTs)),
                        FMT_JST_SHORT.format(Instant.ofEpochMilli(ct.exitTs)),
                        (ct.side == Side.LONG ? "做多" : "做空"),
                        ct.entry, ct.exit, ct.pnlPoints,
                        ct.mfe, ct.mae,
                        ct.reason
                );
            }
        }
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
            List<Candle> m30, List<MacdPoint> macd30,
            List<Candle> h2,  List<MacdPoint> macd1h,
            Trend1HInfo t1hInfo,
            List<Candle> m30Latest,
            int sigIdx
    ) {
        if (m30 == null || macd30 == null || m30.isEmpty()) return null;
        if (sigIdx < 0 || sigIdx >= m30.size()) return null;
        Candle c0 = m30.get(sigIdx);
        long entryTs = c0.ts + BAR30_MS;

        Side side = entrySideByDifDea(m30, macd30, sigIdx);

        //  12月禁止开仓（仅影响新开仓；月份判断只用30m-K1；冲突保护链也一致）
        side = applyDecemberNoEntryGate(side, m30, sigIdx, entryTs, "实盘冲突", true);
        if (side == null) return null;
        if (side == null) return null;

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
            else if (h2 != null && !h2.isEmpty()) h1Idx = upperBoundCandle(h2, entryTs) - 1;
            MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
            if (regime == MarketRegime.RANGE) return null;
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

        // next-open：必须拿到下一根未收K（用于 entryPx）
        Candle entryC = findNextCandleAfter(m30Latest, c0.ts);
        if (entryC == null) return null;
        double entryPx = entryC.o;

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
        if (blockedByStoplossCooldown(entryC.ts, liveCooldownUntilEntryTs)) return null;

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
    static void checkRealtime(List<Candle> m30, List<MacdPoint> macd30,
                              List<Candle> h2,  List<MacdPoint> macd1h,
                              List<Candle> m30Latest) {

        int i = m30.size() - 1;
        Candle c0 = m30.get(i);

        Trend1HInfo t1hInfo = new Trend1HInfo();
        t1hInfo.trend = Trend1H.中性;
        if (USE_1H_FILTER && !h2.isEmpty() && !macd1h.isEmpty()) {
            t1hInfo = get1HTrendInfoAt(h2, macd1h, c0.ts + 1800000L) /* 按下一根30m开盘时间取1H */;
        }

        if (livePos == null) {
            //  若上一根信号K刚触发止损，本轮禁止用同一根信号K再进场（手动重复运行也不会同K再入）
            if (lastStopSigTs == c0.ts) {
                System.out.printf(Locale.US,
                        "【止损后禁止同K再进】信号时间=%s | 说明=上一根触发止损，本次放弃同K入场机会%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts))
                );
                return;
            }

            //  若上一根信号K刚触发止盈，本轮禁止用同一根信号K再进场（手动重复运行也不会同K再入）
            if (lastTpSigTs == c0.ts) {
                System.out.printf(Locale.US,
                        "【止盈后禁止同K再进】信号时间=%s | 说明=上一根触发止盈，本次放弃同K入场机会%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts))
                );
                return;
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
                else if (h2 != null && !h2.isEmpty()) h1Idx = upperBoundCandle(h2, c0.ts + BAR30_MS) - 1;

                MarketRegime regime = (h1Idx >= 0 ? detectMarketRegimeBy1HRangeQuantile(h2, h1Idx) : MarketRegime.RANGE);
                if (regime == MarketRegime.RANGE) {
                    side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "状态机=RANGE（低波动震荡期/弱势月份剥离）");
                }
            }

            //  新增：质量过滤（与回测一致，且复用你原输出模板，不改）
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
            if (side != null) {
                Candle k0 = m30.get(i); // 信号K0（已收盘）
                MacdPoint m0 = macd30.get(i);
                MacdPoint m1 = macd30.get(i - 1);

                //  入场价对位：用“下一根未收K”的开盘价（next.open）
                Candle entryC = findNextCandleAfter(m30Latest, k0.ts);
                if (entryC == null) {
                    System.out.println("【警告】拿不到下一根未收K（下一根开盘价），本次不提示进场（避免用错价）");
                    return;
                }
                double entryPx = entryC.o;

                //  解法一：手续费/波动门槛（不够覆盖往返手续费则不提示进场）
                if (USE_FEE_VOL_GUARD) {
                    double atrSigLive = calcAtr(m30, i, FEE_VOL_ATR_N);
                    if (!passFeeVolGuard(atrSigLive, entryPx)) {
                        side = blockEntry(side, c0.ts + BAR30_MS, "实盘", "手续费/波动不足：" + feeVolGuardReason(atrSigLive, entryPx));
                        return;
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
                        side = blockEntry(side, entryC.ts, "实盘", reason);
                        return;
                    }
                }
                // =====================  止损冷却：止损后等待 N 根K 再允许开仓 =====================
                if (blockedByStoplossCooldown(entryC.ts, liveCooldownUntilEntryTs)) {
                    if (PRINT_STOPLOSS_COOLDOWN_BLOCK) {
                        long remainBars = Math.max(0, (liveCooldownUntilEntryTs - entryC.ts + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【止损冷却拦截-实盘】候选入场=%s | cooldownUntil=%s | remainBars=%d → 不进场%n",
                                FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                FMT_JST.format(Instant.ofEpochMilli(liveCooldownUntilEntryTs)),
                                remainBars
                        );
                    }
                    return;
                }

                // =====================  止盈冷却：止盈后等待 N 根K 再允许开仓 =====================
                if (blockedByTakeprofitCooldown(entryC.ts, liveTpCooldownUntilEntryTs)) {
                    if (PRINT_TAKEPROFIT_COOLDOWN_BLOCK) {
                        long remainBars = Math.max(0, (liveTpCooldownUntilEntryTs - entryC.ts + BAR30_MS - 1) / BAR30_MS);
                        System.out.printf(Locale.US,
                                "【止盈冷却拦截-实盘】候选入场=%s | cooldownUntil=%s | remainBars=%d → 不进场%n",
                                FMT_JST.format(Instant.ofEpochMilli(entryC.ts)),
                                FMT_JST.format(Instant.ofEpochMilli(liveTpCooldownUntilEntryTs)),
                                remainBars
                        );
                    }
                    return;
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
                        return;
                    }
                }

                livePos = new Position();
                livePos.side = side;
                livePos.entry = entryPx;
                livePos.entryTs = entryC.ts;
                livePos.slPoints = calcSlPointsForIndex(m30, i);
                livePos.entryEquity = capital; //  实盘仅提醒：用当前 capital 作为参考资金（避免打印为0）
                livePos.slPointsAtEntry = livePos.slPoints; //  锁定入场止损点数                //  入场时锁定 OKX 撑压线（用于止盈/复盘）
                if (sr.valid) {
                    livePos.okxResistanceAtEntry = sr.resistance;
                    livePos.okxSupportAtEntry = sr.support;
                    livePos.okxGapAtEntry = (side == Side.LONG) ? (sr.resistance - entryPx) : (entryPx - sr.support);
                }
                livePos.mfe = 0.0;
                livePos.mae = 0.0;
                //  SRMF 入场锁定（用于复盘，避免之后状态变化）
                OuterVoteDecision ov = evalTsRndOuterVote(m30, macd30, i, side, "实时", false);
                if (USE_TSRND_OUTER_VOTE && ov.score < TSRND_SCORE_THRESHOLD) {
                    System.out.println("【TSRND 外层投票拦截】" + ov.detail);
                    return;
                }
                livePos.entryVoteScore = ov.score;
                livePos.entryVoteScale = ov.voteScale;
                livePos.entryVoteDirPass = ov.dirPass;
                livePos.entryVoteOverallPass = ov.overallPass;
                livePos.entryVoteDetail = ov.detail;
                livePos.entryTierMult = SRMF.getCurrentMult();
                livePos.entryMult = livePos.entryTierMult * ov.voteScale;
                livePos.regimeAtEntry = SRMF.getRegime().name();
                livePos.trendPtsAtEntry = SRMF.getTrendPts();
                //  放这里：触发进场信号时输出“入场锁定”的 SRMF 状态（最准确）
                System.out.println("【SRMF 入场锁定】方向=" + (livePos.side == Side.LONG ? "做多" : "做空")
                        + "｜入场=" + String.format(Locale.US, "%.2f", livePos.entry)
                        + "｜止损点数=" + String.format(Locale.US, "%.2f", livePos.slPoints)
                        + "｜档位模式=" + SRMF.regimeCn(livePos.regimeAtEntry)
                        + "｜趋势点数(pts)=" + String.format(Locale.US, "%.2f", livePos.trendPtsAtEntry)
                        + "｜mult(档位)=" + String.format(Locale.US, "%.2f*%.1f=%.2f", livePos.entryTierMult, livePos.entryVoteScale, livePos.entryMult)
                        + "｜票=" + livePos.entryVoteScore);
                //  只新增摘要（不改你原输出）
                printEnterAlertSummary(livePos, m30.get(i), t1hInfo, macd30.get(i), macd30.get(i - 1), getMinRangeNeed(m30, i));

                alarmOnceInTick( c0.ts + BAR30_MS);

                int diffInt = (side == Side.LONG) ? (int)(m0.dif - m1.dif) : (int)(m0.dea - m1.dea);

                // ===== 修复：实时触发进场时，同步更新“最近一次机会（回看）” =====
                // scanHistory 只能使用已收K线；当信号来自“最后一根已收K”时，回看拿不到 next-open（未收K开盘），会导致 LAST ENTER 落后一根。
                // 这里在实时触发进场提醒时，用 next-open 直接写入 lastEnterOpp，保证“响铃/文字提示”与“最近一次机会”一致。
                lastEnterOpp = new EnterOpportunity();
                lastEnterOpp.ts = entryC.ts; // next-open（入场K开盘）
                lastEnterOpp.side = side;
                lastEnterOpp.t1h = (USE_1H_FILTER ? t1hInfo.trend : Trend1H.中性);
                lastEnterOpp.entryRef = entryPx;

                lastEnterOpp.dif1 = m0.dif;
                lastEnterOpp.dif2 = m1.dif;
                lastEnterOpp.dea1 = m0.dea;
                lastEnterOpp.dea2 = m1.dea;
                lastEnterOpp.diffInt = diffInt;

                // 回撤入场：实盘此时“下一根K未收盘”，无法判断是否触达限价；标记为 Pending（待确认）
                lastEnterOpp.pbMode = PULLBACK_MODE.name();
                lastEnterOpp.pbEnabled = ENABLE_PULLBACK_ENTRY_BACKTEST;
                lastEnterOpp.pbPending = true;
                double atrSigLiveOpp = calcAtr(m30, i, ATR_N);
                lastEnterOpp.atrAtSignal = atrSigLiveOpp;
                lastEnterOpp.pbAtrPts = atrSigLiveOpp * PULLBACK_ATR_MULT;
                lastEnterOpp.pbFixedPts = PULLBACK_FIXED_POINTS;
                lastEnterOpp.pbUsedPts = (PULLBACK_MODE == PullbackMode.ATR ? lastEnterOpp.pbAtrPts : lastEnterOpp.pbFixedPts);
                lastEnterOpp.pbLimitPx = (side == Side.LONG) ? (entryPx - lastEnterOpp.pbUsedPts) : (entryPx + lastEnterOpp.pbUsedPts);
                lastEnterOpp.pbFilled = false;
                lastEnterOpp.entryFillPx = 0.0;

                System.out.printf(Locale.US,
                        "【进场提醒】 时间=%s | 方向=%s | 1H=%s | 参考入场(下一根开盘)=%.2f | dif(本根/前一)=%.6f/%.6f | dea(本根/前一)=%.6f/%.6f | 动量diffInt=%d%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                        (side == Side.LONG ? "做多" : "做空"),
                        (USE_1H_FILTER ? (t1hInfo.trend + (t1hInfo.isFirstNeutralBar ? "(NEUTRAL_FIRST)" : "")
                                + String.format(Locale.US, "(hist=%.4f prev=%.4f)", t1hInfo.hist, t1hInfo.prevHist)) : "OFF"),
                        livePos.entry,
                        m0.dif, m1.dif, m0.dea, m1.dea,
                        diffInt
                );
                if (ENABLE_PULLBACK_ENTRY_LIVE_PRINT) {
                    double atrSigLive = calcAtr(m30, i, ATR_N);
                    double pbAtrPtsLive = atrSigLive * PULLBACK_ATR_MULT;
                    double pbUsedPtsLive = (PULLBACK_MODE == PullbackMode.ATR ? pbAtrPtsLive : PULLBACK_FIXED_POINTS);
                    double pbLimitPxLive = (side == Side.LONG) ? (entryPx - pbUsedPtsLive) : (entryPx + pbUsedPtsLive);
                    System.out.printf(Locale.US,
                            "【回撤入场参考】mode=%s | ATR(30m,%d)=%.2f | ATR回撤=%.2f(x%.2f) | 固定回撤=%.2f | 使用回撤=%.2f | limit=%.2f%n",
                            PULLBACK_MODE, ATR_N,
                            atrSigLive,
                            pbAtrPtsLive, PULLBACK_ATR_MULT,
                            PULLBACK_FIXED_POINTS,
                            pbUsedPtsLive,
                            pbLimitPxLive
                    );
                }

            }
        }

        if (livePos != null) {
            //  关键修复：如果本轮刚用“下一根开盘价”入场（entryTs=下一根K开盘），
            // 则不能用“信号K0(c0)”的高低点去判断止损/止盈（这些高低点发生在入场之前）。
            // 必须等到下一根已收K（即 c0.ts >= entryTs）再开始判断出场。
            if (c0.ts < livePos.entryTs) return;

            updateMfeMae(livePos, c0);

            boolean stop = hitStopLoss(livePos, c0);
            boolean tp2  = (!stop) && hitTP2(livePos, c0);

            //  OKX 撑压线止盈：多→压力线；空→支撑线（入场时已锁定）
            double okxTpPx = (livePos.side == Side.LONG) ? livePos.okxResistanceAtEntry : livePos.okxSupportAtEntry;
            boolean okxSrTp = (!stop) && USE_OKX_SR_TP && hitOkxSrTP(livePos, c0, okxTpPx);

            boolean macdRev = (!stop && !tp2) && shouldExitByMacdWithGuards(m30, macd30, i, livePos, c0.c, "实盘", c0.ts + BAR30_MS);

            //  冲突保护：同一时间出现“出场 + 同一时间进场”时，保持持仓不处理（避免同K反复）
            // 说明：对“TP全平/OKX撑压止盈/反转出场”(tp2 / okxSrTp / macdRev)启用；止损仍然优先执行。
            if (!stop && (tp2 || okxSrTp || macdRev)) {
                long evalTs = c0.ts + BAR30_MS;
                Side flip = computeEntrySideForConflictRealtime(m30, macd30, h2, macd1h, t1hInfo, m30Latest, i);
                if (flip != null) {
                    double baseRisk = livePos.entryEquity * RISK_PCT;
                    double riskAmt  = baseRisk * livePos.entryMult;
                    double notional = riskAmt * livePos.entry / Math.max(1e-9, livePos.slPointsAtEntry);
                    System.out.println();
                    System.out.printf(Locale.US,
                            "【继续持仓】时间=%s｜原因=同一时间出现出场(%s) + 同一时间进场(%s)，为避免同K反复，保持持仓不处理%n",
                            FMT_JST.format(Instant.ofEpochMilli(evalTs)),
                            (okxSrTp ? (livePos.side == Side.LONG ? "OKX压力线止盈" : "OKX支撑线止盈") : (tp2 ? "TP全平" : "MACD颜色反转")),
                            (flip == Side.LONG ? "做多" : "做空")
                    );
                    System.out.printf(Locale.US,
                            "当前仓位：方向=%s｜入场时间=%s｜入场价=%.2f｜止损点=%.0f｜倍数=%.2f(*%.1f)｜风险=%.2f USDT｜名义=%.0f USDT｜regime=%s｜trendPts=%.2f%n",
                            (livePos.side == Side.LONG ? "做多" : "做空"),
                            FMT_JST.format(Instant.ofEpochMilli(livePos.entryTs)),
                            livePos.entry,
                            livePos.slPointsAtEntry,
                            livePos.entryMult,
                            livePos.entryVoteScale,
                            riskAmt,
                            notional,
                            livePos.regimeAtEntry,
                            livePos.trendPtsAtEntry
                    );
                    System.out.printf(Locale.US,
                            "保持持仓：不提示出场/进场，不更新持仓状态/最近一次/最近十次机会%n"
                    );
                    System.out.println();
                    return;
                }
            }

            if (stop || tp2 || okxSrTp || macdRev) {
                if (stop) {
                    lastStopSigTs = c0.ts; //  记录止损信号K
                    //  止损冷却：从本次止损的 exitTs 开始，等待 N 根30m K 再允许进场
                    long stopExitTs = c0.ts + BAR30_MS;
                    liveCooldownUntilEntryTs = calcCooldownUntilEntryTsAfterStop(stopExitTs);
                }

                //  止盈冷却：TP 全平 / OKX 撑压止盈后，等待 N 根30m K 再允许进场（实盘/闹钟）
                if (!stop && (tp2 || okxSrTp || macdRev)) {
                    lastTpSigTs = c0.ts; //  记录止盈信号K
                    long tpExitTs = c0.ts + BAR30_MS;
                    liveTpCooldownUntilEntryTs = calcCooldownUntilEntryTsAfterTakeProfit(tpExitTs);
                }
                double exitRef;
                String reason;

                if (stop) {
                    exitRef = stopFillPrice(livePos);
                    reason = "止损触发（SL=" + (int)Math.round(livePos.slPoints) + "点）";
                } else if (tp2 || okxSrTp) {
                    //  多个止盈同时触发：LONG 取更小（更先触发）；SHORT 取更大（更先触发）
                    double tp2Px = tp2FillPrice(livePos);
                    double srPx  = okxTpPx;

                    if (tp2 && okxSrTp) {
                        //  多个止盈同时触发：仍按“更先触发”的近似规则选择“哪一个原因”，
                        // 但若选择的是 OKX 撑压线止盈，则【出场价/出场时间】按当前K线收盘确认（避免盘中/盘尾冲突）。
                        boolean chooseTp2;
                        if (livePos.side == Side.LONG) chooseTp2 = (tp2Px <= srPx);  // LONG：更小价更先触发
                        else                           chooseTp2 = (tp2Px >= srPx);  // SHORT：更大价更先触发

                        if (chooseTp2) {
                            exitRef = tp2Px;
                            reason = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                        } else {
                            exitRef = c0.c; //  SR 仍按盘中触价判定，但“以收盘价出”
                            reason = (livePos.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                        }
                    } else if (okxSrTp) {
                        exitRef = c0.c; //  SR 仍按盘中触价判定，但“以收盘价出”
                        reason = (livePos.side == Side.LONG ? "OKX压力线止盈（收盘确认）" : "OKX支撑线止盈（收盘确认）");
                    } else {
                        exitRef = tp2Px;
                        reason = "TP全平（达到" + (int)TP2_TRIGGER + "点）";
                    }
                } else {
                    // macdRev
                    exitRef = c0.c;
                    reason = "MACD颜色反转（strength比较）=> 当前收盘出";
                }
                double remainPnl = (livePos.side == Side.LONG)
                        ? (exitRef - livePos.entry)
                        : (livePos.entry - exitRef) ;
                double feePts = okxFeePoints(livePos.entry, exitRef);
                double finalPnl = remainPnl - feePts;

                //  只新增摘要（不改你原输出）
                printExitAlertSummary(livePos, c0, exitRef, remainPnl, feePts, finalPnl, reason);
                //  exit 用不同key，避免同一根K内进/出场时闹钟被去重
                alarmOnceInTick( c0.ts + BAR30_MS + 123L);
                System.out.printf(Locale.US,
                        "【出场提醒】 时间=%s | 方向=%s | 参考出场=%.2f | 毛=%+.2f点 | 费=%.2f点 | 净=%+.2f点 | MFE=%.2f MAE=%.2f | 原因=%s%n",
                        FMT_JST.format(Instant.ofEpochMilli(c0.ts + BAR30_MS)),
                        (livePos.side == Side.LONG ? "做多" : "做空"),
                        exitRef, remainPnl, feePts, finalPnl,
                        livePos.mfe, livePos.mae,
                        reason
                );
                livePos = null;
            }
        }
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
        System.out.println("模式：只提醒，不下单");
        System.out.println("进场过滤：亚洲盘前半=" + FILTER_ASIA_LOW_LIQ + "（JST " + NO_TRADE_1_START + ":00-" + NO_TRADE_1_END + ":00）"
                +"（JST " + NO_TRADE_2_START + ":00-" + NO_TRADE_2_END + ":00）"+ "；1H中性第一根=" + FILTER_1H_NEUTRAL_FIRST_BAR + "（X=" + NEUTRAL_ABS_HIST_X + ", N=" + NEUTRAL_OSC_N + "）");
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
        //  放这里：每次刷新都能看到 SRMF 当前状态（最准确）
        System.out.println("SRMF状态 = regime=" + SRMF.getRegime()
                + " trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts())
                + " mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult())
                + (APPLY_OKX_FEE ? (" | fee双边=" + String.format(Locale.US, "%.4f%%", OKX_FEE_RATE_PER_SIDE*2*100)) : " | fee=OFF"));

//  快速观察：稳健4档 / 进攻5档 的“参考名义”（按 A：已收K收盘价计算）
        double obsPrice = (last30 != null ? last30.c : lastPriceCache);
        double obsSl = Math.max(1e-9, lastSlPointsCache);
        if (obsPrice > 0) {
            double obsBaseRisk = capital * RISK_PCT;
            double tp = SRMF.getTrendPts();
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

        System.out.println("LAST ENTER：");
        if (lastEnterOpp == null) {
            System.out.println("（暂无）");
        } else {
            EnterOpportunity o = lastEnterOpp;
            System.out.println("时间：" + FMT_JST_SHORT.format(Instant.ofEpochMilli(o.ts)));
            System.out.println("方向：" + (o.side == Side.LONG ? "做多" : "做空"));
            System.out.printf(Locale.US, "参考进场价：%.2f（下一根开盘）%n", o.entryRef);
            System.out.printf(Locale.US,
                    "dif1=%.6f dif2=%.6f dea1=%.6f dea2=%.6f diffInt=%d 1H=%s%n",
                    o.dif1, o.dif2, o.dea1, o.dea2, o.diffInt,
                    (USE_1H_FILTER ? o.t1h : "OFF")
            );
//  回撤入场（回测用）：打印是否启用、使用回撤值、limit、以及成交/未成交状态
            if (o.pbMode != null) {
                if (o.pbEnabled) {
                    String pbStatus = o.pbFilled
                            ? ("成交@" + String.format(Locale.US, "%.2f", o.entryFillPx))
                            : (o.pbPending ? "待确认（下一根K未收盘）" : "未成交（下一根K未触达回撤限价）");
                    System.out.printf(Locale.US,
                            "回撤入场：ON | mode=%s | ATR=%.2f | ATR回撤=%.2f(x%.2f) | 固定回撤=%.2f | 使用回撤=%.2f | limit=%.2f | %s%n",
                            o.pbMode,
                            o.atrAtSignal,
                            o.pbAtrPts, PULLBACK_ATR_MULT,
                            o.pbFixedPts,
                            o.pbUsedPts,
                            o.pbLimitPx,
                            pbStatus
                    );
                } else {
                    System.out.printf(Locale.US,
                            "回撤入场：OFF | mode=%s | ATR=%.2f | ATR回撤=%.2f(x%.2f) | 固定回撤=%.2f | 使用回撤=%.2f | limit=%.2f%n",
                            o.pbMode,
                            o.atrAtSignal,
                            o.pbAtrPts, PULLBACK_ATR_MULT,
                            o.pbFixedPts,
                            o.pbUsedPts,
                            o.pbLimitPx
                    );
                }
            }

        }
        System.out.println();
        System.out.println("LAST EXIT：");
        if (lastExitOpp == null) {
            System.out.println("（暂无）");
        } else {
            ExitOpportunity x = lastExitOpp;
            System.out.println("时间：" + FMT_JST_SHORT.format(Instant.ofEpochMilli(x.ts)));
            System.out.printf(Locale.US, "参考出场价：%.2f%n", x.price);
            System.out.println("出场原因：" + x.reason);
        }
        System.out.println("----------------------------------");
        System.out.println();
    }

    static void printLastTradesTemplate() {
        System.out.println("====== 最近" + LAST_N + "笔（含盈亏/止损/原因/MFE/MAE）======");
        if (lastTrades.isEmpty()) {
            System.out.println("（当前还没有形成完整的进出场记录）");
            System.out.println("========================================");
            System.out.println();
            return;
        }

        int idx = 1;
        for (CompletedTrade t : lastTrades) {
            String enter = FMT_JST_SHORT.format(Instant.ofEpochMilli(t.enterTs));
            String exit  = FMT_JST_SHORT.format(Instant.ofEpochMilli(t.exitTs));
            String sideCN = (t.side == Side.LONG ? "做多" : "做空");
            String stopTag = t.isStop ? "（止损）" : "";

            System.out.printf(Locale.US,
                    "%d) %s 进 → %s 出 | %s%n", idx++, enter, exit, sideCN);
            System.out.printf(Locale.US,
                    "   entry=%.2f exit=%.2f 毛=%+.2f点 费=%.2f点 净=%+.2f点%s | MFE=%.2f MAE=%.2f%n",
                    t.entry, t.exit, t.grossPnlPoints, t.feePoints, t.pnlPoints, stopTag, t.mfe, t.mae);
            System.out.println("   原因：" + t.reason);
            System.out.println();
        }
        System.out.println("========================================");
        System.out.println();
    }
    // ===================== SRMF 下单量打印（USDT名义价值） =====================

    // ===================== 档位映射（用于快速观察名义） =====================
    static double stable4MultByTrendPts(double trendPts) {
        // 稳健4档（Calmar）：<60=1.0 | 60-90=1.8 | 90-120=2.6 | >=120=3.0
        if (trendPts < 45) return 0.9;
        // if (trendPts < 120) return 2.6;
        //if (trendPts < 120) return 2.6;
        return 2.95;
    }

    static double attack5MultByTrendPts(double trendPts) {
        // 进攻5档（Attack）：<20=1.0 | 20-40=1.3 | 40-60=1.6 | 60-80=2.8 | >=80=3.0
        if (trendPts < 45) return 1.0;
        // if (trendPts < 120) return 2.6;
        //if (trendPts < 120) return 2.6;
        return 2.95;
/*        if (trendPts < 20) return 1.0;
        if (trendPts < 40) return 1.3;
        if (trendPts < 60) return 1.6;
        if (trendPts < 80) return 2.8;
        return 3.0;*/
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
        System.out.println("当前 SRMF 状态：regime=" + SRMF.regimeCn(SRMF.getRegime().name()) +
                " | trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts()) +
                " | mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult()));

        //  方便观察：把稳健4档/进攻5档的“下单名义(USDT)”直接打印在 SRMF 状态下面（基于同一价格/止损点数）
        System.out.println(String.format(Locale.US,
                "稳健4档名义(USDT)：x0.9=%.0f x2.95=%.0f",
                baseRisk * 0.9 * p / slPointsUsed,
                baseRisk * 2.95 * p / slPointsUsed
        ));
        System.out.println(String.format(Locale.US,
                "进攻5档名义(USDT)：x1.0=%.0f x2.95=%.0f",
                baseRisk * 1.0 * p / slPointsUsed,
                baseRisk * 2.95 * p / slPointsUsed
        ));
        System.out.println();
        System.out.println("【稳健 2档（Calmar）】 trendPts→mult：<45=0.95 | >=45=2.95");
        System.out.println("【进攻 2档（Attack）】 trendPts→mult：<45=1.0  | >=45=2.95");
        System.out.println();
        System.out.println("以当前 mult 计算（仅参考）：");
        double curMult = SRMF.getCurrentMult();
        double curRisk = baseRisk * curMult;
        double curNotional = curRisk * p / slPointsUsed;
        System.out.printf(Locale.US, "mult=%.2f | 风险=%.2f USDT | 下单名义=%.0f USDT%n", curMult, curRisk, curNotional);
        System.out.println();
        System.out.println("各档位开仓名义（按参考价格/同一止损点数，仅打印）：");

        // 稳健 4档（Calmar）
        System.out.println("【稳健4档（Calmar）】");
        double[] calmarMults = new double[]{0.90,2.95};
        for (double mm : calmarMults) {
            double r = baseRisk * mm;
            double notional = r * p / slPointsUsed;
            System.out.printf(Locale.US, "  mult=%.2f | 风险=%.2f USDT | 名义=%.0f USDT%n", mm, r, notional);
        }
        System.out.println();
        System.out.println("【进攻5档（Attack）】");
        double[] attackMults = new double[]{1.0,2.95};
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
        dyn = clamp(dyn, SL_MIN, SL_MAX);

        // 固定底线：不小于 SL_POINTS（你原设定=28）
        return Math.max(SL_POINTS, dyn);
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
        double clamped = clamp(raw, SL_MIN, SL_MAX);
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

    // ===================== 拉K线（按数量分页） =====================
    static List<Candle> fetchRecentByCount(String bar, int needBars) throws Exception {
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
        int dTsoup;

        boolean matchSide(Side side) {
            if (dir == 0) return false;
            return (side == Side.LONG && dir == 1) || (side == Side.SHORT && dir == -1);
        }

        String dirText() {
            return dir > 0 ? "LONG" : (dir < 0 ? "SHORT" : "FLAT");
        }

        String partsText() {
            return String.format("SQ=%+d CVD=%+d EMA200=%+d FISH=%+d MACD=%+d StochRSI=%+d RSIrev=%+d TSOUP=%+d",
                    dSqueeze, dCvd, dEma200, dFisher, dMacd, dStochRsi, dRsiRev, dTsoup);
        }

        String toOneLine() {
            return String.format("score=%d dir=%s [%s]", score, dirText(), partsText());
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
        private int[] tsoup96;

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

            // 2) CVD_sign: (cvd[idx] - cvd[idx-1]) 的符号（字段不存在则=0，不报错）
            r.dCvd = 0;
            {
                double v = p2_readCvdSafe(m30.get(idx));
                double vPrev = p2_readCvdSafe(m30.get(idx - 1));
                if (Double.isFinite(v) && Double.isFinite(vPrev)) {
                    double d = v - vPrev;
                    r.dCvd = d > 0 ? 1 : (d < 0 ? -1 : 0);
                }
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

            // 8) PS_TSOUP_96
            r.dTsoup = 0;
            if (tsoup96 != null && idx < tsoup96.length) {
                r.dTsoup = tsoup96[idx];
            }

            int score = 0;
            score += 1 * r.dSqueeze;
            score += 1 * r.dCvd;
            score += 1 * r.dEma200;
            score += POOL2_W_FISHER * r.dFisher;
            score += 1 * r.dMacd;
            score += 1 * r.dStochRsi;
            score += POOL2_W_RSI_REV * r.dRsiRev;
            score += 1 * r.dTsoup;

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
            this.tsoup96 = p2_turtleSoupFalseBreakout(m30, 96, 0.002);

            //  不再从外部文件加载 CVD：只按 Candle/DB 的 cvd 字段读取。
        }
    }

    /**
     * Pool-2：安全读取 Candle 的 cvd 字段（可选）。
     * - 不引用任何编译期字段名，避免“没有 cvd 字段就编译失败”。
     * - 若不存在/取不到：返回 NaN（上层会当作贡献=0）。
     */
    static double p2_readCvdSafe(Object candle) {
        if (candle == null) return Double.NaN;
        try {
            java.lang.reflect.Field f = candle.getClass().getDeclaredField("cvd");
            f.setAccessible(true);
            Object v = f.get(candle);
            if (v instanceof Number) return ((Number) v).doubleValue();
        } catch (Exception ignore) {
        }
        try {
            java.lang.reflect.Method m = candle.getClass().getMethod("getCvd");
            Object v = m.invoke(candle);
            if (v instanceof Number) return ((Number) v).doubleValue();
        } catch (Exception ignore) {
        }
        return Double.NaN;
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

    // =====================
    // 外部特征：Long->Double 序列（CVD）
    // =====================

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
                java.nio.file.Path p = java.nio.file.Path.of(path);
                if (!java.nio.file.Files.exists(p)) return null;
                LongDoubleSeries s = new LongDoubleSeries();
                try (java.io.BufferedReader br = java.nio.file.Files.newBufferedReader(p)) {
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

}