package eval;

import eval.engine.Args;
import eval.engine.candidates.ConditionFactory;
import eval.engine.candidates.PoolCondition;
import eval.engine.external.CsvExternalFeatureProvider;
import eval.engine.external.ExternalFeatureProviderHolder;
import eval.engine.external.NoopExternalFeatureProvider;
import eval.engine.external.SqliteExternalFeatureProvider;
import eval.model.Candle;
import eval.okx.OkxCandleCacheEthOsc;
import eval.okx.OkxKlineUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DiscriminativeIndicatorsMain v7 (two-stage TopM -> combo search)
 *
 * ✅ 你要的能力（基于 SRMF 交易日志）：
 * 1) 指标区分度（WIN vs LOSS 的分布差异）：JS / F1 / score
 * 2) 胜率影响（与交易方向一致 agree 时胜率提升多少）：base/agree/lift/cov + effectScore
 * 3) 最终盈利影响（尽量贴近 SRMF 曲线）：用 SRMF 入场/出场资金计算每笔 trade 的 return factor，
 *    对“只保留 agree 交易”的过滤策略做权益重放，输出 profitImpactPct / profitEffectScore
 * 4) 三套输出：OVERALL / LONG_ONLY / SHORT_ONLY
 * 5) 参数增强：--minBinN (默认 50) / --minAgreeN (默认 100)
 *
 * 输入（你现成的日志就行）：
 * - tradeLog: 包含 “【SRMF-进场】” “【SRMF-出场】” “【回测-进场】”
 *
 * 输出：
 * - out/indicator_rank_{overall,long,short}.csv  （区分度）
 * - out/indicator_winimpact_{overall,long,short}.csv（胜率影响）
 * - out/indicator_profitimpact_{overall,long,short}.csv（盈利影响）
 *
 * 运行示例：
 *   java eval.DiscriminativeIndicatorsMain --tradeLog=./out/trade_log.txt --db=./okx_candles_osc.db --instId=ETH-USDT-SWAP --bar=30m --topN=10 --lookback=12 --minBinN=50 --minAgreeN=100
 */
public class DiscriminativeIndicatorsMain {

    // -------------------- CLI (本类自解析：支持 --k=v) --------------------
    private static final String ARG_TRADE_LOG = "--tradeLog=";
    private static final String ARG_TOPN = "--topN=";
    private static final String ARG_LOOKBACK = "--lookback=";
        private static final String ARG_FAMILYCAP = "--familyCap="; // 每个family最多入池数量（默认2）
private static final String ARG_MAX_EVENTS = "--maxEvents=";   // 0=不限
    private static final String ARG_MIN_BIN_N = "--minBinN=";      // best/worst 档位最小样本数（避免虚高）
    private static final String ARG_MIN_AGREE_N = "--minAgreeN=";  // agree 样本 < N：不进入 TopN（仍写 CSV）
    private static final String ARG_RAND_COMBOS = "--randCombos="; // 两阶段组合数量（采样）
    private static final String ARG_COMBO_MIN_K = "--comboMinK="; // 组合最小指标数
    private static final String ARG_COMBO_MAX_K = "--comboMaxK="; // 组合最大指标数
    private static final String ARG_RAND_SEED = "--randSeed=";    // 随机种子
    private static final String ARG_TOPM = "--topM=";                 // 两阶段：先选 TopM 单指标
    private static final String ARG_TOPM_METRIC = "--topMMetric=";    // hybrid|win|profit|disc
    private static final String ARG_COMBO_SEARCH = "--comboSearch=";  // random|beam
    private static final String ARG_BEAM_WIDTH = "--beamWidth=";      // beam 搜索宽度

    private static final String ARG_FAILMONTH_COMBOS = "--failMonthCombos="; // 失败月+总体提升：随机组合数量
    private static final String ARG_MIN_FAIL_AGREE_N = "--minFailAgreeN=";    // 失败月内 agree 最小样本数（避免虚高）
    private static final String ARG_FAIL_POOL = "--failPool=";               // topM|all（默认 topM）

    // --- Enhancements: OOS + Fixed-R (no compounding) ---
    private static final String ARG_OOS6 = "--oos6=";   // OOS window months (default 6)
    private static final String ARG_OOS12 = "--oos12="; // OOS window months (default 12)
    private static final String ARG_COST_PCT = "--costPct="; // per-trade cost pct on |PnL| (fee+slippage), default 0.0008



    // -------------------- Parse patterns --------------------
    // 注意：你的日志里【SRMF-进场】行通常没有“方向=”，方向在【回测-进场】或【SRMF-出场】里。
    private static final Pattern P_SRMF_ENTRY = Pattern.compile("SRMF-进场.*?时间=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_BT_ENTRY_DIR = Pattern.compile("回测-进场.*?时间=([^|]+).*?方向=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_DIR = Pattern.compile("SRMF-出场.*?方向=([^|]+)", Pattern.CASE_INSENSITIVE);

    // 净点数：净= -9.41点(...) 或 PnL=-10.18点(止损)
    private static final Pattern P_SRMF_EXIT_NET_PTS = Pattern.compile("SRMF-出场.*?(净=|净点=|净点数=|PnL=)([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
    // 净 U：净=-9.41点(-1029.41U)
    private static final Pattern P_SRMF_EXIT_NET_U = Pattern.compile("净=\\s*[-+]?\\d+(?:\\.\\d+)?点\\(([-+]?\\d+(?:\\.\\d+)?)U\\)", Pattern.CASE_INSENSITIVE);

    // 资金（SRMF 入场/出场行都有）：资金=43747.98
    private static final Pattern P_CAPITAL = Pattern.compile("资金=([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);

    // 时间格式：2023-03-07 09:30:00
    private static final ZoneId TOKYO = ZoneId.of("Asia/Tokyo");
    private static final DateTimeFormatter[] DT_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    enum Dir { LONG, SHORT, UNKNOWN }

    static final class TradeEvent {
        long entryTsMs;
        Dir dir;
        double netPoints;
        Double netU;          // 可能为空（但你的日志里通常有）
        Double equityBefore;  // SRMF-进场的 资金
        Double equityAfter;   // SRMF-出场的 资金
        boolean win;

        // 映射到 K 线
        int candleIdx = -1;
        long matchedCandleTs = 0;
        long deltaMs = 0;

        // 交易“收益因子”：尽量用 SRMF 入/出资金比值，贴近实盘曲线
        double factor = 1.0;

        @Override public String toString() {
            return "TradeEvent{" +
                    "entryTsMs=" + entryTsMs +
                    ", dir=" + dir +
                    ", netPoints=" + netPoints +
                    ", netU=" + netU +
                    ", equityBefore=" + equityBefore +
                    ", equityAfter=" + equityAfter +
                    ", win=" + win +
                    ", candleIdx=" + candleIdx +
                    '}';
        }
    }

    static final class CondSeries {
        final PoolCondition cond;
        final int[] dir;      // -1/0/+1 per bar
        final int[] pref;     // prefix sum of dir (len+1)
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

    static final class ScoreRow {
        final String id;
        final String group;
        final double score;   // 0.6*JS + 0.4*F1
        final double f1;
        final double js;
        final double meanWinBin;
        final double meanLossBin;
        final int nWin;
        final int nLoss;

        ScoreRow(String id, String group, double score, double f1, double js,
                 double meanWinBin, double meanLossBin, int nWin, int nLoss) {
            this.id = id;
            this.group = group;
            this.score = score;
            this.f1 = f1;
            this.js = js;
            this.meanWinBin = meanWinBin;
            this.meanLossBin = meanLossBin;
            this.nWin = nWin;
            this.nLoss = nLoss;
        }
    }

    static final class ImpactRow {
        final String id;
        final String group;

        final int totalN;
        final int winN;
        final int lossN;
        final double baseWinRate;

        final int agreeN;
        final int agreeWinN;
        final double agreeWinRate;
        final double agreeLift;      // agreeWinRate - baseWinRate
        final double coverage;       // agreeN/totalN
        final double winEffectScore; // agreeLift * sqrt(coverage)

        final double baseFinalEquity;
        final double filteredFinalEquity;
        final double baseReturnPct;       // vs init
        final double filteredReturnPct;   // vs init
        final double profitImpactPct;     // vs base final equity
        final double profitEffectScore;   // (profitImpactPct/100) * sqrt(coverage)
        // --- Fixed-R (no compounding) metrics (approx via additive equity on netU - costs) ---
        final double baseFinalEqFixedR;
        final double filteredFinalEqFixedR;
        final double profitImpactPctFixedR;
        final double profitEffectScoreFixedR;
        final double pfBaseFixedR;
        final double pfFiltFixedR;
        final double ddBaseFixedR;
        final double ddFiltFixedR;

        // --- OOS windows (last N months from data end), computed under Fixed-R metrics ---
        final double oos6_winLift;
        final double oos6_pfBase;
        final double oos6_pfFilt;
        final double oos6_ddBase;
        final double oos6_ddFilt;
        final double oos6_profitImpactPct;

        final double oos12_winLift;
        final double oos12_pfBase;
        final double oos12_pfFilt;
        final double oos12_ddBase;
        final double oos12_ddFilt;
        final double oos12_profitImpactPct;


        final int bestBin;            // 0..4, -1=NA
        final int bestN;
        final double bestWinRate;

        final int worstBin;           // 0..4, -1=NA
        final int worstN;
        final double worstWinRate;

        final boolean passMinAgreeN;

        ImpactRow(String id, String group,
                  int totalN, int winN, int lossN, double baseWinRate,
                  int agreeN, int agreeWinN, double agreeWinRate, double agreeLift, double coverage, double winEffectScore,
                  double baseFinalEquity, double filteredFinalEquity, double baseReturnPct, double filteredReturnPct, double profitImpactPct, double profitEffectScore,
                  double baseFinalEqFixedR, double filteredFinalEqFixedR, double profitImpactPctFixedR, double profitEffectScoreFixedR,
                  double pfBaseFixedR, double pfFiltFixedR, double ddBaseFixedR, double ddFiltFixedR,
                  double oos6_winLift, double oos6_pfBase, double oos6_pfFilt, double oos6_ddBase, double oos6_ddFilt, double oos6_profitImpactPct,
                  double oos12_winLift, double oos12_pfBase, double oos12_pfFilt, double oos12_ddBase, double oos12_ddFilt, double oos12_profitImpactPct,
                  int bestBin, int bestN, double bestWinRate,
                  int worstBin, int worstN, double worstWinRate,
                  boolean passMinAgreeN) {
            this.id = id;
            this.group = group;
            this.totalN = totalN;
            this.winN = winN;
            this.lossN = lossN;
            this.baseWinRate = baseWinRate;
            this.agreeN = agreeN;
            this.agreeWinN = agreeWinN;
            this.agreeWinRate = agreeWinRate;
            this.agreeLift = agreeLift;
            this.coverage = coverage;
            this.winEffectScore = winEffectScore;
            this.baseFinalEquity = baseFinalEquity;
            this.filteredFinalEquity = filteredFinalEquity;
            this.baseReturnPct = baseReturnPct;
            this.filteredReturnPct = filteredReturnPct;
            this.profitImpactPct = profitImpactPct;
            this.profitEffectScore = profitEffectScore;
            this.baseFinalEqFixedR = baseFinalEqFixedR;
            this.filteredFinalEqFixedR = filteredFinalEqFixedR;
            this.profitImpactPctFixedR = profitImpactPctFixedR;
            this.profitEffectScoreFixedR = profitEffectScoreFixedR;
            this.pfBaseFixedR = pfBaseFixedR;
            this.pfFiltFixedR = pfFiltFixedR;
            this.ddBaseFixedR = ddBaseFixedR;
            this.ddFiltFixedR = ddFiltFixedR;

            this.oos6_winLift = oos6_winLift;
            this.oos6_pfBase = oos6_pfBase;
            this.oos6_pfFilt = oos6_pfFilt;
            this.oos6_ddBase = oos6_ddBase;
            this.oos6_ddFilt = oos6_ddFilt;
            this.oos6_profitImpactPct = oos6_profitImpactPct;

            this.oos12_winLift = oos12_winLift;
            this.oos12_pfBase = oos12_pfBase;
            this.oos12_pfFilt = oos12_pfFilt;
            this.oos12_ddBase = oos12_ddBase;
            this.oos12_ddFilt = oos12_ddFilt;
            this.oos12_profitImpactPct = oos12_profitImpactPct;
            this.bestBin = bestBin;
            this.bestN = bestN;
            this.bestWinRate = bestWinRate;
            this.worstBin = worstBin;
            this.worstN = worstN;
            this.worstWinRate = worstWinRate;
            this.passMinAgreeN = passMinAgreeN;
        }
    }


    
    // ==================== Fail-Month (失败月) 加权组合挖掘：同时要求“失败月提升 + 总体提升” ====================
    static final class FailMonthRow {
        final String id;
        final String group;

        final int totalN;
        final double baseWinRate;

        final int agreeN;
        final double agreeWinRate;
        final double agreeLift;
        final double coverage;

        final double profitImpactPct;     // vs base final equity
        final double profitEffectScore;   // (profitImpactPct/100)*sqrt(coverage)
        // --- Fixed-R (no compounding) metrics ---
        final double profitImpactPctFixedR;
        final double profitEffectScoreFixedR;
        final double pfBaseFixedR;
        final double pfFiltFixedR;
        final double ddBaseFixedR;
        final double ddFiltFixedR;

        // --- OOS windows (Fixed-R) ---
        final double oos6_winLift;
        final double oos6_pfBase;
        final double oos6_pfFilt;
        final double oos6_ddBase;
        final double oos6_ddFilt;
        final double oos6_profitImpactPct;

        final double oos12_winLift;
        final double oos12_pfBase;
        final double oos12_pfFilt;
        final double oos12_ddBase;
        final double oos12_ddFilt;
        final double oos12_profitImpactPct;

        final double winEffectScore;      // agreeLift*sqrt(coverage)

        // 失败月相关
        final int failMonths;
        final int failTotalN;
        final int failAgreeN;
        final double failCoverage;
        final double failImpactPct;       // (filteredFail/baseFail - 1) * 100

        // 动态权重（你要求：failW = failTradeRatio）
        final double failTradeRatio;      // failTotalN / totalN
        final double failW;               // = failTradeRatio
        final double overallW;            // 固定 1.0
        final double weightedImpactPct;   // failImpactPct*(1+failW) + profitImpactPct*overallW
        final double weightedEffectScore; // (weightedImpactPct/100) * sqrt(coverage)

        final boolean passMinAgreeN;
        final boolean passMinFailAgreeN;
        final boolean passBothImprove;

        FailMonthRow(String id, String group,
                     int totalN, double baseWinRate,
                     int agreeN, double agreeWinRate, double agreeLift, double coverage,
                     double profitImpactPct, double profitEffectScore, double winEffectScore,
                  double profitImpactPctFixedR, double profitEffectScoreFixedR, double pfBaseFixedR, double pfFiltFixedR, double ddBaseFixedR, double ddFiltFixedR,
                  double oos6_winLift, double oos6_pfBase, double oos6_pfFilt, double oos6_ddBase, double oos6_ddFilt, double oos6_profitImpactPct,
                  double oos12_winLift, double oos12_pfBase, double oos12_pfFilt, double oos12_ddBase, double oos12_ddFilt, double oos12_profitImpactPct,
                     int failMonths, int failTotalN, int failAgreeN, double failCoverage, double failImpactPct,
                     double failTradeRatio, double failW, double overallW,
                     double weightedImpactPct, double weightedEffectScore,
                     boolean passMinAgreeN, boolean passMinFailAgreeN, boolean passBothImprove) {
            this.id = id;
            this.group = group;
            this.totalN = totalN;
            this.baseWinRate = baseWinRate;
            this.agreeN = agreeN;
            this.agreeWinRate = agreeWinRate;
            this.agreeLift = agreeLift;
            this.coverage = coverage;
            this.profitImpactPct = profitImpactPct;
            this.profitEffectScore = profitEffectScore;
            this.winEffectScore = winEffectScore;
            this.profitImpactPctFixedR = profitImpactPctFixedR;
            this.profitEffectScoreFixedR = profitEffectScoreFixedR;
            this.pfBaseFixedR = pfBaseFixedR;
            this.pfFiltFixedR = pfFiltFixedR;
            this.ddBaseFixedR = ddBaseFixedR;
            this.ddFiltFixedR = ddFiltFixedR;

            this.oos6_winLift = oos6_winLift;
            this.oos6_pfBase = oos6_pfBase;
            this.oos6_pfFilt = oos6_pfFilt;
            this.oos6_ddBase = oos6_ddBase;
            this.oos6_ddFilt = oos6_ddFilt;
            this.oos6_profitImpactPct = oos6_profitImpactPct;

            this.oos12_winLift = oos12_winLift;
            this.oos12_pfBase = oos12_pfBase;
            this.oos12_pfFilt = oos12_pfFilt;
            this.oos12_ddBase = oos12_ddBase;
            this.oos12_ddFilt = oos12_ddFilt;
            this.oos12_profitImpactPct = oos12_profitImpactPct;
            this.failMonths = failMonths;
            this.failTotalN = failTotalN;
            this.failAgreeN = failAgreeN;
            this.failCoverage = failCoverage;
            this.failImpactPct = failImpactPct;
            this.failTradeRatio = failTradeRatio;
            this.failW = failW;
            this.overallW = overallW;
            this.weightedImpactPct = weightedImpactPct;
            this.weightedEffectScore = weightedEffectScore;
            this.passMinAgreeN = passMinAgreeN;
            this.passMinFailAgreeN = passMinFailAgreeN;
            this.passBothImprove = passBothImprove;
        }
    }

static final class ComboSpec {
        final int k;        // 组合指标数量
        final int[] idx;    // 指标在 series 列表里的索引（升序、去重）
        final String id;    // 组合ID（用于输出）
        final String group; // 输出分组

        ComboSpec(int k, int[] idx, String id, String group) {
            this.k = k;
            this.idx = idx;
            this.id = id;
            this.group = group;
        }
    }

    // ======================== main ========================
    public static void main(String[] args) throws Exception {

        // 1) 先拆出我们自己的参数，其余给 Args.parse（你原项目已有）
        String tradeLog = null;
        int topN = 50;
        int lookback = 12;
        int maxEvents = 0;
        int minBinN = 200;     // 你建议 50
        int minAgreeN = 200;  // 你建议 ≥100 才进榜/才硬过滤
        int randCombos = 400000; // 两阶段组合采样数量
        int comboMinK = 2;
        int comboMaxK = 10;
        long randSeed = 1L;
        int topM = 200;
        int familyCap = 15;
        String topMMetric = "hybrid";
        String comboSearch = "random";
        int beamWidth = 60;

        int failMonthCombos = 200000; // 失败月+总体提升：随机组合数量（建议 20万~100万）
        int minFailAgreeN = 50;       // 失败月内 agree 最小样本数（避免小样本虚高）
        String failPool = "all";

        // --- Enhancements: OOS + Fixed-R (no compounding) ---
        int oos6 = 1;
        int oos12 = 3;
        double costPct = 0.002; // fee+slippage, applied on |PnL|
     // topM|all

        List<String> pass = new ArrayList<>();
        for (String a : args) {
            if (a.startsWith(ARG_TRADE_LOG)) tradeLog = a.substring(ARG_TRADE_LOG.length());
            else if (a.startsWith(ARG_TOPN)) topN = Integer.parseInt(a.substring(ARG_TOPN.length()));
            else if (a.startsWith(ARG_LOOKBACK)) lookback = Integer.parseInt(a.substring(ARG_LOOKBACK.length()));
            else if (a.startsWith(ARG_MAX_EVENTS)) maxEvents = Integer.parseInt(a.substring(ARG_MAX_EVENTS.length()));
            else if (a.startsWith(ARG_MIN_BIN_N)) minBinN = Integer.parseInt(a.substring(ARG_MIN_BIN_N.length()));
            else if (a.startsWith(ARG_MIN_AGREE_N)) minAgreeN = Integer.parseInt(a.substring(ARG_MIN_AGREE_N.length()));
            else if (a.startsWith(ARG_RAND_COMBOS)) randCombos = Integer.parseInt(a.substring(ARG_RAND_COMBOS.length()));
            else if (a.startsWith(ARG_COMBO_MIN_K)) comboMinK = Integer.parseInt(a.substring(ARG_COMBO_MIN_K.length()));
            else if (a.startsWith(ARG_COMBO_MAX_K)) comboMaxK = Integer.parseInt(a.substring(ARG_COMBO_MAX_K.length()));
            else if (a.startsWith(ARG_RAND_SEED)) randSeed = Long.parseLong(a.substring(ARG_RAND_SEED.length()));
            else if (a.startsWith(ARG_TOPM)) topM = Integer.parseInt(a.substring(ARG_TOPM.length()));
            else if (a.startsWith(ARG_FAMILYCAP)) familyCap = Integer.parseInt(a.substring(ARG_FAMILYCAP.length()));
            else if (a.startsWith(ARG_TOPM_METRIC)) topMMetric = a.substring(ARG_TOPM_METRIC.length());
            else if (a.startsWith(ARG_COMBO_SEARCH)) comboSearch = a.substring(ARG_COMBO_SEARCH.length());
            else if (a.startsWith(ARG_BEAM_WIDTH)) beamWidth = Integer.parseInt(a.substring(ARG_BEAM_WIDTH.length()));
            else if (a.startsWith(ARG_FAILMONTH_COMBOS)) failMonthCombos = Integer.parseInt(a.substring(ARG_FAILMONTH_COMBOS.length()));
            else if (a.startsWith(ARG_MIN_FAIL_AGREE_N)) minFailAgreeN = Integer.parseInt(a.substring(ARG_MIN_FAIL_AGREE_N.length()));
            else if (a.startsWith(ARG_FAIL_POOL)) failPool = a.substring(ARG_FAIL_POOL.length());
            else if (a.startsWith(ARG_OOS6)) oos6 = Integer.parseInt(a.substring(ARG_OOS6.length()));
            else if (a.startsWith(ARG_OOS12)) oos12 = Integer.parseInt(a.substring(ARG_OOS12.length()));
            else if (a.startsWith(ARG_COST_PCT)) costPct = Double.parseDouble(a.substring(ARG_COST_PCT.length()));
            else pass.add(a);
        }

        Args cfg = Args.parse(pass.toArray(new String[0]));
        new File(cfg.outDir).mkdirs();

        if (tradeLog == null || tradeLog.trim().isEmpty()) {
            tradeLog = cfg.outDir + "/trade_log.txt";
        }

        // 2) 外部特征 provider（复用你项目 Main 的模式）
        String fm = (cfg.featureMode == null ? "sqlite" : cfg.featureMode.trim().toLowerCase());
        if ("none".equals(fm) || "off".equals(fm)) {
            ExternalFeatureProviderHolder.set(new NoopExternalFeatureProvider());
            System.out.println("[ExternalFeatures] disabled (--featureMode=none)");
        } else if ("csv".equals(fm)) {
            ExternalFeatureProviderHolder.set(new CsvExternalFeatureProvider(cfg.featureDir));
            System.out.println("[ExternalFeatures] csv enabled: dir=" + cfg.featureDir);
        } else { // sqlite
            String fdb = (cfg.featureDb == null || cfg.featureDb.isEmpty()) ? cfg.db : cfg.featureDb;
            ExternalFeatureProviderHolder.set(new SqliteExternalFeatureProvider(fdb, cfg.instId, cfg.bar));
            System.out.println("[ExternalFeatures] sqlite enabled: db=" + fdb + " table=okx_ext_features");
        }

        // 3) 读取交易日志 -> 事件列表（含方向补全/资金/净U）
        List<TradeEvent> events = parseTradeLog(tradeLog);
        if (events.isEmpty()) {
            throw new IllegalStateException("tradeLog 解析不到任何交易，请确认文件是否包含【SRMF-进场】与【SRMF-出场】行： " + tradeLog);
        }
        if (maxEvents > 0 && events.size() > maxEvents) {
            events = events.subList(0, maxEvents);
        }

        // 排序（保险）
        events.sort(Comparator.comparingLong(a -> a.entryTsMs));

        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        int nLong=0, nShort=0;
        int nWin=0, nLoss=0;
        for (TradeEvent e : events) {
            minTs = Math.min(minTs, e.entryTsMs);
            maxTs = Math.max(maxTs, e.entryTsMs);
            if (e.dir == Dir.LONG) nLong++;
            else if (e.dir == Dir.SHORT) nShort++;
            if (e.win) nWin++; else nLoss++;
        }

        System.out.println("========================================================");
        System.out.println("DiscriminativeIndicatorsMain v7 (two-stage TopM -> combo search)（区分度 + 胜率影响 + 盈利影响）");
        System.out.println("tradeLog=" + tradeLog);
        System.out.println("events=" + events.size() + " (WIN=" + nWin + ", LOSS=" + nLoss + ", LONG=" + nLong + ", SHORT=" + nShort + ")");
        System.out.println("lookback=" + lookback + " bars | topN=" + topN + " | minBinN=" + minBinN + " | minAgreeN=" + minAgreeN);
        System.out.println("randCombos=" + randCombos + " | comboK=" + comboMinK + "~" + comboMaxK + " | randSeed=" + randSeed);
        System.out.println("db=" + cfg.db + " | instId=" + cfg.instId + " | bar=" + cfg.bar);
        System.out.println("timeRange=" + fmtTs(minTs) + " ~ " + fmtTs(maxTs));
        System.out.println("outDir=" + cfg.outDir);
        System.out.println("========================================================");

        // 4) 加载K线（只拉必要区间，省时间）
        long now = System.currentTimeMillis();
        long barMs = OkxKlineUtils.barMs(cfg.bar);
        long safeCloseMs = cfg.safeCloseMs;

        long start = Math.max(0, minTs - barMs * 300); // 左侧多取一些，防止 lookback 不够
        long end = maxTs + barMs * 50;

        OkxCandleCacheEthOsc cache = OkxCandleCacheEthOsc.get(cfg.db, cfg.years);
        List<Candle> candles = cache.getOrFetchRange(cfg.instId, cfg.bar, start, end, now, barMs, safeCloseMs);
        if (candles == null || candles.size() < 200) {
            throw new IllegalStateException("K线数量太少：" + (candles == null ? 0 : candles.size()) +
                    "。请确认 SQLite 是否已有数据，或你的 OKX fetch 接口是否可用。");
        }

        // 5) 映射事件到K线 idx（用最近时间）
        int mapped = mapEventsToCandles(events, candles, barMs);
        System.out.println("映射到K线成功：" + mapped + " / " + events.size());
        if (mapped < Math.max(20, events.size() / 10)) {
            System.out.println("⚠️ 映射成功率偏低：请确认 tradeLog 里的时间是 30m K线 openTs，并且 db 覆盖该时间段。");
        }

        // 6) 枚举“指标库的单指标”
        List<PoolCondition> conds = ConditionFactory.defaultConditionsWithInst(cfg.instId, cfg.bar);
        System.out.println("指标库单指标数量：" + conds.size());

        // 7) 预计算每个指标的 dir + prefix（加速）
        List<CondSeries> series = buildCondSeries(conds, candles);
        // 7b) 两阶段：先做单指标排名，选 TopM，再在 TopM 内做组合搜索（random/beam）
        //     注意：我们会对 OVERALL / LONG_ONLY / SHORT_ONLY 分别选 TopM（更贴近方向差异）
        // 8) 三套：总体/Long/Short（只用映射成功的）
        List<TradeEvent> all = filter(events, null);
        List<TradeEvent> longs = filter(events, Dir.LONG);
        List<TradeEvent> shorts = filter(events, Dir.SHORT);

        // 9) 先算单指标（区分度 / 胜率影响 / 盈利影响）
        List<ScoreRow> overallDiscSingles = calcDiscriminativeSingles("OVERALL", all, series, lookback);
        List<ScoreRow> longDiscSingles = calcDiscriminativeSingles("LONG_ONLY", longs, series, lookback);
        List<ScoreRow> shortDiscSingles = calcDiscriminativeSingles("SHORT_ONLY", shorts, series, lookback);

        List<ImpactRow> overallImpactSingles = buildImpact("OVERALL", all, series, lookback, minBinN, minAgreeN, oos6, oos12, costPct);
        List<ImpactRow> longImpactSingles = buildImpact("LONG_ONLY", longs, series, lookback, minBinN, minAgreeN, oos6, oos12, costPct);
        List<ImpactRow> shortImpactSingles = buildImpact("SHORT_ONLY", shorts, series, lookback, minBinN, minAgreeN, oos6, oos12, costPct);

        // 10) 两阶段组合搜索：先从单指标里选 TopM（默认 hybrid），再只在 TopM 内搜索 2~8 指标组合
        //     - pool（TopM）对 OVERALL / LONG_ONLY / SHORT_ONLY 分别挑选（更贴近方向差异）
        //     - comboSearch=random|beam
        int[] poolOverall = selectTopMIndices("OVERALL", topMMetric, topM, familyCap, series, overallDiscSingles, overallImpactSingles);
        int[] poolLong = selectTopMIndices("LONG_ONLY", topMMetric, topM, familyCap, series, longDiscSingles, longImpactSingles);
        int[] poolShort = selectTopMIndices("SHORT_ONLY", topMMetric, topM, familyCap, series, shortDiscSingles, shortImpactSingles);

        List<ComboSpec> overallCombSpecs = genTwoStageCombos("OVERALL", series, poolOverall, randCombos, comboMinK, comboMaxK, randSeed + 11,
                comboSearch, beamWidth, topMMetric, all, lookback, minBinN, minAgreeN);
        List<ComboSpec> longCombSpecs = genTwoStageCombos("LONG_ONLY", series, poolLong, randCombos, comboMinK, comboMaxK, randSeed + 22,
                comboSearch, beamWidth, topMMetric, longs, lookback, minBinN, minAgreeN);
        List<ComboSpec> shortCombSpecs = genTwoStageCombos("SHORT_ONLY", series, poolShort, randCombos, comboMinK, comboMaxK, randSeed + 33,
                comboSearch, beamWidth, topMMetric, shorts, lookback, minBinN, minAgreeN);

        String comboMetaOverall = buildComboMeta("OVERALL", topMMetric, topM, comboSearch, beamWidth, comboMinK, comboMaxK, randCombos, randSeed + 11, overallCombSpecs.size(), poolOverall.length);
        String comboMetaLong = buildComboMeta("LONG_ONLY", topMMetric, topM, comboSearch, beamWidth, comboMinK, comboMaxK, randCombos, randSeed + 22, longCombSpecs.size(), poolLong.length);
        String comboMetaShort = buildComboMeta("SHORT_ONLY", topMMetric, topM, comboSearch, beamWidth, comboMinK, comboMaxK, randCombos, randSeed + 33, shortCombSpecs.size(), poolShort.length);

        // 11) 区分度（单指标 + 两阶段组合），拼接输出
        List<ScoreRow> overallDiscCombos = calcDiscriminativeCombos("OVERALL", all, series, overallCombSpecs, lookback);
        List<ScoreRow> longDiscCombos = calcDiscriminativeCombos("LONG_ONLY", longs, series, longCombSpecs, lookback);
        List<ScoreRow> shortDiscCombos = calcDiscriminativeCombos("SHORT_ONLY", shorts, series, shortCombSpecs, lookback);

        writeDiscCsvAppended(cfg.outDir + "/indicator_rank_overall.csv", "OVERALL", overallDiscSingles, overallDiscCombos, comboMetaOverall);
        writeDiscCsvAppended(cfg.outDir + "/indicator_rank_long.csv", "LONG_ONLY", longDiscSingles, longDiscCombos, comboMetaLong);
        writeDiscCsvAppended(cfg.outDir + "/indicator_rank_short.csv", "SHORT_ONLY", shortDiscSingles, shortDiscCombos, comboMetaShort);

        // 12) 胜率影响 + 盈利影响（单指标 + 两阶段组合），拼接输出
        List<ImpactRow> overallImpactCombos = buildImpactCombos("OVERALL", all, series, overallCombSpecs, lookback, minBinN, minAgreeN, oos6, oos12, costPct);
        List<ImpactRow> longImpactCombos = buildImpactCombos("LONG_ONLY", longs, series, longCombSpecs, lookback, minBinN, minAgreeN, oos6, oos12, costPct);
        List<ImpactRow> shortImpactCombos = buildImpactCombos("SHORT_ONLY", shorts, series, shortCombSpecs, lookback, minBinN, minAgreeN, oos6, oos12, costPct);

        writeWinImpactCsvAppended(cfg.outDir + "/indicator_winimpact_overall.csv", "OVERALL", overallImpactSingles, overallImpactCombos, comboMetaOverall);
        writeWinImpactCsvAppended(cfg.outDir + "/indicator_winimpact_long.csv", "LONG_ONLY", longImpactSingles, longImpactCombos, comboMetaLong);
        writeWinImpactCsvAppended(cfg.outDir + "/indicator_winimpact_short.csv", "SHORT_ONLY", shortImpactSingles, shortImpactCombos, comboMetaShort);

        // 11) 失败月 vs 成功月：随机组合挖掘（要求：失败月提升 + 总体提升）
//     权重：failW 动态取值 = 失败月trade占比（failTotalN/totalN），总体权重=1.0
int[] candOverall = "all".equalsIgnoreCase(failPool) ? buildAllIndices(series.size()) : poolOverall;
int[] candLong = "all".equalsIgnoreCase(failPool) ? buildAllIndices(series.size()) : poolLong;
int[] candShort = "all".equalsIgnoreCase(failPool) ? buildAllIndices(series.size()) : poolShort;

List<ComboSpec> overallFailSpecs = genRandomCombosFromPool("OVERALL", series, candOverall, failMonthCombos, comboMinK, comboMaxK, randSeed + 101, "TSRND_OVERALL_FAILM");
List<ComboSpec> longFailSpecs = genRandomCombosFromPool("LONG_ONLY", series, candLong, failMonthCombos, comboMinK, comboMaxK, randSeed + 202, "TSRND_LONG_ONLY_FAILM");
List<ComboSpec> shortFailSpecs = genRandomCombosFromPool("SHORT_ONLY", series, candShort, failMonthCombos, comboMinK, comboMaxK, randSeed + 303, "TSRND_SHORT_ONLY_FAILM");

List<FailMonthRow> overallFailRows = buildFailMonthImpactCombos("OVERALL", all, series, overallFailSpecs, lookback, minBinN, minAgreeN, minFailAgreeN, oos6, oos12, costPct);
List<FailMonthRow> longFailRows = buildFailMonthImpactCombos("LONG_ONLY", longs, series, longFailSpecs, lookback, minBinN, minAgreeN, minFailAgreeN, oos6, oos12, costPct);
List<FailMonthRow> shortFailRows = buildFailMonthImpactCombos("SHORT_ONLY", shorts, series, shortFailSpecs, lookback, minBinN, minAgreeN, minFailAgreeN, oos6, oos12, costPct);

String failMetaOverall = "# FAILMONTH_COMBOS meta: pool=" + failPool + " want=" + failMonthCombos + " k=" + comboMinK + "~" + comboMaxK +
        " minAgreeN=" + minAgreeN + " minFailAgreeN=" + minFailAgreeN +
        " failW = failTotalN/totalN (dynamic)";
String failMetaLong = "# FAILMONTH_COMBOS meta: pool=" + failPool + " want=" + failMonthCombos + " k=" + comboMinK + "~" + comboMaxK +
        " minAgreeN=" + minAgreeN + " minFailAgreeN=" + minFailAgreeN +
        " failW = failTotalN/totalN (dynamic)";
String failMetaShort = "# FAILMONTH_COMBOS meta: pool=" + failPool + " want=" + failMonthCombos + " k=" + comboMinK + "~" + comboMaxK +
        " minAgreeN=" + minAgreeN + " minFailAgreeN=" + minFailAgreeN +
        " failW = failTotalN/totalN (dynamic)";

writeProfitImpactCsvAppended3(cfg.outDir + "/indicator_profitimpact_overall.csv", "OVERALL",
        overallImpactSingles, overallImpactCombos, comboMetaOverall,
        overallFailRows, failMetaOverall, topN);
writeProfitImpactCsvAppended3(cfg.outDir + "/indicator_profitimpact_long.csv", "LONG_ONLY",
        longImpactSingles, longImpactCombos, comboMetaLong,
        longFailRows, failMetaLong, topN);
writeProfitImpactCsvAppended3(cfg.outDir + "/indicator_profitimpact_short.csv", "SHORT_ONLY",
        shortImpactSingles, shortImpactCombos, comboMetaShort,
        shortFailRows, failMetaShort, topN);


        // 11) 控制台摘要（TopN：应用 minAgreeN 过滤）
        printTopDisc("总体 区分度 单指标 Top" + topN, overallDiscSingles, topN);
        printTopDisc("总体 区分度 两阶段组合 Top" + topN, overallDiscCombos, topN);
        printTopDisc("Long 区分度 单指标 Top" + topN, longDiscSingles, topN);
        printTopDisc("Long 区分度 两阶段组合 Top" + topN, longDiscCombos, topN);
        printTopDisc("Short 区分度 单指标 Top" + topN, shortDiscSingles, topN);
        printTopDisc("Short 区分度 两阶段组合 Top" + topN, shortDiscCombos, topN);

        printTopWin("总体 胜率影响 单指标 Top" + topN, overallImpactSingles, topN);
        printTopWin("总体 胜率影响 两阶段组合 Top" + topN, overallImpactCombos, topN);
        printTopWin("Long 胜率影响 单指标 Top" + topN, longImpactSingles, topN);
        printTopWin("Long 胜率影响 两阶段组合 Top" + topN, longImpactCombos, topN);
        printTopWin("Short 胜率影响 单指标 Top" + topN, shortImpactSingles, topN);
        printTopWin("Short 胜率影响 两阶段组合 Top" + topN, shortImpactCombos, topN);

        printTopProfit("总体 盈利影响 单指标 Top" + topN, overallImpactSingles, topN);
        printTopProfit("总体 盈利影响 两阶段组合 Top" + topN, overallImpactCombos, topN);
        printTopProfit("Long 盈利影响 单指标 Top" + topN, longImpactSingles, topN);
        printTopProfit("Long 盈利影响 两阶段组合 Top" + topN, longImpactCombos, topN);
        printTopProfit("Short 盈利影响 单指标 Top" + topN, shortImpactSingles, topN);
        printTopProfit("Short 盈利影响 两阶段组合 Top" + topN, shortImpactCombos, topN);

        // Fail-Month（失败月）加权组合 Top10（排序：weightedEffectScore；同时要求 failImpact>0 且 overallProfitImpact>0）
        printTopFailMonth("总体 失败月加权组合 Top" + topN, overallFailRows, topN, minAgreeN, minFailAgreeN);
        printTopFailMonth("Long 失败月加权组合 Top" + topN, longFailRows, topN, minAgreeN, minFailAgreeN);
        printTopFailMonth("Short 失败月加权组合 Top" + topN, shortFailRows, topN, minAgreeN, minFailAgreeN);


        System.out.println("\n✅ 输出完成：");
        System.out.println(" - " + cfg.outDir + "/indicator_rank_overall.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_rank_long.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_rank_short.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_winimpact_overall.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_winimpact_long.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_winimpact_short.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_profitimpact_overall.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_profitimpact_long.csv");
        System.out.println(" - " + cfg.outDir + "/indicator_profitimpact_short.csv");

        System.out.println("\n🔧 使用建议（你提的增强2）：");
        System.out.println(" - agreeN < minAgreeN 或 coverage 很低：只建议“加权投票加分”，不要一票否决。");
        System.out.println(" - bestBin 样本数(bestN) >= minBinN 才考虑做‘只在 bestBin 触发’的硬条件。");
        System.out.println(" - 你会看到：有的指标 lift 高但 coverage 极低；有的 coverage 高但 lift 小。");
        System.out.println("   → 直接看 winEffectScore / profitEffectScore（已把‘提升×覆盖’折中）。");
    }

    // ======================== core: build series ========================
    private static List<CondSeries> buildCondSeries(List<PoolCondition> conds, List<Candle> candles) {
        List<CondSeries> out = new ArrayList<>(conds.size());
        for (PoolCondition c : conds) {
            try {
                int[] d = c.dir(candles, 0, candles.size());
                if (d == null || d.length != candles.size()) {
                    System.out.println("⚠️ 指标 " + c.id() + " dir长度异常，已置零。len=" + (d == null ? 0 : d.length) + ", candles=" + candles.size());
                    d = new int[candles.size()];
                }
                out.add(new CondSeries(c, d));
            } catch (Exception ex) {
                System.out.println("⚠️ 指标 " + c.id() + " 计算失败，已置零。原因=" + ex.getMessage());
                out.add(new CondSeries(c, new int[candles.size()]));
            }
        }
        return out;
    }

    
    // ======================== random combos ========================
    private static List<ComboSpec> genRandomCombos(List<CondSeries> series, int want, int minK, int maxK, long seed) {
        int n = series.size();
        if (n < 2 || want <= 0) return new ArrayList<>();

        minK = Math.max(2, minK);
        maxK = Math.min(maxK, n);
        if (minK > maxK) { int t = minK; minK = maxK; maxK = t; }

        Random rnd = new Random(seed);
        HashSet<String> seen = new HashSet<>();
        ArrayList<ComboSpec> out = new ArrayList<>(want);

        int maxAttempts = Math.max(10000, want * 60);
        for (int attempts = 0; out.size() < want && attempts < maxAttempts; attempts++) {
            int k = minK + rnd.nextInt(maxK - minK + 1);
            int[] idx = pickDistinctIdx(n, k, rnd);
            Arrays.sort(idx);
            String key = Arrays.toString(idx);
            if (!seen.add(key)) continue;

            String id = buildComboId(idx, series, k);
            String group = "RND_COMBO_K" + k;
            out.add(new ComboSpec(k, idx, id, group));
        }
        return out;
    }

    // ======================== Two-Stage (TopM -> Combo Search) ========================

    /**
     * 组装 CSV 里“组合段”的头部说明（拼接在单指标段下方）
     */
    private static String buildComboMeta(String title,
                                         String topMMetric, int topM,
                                         String comboSearch, int beamWidth,
                                         int comboMinK, int comboMaxK,
                                         int requested, long seed,
                                         int actualCombos, int poolN) {
        String mode = (comboSearch == null ? "random" : comboSearch.trim());
        String metric = (topMMetric == null ? "hybrid" : topMMetric.trim());
        String bw = "beam".equalsIgnoreCase(mode) ? (" beamWidth=" + beamWidth) : "";
        return "# --- TWO_STAGE_COMBOS title=" + title +
                " poolN=" + poolN + " topM=" + topM + " metric=" + metric +
                " mode=" + mode + bw +
                " k=" + comboMinK + "~" + comboMaxK +
                " requested=" + requested + " actual=" + actualCombos +
                " seed=" + seed + " ---";
    }

    /**
     * 从单指标里选 TopM 进入组合候选池（返回 series 的原始索引列表）
     *
     * topMMetric：
     * - disc   : 用区分度 score 排序
     * - win    : 用 winEffectScore 排序
     * - profit : 用 profitEffectScore 排序
     * - hybrid : winEffectScore + 0.05 * profitEffectScore（默认，避免只追 lift 却砍利润）
     */
    private static int[] selectTopMIndices(String title,
                                          String topMMetric,
                                          int topM,
                                          int familyCap,
                                          List<CondSeries> series,
                                          List<ScoreRow> discSingles,
                                          List<ImpactRow> impactSingles) {

        HashMap<String, Integer> id2idx = new HashMap<>();
        for (int i = 0; i < series.size(); i++) {
            String id = series.get(i).cond.id();
            id2idx.put(id, i);
            String nid = normalizeId(id);
            if (!nid.equals(id)) id2idx.put(nid, i);
        }

        String metric = (topMMetric == null ? "hybrid" : topMMetric.trim().toLowerCase(Locale.ROOT));
        if (topM <= 0) topM = 20;

        ArrayList<Integer> out = new ArrayList<>();

        // familyCap：每个指标族最多入池数量（<=0 则不限制）
        HashMap<String, Integer> famCnt = new HashMap<>();


        if ("disc".equals(metric)) {
            ArrayList<ScoreRow> tmp = new ArrayList<>(discSingles);
            tmp.sort((a, b) -> Double.compare(b.score, a.score));
            for (ScoreRow r : tmp) {
                Integer idx = id2idx.get(r.id);
                if (idx == null) idx = id2idx.get(normalizeId(r.id));
                if (idx == null) continue;
                if (out.contains(idx)) continue;
                if (familyCap > 0) {
                    String fam = parseFamilyFromId(series.get(idx).cond.id());
                    int cnt = famCnt.getOrDefault(fam, 0);
                    if (cnt >= familyCap) continue;
                    famCnt.put(fam, cnt + 1);
                }
                out.add(idx);
                if (out.size() >= topM) break;
            }
        } else {
            ArrayList<ImpactRow> tmp = new ArrayList<>(impactSingles);
            // 先按我们选择的 metric 排序
            tmp.sort((a, b) -> Double.compare(scoreForPool(metric, b), scoreForPool(metric, a)));
            for (ImpactRow r : tmp) {
                Integer idx = id2idx.get(r.id);
                if (idx == null) idx = id2idx.get(normalizeId(r.id));
                if (idx == null) continue;
                // 只从“样本足够”的单指标里选（避免 n=20 这种虚高）
                if (!r.passMinAgreeN) continue;
                if (out.contains(idx)) continue;
                if (familyCap > 0) {
                    String fam = parseFamilyFromId(series.get(idx).cond.id());
                    int cnt = famCnt.getOrDefault(fam, 0);
                    if (cnt >= familyCap) continue;
                    famCnt.put(fam, cnt + 1);
                }
                out.add(idx);
                if (out.size() >= topM) break;
            }
        }

        // fallback：如果池子太小（通常是 id 不匹配或样本门槛过严），分两次放宽：先忽略 passMinAgreeN，再忽略 familyCap
        int minNeed = Math.min(5, Math.max(2, topM / 5));
        if (out.size() < minNeed) {
            out.clear();
            famCnt.clear();
            if ("disc".equals(metric)) {
                ArrayList<ScoreRow> tmp2 = new ArrayList<>(discSingles);
                tmp2.sort((a, b) -> Double.compare(b.score, a.score));
                for (ScoreRow r : tmp2) {
                    Integer idx = id2idx.get(r.id);
                    if (idx == null) idx = id2idx.get(normalizeId(r.id));
                    if (idx == null) continue;
                    if (out.contains(idx)) continue;
                    if (familyCap > 0) {
                        String fam = parseFamilyFromId(series.get(idx).cond.id());
                        int cnt = famCnt.getOrDefault(fam, 0);
                        if (cnt >= familyCap) continue;
                        famCnt.put(fam, cnt + 1);
                    }
                    out.add(idx);
                    if (out.size() >= topM) break;
                }
            } else {
                ArrayList<ImpactRow> tmp2 = new ArrayList<>(impactSingles);
                tmp2.sort((a, b) -> Double.compare(scoreForPool(metric, b), scoreForPool(metric, a)));
                for (ImpactRow r : tmp2) {
                    Integer idx = id2idx.get(r.id);
                    if (idx == null) idx = id2idx.get(normalizeId(r.id));
                    if (idx == null) continue;
                    if (out.contains(idx)) continue;
                    // 注意：fallback1 不再要求 passMinAgreeN
                    if (familyCap > 0) {
                        String fam = parseFamilyFromId(series.get(idx).cond.id());
                        int cnt = famCnt.getOrDefault(fam, 0);
                        if (cnt >= familyCap) continue;
                        famCnt.put(fam, cnt + 1);
                    }
                    out.add(idx);
                    if (out.size() >= topM) break;
                }
            }
        }

        if (out.size() < minNeed) {
            out.clear();
            // fallback2：完全不做 family 限制，至少保证 pool 能达到 topM
            if ("disc".equals(metric)) {
                ArrayList<ScoreRow> tmp3 = new ArrayList<>(discSingles);
                tmp3.sort((a, b) -> Double.compare(b.score, a.score));
                for (ScoreRow r : tmp3) {
                    Integer idx = id2idx.get(r.id);
                    if (idx == null) idx = id2idx.get(normalizeId(r.id));
                    if (idx == null) continue;
                    if (out.contains(idx)) continue;
                    out.add(idx);
                    if (out.size() >= topM) break;
                }
            } else {
                ArrayList<ImpactRow> tmp3 = new ArrayList<>(impactSingles);
                tmp3.sort((a, b) -> Double.compare(scoreForPool(metric, b), scoreForPool(metric, a)));
                for (ImpactRow r : tmp3) {
                    Integer idx = id2idx.get(r.id);
                    if (idx == null) idx = id2idx.get(normalizeId(r.id));
                    if (idx == null) continue;
                    if (out.contains(idx)) continue;
                    out.add(idx);
                    if (out.size() >= topM) break;
                }
            }
        }

int[] arr = new int[out.size()];
        for (int i = 0; i < out.size(); i++) arr[i] = out.get(i);
        Arrays.sort(arr);
        System.out.println("两阶段候选池[" + title + "] metric=" + metric + " topM=" + topM + " familyCap=" + familyCap + " => poolN=" + arr.length);
        if (familyCap > 0) {
            HashMap<String,Integer> dist=new HashMap<>();
            for (int idx : arr) {
                String fam=parseFamilyFromId(series.get(idx).cond.id());
                dist.put(fam, dist.getOrDefault(fam,0)+1);
            }
            ArrayList<String> keys=new ArrayList<>(dist.keySet());
            keys.sort(String::compareTo);
            StringBuilder sb=new StringBuilder();
            for (String k: keys) { sb.append(k).append(":").append(dist.get(k)).append(" "); }
            System.out.println("  poolFamilyDist: " + sb.toString().trim());
        }
        return arr;
    }

    
    
    private static String normalizeId(String id) {
        if (id == null) return "";
        // 支持两种编码：F=XXX|... 或者直接是 XXX_...
        String s = id.trim();
        int p = s.indexOf('|');
        if (p > 0 && s.startsWith("F=")) {
            return s.substring(p + 1);
        }
        return s;
    }

private static String parseFamilyFromId(String id) {
        if (id == null) return "UNKNOWN";
        // 优先解析我们统一的前缀：F=<FAMILY>|
        if (id.startsWith("F=")) {
            int p = id.indexOf('|');
            if (p > 2) return id.substring(2, p);
        }
        // fallback：取第一个非字母数字分隔前的前缀（例如 EMA200 -> EMA）
        int i = 0;
        while (i < id.length()) {
            char ch = id.charAt(i);
            if (Character.isLetter(ch) || ch == '_' ) { i++; continue; }
            break;
        }
        if (i >= 1) return id.substring(0, i);
        return "UNKNOWN";
    }

private static double scoreForPool(String metric, ImpactRow r) {
        double w = r.winEffectScore;
        double p = r.profitEffectScore;
        if ("win".equals(metric)) return w;
        if ("profit".equals(metric)) return p;
        // hybrid
        return w + 0.05 * p;
    }

    /**
     * 两阶段组合生成：
     * 1) 只在 poolIdx（TopM）里取指标
     * 2) comboSearch=random|beam
     *
     * 注意：这里仅生成 ComboSpec（组合描述），真正的详细统计仍复用你原来的 buildImpactCombos / calcDiscriminativeCombos，
     *       这样“单指标 vs 组合”完全同口径。
     */
    private static List<ComboSpec> genTwoStageCombos(String title,
                                                     List<CondSeries> series,
                                                     int[] poolIdx,
                                                     int want,
                                                     int minK,
                                                     int maxK,
                                                     long seed,
                                                     String comboSearch,
                                                     int beamWidth,
                                                     String topMMetric,
                                                     List<TradeEvent> events,
                                                     int lookback,
                                                     int minBinN,
                                                     int minAgreeN) {

        if (poolIdx == null || poolIdx.length < 2 || want <= 0) return new ArrayList<>();
        int poolN = poolIdx.length;
        int kMin = Math.max(2, minK);
        int kMax = Math.min(Math.max(kMin, maxK), poolN);

        String mode = (comboSearch == null ? "random" : comboSearch.trim().toLowerCase(Locale.ROOT));

        if ("beam".equals(mode)) {
            return genBeamCombosFromPool(title, series, poolIdx, want, kMin, kMax, seed,
                    beamWidth, topMMetric, events, lookback, minBinN, minAgreeN);
        } else {
            return genRandomCombosFromPool(series, poolIdx, want, kMin, kMax, seed, "TSRND_" + title);
        }
    }

    /**
     * 随机采样：只在 TopM pool 里采样（比全量随机更有效）
     */
    
    private static String comboId(List<CondSeries> series, int[] idx, String tag, int k) {
        // buildComboId: COMBOk:IND1+IND2+...
        String base = buildComboId(idx, series, k);
        if (tag == null || tag.isEmpty()) return base;
        return tag + "_" + base;
    }

private static List<ComboSpec> genRandomCombosFromPool(List<CondSeries> series,
                                                           int[] poolIdx,
                                                           int want,
                                                           int minK,
                                                           int maxK,
                                                           long seed,
                                                           String tag) {
        int poolN = poolIdx.length;
        if (poolN < 2 || want <= 0) return new ArrayList<>();

        Random rnd = new Random(seed);
        HashSet<String> seen = new HashSet<>();
        ArrayList<ComboSpec> out = new ArrayList<>();

        int safety = want * 50 + 200;
        for (int t = 0; t < safety && out.size() < want; t++) {
            int k = minK + rnd.nextInt(maxK - minK + 1);
            if (k > poolN) k = poolN;

            // 从 pool 里挑 k 个不同的
            int[] pickPos = pickDistinctIdx(poolN, k, rnd); // 0..poolN-1
            int[] idx = new int[k];
            for (int i = 0; i < k; i++) idx[i] = poolIdx[pickPos[i]];
            Arrays.sort(idx);

            String key = Arrays.toString(idx);
            if (!seen.add(key)) continue;

            String id = comboId(series, idx, tag, k);
            String group = tag + "_K" + k;
            out.add(new ComboSpec(k, idx, id, group));
        }

        return out;
    }

    /**
     * Beam 搜索：在 TopM pool 内从 2..kMax 逐层扩展，保留每层 top beamWidth，
     * 最终把所有层的候选合并后取前 want。
     *
     * 排序口径：默认用 topMMetric（hybrid|win|profit|disc）
     * - 如果 topMMetric=disc：用区分度 score
     * - 否则：用 hybrid/win/profit 的 effectScore
     */
    private static List<ComboSpec> genBeamCombosFromPool(String title,
                                                         List<CondSeries> series,
                                                         int[] poolIdx,
                                                         int want,
                                                         int minK,
                                                         int maxK,
                                                         long seed,
                                                         int beamWidth,
                                                         String topMMetric,
                                                         List<TradeEvent> events,
                                                         int lookback,
                                                         int minBinN,
                                                         int minAgreeN) {

        if (beamWidth <= 0) beamWidth = 60;

        String metric = (topMMetric == null ? "hybrid" : topMMetric.trim().toLowerCase(Locale.ROOT));
        Random rnd = new Random(seed);

        // 预先计算 base stats（用于 profit/win/hybrid）
        final int totalN = events.size();
        final int winN = countWins(events);
        final double baseWinRate = totalN == 0 ? 0.0 : (winN * 1.0 / totalN);
        final double initEq = guessInitEquity(events);
        final double baseFinalEq = replayEquity(events, initEq, null);

        // Candidate 表示一个组合 + score
        class Cand {
            final int[] idx;  // series 原始索引（升序）
            final double score;
            Cand(int[] idx, double score) { this.idx = idx; this.score = score; }
        }

        ArrayList<Cand> allCands = new ArrayList<>();

        // 起始层：所有 pairs（poolN<=20，pairs<=190，很便宜）
        int poolN = poolIdx.length;

        ArrayList<Cand> beam = new ArrayList<>();
        for (int i = 0; i < poolN; i++) {
            for (int j = i + 1; j < poolN; j++) {
                int[] idx = new int[]{poolIdx[i], poolIdx[j]};
                double sc = scoreCombo(metric, events, series, idx, lookback, minBinN, minAgreeN, baseWinRate, initEq, baseFinalEq);
                beam.add(new Cand(idx, sc));
            }
        }
        beam.sort((a, b) -> Double.compare(b.score, a.score));
        if (beam.size() > beamWidth) beam = new ArrayList<>(beam.subList(0, beamWidth));
        allCands.addAll(beam);

        // 逐层扩展到 maxK
        for (int k = 3; k <= maxK; k++) {
            HashSet<String> seen = new HashSet<>();
            ArrayList<Cand> next = new ArrayList<>();

            for (Cand c : beam) {
                // 从 pool 里随机打散一下扩展顺序，避免总是同一批被扩展
                int[] poolOrder = Arrays.copyOf(poolIdx, poolIdx.length);
                for (int z = poolOrder.length - 1; z > 0; z--) {
                    int r = rnd.nextInt(z + 1);
                    int tmp = poolOrder[z];
                    poolOrder[z] = poolOrder[r];
                    poolOrder[r] = tmp;
                }

                for (int add : poolOrder) {
                    if (contains(c.idx, add)) continue;
                    int[] idx2 = appendSorted(c.idx, add);
                    if (idx2.length != k) continue;
                    String key = Arrays.toString(idx2);
                    if (!seen.add(key)) continue;

                    double sc = scoreCombo(metric, events, series, idx2, lookback, minBinN, minAgreeN, baseWinRate, initEq, baseFinalEq);
                    next.add(new Cand(idx2, sc));
                }
            }

            if (next.isEmpty()) break;
            next.sort((a, b) -> Double.compare(b.score, a.score));
            if (next.size() > beamWidth) next = new ArrayList<>(next.subList(0, beamWidth));
            beam = next;
            allCands.addAll(beam);
        }

        // 汇总：按 score 取前 want，再转 ComboSpec
        allCands.sort((a, b) -> Double.compare(b.score, a.score));

        HashSet<String> uniq = new HashSet<>();
        ArrayList<ComboSpec> out = new ArrayList<>();
        for (Cand c : allCands) {
            if (c.idx.length < minK) continue;
            if (c.idx.length > maxK) continue;
            String key = Arrays.toString(c.idx);
            if (!uniq.add(key)) continue;

            int k = c.idx.length;
            String tag = "TSBEAM_" + title;
            String id = comboId(series, c.idx, tag, k);
            String group = tag + "_K" + k;
            out.add(new ComboSpec(k, c.idx, id, group));
            if (out.size() >= want) break;
        }

        return out;
    }

    private static int countWins(List<TradeEvent> events) {
        int w = 0;
        for (TradeEvent e : events) if (e.win) w++;
        return w;
    }

    private static double scoreCombo(String metric,
                                     List<TradeEvent> events,
                                     List<CondSeries> series,
                                     int[] idx,
                                     int lookback,
                                     int minBinN,
                                     int minAgreeN,
                                     double baseWinRate,
                                     double initEq,
                                     double baseFinalEq) {

        // disc：用区分度 score
        if ("disc".equals(metric)) {
            return scoreComboDisc(events, series, idx, lookback);
        }

        // win/profit/hybrid：用 impact effectScore
        // 先计算 agree、coverage、winEffect、profitEffect
        int totalN = events.size();
        if (totalN == 0) return -1e9;

        int agreeN = 0;
        int agreeW = 0;

        boolean[] include = new boolean[totalN];

        for (int i = 0; i < totalN; i++) {
            TradeEvent e = events.get(i);
            int sumStrength = 0;
            for (int j : idx) sumStrength += series.get(j).strengthAt(e.candleIdx, lookback);
            int L = lookback * idx.length;
            int bin = to5Bin(sumStrength, L);
            if (isAgree(e.dir, bin)) {
                include[i] = true;
                agreeN++;
                if (e.win) agreeW++;
            }
        }

        if (agreeN <= 0) return -1e9;

        double agreeWR = agreeW * 1.0 / agreeN;
        double lift = agreeWR - baseWinRate;
        double cov = agreeN * 1.0 / totalN;

        double winEffect = lift * Math.sqrt(cov);

        // profitEffect：如果 baseFinalEq 无效（=0），就退化成 winEffect
        double profitEffect = 0.0;
        if (Double.isFinite(baseFinalEq) && baseFinalEq > 0) {
            double filtFinal = replayEquity(events, initEq, include);
            double profitImpactPct = (filtFinal / baseFinalEq - 1.0) * 100.0;
            profitEffect = (profitImpactPct / 100.0) * Math.sqrt(cov);
        }

        if ("win".equals(metric)) return winEffect;
        if ("profit".equals(metric)) return profitEffect;

        // hybrid
        return winEffect + 0.05 * profitEffect;
    }

    private static double scoreComboDisc(List<TradeEvent> events,
                                         List<CondSeries> series,
                                         int[] idx,
                                         int lookback) {

        int[] winBin = new int[5];
        int[] lossBin = new int[5];

        for (TradeEvent e : events) {
            int sumStrength = 0;
            for (int j : idx) sumStrength += series.get(j).strengthAt(e.candleIdx, lookback);
            int L = lookback * idx.length;
            int bin = to5Bin(sumStrength, L);
            if (e.win) winBin[bin]++; else lossBin[bin]++;
        }

        // 预测规则：每个 bin 里 WIN 多 -> 预测 WIN，否则预测 LOSS（平票算 WIN）
        int tp = 0, fp = 0, fn = 0;
        for (int b = 0; b < 5; b++) {
            boolean predWin = winBin[b] >= lossBin[b];
            if (predWin) {
                tp += winBin[b];
                fp += lossBin[b];
            } else {
                fn += winBin[b];
            }
        }
        double precision = (tp + fp) == 0 ? 0.0 : (tp * 1.0 / (tp + fp));
        double recall = (tp + fn) == 0 ? 0.0 : (tp * 1.0 / (tp + fn));
        double f1 = (precision + recall) == 0 ? 0.0 : (2.0 * precision * recall / (precision + recall));

        double js = jsDivergence(winBin, lossBin);
        return 0.6 * js + 0.4 * f1;
    }

    private static boolean contains(int[] arr, int x) {
        for (int v : arr) if (v == x) return true;
        return false;
    }

    private static int[] appendSorted(int[] arr, int add) {
        int n = arr.length;
        int[] out = new int[n + 1];
        int i = 0, j = 0;
        boolean done = false;
        while (i < n) {
            if (!done && add < arr[i]) {
                out[j++] = add;
                done = true;
            } else {
                out[j++] = arr[i++];
            }
        }
        if (!done) out[j] = add;
        // 去重保护（理论上不会进来）
        for (int k = 1; k < out.length; k++) {
            if (out[k] == out[k - 1]) return arr;
        }
        return out;
    }



    private static int[] pickDistinctIdx(int n, int k, Random rnd) {
        boolean[] used = new boolean[n];
        int[] out = new int[k];
        int filled = 0;
        while (filled < k) {
            int v = rnd.nextInt(n);
            if (used[v]) continue;
            used[v] = true;
            out[filled++] = v;
        }
        return out;
    }

    private static String buildComboId(int[] idx, List<CondSeries> series, int k) {
        StringBuilder sb = new StringBuilder();
        sb.append("COMBO").append(k).append(":");
        for (int i = 0; i < idx.length; i++) {
            if (i > 0) sb.append("+");
            sb.append(series.get(idx[i]).cond.id());
        }
        return sb.toString();
    }

// ======================== core: filtering ========================
    private static List<TradeEvent> filter(List<TradeEvent> all, Dir dir) {
        List<TradeEvent> out = new ArrayList<>();
        for (TradeEvent e : all) {
            if (e.candleIdx < 0) continue;                 // 没映射上的跳过
            if (Math.abs(e.netPoints) < 1e-12) continue;   // 净=0 跳过
            if (dir == null || e.dir == dir) out.add(e);
        }
        return out;
    }

    // ======================== Part A: discriminative rank ========================

    /**
     * 单指标：区分度（JS + F1）评分
     */
    private static List<ScoreRow> calcDiscriminativeSingles(String title, List<TradeEvent> events, List<CondSeries> series,
                                                            int lookback) {
        List<ScoreRow> rows = new ArrayList<>(series.size());

        for (CondSeries cs : series) {

            int[] winBin = new int[5];
            int[] lossBin = new int[5];

            for (TradeEvent e : events) {
                int strength = cs.strengthAt(e.candleIdx, lookback);
                int bin = to5Bin(strength, lookback);
                if (e.win) winBin[bin]++; else lossBin[bin]++;
            }

            int nWin = sum(winBin);
            int nLoss = sum(lossBin);

            // 预测规则：每个 bin 里 WIN 多 -> 预测 WIN，否则预测 LOSS（平票算 WIN）
            int tp = 0, fp = 0, fn = 0;
            for (int b = 0; b < 5; b++) {
                boolean predWin = winBin[b] >= lossBin[b];
                if (predWin) {
                    tp += winBin[b];
                    fp += lossBin[b];
                } else {
                    fn += winBin[b];
                }
            }

            double precision = (tp + fp) == 0 ? 0.0 : (tp * 1.0 / (tp + fp));
            double recall = (tp + fn) == 0 ? 0.0 : (tp * 1.0 / (tp + fn));
            double f1 = (precision + recall) == 0 ? 0.0 : (2.0 * precision * recall / (precision + recall));

            double js = jsDivergence(winBin, lossBin);

            double meanWin = meanBin(winBin);
            double meanLoss = meanBin(lossBin);

            double score = 0.6 * js + 0.4 * f1;

            rows.add(new ScoreRow(cs.cond.id(), cs.cond.group(), score, f1, js, meanWin, meanLoss, nWin, nLoss));
        }

        rows.sort((a, b) -> Double.compare(b.score, a.score));
        return rows;
    }

    /**
     * 两阶段组合：把多个指标的 strength 相加后，再做 5-bin 分类，计算区分度（JS + F1）
     */
    private static List<ScoreRow> calcDiscriminativeCombos(String title, List<TradeEvent> events, List<CondSeries> series,
                                                           List<ComboSpec> combos, int lookback) {
        List<ScoreRow> rows = new ArrayList<>(combos.size());

        for (ComboSpec sp : combos) {

            int[] winBin = new int[5];
            int[] lossBin = new int[5];

            int L = lookback * sp.k;

            for (TradeEvent e : events) {
                int strength = 0;
                for (int idx : sp.idx) strength += series.get(idx).strengthAt(e.candleIdx, lookback);

                int bin = to5Bin(strength, L);
                if (e.win) winBin[bin]++; else lossBin[bin]++;
            }

            int nWin = sum(winBin);
            int nLoss = sum(lossBin);

            int tp = 0, fp = 0, fn = 0;
            for (int b = 0; b < 5; b++) {
                boolean predWin = winBin[b] >= lossBin[b];
                if (predWin) {
                    tp += winBin[b];
                    fp += lossBin[b];
                } else {
                    fn += winBin[b];
                }
            }

            double precision = (tp + fp) == 0 ? 0.0 : (tp * 1.0 / (tp + fp));
            double recall = (tp + fn) == 0 ? 0.0 : (tp * 1.0 / (tp + fn));
            double f1 = (precision + recall) == 0 ? 0.0 : (2.0 * precision * recall / (precision + recall));

            double js = jsDivergence(winBin, lossBin);

            double meanWin = meanBin(winBin);
            double meanLoss = meanBin(lossBin);

            double score = 0.6 * js + 0.4 * f1;

            rows.add(new ScoreRow(sp.id, sp.group, score, f1, js, meanWin, meanLoss, nWin, nLoss));
        }

        rows.sort((a, b) -> Double.compare(b.score, a.score));
        return rows;
    }

    private static void writeDiscCsvAppended(String path, String title,
                                             List<ScoreRow> singles, List<ScoreRow> combos,
                                             String comboMeta) throws IOException {
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title);
            pw.println("rank,id,group,score,f1,js,meanWinBin,meanLossBin,nWin,nLoss");

            int r = 1;
            for (ScoreRow x : singles) {
                pw.printf(Locale.US, "%d,%s,%s,%.8f,%.8f,%.8f,%.4f,%.4f,%d,%d%n",
                        r++, esc(x.id), esc(x.group), x.score, x.f1, x.js, x.meanWinBin, x.meanLossBin, x.nWin, x.nLoss);
            }

            pw.println();
            pw.println(comboMeta);
            pw.println("rank,id,group,score,f1,js,meanWinBin,meanLossBin,nWin,nLoss");

            int r2 = 1;
            for (ScoreRow x : combos) {
                pw.printf(Locale.US, "%d,%s,%s,%.8f,%.8f,%.8f,%.4f,%.4f,%d,%d%n",
                        r2++, esc(x.id), esc(x.group), x.score, x.f1, x.js, x.meanWinBin, x.meanLossBin, x.nWin, x.nLoss);
            }
        }
    }

    private static List<ScoreRow> rankDiscriminative(String title, List<TradeEvent> events, List<CondSeries> series,
                                                     int lookback, String outCsv) throws IOException {

        List<ScoreRow> rows = new ArrayList<>(series.size());

        for (CondSeries cs : series) {

            int[] winBin = new int[5];
            int[] lossBin = new int[5];

            for (TradeEvent e : events) {
                int strength = cs.strengthAt(e.candleIdx, lookback);
                int bin = to5Bin(strength, lookback);
                if (e.win) winBin[bin]++; else lossBin[bin]++;
            }

            int nWin = sum(winBin);
            int nLoss = sum(lossBin);

            // 预测规则：每个bin里 WIN多->预测WIN，否则预测LOSS
            int tp = 0, fp = 0, fn = 0;
            for (int b = 0; b < 5; b++) {
                boolean predWin = winBin[b] >= lossBin[b]; // 平票算WIN（你也可以改）
                if (predWin) {
                    tp += winBin[b];
                    fp += lossBin[b];
                } else {
                    fn += winBin[b];
                }
            }

            double precision = (tp + fp) == 0 ? 0.0 : (tp * 1.0 / (tp + fp));
            double recall = (tp + fn) == 0 ? 0.0 : (tp * 1.0 / (tp + fn));
            double f1 = (precision + recall) == 0 ? 0.0 : (2.0 * precision * recall / (precision + recall));

            double js = jsDivergence(winBin, lossBin);

            double meanWin = meanBin(winBin);
            double meanLoss = meanBin(lossBin);

            // “最不相似”更偏 JS，“可预测”偏 F1
            double score = 0.6 * js + 0.4 * f1;

            rows.add(new ScoreRow(cs.cond.id(), cs.cond.group(), score, f1, js, meanWin, meanLoss, nWin, nLoss));
        }

        rows.sort((a, b) -> Double.compare(b.score, a.score));
        writeDiscCsv(outCsv, title, rows);
        return rows;
    }

    private static void writeDiscCsv(String path, String title, List<ScoreRow> rows) throws IOException {
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title);
            pw.println("rank,id,group,score,f1,js,meanWinBin,meanLossBin,nWin,nLoss");
            int r = 1;
            for (ScoreRow x : rows) {
                pw.printf(Locale.US, "%d,%s,%s,%.8f,%.8f,%.8f,%.4f,%.4f,%d,%d%n",
                        r++, esc(x.id), esc(x.group), x.score, x.f1, x.js, x.meanWinBin, x.meanLossBin, x.nWin, x.nLoss);
            }
        }
    }

    private static void printTopDisc(String title, List<ScoreRow> rows, int n) {
        System.out.println("\n==================== " + title + " ====================");
        for (int i = 0; i < Math.min(n, rows.size()); i++) {
            ScoreRow x = rows.get(i);
            System.out.printf(Locale.US, "%2d) %-26s [%s]  score=%.4f  (JS=%.4f, F1=%.4f)  nW=%d nL=%d%n",
                    i + 1, x.id, x.group, x.score, x.js, x.f1, x.nWin, x.nLoss);
        }
        System.out.println("=====================================================");
    }

    // ======================== Part B: win/profit impact ========================
    private static List<ImpactRow> buildImpact(String title, List<TradeEvent> events, List<CondSeries> series,
                                               int lookback, int minBinN, int minAgreeN,
                                               int oos6Months, int oos12Months, double costPct) {

        int totalN = events.size();
        int winN = 0;
        for (TradeEvent e : events) if (e.win) winN++;
        int lossN = totalN - winN;

        double baseWinRate = totalN == 0 ? 0.0 : (winN * 1.0 / totalN);

        // base equity replay（尽量贴近 SRMF 曲线）
        double initEq = guessInitEquity(events);
        double baseFinalEq = replayEquity(events, initEq, null); // null = include all trades
        double baseReturnPct = initEq <= 0 ? 0.0 : (baseFinalEq / initEq - 1.0) * 100.0;

        List<ImpactRow> rows = new ArrayList<>(series.size());

        for (CondSeries cs : series) {

            int[] binN = new int[5];
            int[] binW = new int[5];

            int agreeN = 0;
            int agreeW = 0;

            // includeMask: true=保留这笔交易（agree），false=过滤掉
            boolean[] include = new boolean[events.size()];

            for (int i = 0; i < events.size(); i++) {
                TradeEvent e = events.get(i);
                int strength = cs.strengthAt(e.candleIdx, lookback);
                int bin = to5Bin(strength, lookback);

                binN[bin]++;
                if (e.win) binW[bin]++;

                boolean isAgree = isAgree(e.dir, bin);
                include[i] = isAgree;
                if (isAgree) {
                    agreeN++;
                    if (e.win) agreeW++;
                }
            }

            double coverage = totalN == 0 ? 0.0 : (agreeN * 1.0 / totalN);
            double agreeWinRate = agreeN == 0 ? 0.0 : (agreeW * 1.0 / agreeN);
            double agreeLift = agreeWinRate - baseWinRate;
            double winEffectScore = agreeLift * Math.sqrt(Math.max(0.0, coverage));

            // 盈利影响（只保留 agree 的“硬过滤”版本）
            double filteredFinalEq = replayEquity(events, initEq, include);
            double filteredReturnPct = initEq <= 0 ? 0.0 : (filteredFinalEq / initEq - 1.0) * 100.0;

            // 对最终盈利的百分比影响（相对 base final）
            double profitImpactPct = baseFinalEq == 0 ? 0.0 : (filteredFinalEq / baseFinalEq - 1.0) * 100.0;
            double profitEffectScore = (profitImpactPct / 100.0) * Math.sqrt(Math.max(0.0, coverage));

            // --- Fixed-R (no compounding) re-eval: use additive equity on netU minus costs ---
            FixedRStats frAll = computeFixedRStats(events, include, Long.MIN_VALUE, costPct);
            double baseFinalEqFixedR = frAll.baseFinalEq;
            double filteredFinalEqFixedR = frAll.filtFinalEq;
            double profitImpactPctFixedR = frAll.profitImpactPct;
            double profitEffectScoreFixedR = (profitImpactPctFixedR / 100.0) * Math.sqrt(Math.max(0.0, coverage));
            double pfBaseFixedR = frAll.pfBase;
            double pfFiltFixedR = frAll.pfFilt;
            double ddBaseFixedR = frAll.ddBase;
            double ddFiltFixedR = frAll.ddFilt;

            long maxTs = maxEntryTs(events);
            long cutoff6 = cutoffTsByMonths(maxTs, oos6Months);
            long cutoff12 = cutoffTsByMonths(maxTs, oos12Months);

            FixedRStats fr6 = computeFixedRStats(events, include, cutoff6, costPct);
            double oos6_winLift = fr6.winLift;
            double oos6_pfBase = fr6.pfBase;
            double oos6_pfFilt = fr6.pfFilt;
            double oos6_ddBase = fr6.ddBase;
            double oos6_ddFilt = fr6.ddFilt;
            double oos6_profitImpactPct = fr6.profitImpactPct;

            FixedRStats fr12 = computeFixedRStats(events, include, cutoff12, costPct);
            double oos12_winLift = fr12.winLift;
            double oos12_pfBase = fr12.pfBase;
            double oos12_pfFilt = fr12.pfFilt;
            double oos12_ddBase = fr12.ddBase;
            double oos12_ddFilt = fr12.ddFilt;
            double oos12_profitImpactPct = fr12.profitImpactPct;

            // best/worst 档位（只在 binN>=minBinN 的档位里找）
            int bestBin = -1, worstBin = -1;
            double bestWR = -1.0, worstWR = 2.0;
            int bestN = 0, worstN = 0;

            for (int b = 0; b < 5; b++) {
                if (binN[b] < minBinN) continue;
                double wr = binN[b] == 0 ? 0.0 : (binW[b] * 1.0 / binN[b]);
                if (wr > bestWR) {
                    bestWR = wr; bestBin = b; bestN = binN[b];
                }
                if (wr < worstWR) {
                    worstWR = wr; worstBin = b; worstN = binN[b];
                }
            }
            if (bestBin < 0) { bestWR = 0.0; }
            if (worstBin < 0) { worstWR = 0.0; }

            boolean passMinAgree = agreeN >= minAgreeN;

            rows.add(new ImpactRow(
                    cs.cond.id(), cs.cond.group(),
                    totalN, winN, lossN, baseWinRate,
                    agreeN, agreeW, agreeWinRate, agreeLift, coverage, winEffectScore,
                    baseFinalEq, filteredFinalEq, baseReturnPct, filteredReturnPct, profitImpactPct, profitEffectScore,
                    baseFinalEqFixedR, filteredFinalEqFixedR, profitImpactPctFixedR, profitEffectScoreFixedR,
                    pfBaseFixedR, pfFiltFixedR, ddBaseFixedR, ddFiltFixedR,
                    oos6_winLift, oos6_pfBase, oos6_pfFilt, oos6_ddBase, oos6_ddFilt, oos6_profitImpactPct,
                    oos12_winLift, oos12_pfBase, oos12_pfFilt, oos12_ddBase, oos12_ddFilt, oos12_profitImpactPct,
                    bestBin, bestN, bestWR,
                    worstBin, worstN, worstWR,
                    passMinAgree
            ));
        }

        // rows 不排序（写 CSV 时分别排序）
        return rows;
    }

    
    /**
     * 两阶段组合：把多个指标的 strength 相加后，再做 5-bin 分类，计算：
     * - agree 胜率提升（lift）
     * - 过滤后的 equity replay 对最终盈利的影响（profitImpactPct）
     */
    private static List<ImpactRow> buildImpactCombos(String title, List<TradeEvent> events, List<CondSeries> series,
                                                     List<ComboSpec> combos, int lookback, int minBinN, int minAgreeN,
                                                     int oos6Months, int oos12Months, double costPct) {

        int totalN = events.size();
        int winN = 0;
        for (TradeEvent e : events) if (e.win) winN++;
        int lossN = totalN - winN;
        double baseWinRate = totalN == 0 ? 0.0 : (winN * 1.0 / totalN);

        // 基准 equity（不加过滤）
        double initEq = guessInitEquity(events);
        double baseFinalEq = replayEquity(events, initEq, null);
        double baseReturnPct = initEq <= 0 ? 0.0 : (baseFinalEq / initEq - 1.0) * 100.0;

        List<ImpactRow> rows = new ArrayList<>(combos.size());

        for (ComboSpec sp : combos) {

            int[] binN = new int[5];
            int[] binW = new int[5];

            int agreeN = 0;
            int agreeW = 0;

            boolean[] include = new boolean[totalN];

            int L = lookback * sp.k;

            for (int i = 0; i < totalN; i++) {
                TradeEvent e = events.get(i);

                int strength = 0;
                for (int idx : sp.idx) strength += series.get(idx).strengthAt(e.candleIdx, lookback);

                int bin = to5Bin(strength, L);

                binN[bin]++;
                if (e.win) binW[bin]++;

                boolean ok = isAgree(e.dir, bin);
                include[i] = ok;
                if (ok) {
                    agreeN++;
                    if (e.win) agreeW++;
                }
            }

            double coverage = totalN == 0 ? 0.0 : (agreeN * 1.0 / totalN);
            double agreeWinRate = agreeN == 0 ? 0.0 : (agreeW * 1.0 / agreeN);
            double agreeLift = agreeWinRate - baseWinRate;

            double winEffectScore = agreeLift * Math.sqrt(Math.max(0.0, coverage));

            // equity replay（只保留 include=true 的交易）
            double filteredFinalEq = replayEquity(events, initEq, include);
            double filteredReturnPct = initEq <= 0 ? 0.0 : (filteredFinalEq / initEq - 1.0) * 100.0;
            double profitImpactPct = baseFinalEq == 0 ? 0.0 : (filteredFinalEq / baseFinalEq - 1.0) * 100.0;
            double profitEffectScore = (profitImpactPct / 100.0) * Math.sqrt(Math.max(0.0, coverage));

            // --- Fixed-R (no compounding) re-eval: use additive equity on netU minus costs ---
            FixedRStats frAll = computeFixedRStats(events, include, Long.MIN_VALUE, costPct);
            double baseFinalEqFixedR = frAll.baseFinalEq;
            double filteredFinalEqFixedR = frAll.filtFinalEq;
            double profitImpactPctFixedR = frAll.profitImpactPct;
            double profitEffectScoreFixedR = (profitImpactPctFixedR / 100.0) * Math.sqrt(Math.max(0.0, coverage));
            double pfBaseFixedR = frAll.pfBase;
            double pfFiltFixedR = frAll.pfFilt;
            double ddBaseFixedR = frAll.ddBase;
            double ddFiltFixedR = frAll.ddFilt;

            long maxTs = maxEntryTs(events);
            long cutoff6 = cutoffTsByMonths(maxTs, oos6Months);
            long cutoff12 = cutoffTsByMonths(maxTs, oos12Months);

            FixedRStats fr6 = computeFixedRStats(events, include, cutoff6, costPct);
            double oos6_winLift = fr6.winLift;
            double oos6_pfBase = fr6.pfBase;
            double oos6_pfFilt = fr6.pfFilt;
            double oos6_ddBase = fr6.ddBase;
            double oos6_ddFilt = fr6.ddFilt;
            double oos6_profitImpactPct = fr6.profitImpactPct;

            FixedRStats fr12 = computeFixedRStats(events, include, cutoff12, costPct);
            double oos12_winLift = fr12.winLift;
            double oos12_pfBase = fr12.pfBase;
            double oos12_pfFilt = fr12.pfFilt;
            double oos12_ddBase = fr12.ddBase;
            double oos12_ddFilt = fr12.ddFilt;
            double oos12_profitImpactPct = fr12.profitImpactPct;

            // best/worst 档位（只在 binN>=minBinN 的档位里找）
            int bestBin = -1, worstBin = -1;
            double bestWR = -1.0, worstWR = 2.0;
            int bestN = 0, worstN = 0;

            for (int b = 0; b < 5; b++) {
                if (binN[b] < minBinN) continue;
                double wr = binN[b] == 0 ? 0.0 : (binW[b] * 1.0 / binN[b]);
                if (wr > bestWR) {
                    bestWR = wr; bestBin = b; bestN = binN[b];
                }
                if (wr < worstWR) {
                    worstWR = wr; worstBin = b; worstN = binN[b];
                }
            }
            if (bestBin < 0) { bestWR = 0.0; }
            if (worstBin < 0) { worstWR = 0.0; }

            boolean passMinAgree = agreeN >= minAgreeN;

            rows.add(new ImpactRow(
                    sp.id, sp.group,
                    totalN, winN, lossN, baseWinRate,
                    agreeN, agreeW, agreeWinRate, agreeLift, coverage, winEffectScore,
                    baseFinalEq, filteredFinalEq, baseReturnPct, filteredReturnPct, profitImpactPct, profitEffectScore,
                    baseFinalEqFixedR, filteredFinalEqFixedR, profitImpactPctFixedR, profitEffectScoreFixedR,
                    pfBaseFixedR, pfFiltFixedR, ddBaseFixedR, ddFiltFixedR,
                    oos6_winLift, oos6_pfBase, oos6_pfFilt, oos6_ddBase, oos6_ddFilt, oos6_profitImpactPct,
                    oos12_winLift, oos12_pfBase, oos12_pfFilt, oos12_ddBase, oos12_ddFilt, oos12_profitImpactPct,
                    bestBin, bestN, bestWR,
                    worstBin, worstN, worstWR,
                    passMinAgree
            ));
        }

        return rows;
    }

    

    // ======== Fail-month helpers ========
    private static YearMonth ymTokyo(long tsMs) {
        return YearMonth.from(Instant.ofEpochMilli(tsMs).atZone(TOKYO).toLocalDate());
    }

    private static Map<YearMonth, Double> monthFactorProduct(List<TradeEvent> events) {
        Map<YearMonth, Double> mp = new HashMap<>();
        for (TradeEvent e : events) {
            YearMonth ym = ymTokyo(e.entryTsMs);
            mp.put(ym, mp.getOrDefault(ym, 1.0) * safeFactor(e.factor));
        }
        return mp;
    }

    private static double safeFactor(double f) {
        if (!Double.isFinite(f) || f <= 0.0) return 1.0;
        return f;
    }

    private static Set<YearMonth> selectFailMonths(Map<YearMonth, Double> monthFactor) {
        Set<YearMonth> fail = new HashSet<>();
        for (Map.Entry<YearMonth, Double> en : monthFactor.entrySet()) {
            double f = en.getValue();
            if (Double.isFinite(f) && f < 1.0) fail.add(en.getKey());
        }
        return fail;
    }

    private static double replayFactor(List<TradeEvent> events, boolean[] includeMask, Set<YearMonth> monthMask) {
        double eq = 1.0;
        for (int i = 0; i < events.size(); i++) {
            TradeEvent e = events.get(i);
            if (monthMask != null) {
                YearMonth ym = ymTokyo(e.entryTsMs);
                if (!monthMask.contains(ym)) continue;
            }
            if (includeMask != null && !includeMask[i]) continue;
            eq *= safeFactor(e.factor);
        }
        return eq;
    }

    /**
     * 失败月 vs 成功月：随机组合挖掘（2~8 指标），并且必须满足：
     * ✅ 失败月提升（failImpactPct > 0）
     * ✅ 总体提升（profitImpactPct > 0）
     *
     * 排名评分：weightedImpactPct = failImpactPct*(1+failW) + profitImpactPct*overallW
     * 其中 failW = failTradeRatio = failTotalN/totalN（你要求的动态权重），overallW 固定 1.0
     */
    private static List<FailMonthRow> buildFailMonthImpactCombos(String title,
                                                                 List<TradeEvent> events,
                                                                 List<CondSeries> series,
                                                                 List<ComboSpec> combos,
                                                                 int lookback,
                                                                 int minBinN,
                                                                 int minAgreeN,
                                                                 int minFailAgreeN,
                                                                 int oos6Months, int oos12Months, double costPct) {

        int totalN = events.size();
        int winN = 0;
        for (TradeEvent e : events) if (e.win) winN++;
        double baseWinRate = totalN == 0 ? 0.0 : (winN * 1.0 / totalN);

        // 失败月集合：基于 base 策略（不加过滤）按月的 factor product < 1 的月份
        Set<YearMonth> failMonths = selectFailMonths(monthFactorProduct(events));
        int failMonthsCount = failMonths.size();

        int failTotalN = 0;
        for (TradeEvent e : events) {
            if (failMonths.contains(ymTokyo(e.entryTsMs))) failTotalN++;
        }
        double failTradeRatio = totalN == 0 ? 0.0 : (failTotalN * 1.0 / totalN);
        double failW = failTradeRatio; // 你要求：failW 动态取值 = 失败月trade占比
        double overallW = 1.0;

        // base factors（只看失败月）
        double baseFailEq = replayFactor(events, null, failMonths);

        // 总体基准 equity（不加过滤）
        double initEq = guessInitEquity(events);
        double baseFinalEq = replayEquity(events, initEq, null);

        List<FailMonthRow> out = new ArrayList<>(combos.size());

        for (ComboSpec sp : combos) {

            int[] binN = new int[5];
            int[] binW = new int[5];

            int agreeN = 0, agreeW = 0;
            int failAgreeN = 0;

            boolean[] include = new boolean[totalN];

            int L = lookback * sp.k;

            for (int i = 0; i < totalN; i++) {
                TradeEvent e = events.get(i);

                int strength = 0;
                for (int idx : sp.idx) strength += series.get(idx).strengthAt(e.candleIdx, lookback);

                int bin = to5Bin(strength, L);

                binN[bin]++;
                if (e.win) binW[bin]++;

                boolean ok = isAgree(e.dir, bin);
                include[i] = ok;
                if (ok) {
                    agreeN++;
                    if (e.win) agreeW++;
                    if (failMonths.contains(ymTokyo(e.entryTsMs))) failAgreeN++;
                }
            }

            double coverage = totalN == 0 ? 0.0 : (agreeN * 1.0 / totalN);
            double agreeWinRate = agreeN == 0 ? 0.0 : (agreeW * 1.0 / agreeN);
            double agreeLift = agreeWinRate - baseWinRate;
            double winEffectScore = agreeLift * Math.sqrt(Math.max(0.0, coverage));

            // 总体盈利影响：用 baseFinalEq vs filteredFinalEq
            double filteredFinalEq = replayEquity(events, initEq, include);
            double profitImpactPct = baseFinalEq == 0 ? 0.0 : (filteredFinalEq / baseFinalEq - 1.0) * 100.0;
            double profitEffectScore = (profitImpactPct / 100.0) * Math.sqrt(Math.max(0.0, coverage));

            // 失败月盈利影响：只看失败月内 trades 的 factor product
            double filtFailEq = replayFactor(events, include, failMonths);
            double failImpactPct = baseFailEq == 0 ? 0.0 : (filtFailEq / baseFailEq - 1.0) * 100.0;

            double failCoverage = failTotalN == 0 ? 0.0 : (failAgreeN * 1.0 / failTotalN);

            boolean passMinAgree = agreeN >= minAgreeN;
            boolean passMinFailAgree = failAgreeN >= minFailAgreeN;

            boolean passBothImprove = (profitImpactPct > 0.0) && (failImpactPct > 0.0);

            // 加权：失败月更重要（failWeight = 1 + failW），同时整体也要提升
            double weightedImpactPct = failImpactPct * (1.0 + failW) + profitImpactPct * overallW;
            double weightedEffectScore = (weightedImpactPct / 100.0) * Math.sqrt(Math.max(0.0, coverage));

            // --- Fixed-R (no compounding) re-eval (additive PnL, with per-trade costs) ---
            FixedRStats frAll = computeFixedRStats(events, include, Long.MIN_VALUE, costPct);
            double profitImpactPctFixedR = frAll.profitImpactPct;
            double profitEffectScoreFixedR = (profitImpactPctFixedR / 100.0) * Math.sqrt(Math.max(0.0, coverage));
            double pfBaseFixedR = frAll.pfBase;
            double pfFiltFixedR = frAll.pfFilt;
            double ddBaseFixedR = frAll.ddBase;
            double ddFiltFixedR = frAll.ddFilt;

            long maxTs = maxEntryTs(events);
            long cutoff6 = cutoffTsByMonths(maxTs, oos6Months);
            long cutoff12 = cutoffTsByMonths(maxTs, oos12Months);

            FixedRStats fr6 = computeFixedRStats(events, include, cutoff6, costPct);
            double oos6_winLift = fr6.winLift;
            double oos6_pfBase = fr6.pfBase;
            double oos6_pfFilt = fr6.pfFilt;
            double oos6_ddBase = fr6.ddBase;
            double oos6_ddFilt = fr6.ddFilt;
            double oos6_profitImpactPct = fr6.profitImpactPct;

            FixedRStats fr12 = computeFixedRStats(events, include, cutoff12, costPct);
            double oos12_winLift = fr12.winLift;
            double oos12_pfBase = fr12.pfBase;
            double oos12_pfFilt = fr12.pfFilt;
            double oos12_ddBase = fr12.ddBase;
            double oos12_ddFilt = fr12.ddFilt;
            double oos12_profitImpactPct = fr12.profitImpactPct;

            out.add(new FailMonthRow(
                    sp.id, sp.group,
                    totalN, baseWinRate,
                    agreeN, agreeWinRate, agreeLift, coverage,
                    profitImpactPct, profitEffectScore, winEffectScore,
                    profitImpactPctFixedR, profitEffectScoreFixedR, pfBaseFixedR, pfFiltFixedR, ddBaseFixedR, ddFiltFixedR,
                    oos6_winLift, oos6_pfBase, oos6_pfFilt, oos6_ddBase, oos6_ddFilt, oos6_profitImpactPct,
                    oos12_winLift, oos12_pfBase, oos12_pfFilt, oos12_ddBase, oos12_ddFilt, oos12_profitImpactPct,
                    failMonthsCount, failTotalN, failAgreeN, failCoverage, failImpactPct,
                    failTradeRatio, failW, overallW,
                    weightedImpactPct, weightedEffectScore,
                    passMinAgree, passMinFailAgree, passBothImprove
            ));
        }

        return out;
    }

    private static List<ComboSpec> genRandomCombosFromPool(String title, List<CondSeries> series, int[] candidateIdx,
                                                           int want, int kMin, int kMax, long seed,
                                                           String idPrefix) {
        Random rnd = new Random(seed);
        int n = candidateIdx.length;
        if (n <= 0 || want <= 0) return Collections.emptyList();

        List<ComboSpec> out = new ArrayList<>(want);
        HashSet<String> seen = new HashSet<>(want * 2);

        int tries = 0;
        int maxTries = Math.max(want * 30, 200000);

        while (out.size() < want && tries++ < maxTries) {
            int k = kMin + rnd.nextInt(Math.max(1, (kMax - kMin + 1)));
            if (k > n) k = n;

            int[] pick = new int[k];
            for (int i = 0; i < k; i++) pick[i] = candidateIdx[rnd.nextInt(n)];
            Arrays.sort(pick);

            // unique indices
            int uniq = 0;
            for (int i = 0; i < k; i++) {
                if (i == 0 || pick[i] != pick[i - 1]) pick[uniq++] = pick[i];
            }
            if (uniq < 2) continue;
            int[] idx = Arrays.copyOf(pick, uniq);

            String key = Arrays.toString(idx);
            if (!seen.add(key)) continue;

            StringBuilder names = new StringBuilder();
            for (int i = 0; i < idx.length; i++) {
                if (i > 0) names.append("+");
                names.append(series.get(idx[i]).cond.id());
            }

            String id = idPrefix + "_COMBO" + idx.length + ":" + names;
            String group = idPrefix + "_K" + idx.length;

            out.add(new ComboSpec(idx.length, idx, id, group));
        }

        return out;
    }

    private static int[] buildAllIndices(int n) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = i;
        return a;
    }

private static void writeWinImpactCsvAppended(String path, String title,
                                                  List<ImpactRow> singles, List<ImpactRow> combos,
                                                  String comboMeta) throws IOException {

        List<ImpactRow> s1 = new ArrayList<>(singles);
        s1.sort((a, b) -> Double.compare(b.winEffectScore, a.winEffectScore));

        List<ImpactRow> s2 = new ArrayList<>(combos);
        s2.sort((a, b) -> Double.compare(b.winEffectScore, a.winEffectScore));

        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title + " win-impact (rank by winEffectScore = agreeLift * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,winEffectScore,profitImpactPct,profitEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");

            int r = 1;
            for (ImpactRow x : s1) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.6f,%.4f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage, x.winEffectScore,
                        x.profitImpactPct, x.profitEffectScore,
                        (x.bestBin < 0 ? "" : String.valueOf(x.bestBin)), x.bestWinRate, x.bestN,
                        (x.worstBin < 0 ? "" : String.valueOf(x.worstBin)), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "Y" : "N"
                );
            }

            pw.println();
            pw.println(comboMeta);
            pw.println("# " + title + " win-impact (rank by winEffectScore = agreeLift * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,winEffectScore,profitImpactPct,profitEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");

            int r2 = 1;
            for (ImpactRow x : s2) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.6f,%.4f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r2++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage, x.winEffectScore,
                        x.profitImpactPct, x.profitEffectScore,
                        (x.bestBin < 0 ? "" : String.valueOf(x.bestBin)), x.bestWinRate, x.bestN,
                        (x.worstBin < 0 ? "" : String.valueOf(x.worstBin)), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "Y" : "N"
                );
            }
        }
    }

    private static void writeProfitImpactCsvAppended(String path, String title,
                                                     List<ImpactRow> singles, List<ImpactRow> combos,
                                                     String comboMeta) throws IOException {

        List<ImpactRow> s1 = new ArrayList<>(singles);
        s1.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        List<ImpactRow> s2 = new ArrayList<>(combos);
        s2.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title + " profit-impact (rank by profitEffectScore = (profitImpactPct/100) * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,profitImpactPct,profitEffectScore,winEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");

            int r = 1;
            for (ImpactRow x : s1) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.4f,%.6f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage,
                        x.profitImpactPct, x.profitEffectScore, x.winEffectScore,
                        (x.bestBin < 0 ? "" : String.valueOf(x.bestBin)), x.bestWinRate, x.bestN,
                        (x.worstBin < 0 ? "" : String.valueOf(x.worstBin)), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "Y" : "N"
                );
            }

            pw.println();
            pw.println(comboMeta);
            pw.println("# " + title + " profit-impact (rank by profitEffectScore = (profitImpactPct/100) * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,profitImpactPct,profitEffectScore,winEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");

            int r2 = 1;
            for (ImpactRow x : s2) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.4f,%.6f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r2++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage,
                        x.profitImpactPct, x.profitEffectScore, x.winEffectScore,
                        (x.bestBin < 0 ? "" : String.valueOf(x.bestBin)), x.bestWinRate, x.bestN,
                        (x.worstBin < 0 ? "" : String.valueOf(x.worstBin)), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "Y" : "N"
                );
            }
        
        }
    }

    private static void writeProfitImpactCsvAppended3(String path, String title,
                                                      List<ImpactRow> singles,
                                                      List<ImpactRow> combos,
                                                      String comboMeta,
                                                      List<FailMonthRow> failCombos,
                                                      String failComboMeta,
                                                      int topN) throws IOException {

        List<ImpactRow> s1 = new ArrayList<>(singles);
        s1.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        List<ImpactRow> s2 = new ArrayList<>(combos);
        s2.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        List<FailMonthRow> s3 = new ArrayList<>(failCombos);
        // 这里按 weightedEffectScore（加权×覆盖）排序，更实用
        s3.sort((a, b) -> Double.compare(b.weightedEffectScore, a.weightedEffectScore));

        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            // --- singles ---
            pw.println("# " + title + " profit-impact (rank by profitEffectScore = (profitImpactPct/100) * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,profitImpactPct,profitEffectScore,winEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");

            int r = 1;
            for (ImpactRow x : s1) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.4f,%.6f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage,
                        x.profitImpactPct, x.profitEffectScore, x.winEffectScore,
                        (x.bestBin < 0 ? "" : String.valueOf(x.bestBin)), x.bestWinRate, x.bestN,
                        (x.worstBin < 0 ? "" : String.valueOf(x.worstBin)), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "Y" : "N"
                );
            }

            // --- combos ---
            pw.println();
            pw.println(comboMeta);
            pw.println("# " + title + " profit-impact (rank by profitEffectScore = (profitImpactPct/100) * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,profitImpactPct,profitEffectScore,winEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");

            int r2 = 1;
            for (ImpactRow x : s2) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.4f,%.6f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r2++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage,
                        x.profitImpactPct, x.profitEffectScore, x.winEffectScore,
                        (x.bestBin < 0 ? "" : String.valueOf(x.bestBin)), x.bestWinRate, x.bestN,
                        (x.worstBin < 0 ? "" : String.valueOf(x.worstBin)), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "Y" : "N"
                );
            }

            // --- fail-month combos ---
            pw.println();
            pw.println(failComboMeta);
            pw.println("# " + title + " FAIL-MONTH weighted combos (require: failImpactPct>0 && profitImpactPct>0; rank by weightedEffectScore)");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,profitImpactPct,profitEffectScore,failMonths,failTotalN,failAgreeN,failCoverage,failImpactPct,failW,overallW,weightedImpactPct,weightedEffectScore,passMinAgreeN,passMinFailAgreeN,passBothImprove");

            int r3 = 1;
            for (FailMonthRow x : s3) {
                // 只输出“同时提升 + 样本够”的
                if (!(x.passBothImprove && x.passMinAgreeN && x.passMinFailAgreeN)) continue;
                if (r3 > topN) break;

                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.4f,%.6f,%d,%d,%d,%.6f,%.4f,%.6f,%.6f,%.4f,%.6f,%s,%s,%s%n",
                        r3++,
                        esc(x.id), esc(x.group),
                        x.totalN, x.baseWinRate,
                        x.agreeN, x.agreeWinRate, x.agreeLift, x.coverage,
                        x.profitImpactPct, x.profitEffectScore,
                        x.failMonths, x.failTotalN, x.failAgreeN, x.failCoverage, x.failImpactPct,
                        x.failW, x.overallW, x.weightedImpactPct, x.weightedEffectScore,
                        x.passMinAgreeN ? "Y" : "N",
                        x.passMinFailAgreeN ? "Y" : "N",
                        x.passBothImprove ? "Y" : "N"
                );
            }
        }
    }


private static void writeWinImpactCsv(String path, String title, List<ImpactRow> rows) throws IOException {
        // 按 winEffectScore 排序（你说的“实用性”：提升×覆盖）
        List<ImpactRow> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> Double.compare(b.winEffectScore, a.winEffectScore));

        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title + " win-impact (rank by winEffectScore = agreeLift * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,winEffectScore,profitImpactPct,profitEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");
            int r = 1;
            for (ImpactRow x : sorted) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.6f,%.4f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r++,
                        esc(x.id), esc(x.group),
                        x.totalN,
                        x.baseWinRate,
                        x.agreeN,
                        x.agreeWinRate,
                        x.agreeLift,
                        x.coverage,
                        x.winEffectScore,
                        x.profitImpactPct,
                        x.profitEffectScore,
                        binName(x.bestBin), x.bestWinRate, x.bestN,
                        binName(x.worstBin), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "YES" : "NO"
                );
            }
        }
    }

    private static void writeProfitImpactCsv(String path, String title, List<ImpactRow> rows) throws IOException {
        // 按 profitEffectScore 排序（盈利提升×覆盖）
        List<ImpactRow> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("# " + title + " profit-impact (rank by profitEffectScore = (profitImpactPct/100) * sqrt(coverage))");
            pw.println("rank,id,group,totalN,baseWinRate,agreeN,agreeWinRate,agreeLift,coverage,winEffectScore,baseFinalEq,filteredFinalEq,baseReturnPct,filteredReturnPct,profitImpactPct,profitEffectScore,bestBin,bestWinRate,bestN,worstBin,worstWinRate,worstN,passMinAgreeN");
            int r = 1;
            for (ImpactRow x : sorted) {
                pw.printf(Locale.US,
                        "%d,%s,%s,%d,%.6f,%d,%.6f,%.6f,%.6f,%.6f,%.2f,%.2f,%.4f,%.4f,%.4f,%.6f,%s,%.6f,%d,%s,%.6f,%d,%s%n",
                        r++,
                        esc(x.id), esc(x.group),
                        x.totalN,
                        x.baseWinRate,
                        x.agreeN,
                        x.agreeWinRate,
                        x.agreeLift,
                        x.coverage,
                        x.winEffectScore,
                        x.baseFinalEquity,
                        x.filteredFinalEquity,
                        x.baseReturnPct,
                        x.filteredReturnPct,
                        x.profitImpactPct,
                        x.profitEffectScore,
                        binName(x.bestBin), x.bestWinRate, x.bestN,
                        binName(x.worstBin), x.worstWinRate, x.worstN,
                        x.passMinAgreeN ? "YES" : "NO"
                );
            }
        }
    }

    private static void printTopWin(String title, List<ImpactRow> rows, int topN) {
        // 只展示 passMinAgreeN 的 TopN（你说的“才允许进榜”）
        List<ImpactRow> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> Double.compare(b.winEffectScore, a.winEffectScore));

        System.out.println("\n==================== " + title + " ====================");
        int shown = 0;
        for (ImpactRow x : sorted) {
            if (!x.passMinAgreeN) continue;
            System.out.printf(Locale.US,
                    "%2d) %-26s [%s] winLift=%+.2f%% cov=%.1f%% effect=%.4f  baseWR=%.2f%% agreeWR=%.2f%% agreeN=%d  best=%s(%.2f%%,n=%d) worst=%s(%.2f%%,n=%d)%n",
                    ++shown,
                    x.id, x.group,
                    x.agreeLift * 100.0,
                    x.coverage * 100.0,
                    x.winEffectScore,
                    x.baseWinRate * 100.0,
                    x.agreeWinRate * 100.0,
                    x.agreeN,
                    binName(x.bestBin), x.bestWinRate * 100.0, x.bestN,
                    binName(x.worstBin), x.worstWinRate * 100.0, x.worstN
            );
            if (shown >= topN) break;
        }
        if (shown == 0) {
            System.out.println("（没有任何指标满足 minAgreeN 的门槛，说明 minAgreeN 设得太高或样本太少）");
        }
        System.out.println("=====================================================");
    }

    private static void printTopProfit(String title, List<ImpactRow> rows, int topN) {
        List<ImpactRow> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        System.out.println("\n==================== " + title + " ====================");
        int shown = 0;
        for (ImpactRow x : sorted) {
            if (!x.passMinAgreeN) continue;
            System.out.printf(Locale.US,
                    "%2d) %-26s [%s] profitImpact=%+.2f%% cov=%.1f%% effect=%.4f  baseFinal=%.2f filtFinal=%.2f  baseRet=%.2f%% filtRet=%.2f%% | fixedRImpact=%+.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% | OOS6 lift=%+.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% imp=%+.2f%% | OOS12 lift=%+.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% imp=%+.2f%%%n",
                    ++shown,
                    x.id, x.group,
                    x.profitImpactPct,
                    x.coverage * 100.0,
                    x.profitEffectScore,
                    x.baseFinalEquity,
                    x.filteredFinalEquity,
                    x.baseReturnPct,
                    x.filteredReturnPct,
                    x.profitImpactPctFixedR, x.pfBaseFixedR, x.pfFiltFixedR, x.ddBaseFixedR*100.0, x.ddFiltFixedR*100.0,
                    x.oos6_winLift, x.oos6_pfBase, x.oos6_pfFilt, x.oos6_ddBase*100.0, x.oos6_ddFilt*100.0, x.oos6_profitImpactPct,
                    x.oos12_winLift, x.oos12_pfBase, x.oos12_pfFilt, x.oos12_ddBase*100.0, x.oos12_ddFilt*100.0, x.oos12_profitImpactPct
            );
            if (shown >= topN) break;
        }
        if (shown == 0) {
            System.out.println("（没有任何指标满足 minAgreeN 的门槛，说明 minAgreeN 设得太高或样本太少）");
        }
        System.out.println("=====================================================");
    }

private static void printTopFailMonth(String title,
                                     List<FailMonthRow> rows,
                                     int topN,
                                     int minAgreeN,
                                     int minFailAgreeN) {
    List<FailMonthRow> sorted = new ArrayList<>(rows);
    // 更实用：优先看“失败月提升 + 总体提升”的加权效果（你设置：failW = failTradeRatio）
    sorted.sort((a, b) -> Double.compare(b.weightedEffectScore, a.weightedEffectScore));

    System.out.println("\n==================== " + title + " ====================");
    int shown = 0;
    for (FailMonthRow x : sorted) {
        // 必须：失败月样本足够 + 总体样本足够 + 两者都提升
        if (!x.passMinAgreeN) continue;
        if (!x.passMinFailAgreeN) continue;
        if (!x.passBothImprove) continue;

        System.out.printf(Locale.US,
                "%2d) %-26s [%s] weighted=%.2f%% (effect=%.4f)  failImpact=%+.2f%% (failCov=%.1f%%, failN=%d/%d, failW=%.3f, failMonths=%d)  overallImpact=%+.2f%% (cov=%.1f%%, agreeN=%d/%d) | fixedRImpact=%+.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% | OOS6 lift=%+.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% imp=%+.2f%% | OOS12 lift=%+.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% imp=%+.2f%%%n",
                ++shown,
                x.id, x.group,
                x.weightedImpactPct, x.weightedEffectScore,
                x.failImpactPct, x.failCoverage * 100.0, x.failAgreeN, x.failTotalN, x.failW, x.failMonths,
                x.profitImpactPct, x.coverage * 100.0, x.agreeN, x.totalN,
                x.profitImpactPctFixedR, x.pfBaseFixedR, x.pfFiltFixedR, x.ddBaseFixedR*100.0, x.ddFiltFixedR*100.0,
                x.oos6_winLift, x.oos6_pfBase, x.oos6_pfFilt, x.oos6_ddBase*100.0, x.oos6_ddFilt*100.0, x.oos6_profitImpactPct,
                x.oos12_winLift, x.oos12_pfBase, x.oos12_pfFilt, x.oos12_ddBase*100.0, x.oos12_ddFilt*100.0, x.oos12_profitImpactPct
        );
        if (shown >= topN) break;
    }

    if (shown == 0) {
        System.out.println("（没有组合同时满足：总体 agreeN≥" + minAgreeN +
                "、失败月 failAgreeN≥" + minFailAgreeN +
                "、并且 failImpact>0 且 overallImpact>0。你可以降低 minFailAgreeN 或扩大随机组合数量。）");
    }
    System.out.println("=====================================================");
}



    // ======================== agree definition ========================
    /**
     * 方向一致 (agree) 的定义：
     * - Long: bin 3/4（弱多/强多）算 agree；0/1 算 disagree；2 中性
     * - Short: bin 0/1（强空/弱空）算 agree；3/4 算 disagree；2 中性
     * 你后续接投票机：
     * - 小覆盖高 lift：建议 soft vote（加分），别一票否决
     * - 只在 bestBin 且 bestN>=minBinN 时触发硬条件（更稳）
     */
    private static boolean isAgree(Dir dir, int bin) {
        if (dir == Dir.LONG) return (bin >= 3);
        if (dir == Dir.SHORT) return (bin <= 1);
        return false;
    }

    // ======================== 5-bin mapping ========================
    /**
     * strength ∈ [-L, +L] → 5 bins:
     * 0 强空, 1 弱空, 2 中性, 3 弱多, 4 强多
     */
    private static int to5Bin(int strength, int L) {
        if (L <= 0) return 2;
        int tWeak = Math.max(1, (int) Math.ceil(0.2 * L));
        int tStrong = Math.max(tWeak + 1, (int) Math.ceil(0.6 * L));

        if (strength <= -tStrong) return 0;
        if (strength <= -tWeak) return 1;
        if (Math.abs(strength) < tWeak) return 2;
        if (strength < tStrong) return 3;
        return 4;
    }

    private static String binName(int b) {
        switch (b) {
            case 0: return "STRONG_SHORT";
            case 1: return "WEAK_SHORT";
            case 2: return "NEUTRAL";
            case 3: return "WEAK_LONG";
            case 4: return "STRONG_LONG";
            default: return "NA";
        }
    }

    // ======================== scoring helpers ========================
    private static int sum(int[] a) {
        int s = 0;
        for (int x : a) s += x;
        return s;
    }

    private static double meanBin(int[] cnt) {
        int n = sum(cnt);
        if (n <= 0) return 0.0;
        double s = 0.0;
        for (int i = 0; i < cnt.length; i++) s += i * (double) cnt[i];
        return s / n;
    }

    /** Jensen-Shannon divergence on 5-bin distributions (natural log). */
    private static double jsDivergence(int[] winBin, int[] lossBin) {
        double[] p = norm(winBin);
        double[] q = norm(lossBin);
        double[] m = new double[5];
        for (int i = 0; i < 5; i++) m[i] = 0.5 * (p[i] + q[i]);
        return 0.5 * kl(p, m) + 0.5 * kl(q, m);
    }

    private static double[] norm(int[] cnt) {
        double[] p = new double[cnt.length];
        double s = 0;
        for (int x : cnt) s += x;
        if (s <= 0) return p;
        for (int i = 0; i < cnt.length; i++) p[i] = cnt[i] / s;
        return p;
    }

    private static double kl(double[] p, double[] q) {
        double out = 0.0;
        for (int i = 0; i < p.length; i++) {
            double pi = p[i];
            if (pi <= 0) continue;
            double qi = q[i];
            if (qi <= 0) continue;
            out += pi * Math.log(pi / qi);
        }
        return out;
    }

    // ======================== equity replay ========================
    private static double guessInitEquity(List<TradeEvent> events) {
        for (TradeEvent e : events) {
            if (e.equityBefore != null && e.equityBefore > 0) return e.equityBefore;
            if (e.equityAfter != null && e.equityAfter > 0 && e.netU != null) return e.equityAfter - e.netU;
        }
        return 0.0;
    }

    /**
     * 重放权益：
     * - includeMask == null：包含所有交易（base）
     * - includeMask[i] == true：保留该交易（apply factor）
     * - includeMask[i] == false：过滤该交易（factor=1）
     */
    private static double replayEquity(List<TradeEvent> events, double initEq, boolean[] includeMask) {
        double eq = initEq;
        for (int i = 0; i < events.size(); i++) {
            TradeEvent e = events.get(i);
            boolean include = (includeMask == null) || includeMask[i];
            if (include) {
                double f = e.factor;
                if (Double.isFinite(f) && f > 0) eq *= f;
            }
        }
        return eq;
    }

    // ======================== Trade log parsing ========================
    private static List<TradeEvent> parseTradeLog(String path) throws IOException {
        File f = new File(path);
        if (!f.exists()) throw new FileNotFoundException("tradeLog not found: " + path);

        // time -> dir（来自 回测-进场）
        Map<Long, Dir> btDirByTs = new HashMap<>();

        List<TradeEvent> out = new ArrayList<>();

        Long curEntryTs = null;
        Dir curDir = Dir.UNKNOWN;
        Double curEquityBefore = null;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if (line.contains("回测-进场")) {
                    Matcher m = P_BT_ENTRY_DIR.matcher(line);
                    if (m.find()) {
                        long ts = parseTs(m.group(1));
                        Dir d = parseDir(m.group(2));
                        if (d != Dir.UNKNOWN) btDirByTs.put(ts, d);
                    }
                    continue;
                }

                if (line.contains("SRMF-进场")) {
                    Matcher m = P_SRMF_ENTRY.matcher(line);
                    if (m.find()) {
                        curEntryTs = parseTs(m.group(1));
                        curDir = btDirByTs.getOrDefault(curEntryTs, Dir.UNKNOWN);
                        curEquityBefore = parseCapital(line);
                    }
                    continue;
                }

                if (line.contains("SRMF-出场")) {
                    if (curEntryTs == null) continue; // 日志截断：没有对应进场

                    // 净点数
                    Matcher mPts = P_SRMF_EXIT_NET_PTS.matcher(line);
                    if (!mPts.find()) {
                        // 没净点数就不算一笔交易
                        curEntryTs = null; curDir = Dir.UNKNOWN; curEquityBefore = null;
                        continue;
                    }
                    double netPts = Double.parseDouble(mPts.group(2));

                    // 方向（兜底）
                    if (curDir == Dir.UNKNOWN) {
                        Matcher md = P_SRMF_EXIT_DIR.matcher(line);
                        if (md.find()) curDir = parseDir(md.group(1));
                    }

                    // 净U
                    Double netU = null;
                    Matcher mU = P_SRMF_EXIT_NET_U.matcher(line);
                    if (mU.find()) {
                        try { netU = Double.parseDouble(mU.group(1)); } catch (Exception ignored) {}
                    }

                    // 出场资金
                    Double eqAfter = parseCapital(line);

                    TradeEvent e = new TradeEvent();
                    e.entryTsMs = curEntryTs;
                    e.dir = curDir;
                    e.netPoints = netPts;
                    e.netU = netU;
                    e.equityBefore = curEquityBefore;
                    e.equityAfter = eqAfter;

                    // win 判定：优先用净U，其次净点数
                    if (netU != null) e.win = netU > 0;
                    else e.win = netPts > 0;

                    // factor：优先用 equityAfter/equityBefore（贴近 SRMF 曲线）
                    e.factor = computeFactor(e);

                    out.add(e);

                    // reset
                    curEntryTs = null;
                    curDir = Dir.UNKNOWN;
                    curEquityBefore = null;
                }
            }
        }

        return out;
    }

    private static double computeFactor(TradeEvent e) {
        // 1) equity after / before
        if (e.equityBefore != null && e.equityAfter != null && e.equityBefore > 0 && e.equityAfter > 0) {
            double f = e.equityAfter / e.equityBefore;
            if (Double.isFinite(f) && f > 0) return f;
        }
        // 2) 1 + netU/equityBefore
        if (e.netU != null && e.equityBefore != null && e.equityBefore > 0) {
            double f = 1.0 + (e.netU / e.equityBefore);
            if (Double.isFinite(f) && f > 0) return f;
        }
        return 1.0;
    }



    // =========================
    // Fixed-R (no compounding) evaluation helpers
    // =========================
    static final class FixedRStats {
        final int baseN, filtN;
        final double baseWR, filtWR, winLift;
        final double pfBase, pfFilt;
        final double ddBase, ddFilt; // 0~1 fraction
        final double baseFinalEq, filtFinalEq;
        final double profitImpactPct; // filtFinal/baseFinal - 1

        FixedRStats(int baseN, int filtN, double baseWR, double filtWR,
                    double pfBase, double pfFilt, double ddBase, double ddFilt,
                    double baseFinalEq, double filtFinalEq) {
            this.baseN = baseN;
            this.filtN = filtN;
            this.baseWR = baseWR;
            this.filtWR = filtWR;
            this.winLift = (filtWR - baseWR);
            this.pfBase = pfBase;
            this.pfFilt = pfFilt;
            this.ddBase = ddBase;
            this.ddFilt = ddFilt;
            this.baseFinalEq = baseFinalEq;
            this.filtFinalEq = filtFinalEq;
            this.profitImpactPct = baseFinalEq == 0 ? 0.0 : (filtFinalEq / baseFinalEq - 1.0) * 100.0;
        }
    }

    private static FixedRStats computeFixedRStats(List<TradeEvent> events, boolean[] include, long startTsMs, double costPct) {
        // Base = all events in window; Filtered = include==true
        double initEq = 1.0; // arbitrary anchor, additive PnL so anchor cancels in ratios
        double eqBase = initEq;
        double eqFilt = initEq;

        double peakBase = initEq, peakFilt = initEq;
        double maxDdBase = 0.0, maxDdFilt = 0.0;

        double gpBase = 0.0, glBase = 0.0;
        double gpFilt = 0.0, glFilt = 0.0;

        int baseN = 0, baseW = 0;
        int filtN = 0, filtW = 0;

        for (int i = 0; i < events.size(); i++) {
            TradeEvent e = events.get(i);
            if (e == null) continue;
            if (e.entryTsMs < startTsMs) continue;

            Double pnlObj = e.netU;
            double pnl = pnlObj != null ? pnlObj : 0.0;
            double cost = Math.abs(pnl) * Math.max(0.0, costPct);
            double pnlNet = pnl - cost;

            // base
            baseN++;
            if (e.win) baseW++;
            if (pnlNet >= 0) gpBase += pnlNet; else glBase += -pnlNet;
            eqBase += pnlNet;
            if (eqBase > peakBase) peakBase = eqBase;
            double ddB = peakBase <= 0 ? 0.0 : (peakBase - eqBase) / peakBase;
            if (ddB > maxDdBase) maxDdBase = ddB;

            // filtered
            if (include != null && i < include.length && include[i]) {
                filtN++;
                if (e.win) filtW++;
                if (pnlNet >= 0) gpFilt += pnlNet; else glFilt += -pnlNet;
                eqFilt += pnlNet;
                if (eqFilt > peakFilt) peakFilt = eqFilt;
                double ddF = peakFilt <= 0 ? 0.0 : (peakFilt - eqFilt) / peakFilt;
                if (ddF > maxDdFilt) maxDdFilt = ddF;
            }
        }

        double baseWR = baseN == 0 ? 0.0 : (baseW * 1.0 / baseN) * 100.0;
        double filtWR = filtN == 0 ? 0.0 : (filtW * 1.0 / filtN) * 100.0;

        double pfBase = glBase == 0 ? (gpBase > 0 ? 999.0 : 0.0) : (gpBase / glBase);
        double pfFilt = glFilt == 0 ? (gpFilt > 0 ? 999.0 : 0.0) : (gpFilt / glFilt);

        return new FixedRStats(baseN, filtN, baseWR, filtWR, pfBase, pfFilt, maxDdBase, maxDdFilt, eqBase, eqFilt);
    }

    private static long maxEntryTs(List<TradeEvent> events) {
        long max = 0L;
        for (TradeEvent e : events) {
            if (e != null && e.entryTsMs > max) max = e.entryTsMs;
        }
        return max;
    }

    private static long cutoffTsByMonths(long maxTsMs, int monthsBack) {
        if (monthsBack <= 0 || maxTsMs <= 0) return Long.MIN_VALUE;
        ZonedDateTime z = Instant.ofEpochMilli(maxTsMs).atZone(ZoneId.of("Asia/Tokyo"));
        ZonedDateTime cut = z.minusMonths(monthsBack).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        return cut.toInstant().toEpochMilli();
    }

    private static Double parseCapital(String line) {
        Matcher m = P_CAPITAL.matcher(line);
        if (m.find()) {
            try { return Double.parseDouble(m.group(1)); } catch (Exception ignored) {}
        }
        return null;
    }

    private static Dir parseDir(String s) {
        if (s == null) return Dir.UNKNOWN;
        String t = s.trim().toUpperCase(Locale.ROOT);
        if (t.contains("做多") || t.contains("多头") || t.contains("LONG")) return Dir.LONG;
        if (t.contains("做空") || t.contains("空头") || t.contains("SHORT")) return Dir.SHORT;
        return Dir.UNKNOWN;
    }

    private static long parseTs(String raw) {
        if (raw == null) return 0;
        String s = raw.trim();
        if (s.contains(" |")) s = s.split("\\|")[0].trim();

        // 纯数字：毫秒/秒
        if (s.matches("^\\d{10,}$")) {
            long x = Long.parseLong(s);
            if (s.length() <= 10) return x * 1000L;
            return x;
        }

        for (DateTimeFormatter f : DT_FORMATS) {
            try {
                LocalDateTime dt = LocalDateTime.parse(s, f);
                return dt.atZone(TOKYO).toInstant().toEpochMilli();
            } catch (DateTimeParseException ignored) {}
        }

        if (s.length() >= 16) {
            String s2 = s.substring(0, 16);
            try {
                LocalDateTime dt = LocalDateTime.parse(s2, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
                return dt.atZone(TOKYO).toInstant().toEpochMilli();
            } catch (Exception ignored) {}
        }

        throw new IllegalArgumentException("无法解析时间: '" + raw + "'");
    }

    // ======================== Map events to candles ========================
    private static int mapEventsToCandles(List<TradeEvent> events, List<Candle> candles, long barMs) {
        int ok = 0;
        for (TradeEvent e : events) {
            int idx = nearestCandleIdx(candles, e.entryTsMs);
            if (idx < 0) continue;
            long ts = candles.get(idx).ts;
            long delta = ts - e.entryTsMs;
            if (Math.abs(delta) > Math.max(barMs, 60_000L)) continue;
            e.candleIdx = idx;
            e.matchedCandleTs = ts;
            e.deltaMs = delta;
            ok++;
        }
        return ok;
    }

    private static int nearestCandleIdx(List<Candle> candles, long tsMs) {
        int n = candles.size();
        if (n == 0) return -1;
        int lo = 0, hi = n - 1;
        if (tsMs <= candles.get(lo).ts) return lo;
        if (tsMs >= candles.get(hi).ts) return hi;

        int l = 0, r = n;
        while (l < r) {
            int mid = (l + r) >>> 1;
            if (candles.get(mid).ts < tsMs) l = mid + 1;
            else r = mid;
        }
        int idx = l;
        int left = Math.max(0, idx - 1);
        int right = Math.min(n - 1, idx);
        long dl = Math.abs(candles.get(left).ts - tsMs);
        long dr = Math.abs(candles.get(right).ts - tsMs);
        return (dl <= dr) ? left : right;
    }

    // ======================== Utils ========================
    private static String fmtTs(long tsMs) {
        Instant ins = Instant.ofEpochMilli(tsMs);
        return LocalDateTime.ofInstant(ins, TOKYO).toString().replace('T', ' ');
    }

    private static String esc(String s) {
        if (s == null) return "";
        String t = s.replace("\"", "\"\"");
        if (t.contains(",") || t.contains("\n")) return "\"" + t + "\"";
        return t;
    }
}
