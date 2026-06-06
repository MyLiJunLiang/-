
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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PatternIndicatorsMinerMain (v1)
 *
 * 目标：挖掘“2/3/4 根K线的单指标/组合指标 单调规律”对你现有交易样本（SRMF日志）的提升。
 *
 * ✅ 规则（以某个指标的数值序列 v 为例）：
 * - 2根：v[k0] > v[k1] => signal=LONG；v[k0] < v[k1] => signal=SHORT
 * - 3根：v[k0] > v[k1] > v[k2] => LONG；v[k0] < v[k1] < v[k2] => SHORT
 * - 4根：v[k0] > v[k1] > v[k2] > v[k3] => LONG；v[k0] < v[k1] < v[k2] < v[k3] => SHORT
 * 其中 k0 = 交易发生时刻对应的“最新已收盘K线”（沿用你 DiscriminativeIndicatorsMain 的映射口径）。
 *
 * ✅ 分类输出（单指标 & 组合指标）：
 * - bars=2 / 3 / 4
 * - side=ALL(全总: 多空都算，要求 signal==交易方向) / LONG(只挖多) / SHORT(只挖空)
 *
 * ✅ 评价指标（对“过滤后只保留匹配交易”的权益重放曲线）：
 * - winRate
 * - totalReturnPct（= 最终权益 - 1）
 * - maxDDPct（过滤后权益曲线最大回撤）
 * - monthlySharpeAnn（按月收益计算 Sharpe，年化）
 * - posMonthRatio（正收益月占比）
 * - monthStd（按月收益标准差）
 * - score（同一分类内做 z-score 加权综合）
 *
 * 重要说明：
 * - 若你的指标库没有暴露“数值序列”，本程序会尝试用反射调用 cond.values(...)。
 *   如果找不到，会退化为使用 cond.dir(...)（只有 -1/0/+1 的粗粒度），仍可跑通但不如真实数值好。
 *   你后续只要在关键指标里补一个 values(...)，立刻就能按真实 dif/dea/... 挖掘。
 *
 * 运行示例：
 * java eval.PatternIndicatorsMinerMain --tradeLog=./out/trade_log.txt --db=./okx_candles_osc.db --instId=ETH-USDT-SWAP --bar=30m --years=4 --outDir=./out
 *   --topN=10 --topM=30 --randCombos=200000 --comboMinK=2 --comboMaxK=8 --minN=200 --eps=1e-9
 */
public class PatternIndicatorsMinerMain {

    // -------------------- CLI (本类自解析：支持 --k=v) --------------------
    private static final String ARG_TRADE_LOG   = "--tradeLog=";
    private static final String ARG_TOPN        = "--topN=";
    private static final String ARG_TOPM        = "--topM=";          // 组合候选池：先选 topM 单指标（按 ALL 分类 score）
    private static final String ARG_RAND_COMBOS = "--randCombos=";    // 组合随机采样数量（每个 bars 一次）
    private static final String ARG_COMBO_MIN_K = "--comboMinK=";     // 组合最小指标数（默认 2）
    private static final String ARG_COMBO_MAX_K = "--comboMaxK=";     // 组合最大指标数（默认 8）
    private static final String ARG_COMBO_VOTE_RATIO = "--comboVoteRatio="; // 组合投票阈值(0..1)，默认0.6 => 需要票数=ceil(k*ratio)；设为1.0等价AND
    private static final String ARG_COMBO_MIN_VOTES  = "--comboMinVotes=";  // 组合投票最少票数(>0优先)，例如3表示>=3票就算命中
    private static final String ARG_MIN_N       = "--minN=";          // 最小样本数（过滤后交易数）
    private static final String ARG_EPS         = "--eps=";           // 比较容忍（避免等值抖动）
    private static final String ARG_BARS        = "--bars=";          // 例如 2,3,4 或 2
    private static final String ARG_SEED        = "--seed=";          // 随机种子（组合采样可复现）
    private static final String ARG_SAMPLE      = "--sample=";        // trades|bars（默认 trades）
    private static final String ARG_HOLD_BARS   = "--holdBars=";      // bars 模式：持有 N 根K 后平仓（默认1）
    private static final String ARG_FEE_BPS     = "--feeBps=";        // bars 模式：每笔往返手续费(bps)，6=0.06%（默认0）

    // -------------------- Parse patterns (复用你旧Main口径) --------------------
    private static final Pattern P_SRMF_ENTRY      = Pattern.compile("SRMF-进场.*?时间=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_BT_ENTRY_DIR    = Pattern.compile("回测-进场.*?时间=([^|]+).*?方向=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_DIR   = Pattern.compile("SRMF-出场.*?方向=([^|]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_NET_PTS = Pattern.compile("SRMF-出场.*?(净=|净点=|净点数=|PnL=)([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_SRMF_EXIT_NET_U   = Pattern.compile("净=\\s*[-+]?\\d+(?:\\.\\d+)?点\\(([-+]?\\d+(?:\\.\\d+)?)U\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_CAPITAL          = Pattern.compile("资金=([-+]?\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);

    private static final ZoneId TOKYO = ZoneId.of("Asia/Tokyo");
    private static final DateTimeFormatter[] DT_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    enum Dir { LONG, SHORT, UNKNOWN }

    enum SampleMode { TRADES, BARS }

    static final class TradeEvent {
        long entryTsMs;
        Dir dir;
        double netPoints;
        Double netU;
        Double equityBefore;
        Double equityAfter;
        boolean win;
        double factor = 1.0;

        int candleIdx = -1;
        long matchedCandleTs = 0;
        long deltaMs = 0;
    }

    static final class Stats {
        int n;
        int win;
        double winRate;
        double totalReturnPct;
        double maxDDPct;
        double monthlySharpeAnn;
        double posMonthRatio;
        double monthStd;
        int months;
        double score; // after z-score
    }

    static final class ResultRow {
        String type; // SINGLE|COMBO
        int bars;
        String side; // ALL|LONG|SHORT
        String id;   // indicator id or combo id
        String group;
        int k;       // 1 for single, >1 for combo
        int[] inds;  // indicator indices (for SINGLE: length=1)
        Stats st;
    }

    // -------------------- main --------------------
    public static void main(String[] args) throws Exception {
        String tradeLog = null;
        int topN = 50;
        int topM = 50;
        int randCombos = 200000;
        int comboMinK = 2;
        int comboMaxK = 3;
        double comboVoteRatio = 0.40;
        int comboMinVotes = -1;
        int minN = 2000;
        double eps = 1e-9;
        long seed = 1L;
        SampleMode sample = SampleMode.BARS;//TRADES=按log文件挖掘，BARS=按四年k线挖掘
        int holdBars = 240;//5根k线之后出场
        double feeBps = 0.002;//手续费0.0002
        Set<Integer> barsSet = new LinkedHashSet<>(Arrays.asList(2,3,4));

        List<String> pass = new ArrayList<>();
        for (String a : args) {
            if (a.startsWith(ARG_TRADE_LOG)) tradeLog = a.substring(ARG_TRADE_LOG.length());
            else if (a.startsWith(ARG_TOPN)) topN = Integer.parseInt(a.substring(ARG_TOPN.length()));
            else if (a.startsWith(ARG_TOPM)) topM = Integer.parseInt(a.substring(ARG_TOPM.length()));
            else if (a.startsWith(ARG_RAND_COMBOS)) randCombos = Integer.parseInt(a.substring(ARG_RAND_COMBOS.length()));
            else if (a.startsWith(ARG_COMBO_MIN_K)) comboMinK = Integer.parseInt(a.substring(ARG_COMBO_MIN_K.length()));
            else if (a.startsWith(ARG_COMBO_MAX_K)) comboMaxK = Integer.parseInt(a.substring(ARG_COMBO_MAX_K.length()));
            else if (a.startsWith(ARG_COMBO_VOTE_RATIO)) comboVoteRatio = Double.parseDouble(a.substring(ARG_COMBO_VOTE_RATIO.length()));
            else if (a.startsWith(ARG_COMBO_MIN_VOTES)) comboMinVotes = Integer.parseInt(a.substring(ARG_COMBO_MIN_VOTES.length()));
            else if (a.startsWith(ARG_MIN_N)) minN = Integer.parseInt(a.substring(ARG_MIN_N.length()));
            else if (a.startsWith(ARG_EPS)) eps = Double.parseDouble(a.substring(ARG_EPS.length()));
            else if (a.startsWith(ARG_SEED)) seed = Long.parseLong(a.substring(ARG_SEED.length()));
            else if (a.startsWith(ARG_SAMPLE)) {
                String v = a.substring(ARG_SAMPLE.length()).trim().toLowerCase(Locale.ROOT);
                if (v.equals("trades") || v.equals("trade") || v.equals("log")) sample = SampleMode.TRADES;
                else if (v.equals("bars") || v.equals("kline") || v.equals("all")) sample = SampleMode.BARS;
                else throw new IllegalArgumentException("unknown --sample=: " + v + " (use trades|bars)");
            }
            else if (a.startsWith(ARG_HOLD_BARS)) holdBars = Integer.parseInt(a.substring(ARG_HOLD_BARS.length()));
            else if (a.startsWith(ARG_FEE_BPS)) feeBps = Double.parseDouble(a.substring(ARG_FEE_BPS.length()));
            else if (a.startsWith(ARG_BARS)) {
                barsSet.clear();
                String v = a.substring(ARG_BARS.length()).trim();
                for (String s : v.split(",")) {
                    s = s.trim();
                    if (s.isEmpty()) continue;
                    barsSet.add(Integer.parseInt(s));
                }
            } else pass.add(a);
        }

        // combo vote params sanity
        if (!Double.isFinite(comboVoteRatio)) comboVoteRatio = 1.0;
        if (comboVoteRatio < 0.0) comboVoteRatio = 0.0;
        if (comboVoteRatio > 1.0) comboVoteRatio = 1.0;
        if (comboVoteRatio < 0.999 && comboMaxK > 5) {
            System.out.println("⚠️ vote mode 建议 comboMaxK<=5（当前=" + comboMaxK + "），已自动截断为5；如需AND可设 --comboVoteRatio=1.0");
            comboMaxK = 5;
        }
        if (comboMinK < 1) comboMinK = 1;
        if (comboMaxK < comboMinK) comboMaxK = comboMinK;

        Args cfg = Args.parse(pass.toArray(new String[0]));
        new File(cfg.outDir).mkdirs();
        if (sample == SampleMode.TRADES) {
            if (tradeLog == null || tradeLog.trim().isEmpty()) tradeLog = cfg.outDir + "/trade_log.txt";
        }

        // 外部特征 provider（复用你旧Main模式）
        String fm = (cfg.featureMode == null ? "sqlite" : cfg.featureMode.trim().toLowerCase(Locale.ROOT));
        if ("none".equals(fm) || "off".equals(fm)) {
            ExternalFeatureProviderHolder.set(new NoopExternalFeatureProvider());
            System.out.println("[ExternalFeatures] disabled (--featureMode=none)");
        } else if ("csv".equals(fm)) {
            ExternalFeatureProviderHolder.set(new CsvExternalFeatureProvider(cfg.featureDir));
            System.out.println("[ExternalFeatures] csv enabled: dir=" + cfg.featureDir);
        } else {
            String fdb = (cfg.featureDb == null || cfg.featureDb.isEmpty()) ? cfg.db : cfg.featureDb;
            ExternalFeatureProviderHolder.set(new SqliteExternalFeatureProvider(fdb, cfg.instId, cfg.bar));
            System.out.println("[ExternalFeatures] sqlite enabled: db=" + fdb + " table=okx_ext_features");
        }

        
// 1) 读取样本
if (holdBars < 1) holdBars = 1;
if (feeBps < 0) feeBps = 0.0;

List<TradeEvent> events;
long minTs = 0;
long maxTs = 0;

if (sample == SampleMode.TRADES) {
    events = parseTradeLog(tradeLog);
    if (events.isEmpty()) throw new IllegalStateException("tradeLog 解析不到任何交易: " + tradeLog);
    events.sort(Comparator.comparingLong(a -> a.entryTsMs));

    minTs = events.get(0).entryTsMs;
    maxTs = events.get(events.size() - 1).entryTsMs;
} else {
    events = new ArrayList<>();
}

System.out.println("========================================================");
        System.out.println("PatternIndicatorsMinerMain v1");
        System.out.println("sample=" + sample + (sample == SampleMode.BARS ? (" | holdBars=" + holdBars + " | feeBps=" + feeBps) : ""));
        System.out.println("tradeLog=" + (tradeLog == null ? "(none)" : tradeLog));
        System.out.println("events=" + (sample == SampleMode.TRADES ? events.size() : "(bars模式-构造后显示)"));
        System.out.println("bars=" + barsSet + " | topN=" + topN + " | topM=" + topM + " | randCombos=" + randCombos + " | comboK=" + comboMinK + "~" + comboMaxK + " | seed=" + seed);
        System.out.println("minN=" + minN + " | eps=" + eps);
        System.out.println("db=" + cfg.db + " | instId=" + cfg.instId + " | bar=" + cfg.bar + " | years=" + cfg.years);
        System.out.println("outDir=" + cfg.outDir);
        System.out.println("========================================================");

        
// 2) 加载 K 线
long now = System.currentTimeMillis();
long barMs = OkxKlineUtils.barMs(cfg.bar);

long start;
long end;

if (sample == SampleMode.TRADES) {
    // 只拉必要区间（围绕 trade_log 的时间范围）
    start = Math.max(0, minTs - barMs * 400);
    end = maxTs + barMs * 80;
} else {
    // bars 模式：拉近 N 年全量区间（并额外留出 warmup）
    long yearsMs = (long) cfg.years * 365L * 24L * 3600L * 1000L;
    end = now;
    start = Math.max(0, end - yearsMs - barMs * 800);
}

OkxCandleCacheEthOsc cache = OkxCandleCacheEthOsc.get(cfg.db, cfg.years);
        List<Candle> candles = cache.getOrFetchRange(cfg.instId, cfg.bar, start, end, now, barMs, cfg.safeCloseMs);
        if (candles == null || candles.size() < 200) throw new IllegalStateException("K线数量太少: " + (candles==null?0:candles.size()));

        
// 3) 构造 / 映射样本到 candleIdx（k0）
if (sample == SampleMode.TRADES) {
    int mapped = mapEventsToCandles(events, candles, barMs);
    System.out.println("映射到K线成功: " + mapped + "/" + events.size());
    List<TradeEvent> mappedEvents = new ArrayList<>();
    for (TradeEvent e : events) if (e.candleIdx >= 0) mappedEvents.add(e);
    if (mappedEvents.size() < Math.max(20, events.size() / 10)) {
        System.out.println("⚠️ 映射成功率偏低，请确认日志时间=K线openTs（或同口径）。");
    }
    events = mappedEvents;
} else {
    events = buildBarEventsFromCandles(candles, holdBars, feeBps);
    System.out.println("bars样本 events=" + events.size());
}

// 4) 指标库
        List<PoolCondition> conds = ConditionFactory.defaultConditionsWithInst(cfg.instId, cfg.bar);
        System.out.println("指标库数量=" + conds.size());
        // group 统计（方便确认 OKX 指标已入池）
        {
            Map<String, Integer> groupCnt = new TreeMap<>();
            for (PoolCondition c : conds) groupCnt.put(c.group(), groupCnt.getOrDefault(c.group(), 0) + 1);
            System.out.println("指标分组统计=" + groupCnt);
        }

        // 组合参数保护
        if (comboMinK < 2) comboMinK = 2;
        if (comboMaxK < comboMinK) comboMaxK = comboMinK;

        // 5) 每个指标构造“数值序列”values（尽力获取；失败则退化用 dir）
        final int nInd = conds.size();
        final int nBars = candles.size();
        double[][] values = new double[nInd][];
        String[] id = new String[nInd];
        String[] group = new String[nInd];
        for (int i = 0; i < nInd; i++) {
            PoolCondition c = conds.get(i);
            id[i] = c.id();
            group[i] = c.group();
            values[i] = buildValueSeries(c, candles);
        }

        // 6) 事件数组化（便于 BitSet 统计）
        final int nE = events.size();
        Dir[] eDir = new Dir[nE];
        boolean[] eWin = new boolean[nE];
        double[] eFactor = new double[nE];
        YearMonth[] eYm = new YearMonth[nE];
        for (int i = 0; i < nE; i++) {
            TradeEvent e = events.get(i);
            eDir[i] = e.dir;
            eWin[i] = e.win;
            eFactor[i] = safeFactor(e.factor);
            eYm[i] = YearMonth.from(Instant.ofEpochMilli(e.entryTsMs).atZone(TOKYO).toLocalDate());
        }
        BitSet tradeLong = new BitSet(nE);
        BitSet tradeShort = new BitSet(nE);
        for (int i = 0; i < nE; i++) {
            if (eDir[i] == Dir.LONG) tradeLong.set(i);
            else if (eDir[i] == Dir.SHORT) tradeShort.set(i);
        }

        // 7) 预计算：每个 bars 下，每个指标在“events”上的信号 bitset（LONG / SHORT）
        Map<Integer, BitSet[]> longSigByBars = new HashMap<>();
        Map<Integer, BitSet[]> shortSigByBars = new HashMap<>();

        for (int bars : barsSet) {
            BitSet[] longBits = new BitSet[nInd];
            BitSet[] shortBits = new BitSet[nInd];
            for (int i = 0; i < nInd; i++) {
                longBits[i] = new BitSet(nE);
                shortBits[i] = new BitSet(nE);
            }

            for (int ei = 0; ei < nE; ei++) {
                int k0 = events.get(ei).candleIdx;
                for (int ind = 0; ind < nInd; ind++) {
                    int sig = signalAt(values[ind], k0, bars, eps);
                    if (sig > 0) longBits[ind].set(ei);
                    else if (sig < 0) shortBits[ind].set(ei);
                }
            }

            longSigByBars.put(bars, longBits);
            shortSigByBars.put(bars, shortBits);
            System.out.println("signals prepared: bars=" + bars + " (bitsets)");
        }

        // 8) 单指标：跑出 2/3/4 × ALL/LONG/SHORT 的全量结果
        List<ResultRow> allSingles = new ArrayList<>(nInd * barsSet.size() * 3);
        for (int bars : barsSet) {
            BitSet[] longBits = longSigByBars.get(bars);
            BitSet[] shortBits = shortSigByBars.get(bars);

            for (int ind = 0; ind < nInd; ind++) {
                // ALL
                ResultRow rAll = buildRowSingle("SINGLE", bars, "ALL", id[ind], group[ind], 1,
                        new int[]{ind},
                        includeBitsAll(longBits[ind], shortBits[ind], tradeLong, tradeShort),
                        eWin, eFactor, eYm, minN);
                if (rAll != null) allSingles.add(rAll);

                // LONG
                ResultRow rL = buildRowSingle("SINGLE", bars, "LONG", id[ind], group[ind], 1,
                        new int[]{ind},
                        includeBitsSide(longBits[ind], tradeLong),
                        eWin, eFactor, eYm, minN);
                if (rL != null) allSingles.add(rL);

                // SHORT
                ResultRow rS = buildRowSingle("SINGLE", bars, "SHORT", id[ind], group[ind], 1,
                        new int[]{ind},
                        includeBitsSide(shortBits[ind], tradeShort),
                        eWin, eFactor, eYm, minN);
                if (rS != null) allSingles.add(rS);
            }
        }

        // 9) 给单指标每个分类打分 & 输出 TopN
        scoreAndReport(cfg.outDir + "/pattern_rank_singles.csv", allSingles, topN);

        // 10) 组合：每个 bars 先从单指标 ALL 分类里选 topM，再随机采样组合（2..8）
        List<ResultRow> allCombos = new ArrayList<>();
        Random rnd = new Random(seed);

        for (int bars : barsSet) {
            List<Integer> pool = selectTopMFromSingles(allSingles, bars, topM);
            if (pool.size() < 2) {
                System.out.println("⚠️ bars=" + bars + " pool too small, skip combos");
                continue;
            }
            System.out.println("bars=" + bars + " combo poolN=" + pool.size());

            BitSet[] longBits = longSigByBars.get(bars);
            BitSet[] shortBits = shortSigByBars.get(bars);

            
HashSet<String> seen = new HashSet<>(Math.max(1024, randCombos * 2));
int attempts = Math.max(10000, randCombos * 30);

int generated = 0;
int tries = 0;
while (generated < randCombos && tries < attempts) {
    tries++;

    int k = comboMinK + rnd.nextInt(Math.max(1, comboMaxK - comboMinK + 1));
    if (k > pool.size()) k = pool.size();

    int[] pick = pickDistinctFromPool(pool, k, rnd);
    Arrays.sort(pick);

    String key = Arrays.toString(pick);
    if (!seen.add(key)) continue;

    String comboId = buildComboId(pick, id);
    String comboGroup = "COMBO_K" + pick.length;

    // consensus by VOTE（默认 ratio=0.6 => >=ceil(k*0.6)；设 --comboVoteRatio=1.0 等价 AND）
    int needVotes = calcNeedVotes(pick.length, comboMinVotes, comboVoteRatio);
    BitSet longC = consensusByVotes(longBits, pick, needVotes);
    BitSet shortC = consensusByVotes(shortBits, pick, needVotes);

    // resolve conflicts (both long & short) -> drop
    BitSet conflict = (BitSet) longC.clone();
    conflict.and(shortC);
    if (!conflict.isEmpty()) {
        longC.andNot(conflict);
        shortC.andNot(conflict);
    }

    // ALL/LONG/SHORT 三类
    ResultRow rAll = buildRowSingle("COMBO", bars, "ALL", comboId, comboGroup, pick.length,
            pick,
            includeBitsAll(longC, shortC, tradeLong, tradeShort),
            eWin, eFactor, eYm, minN);
    if (rAll != null) allCombos.add(rAll);

    ResultRow rL = buildRowSingle("COMBO", bars, "LONG", comboId, comboGroup, pick.length,
            pick,
            includeBitsSide(longC, tradeLong),
            eWin, eFactor, eYm, minN);
    if (rL != null) allCombos.add(rL);

    ResultRow rS = buildRowSingle("COMBO", bars, "SHORT", comboId, comboGroup, pick.length,
            pick,
            includeBitsSide(shortC, tradeShort),
            eWin, eFactor, eYm, minN);
    if (rS != null) allCombos.add(rS);

    generated++;
}

            System.out.println("bars=" + bars + " combos generated unique=" + seen.size() + " / target=" + randCombos);
        }

        // 11) 组合评分 & 输出 TopN
        scoreAndReport(cfg.outDir + "/pattern_rank_combos.csv", allCombos, topN);

        System.out.println("\n✅ 输出完成：");
        System.out.println(" - " + cfg.outDir + "/pattern_rank_singles.csv");
        System.out.println(" - " + cfg.outDir + "/pattern_rank_combos.csv");
    }

    // -------------------- build value series (try reflection, else fallback to dir) --------------------
    private static double[] buildValueSeries(PoolCondition cond, List<Candle> candles) {
        final int n = candles.size();

        // 1) try cond.values(List<Candle>, int, int) => double[]
        String[] methodNames = new String[]{"values", "vals", "valueSeries", "series", "value"};
        for (String mn : methodNames) {
            try {
                Method m = cond.getClass().getMethod(mn, List.class, int.class, int.class);
                Object r = m.invoke(cond, candles, 0, n);
                if (r instanceof double[]) {
                    double[] v = (double[]) r;
                    if (v.length == n) return v;
                }
            } catch (Throwable ignored) {}
        }

        // 2) fallback: dir -> double
        try {
            int[] d = cond.dir(candles, 0, n);
            double[] v = new double[n];
            if (d != null) {
                for (int i = 0; i < Math.min(n, d.length); i++) v[i] = d[i];
            }
            return v;
        } catch (Throwable ex) {
            double[] v = new double[n];
            Arrays.fill(v, 0.0);
            return v;
        }
    }

    // -------------------- signal logic --------------------
    // return +1 long, -1 short, 0 none
    private static int signalAt(double[] v, int k0, int bars, double eps) {
        if (v == null) return 0;
        if (k0 < 0) return 0;
        int need = bars - 1;
        if (k0 - need < 0) return 0;

        // strict monotonic
        boolean inc = true;
        boolean dec = true;
        for (int i = 0; i < need; i++) {
            double a = v[k0 - i];
            double b = v[k0 - i - 1];
            if (!Double.isFinite(a) || !Double.isFinite(b)) return 0;

            if (!(a > b + eps)) inc = false;
            if (!(a + eps < b)) dec = false;
            if (!inc && !dec) return 0;
        }
        if (inc) return +1;
        if (dec) return -1;
        return 0;
    }

    // -------------------- include mask builders (BitSet) --------------------
    private static BitSet includeBitsAll(BitSet longSig, BitSet shortSig, BitSet tradeLong, BitSet tradeShort) {
        BitSet a = (BitSet) longSig.clone();
        a.and(tradeLong);
        BitSet b = (BitSet) shortSig.clone();
        b.and(tradeShort);
        a.or(b);
        return a;
    }

    private static BitSet includeBitsSide(BitSet sig, BitSet tradeSide) {
        BitSet a = (BitSet) sig.clone();
        a.and(tradeSide);
        return a;
    }

    // -------------------- build row & stats --------------------
    private static ResultRow buildRowSingle(String type, int bars, String side, String id, String group, int k,
                                            int[] inds,
                                            BitSet includeBits,
                                            boolean[] eWin, double[] eFactor, YearMonth[] eYm,
                                            int minN) {

        Stats st = calcStats(includeBits, eWin, eFactor, eYm);
        if (st.n < minN) return null;

        ResultRow r = new ResultRow();
        r.type = type;
        r.bars = bars;
        r.side = side;
        r.id = id;
        r.group = group;
        r.k = k;
        r.inds = inds;
        r.st = st;
        return r;
    }

    private static Stats calcStats(BitSet includeBits, boolean[] win, double[] factor, YearMonth[] ym) {
        Stats st = new Stats();
        double eq = 1.0;
        double peak = 1.0;
        double maxDD = 0.0;

        HashMap<YearMonth, Double> monthFactor = new HashMap<>();

        int idx = includeBits.nextSetBit(0);
        while (idx >= 0) {
            st.n++;
            if (win[idx]) st.win++;

            double f = safeFactor(factor[idx]);
            eq *= f;
            if (eq > peak) peak = eq;
            double dd = (peak - eq) / peak;
            if (dd > maxDD) maxDD = dd;

            YearMonth m = ym[idx];
            monthFactor.put(m, monthFactor.getOrDefault(m, 1.0) * f);

            idx = includeBits.nextSetBit(idx + 1);
        }

        st.winRate = st.n == 0 ? 0.0 : (st.win * 1.0 / st.n);
        st.totalReturnPct = (eq - 1.0) * 100.0;
        st.maxDDPct = maxDD * 100.0;

        // monthly stats
        ArrayList<Double> mrets = new ArrayList<>();
        ArrayList<YearMonth> keys = new ArrayList<>(monthFactor.keySet());
        keys.sort(Comparator.naturalOrder());
        int pos = 0;
        for (YearMonth k : keys) {
            double mf = monthFactor.getOrDefault(k, 1.0);
            double mr = mf - 1.0;
            mrets.add(mr);
            if (mr > 0) pos++;
        }
        st.months = mrets.size();
        st.posMonthRatio = st.months == 0 ? 0.0 : (pos * 1.0 / st.months);

        if (st.months >= 2) {
            double mean = mean(mrets);
            double sd = std(mrets, mean);
            st.monthStd = sd;
            if (sd > 1e-12) st.monthlySharpeAnn = (mean / sd) * Math.sqrt(12.0);
            else st.monthlySharpeAnn = 0.0;
        } else {
            st.monthStd = 0.0;
            st.monthlySharpeAnn = 0.0;
        }

        return st;
    }

    // -------------------- scoring, reporting, csv --------------------
    private static void scoreAndReport(String outCsv, List<ResultRow> rows, int topN) throws IOException {
        // group by (type,bars,side) separately score with z-scores
        Map<String, List<ResultRow>> mp = new LinkedHashMap<>();
        for (ResultRow r : rows) {
            String key = r.type + "|bars=" + r.bars + "|side=" + r.side;
            mp.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }

        // z-score per group
        for (Map.Entry<String, List<ResultRow>> en : mp.entrySet()) {
            List<ResultRow> g = en.getValue();
            if (g.isEmpty()) continue;

            // collect metrics
            double[] winR = new double[g.size()];
            double[] ret  = new double[g.size()];
            double[] dd   = new double[g.size()];
            double[] sh   = new double[g.size()];
            double[] pm   = new double[g.size()];

            for (int i = 0; i < g.size(); i++) {
                Stats s = g.get(i).st;
                winR[i] = s.winRate;
                ret[i]  = s.totalReturnPct;
                dd[i]   = s.maxDDPct;
                sh[i]   = s.monthlySharpeAnn;
                pm[i]   = s.posMonthRatio;
            }

            Z zWin = zOf(winR);
            Z zRet = zOf(ret);
            Z zDD  = zOf(dd);
            Z zSh  = zOf(sh);
            Z zPm  = zOf(pm);

            for (int i = 0; i < g.size(); i++) {
                Stats s = g.get(i).st;
                double zw = zWin.z(winR[i]);
                double zr = zRet.z(ret[i]);
                double zdd = zDD.z(dd[i]);
                double zsh = zSh.z(sh[i]);
                double zpm = zPm.z(pm[i]);

                // 综合分：偏好“利润 + 稳定 + 胜率”，惩罚回撤（你要的那四项都进来）
                // 权重可按你的偏好改：利润 0.40，回撤 -0.35，胜率 0.15，夏普 0.20，正月占比 0.10
                s.score = 0.40 * zr - 0.35 * zdd + 0.15 * zw + 0.20 * zsh + 0.10 * zpm;
            }

            // print topN
            g.sort((a,b) -> Double.compare(b.st.score, a.st.score));
            System.out.println("\n==================== " + en.getKey() + " Top" + topN + " ====================");
            for (int i = 0; i < Math.min(topN, g.size()); i++) {
                ResultRow r = g.get(i);
                Stats s = r.st;
                System.out.printf(Locale.US,
                        "%2d) %-10s k=%d  score=%+.4f  WR=%.2f%%  Ret=%+.2f%%  DD=%.2f%%  SharpeM(ann)=%.2f  PosMonth=%.1f%%  N=%d  id=%s%n",
                        i+1, r.type, r.k, s.score, s.winRate*100.0, s.totalReturnPct, s.maxDDPct, s.monthlySharpeAnn, s.posMonthRatio*100.0, s.n, r.id
                );
            }
            System.out.println("=====================================================");
        }

        // write csv (all rows with score)
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outCsv), StandardCharsets.UTF_8))) {
            pw.println("type,bars,side,rank,score,id,group,k,inds,n,win,winRate,totalReturnPct,maxDDPct,months,posMonthRatio,monthStd,monthlySharpeAnn");
            // sort overall for csv stable
            List<ResultRow> all = new ArrayList<>(rows);
            all.sort(Comparator
                    .comparing((ResultRow r) -> r.type)
                    .thenComparingInt(r -> r.bars)
                    .thenComparing(r -> r.side)
                    .thenComparing((ResultRow r) -> -r.st.score)
            );

            String curKey = "";
            int rank = 0;
            for (ResultRow r : all) {
                String key = r.type + "|" + r.bars + "|" + r.side;
                if (!key.equals(curKey)) { curKey = key; rank = 0; }
                rank++;

                Stats s = r.st;
                pw.printf(Locale.US,
                        "%s,%d,%s,%d,%.6f,%s,%s,%d,%s,%d,%d,%.6f,%.6f,%.6f,%d,%.6f,%.6f,%.6f%n",
                        esc(r.type), r.bars, esc(r.side),
                        rank,
                        s.score,
                        esc(r.id),
                        esc(r.group),
                        r.k,
                        esc(joinInts(r.inds, ";")),
                        s.n,
                        s.win,
                        s.winRate,
                        s.totalReturnPct,
                        s.maxDDPct,
                        s.months,
                        s.posMonthRatio,
                        s.monthStd,
                        s.monthlySharpeAnn
                );
            }
        }
    }

    private static List<Integer> selectTopMFromSingles(List<ResultRow> singles, int bars, int topM) {
        ArrayList<ResultRow> tmp = new ArrayList<>();
        for (ResultRow r : singles) {
            if (!"SINGLE".equals(r.type)) continue;
            if (r.bars != bars) continue;
            if (!"ALL".equals(r.side)) continue; // 用“全总”挑池子（最通用）
            if (r.inds == null || r.inds.length != 1) continue;
            tmp.add(r);
        }
        tmp.sort((a,b) -> Double.compare(b.st.score, a.st.score));

        LinkedHashSet<Integer> uniq = new LinkedHashSet<>();
        for (ResultRow r : tmp) {
            uniq.add(r.inds[0]);
            if (uniq.size() >= topM) break;
        }
        return new ArrayList<>(uniq);
    }

    // -------------------- helpers: combo id, pool pick --------------------
    private static String buildComboId(int[] idx, String[] id) {
        StringBuilder sb = new StringBuilder();
        sb.append("COMBO").append(idx.length).append(":");
        for (int i = 0; i < idx.length; i++) {
            if (i > 0) sb.append("+");
            sb.append(id[idx[i]]);
        }
        return sb.toString();
    }

    private static int[] pickDistinctFromPool(List<Integer> pool, int k, Random rnd) {
        int n = pool.size();
        boolean[] used = new boolean[n];
        int[] out = new int[k];
        int filled = 0;
        while (filled < k) {
            int p = rnd.nextInt(n);
            if (used[p]) continue;
            used[p] = true;
            out[filled++] = pool.get(p);
        }
        return out;
    }

    // -------------------- z-score helper --------------------
    static final class Z {
        final double mean;
        final double std;
        Z(double mean, double std) { this.mean = mean; this.std = std; }
        double z(double x) {
            if (!Double.isFinite(x)) return 0.0;
            if (std <= 1e-12) return 0.0;
            return (x - mean) / std;
        }
    }

    private static Z zOf(double[] a) {
        if (a == null || a.length == 0) return new Z(0, 0);
        double m = 0;
        int n = 0;
        for (double x : a) {
            if (!Double.isFinite(x)) continue;
            m += x;
            n++;
        }
        if (n == 0) return new Z(0, 0);
        m /= n;
        double v = 0;
        for (double x : a) {
            if (!Double.isFinite(x)) continue;
            double d = x - m;
            v += d * d;
        }
        double sd = Math.sqrt(v / Math.max(1, n - 1));
        return new Z(m, sd);
    }

    private static double mean(List<Double> a) {
        double s = 0;
        for (double x : a) s += x;
        return a.isEmpty() ? 0.0 : (s / a.size());
    }

    private static double std(List<Double> a, double mean) {
        if (a.size() < 2) return 0.0;
        double v = 0;
        for (double x : a) {
            double d = x - mean;
            v += d * d;
        }
        return Math.sqrt(v / (a.size() - 1));
    }

    // -------------------- Trade log parsing (copy from your old main) --------------------
    private static List<TradeEvent> parseTradeLog(String path) throws IOException {
        File f = new File(path);
        if (!f.exists()) throw new FileNotFoundException("tradeLog not found: " + path);

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
                    if (curEntryTs == null) continue;

                    Matcher mPts = P_SRMF_EXIT_NET_PTS.matcher(line);
                    if (!mPts.find()) {
                        curEntryTs = null; curDir = Dir.UNKNOWN; curEquityBefore = null;
                        continue;
                    }
                    double netPts = Double.parseDouble(mPts.group(2));

                    if (curDir == Dir.UNKNOWN) {
                        Matcher md = P_SRMF_EXIT_DIR.matcher(line);
                        if (md.find()) curDir = parseDir(md.group(1));
                    }

                    Double netU = null;
                    Matcher mU = P_SRMF_EXIT_NET_U.matcher(line);
                    if (mU.find()) {
                        try { netU = Double.parseDouble(mU.group(1)); } catch (Exception ignored) {}
                    }

                    Double eqAfter = parseCapital(line);

                    TradeEvent e = new TradeEvent();
                    e.entryTsMs = curEntryTs;
                    e.dir = curDir;
                    e.netPoints = netPts;
                    e.netU = netU;
                    e.equityBefore = curEquityBefore;
                    e.equityAfter = eqAfter;

                    if (netU != null) e.win = netU > 0;
                    else e.win = netPts > 0;

                    e.factor = computeFactor(e);
                    out.add(e);

                    curEntryTs = null; curDir = Dir.UNKNOWN; curEquityBefore = null;
                }
            }
        }
        return out;
    }

    private static double computeFactor(TradeEvent e) {
        if (e.equityBefore != null && e.equityAfter != null && e.equityBefore > 0 && e.equityAfter > 0) {
            double f = e.equityAfter / e.equityBefore;
            if (Double.isFinite(f) && f > 0) return f;
        }
        if (e.netU != null && e.equityBefore != null && e.equityBefore > 0) {
            double f = 1.0 + (e.netU / e.equityBefore);
            if (Double.isFinite(f) && f > 0) return f;
        }
        return 1.0;
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

    // -------------------- Map events to candles --------------------
    
// -------------------- Build events from candles (bars mode) --------------------
private static List<TradeEvent> buildBarEventsFromCandles(List<Candle> candles, int holdBars, double feeBpsRoundTrip) {
    List<TradeEvent> out = new ArrayList<>();
    if (candles == null || candles.isEmpty()) return out;
    if (holdBars < 1) holdBars = 1;

    // bps -> pct
    double fee = Math.max(0.0, feeBpsRoundTrip) / 10000.0;

    int n = candles.size();
    // 约定：
    // - k0 是“最新已收盘”K线：candles[i]
    // - 入场发生在下一根K的开盘：entry = candles[i+1].o（对应 entryTsMs=candles[i+1].ts）
    // - 平仓用 i+holdBars 的 close：exit = candles[i+holdBars].c
    for (int i = 0; i + holdBars < n - 1; i++) {
        Candle k0 = candles.get(i);
        Candle kEntry = candles.get(i + 1);
        Candle kExit = candles.get(i + holdBars);

        double entry = kEntry.o;
        double exit = kExit.c;

        if (!Double.isFinite(entry) || entry <= 0) continue;
        if (!Double.isFinite(exit)  || exit  <= 0) continue;

        long entryTs = kEntry.ts;

        // Return (simple) and factor (must be >0)
        double rLong  = (exit - entry) / entry - fee;
        double rShort = (entry - exit) / entry - fee;

        double fLong  = Math.max(1e-9, 1.0 + rLong);
        double fShort = Math.max(1e-9, 1.0 + rShort);

        // LONG event
        TradeEvent eL = new TradeEvent();
        eL.entryTsMs = entryTs;
        eL.dir = Dir.LONG;
        eL.win = rLong > 0;
        eL.factor = fLong;
        eL.candleIdx = i; // k0 index
        eL.matchedCandleTs = k0.ts;
        eL.deltaMs = 0;
        out.add(eL);

        // SHORT event
        TradeEvent eS = new TradeEvent();
        eS.entryTsMs = entryTs;
        eS.dir = Dir.SHORT;
        eS.win = rShort > 0;
        eS.factor = fShort;
        eS.candleIdx = i;
        eS.matchedCandleTs = k0.ts;
        eS.deltaMs = 0;
        out.add(eS);
    }
    return out;
}

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


    // -------------------- combo consensus (vote) --------------------
    private static int calcNeedVotes(int k, int comboMinVotes, double comboVoteRatio) {
        int need;
        if (comboMinVotes > 0) need = comboMinVotes;
        else {
            if (!Double.isFinite(comboVoteRatio)) comboVoteRatio = 1.0;
            if (comboVoteRatio <= 0.0) need = 1;
            else need = (int) Math.ceil(k * comboVoteRatio);
        }
        if (need < 1) need = 1;
        if (need > k) need = k;
        return need;
    }

    /**
     * 返回：在每个 event 上，满足“>=needVotes 票” 的 bitset。
     * 说明：
     * - bits[ind] 表示该指标在 event 上命中(=1)的位置
     * - pick[] 是本次组合选择的指标下标（已排序）
     * - needVotes：需要票数（1=OR，k=AND）
     *
     * ⚠️ 为了速度，这里对 k<=5 做了专门优化；若你强行把 comboMaxK 设置得很大，建议先把 --comboVoteRatio=1.0（回到AND）。
     */
    private static BitSet consensusByVotes(BitSet[] bits, int[] pick, int needVotes) {
        int k = pick.length;
        if (k == 0) return new BitSet();
        if (needVotes <= 1) {
            BitSet r = new BitSet();
            for (int i = 0; i < k; i++) r.or(bits[pick[i]]);
            return r;
        }
        if (needVotes >= k) {
            BitSet r = (BitSet) bits[pick[0]].clone();
            for (int i = 1; i < k; i++) r.and(bits[pick[i]]);
            return r;
        }

        if (k == 2) { // needVotes == 1 handled above, so here only 2
            BitSet r = (BitSet) bits[pick[0]].clone();
            r.and(bits[pick[1]]);
            return r;
        }

        if (k == 3) {
            BitSet A = bits[pick[0]], B = bits[pick[1]], C = bits[pick[2]];
            if (needVotes == 2) {
                BitSet ab = (BitSet) A.clone(); ab.and(B);
                BitSet ac = (BitSet) A.clone(); ac.and(C);
                BitSet bc = (BitSet) B.clone(); bc.and(C);
                ab.or(ac); ab.or(bc);
                return ab;
            }
            // needVotes==1 已处理，needVotes==3 已处理
        }

        if (k == 4) {
            BitSet A = bits[pick[0]], B = bits[pick[1]], C = bits[pick[2]], D = bits[pick[3]];
            if (needVotes == 3) {
                // >=3 of 4 : ABC==3 OR (ABC==2 AND D)
                BitSet s = (BitSet) A.clone(); s.xor(B); s.xor(C); // parity of ABC
                BitSet ab = (BitSet) A.clone(); ab.and(B);
                BitSet ac = (BitSet) A.clone(); ac.and(C);
                BitSet bc = (BitSet) B.clone(); bc.and(C);
                ab.or(ac); ab.or(bc); // c2 = (>=2 of ABC)

                BitSet abc3 = (BitSet) s.clone(); abc3.and(ab);     // countABC==3
                BitSet abc2 = (BitSet) ab.clone(); abc2.andNot(s);  // countABC==2
                abc2.and(D);

                abc3.or(abc2);
                return abc3;
            } else if (needVotes == 2) {
                // >=2 of 4 : 6 pairwise ANDs
                BitSet ab = (BitSet) A.clone(); ab.and(B);
                BitSet ac = (BitSet) A.clone(); ac.and(C);
                BitSet ad = (BitSet) A.clone(); ad.and(D);
                BitSet bc = (BitSet) B.clone(); bc.and(C);
                BitSet bd = (BitSet) B.clone(); bd.and(D);
                BitSet cd = (BitSet) C.clone(); cd.and(D);
                ab.or(ac); ab.or(ad); ab.or(bc); ab.or(bd); ab.or(cd);
                return ab;
            }
        }

        if (k == 5) {
            BitSet A = bits[pick[0]], B = bits[pick[1]], C = bits[pick[2]], D = bits[pick[3]], E = bits[pick[4]];
            // 先对 ABC 做一次“计数压缩”：s=奇偶，c2=至少2票
            BitSet s = (BitSet) A.clone(); s.xor(B); s.xor(C); // parity of ABC
            BitSet ab = (BitSet) A.clone(); ab.and(B);
            BitSet ac = (BitSet) A.clone(); ac.and(C);
            BitSet bc = (BitSet) B.clone(); bc.and(C);
            ab.or(ac); ab.or(bc); // c2 = (>=2 of ABC)

            BitSet abc3 = (BitSet) s.clone(); abc3.and(ab);     // countABC==3
            BitSet abc2 = (BitSet) ab.clone(); abc2.andNot(s);  // countABC==2
            BitSet abc1 = (BitSet) s.clone(); abc1.andNot(ab);  // countABC==1

            if (needVotes == 3) {
                // >=3 of 5 :
                // ABC==3  OR  (ABC==2 AND (D|E))  OR  (ABC==1 AND (D&E))
                BitSet dOrE = (BitSet) D.clone(); dOrE.or(E);
                BitSet dAndE = (BitSet) D.clone(); dAndE.and(E);

                BitSet t1 = (BitSet) abc2.clone(); t1.and(dOrE);
                abc1.and(dAndE);

                abc3.or(t1); abc3.or(abc1);
                return abc3;
            } else if (needVotes == 4) {
                // >=4 of 5 :
                // (ABC==3 AND (D|E)) OR (ABC==2 AND (D&E))
                BitSet dOrE = (BitSet) D.clone(); dOrE.or(E);
                BitSet dAndE = (BitSet) D.clone(); dAndE.and(E);

                abc3.and(dOrE);
                abc2.and(dAndE);
                abc3.or(abc2);
                return abc3;
            }
        }

        // fallback（小概率走到）：用 OR of (k choose needVotes) intersections（k<=5 仍可接受）
        List<int[]> combs = new ArrayList<>();
        buildCombinationsIndices(0, pick.length, needVotes, new int[needVotes], 0, combs);
        BitSet out = new BitSet();
        for (int[] idxs : combs) {
            BitSet t = (BitSet) bits[pick[idxs[0]]].clone();
            for (int j = 1; j < idxs.length; j++) t.and(bits[pick[idxs[j]]]);
            out.or(t);
        }
        return out;
    }

    private static void buildCombinationsIndices(int start, int n, int k, int[] buf, int bi, List<int[]> out) {
        if (bi == k) {
            out.add(Arrays.copyOf(buf, buf.length));
            return;
        }
        for (int i = start; i <= n - (k - bi); i++) {
            buf[bi] = i;
            buildCombinationsIndices(i + 1, n, k, buf, bi + 1, out);
        }
    }

    // -------------------- utils --------------------
    private static double safeFactor(double f) {
        if (!Double.isFinite(f) || f <= 0.0) return 1.0;
        return f;
    }

    
    private static String joinInts(int[] a, String sep) {
        if (a == null || a.length == 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(sep);
            sb.append(a[i]);
        }
        return sb.toString();
    }

private static String esc(String s) {
        if (s == null) return "";
        String t = s.replace("\"", "\"\"");
        if (t.contains(",") || t.contains("\n")) return "\"" + t + "\"";
        return t;
    }
}
