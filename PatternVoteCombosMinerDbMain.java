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

import java.io.File;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * PatternVoteCombosMinerDbMain
 *
 * ✅ 目标：只读 SQLite K线库，做“形态/结构（CANDLE + PS_DT）投票组合”两阶段挖掘（2~8 组合），并回测最近 N 年 30m K线。
 * - 不读取/不依赖 trade_log.txt
 * - 无未来函数：信号只看 K0(已收盘) 及更早；入场用 K1 开盘；出场用 K(i+holdBars) 收盘
 * - 手续费：单边 fee（默认 0.0005），回测因子内按 (1-fee)^2 扣双边
 *
 * ✅ 事件定义（为避免“每根K都交易”导致样本爆炸）：
 * - 仅在“任意形态/结构出现信号（dir!=0）”的 K0 上，才会产生事件
 * - 同一根 K0 可能同时产生 LONG 事件（存在任意 +1）和 SHORT 事件（存在任意 -1）
 *
 * ✅ 两阶段：
 * 1) 单指标评分 -> 选 TopM 进入候选池（仅 CANDLE/PS_DT）
 * 2) 在候选池里随机采样组合（K=2..8），计算盈利影响并排行 TopN（控制台按你指定格式打印）
 *
 * 运行示例：
 * java eval.PatternVoteCombosMinerDbMain --db=./okx_candles_osc.db --instId=ETH-USDT-SWAP --bar=30m --years=4 --outDir=./out
 *
 * 关键参数（本类自解析 --k=v，其他仍走 Args.parse 复用你项目已有参数）：
 * --topN=50
 * --topM=50
 * --familyCap=80
 * --lookback=1
 * --holdBars=12
 * --fee=0.0005                 // 单边手续费
 * --needVotes=1                // 组合净票阈值：LONG 要 >=needVotes；SHORT 要 <=-needVotes
 * --randCombos=200000
 * --comboMinK=2
 * --comboMaxK=8
 * --randSeed=1
 * --oos6=6 --oos12=12
 * --initEq=10000               // 用于 compounding 与 fixed-R 的起始权益
 */
public class PatternVoteCombosMinerDbMain {

    // -------------------- CLI (本类自解析：支持 --k=v) --------------------
    private static final String ARG_TOPN = "--topN=";
    private static final String ARG_TOPM = "--topM=";
    private static final String ARG_FAMILYCAP = "--familyCap=";
    private static final String ARG_LOOKBACK = "--lookback=";
    private static final String ARG_HOLD_BARS = "--holdBars=";
    private static final String ARG_FEE = "--fee=";
    private static final String ARG_NEED_VOTES = "--needVotes=";

    private static final String ARG_RAND_COMBOS = "--randCombos=";
    private static final String ARG_COMBO_MIN_K = "--comboMinK=";
    private static final String ARG_COMBO_MAX_K = "--comboMaxK=";
    private static final String ARG_RAND_SEED = "--randSeed=";

    private static final String ARG_OOS6 = "--oos6=";
    private static final String ARG_OOS12 = "--oos12=";
    private static final String ARG_INIT_EQ = "--initEq=";

    private static final ZoneId TOKYO = ZoneId.of("Asia/Tokyo");
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TOKYO);

    enum Dir { LONG, SHORT }

    static final class BarEvent {
        final int k0;              // 信号K0（已收盘）
        final long entryTsMs;      // K1 开盘时间戳（ms）
        final Dir dir;
        final double entryPx;      // K1 open
        final double exitPx;       // K(i+holdBars) close
        final double factor;       // compounding factor（已含双边手续费）
        final double netRet;       // factor-1（用于 fixed-R）
        final boolean win;

        BarEvent(int k0, long entryTsMs, Dir dir, double entryPx, double exitPx, double factor) {
            this.k0 = k0;
            this.entryTsMs = entryTsMs;
            this.dir = dir;
            this.entryPx = entryPx;
            this.exitPx = exitPx;
            this.factor = factor;
            this.netRet = factor - 1.0;
            this.win = factor > 1.0;
        }
    }

    static final class CondSeries {
        final PoolCondition cond;
        final int[] dir;      // -1/0/+1 per bar
        final int[] pref;     // prefix sum (len+1)
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

    static final class ComboSpec {
        final int k;
        final int[] idx;    // series index (sorted)
        final String id;    // display id
        final String group; // [..]
        ComboSpec(int k, int[] idx, String id, String group) {
            this.k = k;
            this.idx = idx;
            this.id = id;
            this.group = group;
        }
    }

    static final class ImpactRow {
        final String id;
        final String group;

        final int totalN;
        final int agreeN;
        final double coverage; // 0..1

        final double baseFinal;
        final double filtFinal;
        final double baseRetPct;
        final double filtRetPct;
        final double profitImpactPct;
        final double profitEffectScore;

        // fixed-R
        final double baseFinalFixed;
        final double filtFinalFixed;
        final double profitImpactPctFixed;
        final double pfBase;
        final double pfFilt;
        final double ddBasePct;
        final double ddFiltPct;

        // OOS (Fixed-R)
        final double oos6_winLiftPct;
        final double oos6_pfBase;
        final double oos6_pfFilt;
        final double oos6_ddBasePct;
        final double oos6_ddFiltPct;
        final double oos6_profitImpactPct;

        final double oos12_winLiftPct;
        final double oos12_pfBase;
        final double oos12_pfFilt;
        final double oos12_ddBasePct;
        final double oos12_ddFiltPct;
        final double oos12_profitImpactPct;

        ImpactRow(String id, String group,
                  int totalN, int agreeN, double coverage,
                  double baseFinal, double filtFinal, double baseRetPct, double filtRetPct, double profitImpactPct, double profitEffectScore,
                  double baseFinalFixed, double filtFinalFixed, double profitImpactPctFixed,
                  double pfBase, double pfFilt, double ddBasePct, double ddFiltPct,
                  double oos6_winLiftPct, double oos6_pfBase, double oos6_pfFilt, double oos6_ddBasePct, double oos6_ddFiltPct, double oos6_profitImpactPct,
                  double oos12_winLiftPct, double oos12_pfBase, double oos12_pfFilt, double oos12_ddBasePct, double oos12_ddFiltPct, double oos12_profitImpactPct) {
            this.id = id;
            this.group = group;
            this.totalN = totalN;
            this.agreeN = agreeN;
            this.coverage = coverage;
            this.baseFinal = baseFinal;
            this.filtFinal = filtFinal;
            this.baseRetPct = baseRetPct;
            this.filtRetPct = filtRetPct;
            this.profitImpactPct = profitImpactPct;
            this.profitEffectScore = profitEffectScore;

            this.baseFinalFixed = baseFinalFixed;
            this.filtFinalFixed = filtFinalFixed;
            this.profitImpactPctFixed = profitImpactPctFixed;
            this.pfBase = pfBase;
            this.pfFilt = pfFilt;
            this.ddBasePct = ddBasePct;
            this.ddFiltPct = ddFiltPct;

            this.oos6_winLiftPct = oos6_winLiftPct;
            this.oos6_pfBase = oos6_pfBase;
            this.oos6_pfFilt = oos6_pfFilt;
            this.oos6_ddBasePct = oos6_ddBasePct;
            this.oos6_ddFiltPct = oos6_ddFiltPct;
            this.oos6_profitImpactPct = oos6_profitImpactPct;

            this.oos12_winLiftPct = oos12_winLiftPct;
            this.oos12_pfBase = oos12_pfBase;
            this.oos12_pfFilt = oos12_pfFilt;
            this.oos12_ddBasePct = oos12_ddBasePct;
            this.oos12_ddFiltPct = oos12_ddFiltPct;
            this.oos12_profitImpactPct = oos12_profitImpactPct;
        }
    }

    // ======================== main ========================
    public static void main(String[] args) throws Exception {

        int topN = 50;
        int topM = 50;
        int familyCap = 80;
        int lookback = 1;
        int holdBars = 12;
        double fee = 0.0015;      // 单边手续费
        int needVotes = 1;

        int randCombos = 200000;
        int comboMinK = 1;
        int comboMaxK = 2;
        long randSeed = 1L;

        int oos6 = 1;
        int oos12 = 3;
        double initEq = 10000.0;

        List<String> pass = new ArrayList<>();
        for (String a : args) {
            if (a.startsWith(ARG_TOPN)) topN = Integer.parseInt(a.substring(ARG_TOPN.length()));
            else if (a.startsWith(ARG_TOPM)) topM = Integer.parseInt(a.substring(ARG_TOPM.length()));
            else if (a.startsWith(ARG_FAMILYCAP)) familyCap = Integer.parseInt(a.substring(ARG_FAMILYCAP.length()));
            else if (a.startsWith(ARG_LOOKBACK)) lookback = Integer.parseInt(a.substring(ARG_LOOKBACK.length()));
            else if (a.startsWith(ARG_HOLD_BARS)) holdBars = Integer.parseInt(a.substring(ARG_HOLD_BARS.length()));
            else if (a.startsWith(ARG_FEE)) fee = Double.parseDouble(a.substring(ARG_FEE.length()));
            else if (a.startsWith(ARG_NEED_VOTES)) needVotes = Integer.parseInt(a.substring(ARG_NEED_VOTES.length()));
            else if (a.startsWith(ARG_RAND_COMBOS)) randCombos = Integer.parseInt(a.substring(ARG_RAND_COMBOS.length()));
            else if (a.startsWith(ARG_COMBO_MIN_K)) comboMinK = Integer.parseInt(a.substring(ARG_COMBO_MIN_K.length()));
            else if (a.startsWith(ARG_COMBO_MAX_K)) comboMaxK = Integer.parseInt(a.substring(ARG_COMBO_MAX_K.length()));
            else if (a.startsWith(ARG_RAND_SEED)) randSeed = Long.parseLong(a.substring(ARG_RAND_SEED.length()));
            else if (a.startsWith(ARG_OOS6)) oos6 = Integer.parseInt(a.substring(ARG_OOS6.length()));
            else if (a.startsWith(ARG_OOS12)) oos12 = Integer.parseInt(a.substring(ARG_OOS12.length()));
            else if (a.startsWith(ARG_INIT_EQ)) initEq = Double.parseDouble(a.substring(ARG_INIT_EQ.length()));
            else pass.add(a);
        }

        Args cfg = Args.parse(pass.toArray(new String[0]));
        new File(cfg.outDir).mkdirs();

        // 外部特征 provider（保持与你项目一致；本挖掘只用形态/结构，一般可以 --featureMode=none）
        String fm = (cfg.featureMode == null ? "none" : cfg.featureMode.trim().toLowerCase(Locale.ROOT));
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

        long now = System.currentTimeMillis();
        long barMs = OkxKlineUtils.barMs(cfg.bar);
        long safeCloseMs = cfg.safeCloseMs;

        // 近 N 年区间（如果你的 cache 本身会按 years 裁剪，这里仍然给一个合理窗口）
        long yearsMs = (long) (cfg.years * 366.0 * 24 * 3600 * 1000L);
        long end = now;
        long start = Math.max(0, end - yearsMs - barMs * 500);

        OkxCandleCacheEthOsc cache = OkxCandleCacheEthOsc.get(cfg.db, cfg.years);
        List<Candle> candles = cache.getOrFetchRange(cfg.instId, cfg.bar, start, end, now, barMs, safeCloseMs);
        if (candles == null || candles.size() < 5000) {
            throw new IllegalStateException("K线数量太少：" + (candles == null ? 0 : candles.size()) +
                    "。请确认 db=" + cfg.db + " 是否包含 4 年 30m 数据。");
        }

        System.out.println("========================================================");
        System.out.println("PatternVoteCombosMinerDbMain（只读DB回测）");
        System.out.println("db=" + cfg.db + " | instId=" + cfg.instId + " | bar=" + cfg.bar + " | years=" + cfg.years);
        System.out.println("candles=" + candles.size() + " | range=" + fmtTs(candles.get(0).ts) + " ~ " + fmtTs(candles.get(candles.size()-1).ts));
        System.out.println("lookback=" + lookback + " | holdBars=" + holdBars + " | fee(one-way)=" + fee + " | needVotes=" + needVotes);
        System.out.println("two-stage: topM=" + topM + " familyCap=" + familyCap + " | combos=" + randCombos + " | K=" + comboMinK + "~" + comboMaxK);
        System.out.println("oos6=" + oos6 + "m | oos12=" + oos12 + "m | initEq=" + initEq);
        System.out.println("outDir=" + cfg.outDir);
        System.out.println("========================================================");

        // 1) 只取形态/结构条件（要求 ConditionFactory 里已用 tag.apply(CANDLE/PS_DT, ...)）
        List<PoolCondition> condsAll = ConditionFactory.defaultConditionsWithInst(cfg.instId, cfg.bar);
        List<PoolCondition> conds = new ArrayList<>();
        for (PoolCondition c : condsAll) {
            String id = c.id();
            if (id != null && (id.startsWith("F=CANDLE|") || id.startsWith("F=PS_DT|"))) conds.add(c);
        }
        System.out.println("指标库(all)=" + condsAll.size() + " | 形态/结构(CANDLE/PS_DT)=" + conds.size());
        if (conds.isEmpty()) {
            throw new IllegalStateException("未发现任何形态/结构条件（id 需以 F=CANDLE| 或 F=PS_DT| 开头）。请检查 ConditionFactory。");
        }

        // 2) 预计算 dir/prefix
        List<CondSeries> series = buildCondSeries(conds, candles);

        // 3) 生成事件（只在任意形态/结构出现信号时生成，避免样本爆炸）
        List<BarEvent> eventsAll = buildEventsFromAnyPattern(series, candles, holdBars, fee);
        if (eventsAll.isEmpty()) throw new IllegalStateException("事件数为0：说明 4 年内没有任何形态/结构触发。请检查 dir() 实现是否全为0。");

        List<BarEvent> eventsLong = filterByDir(eventsAll, Dir.LONG);
        List<BarEvent> eventsShort = filterByDir(eventsAll, Dir.SHORT);

        System.out.println("events(ALL)=" + eventsAll.size() + " | LONG=" + eventsLong.size() + " | SHORT=" + eventsShort.size());

        // 4) 两阶段：先算单指标（用于选TopM池），再在池内挖组合
        int[] poolOverall = selectTopMIndices("OVERALL", series, eventsAll, lookback, needVotes, topM, familyCap, initEq);
        int[] poolLong = selectTopMIndices("LONG_ONLY", series, eventsLong, lookback, needVotes, topM, familyCap, initEq);
        int[] poolShort = selectTopMIndices("SHORT_ONLY", series, eventsShort, lookback, needVotes, topM, familyCap, initEq);

        List<ComboSpec> combosOverall = genRandomCombosFromPool(series, poolOverall, randCombos, comboMinK, comboMaxK, randSeed + 11, "TSRND_OVERALL");
        List<ComboSpec> combosLong = genRandomCombosFromPool(series, poolLong, randCombos, comboMinK, comboMaxK, randSeed + 22, "TSRND_LONG_ONLY");
        List<ComboSpec> combosShort = genRandomCombosFromPool(series, poolShort, randCombos, comboMinK, comboMaxK, randSeed + 33, "TSRND_SHORT_ONLY");

        // 5) 评估组合（盈利影响）
        List<ImpactRow> rowsOverall = evalCombosProfitImpact(series, eventsAll, combosOverall, lookback, needVotes, initEq, oos6, oos12);
        List<ImpactRow> rowsLong = evalCombosProfitImpact(series, eventsLong, combosLong, lookback, needVotes, initEq, oos6, oos12);
        List<ImpactRow> rowsShort = evalCombosProfitImpact(series, eventsShort, combosShort, lookback, needVotes, initEq, oos6, oos12);

        // sort
        rowsOverall.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));
        rowsLong.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));
        rowsShort.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        // 6) 控制台输出（按你指定格式）
        printTopProfit("总体 盈利影响 两阶段组合 Top" + topN, rowsOverall, topN);
        printTopProfit("Long 盈利影响 两阶段组合 Top" + topN, rowsLong, topN);
        printTopProfit("Short 盈利影响 两阶段组合 Top" + topN, rowsShort, topN);

        // 7) CSV 输出（简单版：只输出组合盈利影响）
        writeProfitCsv(cfg.outDir + "/pattern_profitimpact_overall.csv", rowsOverall);
        writeProfitCsv(cfg.outDir + "/pattern_profitimpact_long.csv", rowsLong);
        writeProfitCsv(cfg.outDir + "/pattern_profitimpact_short.csv", rowsShort);

        System.out.println("\n✅ 输出完成：");
        System.out.println(" - " + cfg.outDir + "/pattern_profitimpact_overall.csv");
        System.out.println(" - " + cfg.outDir + "/pattern_profitimpact_long.csv");
        System.out.println(" - " + cfg.outDir + "/pattern_profitimpact_short.csv");
    }

    // ======================== series build ========================
    private static List<CondSeries> buildCondSeries(List<PoolCondition> conds, List<Candle> candles) {
        List<CondSeries> out = new ArrayList<>(conds.size());
        for (PoolCondition c : conds) {
            try {
                int[] d = c.dir(candles, 0, candles.size());
                if (d == null || d.length != candles.size()) {
                    System.out.println("⚠️ " + c.id() + " dir长度异常，已置零。len=" + (d == null ? 0 : d.length) + ", candles=" + candles.size());
                    d = new int[candles.size()];
                }
                out.add(new CondSeries(c, d));
            } catch (Exception ex) {
                System.out.println("⚠️ " + c.id() + " 计算失败，已置零。原因=" + ex.getMessage());
                out.add(new CondSeries(c, new int[candles.size()]));
            }
        }
        return out;
    }

    // ======================== events ========================
    private static List<BarEvent> buildEventsFromAnyPattern(List<CondSeries> series, List<Candle> candles, int holdBars, double feeOneWay) {
        int n = candles.size();
        int lastK0 = n - 1 - holdBars; // k0 + holdBars <= n-1 AND k0+1 <= n-1
        if (lastK0 <= 2) return new ArrayList<>();

        double feeMul = (1.0 - feeOneWay);
        double feeMul2 = feeMul * feeMul; // 双边

        ArrayList<BarEvent> out = new ArrayList<>();

        for (int k0 = 0; k0 < lastK0; k0++) {
            boolean anyLong = false;
            boolean anyShort = false;

            for (CondSeries cs : series) {
                int d = cs.dir[k0];
                if (d > 0) anyLong = true;
                else if (d < 0) anyShort = true;
                if (anyLong && anyShort) break;
            }

            if (!anyLong && !anyShort) continue;

            Candle entryK = candles.get(k0 + 1);
            Candle exitK = candles.get(k0 + holdBars);
            double entry = entryK.o;
            double exit = exitK.c;

            if (!(entry > 0 && exit > 0)) continue;

            long entryTs = entryK.ts;

            if (anyLong) {
                double factor = (exit / entry) * feeMul2;
                out.add(new BarEvent(k0, entryTs, Dir.LONG, entry, exit, factor));
            }
            if (anyShort) {
                double factor = (entry / exit) * feeMul2;
                out.add(new BarEvent(k0, entryTs, Dir.SHORT, entry, exit, factor));
            }
        }

        // 事件按 entryTs 排序
        out.sort(Comparator.comparingLong(a -> a.entryTsMs));
        return out;
    }

    private static List<BarEvent> filterByDir(List<BarEvent> events, Dir dir) {
        ArrayList<BarEvent> out = new ArrayList<>();
        for (BarEvent e : events) if (e.dir == dir) out.add(e);
        return out;
    }

    // ======================== two-stage pool select ========================
    private static int[] selectTopMIndices(String title,
                                          List<CondSeries> series,
                                          List<BarEvent> events,
                                          int lookback,
                                          int needVotes,
                                          int topM,
                                          int familyCap,
                                          double initEq) {

        // 单指标的 profitEffectScore（作为池子排序依据）
        ArrayList<ImpactRow> rows = (ArrayList<ImpactRow>) evalCombosProfitImpact(series, events, wrapSingles(series), lookback, needVotes, initEq, 0, 0);
        rows.sort((a, b) -> Double.compare(b.profitEffectScore, a.profitEffectScore));

        HashMap<String, Integer> famCnt = new HashMap<>();
        ArrayList<Integer> out = new ArrayList<>();

        for (ImpactRow r : rows) {
            Integer idx = parseSingleIdx(r.group);
            if (idx == null) continue;
            String fam = parseFamilyFromId(series.get(idx).cond.id());
            if (familyCap > 0) {
                int cnt = famCnt.getOrDefault(fam, 0);
                if (cnt >= familyCap) continue;
                famCnt.put(fam, cnt + 1);
            }
            out.add(idx);
            if (out.size() >= topM) break;
        }

        // fallback：不够就放宽 familyCap
        if (out.size() < Math.min(5, topM)) {
            out.clear();
            for (ImpactRow r : rows) {
                Integer idx = parseSingleIdx(r.group);
                if (idx == null) continue;
                out.add(idx);
                if (out.size() >= topM) break;
            }
        }

        int[] arr = new int[out.size()];
        for (int i = 0; i < out.size(); i++) arr[i] = out.get(i);
        Arrays.sort(arr);

        System.out.println("两阶段候选池[" + title + "] topM=" + topM + " familyCap=" + familyCap + " => poolN=" + arr.length);
        if (familyCap > 0) {
            HashMap<String, Integer> dist = new HashMap<>();
            for (int idx : arr) {
                String fam = parseFamilyFromId(series.get(idx).cond.id());
                dist.put(fam, dist.getOrDefault(fam, 0) + 1);
            }
            ArrayList<String> keys = new ArrayList<>(dist.keySet());
            keys.sort(String::compareTo);
            StringBuilder sb = new StringBuilder();
            for (String k : keys) sb.append(k).append(":").append(dist.get(k)).append(" ");
            System.out.println("  poolFamilyDist: " + sb.toString().trim());
        }
        return arr;
    }

    private static List<ComboSpec> wrapSingles(List<CondSeries> series) {
        ArrayList<ComboSpec> out = new ArrayList<>();
        for (int i = 0; i < series.size(); i++) {
            int[] idx = new int[]{i};
            String id = "SINGLE:" + series.get(i).cond.id();
            String group = "SINGLE_IDX_" + i;
            out.add(new ComboSpec(1, idx, id, group));
        }
        return out;
    }

    private static Integer parseSingleIdx(String group) {
        if (group == null) return null;
        if (!group.startsWith("SINGLE_IDX_")) return null;
        try {
            return Integer.parseInt(group.substring("SINGLE_IDX_".length()));
        } catch (Exception e) {
            return null;
        }
    }

    // ======================== combos gen ========================
    private static List<ComboSpec> genRandomCombosFromPool(List<CondSeries> series,
                                                          int[] poolIdx,
                                                          int want,
                                                          int minK,
                                                          int maxK,
                                                          long seed,
                                                          String tag) {
        int poolN = (poolIdx == null ? 0 : poolIdx.length);
        if (poolN < 2 || want <= 0) return new ArrayList<>();

        minK = Math.max(2, minK);
        maxK = Math.min(maxK, poolN);
        if (minK > maxK) { int t = minK; minK = maxK; maxK = t; }

        Random rnd = new Random(seed);
        HashSet<String> seen = new HashSet<>();
        ArrayList<ComboSpec> out = new ArrayList<>(Math.min(want, 200000));

        int safety = want * 50 + 2000;
        for (int t = 0; t < safety && out.size() < want; t++) {
            int k = minK + rnd.nextInt(maxK - minK + 1);
            if (k > poolN) k = poolN;

            int[] pickPos = pickDistinctIdx(poolN, k, rnd);
            int[] idx = new int[k];
            for (int i = 0; i < k; i++) idx[i] = poolIdx[pickPos[i]];
            Arrays.sort(idx);

            String key = Arrays.toString(idx);
            if (!seen.add(key)) continue;

            String id = buildComboId(series, idx, tag, k);
            String group = tag + "_K" + k;
            out.add(new ComboSpec(k, idx, id, group));
        }
        return out;
    }

    private static int[] pickDistinctIdx(int n, int k, Random rnd) {
        int[] out = new int[k];
        HashSet<Integer> used = new HashSet<>();
        while (used.size() < k) used.add(rnd.nextInt(n));
        int i = 0;
        for (int v : used) out[i++] = v;
        return out;
    }

    private static String buildComboId(List<CondSeries> series, int[] idx, String tag, int k) {
        StringBuilder sb = new StringBuilder();
        sb.append(tag).append("_COMBO").append(k).append(":");
        for (int i = 0; i < idx.length; i++) {
            if (i > 0) sb.append("+");
            sb.append(series.get(idx[i]).cond.id());
        }
        return sb.toString();
    }

    // ======================== eval core ========================
    private static List<ImpactRow> evalCombosProfitImpact(List<CondSeries> series,
                                                         List<BarEvent> events,
                                                         List<ComboSpec> combos,
                                                         int lookback,
                                                         int needVotes,
                                                         double initEq,
                                                         int oos6,
                                                         int oos12) {

        ArrayList<ImpactRow> out = new ArrayList<>(combos.size());
        if (events.isEmpty()) return out;

        long endTs = events.get(events.size()-1).entryTsMs;
        long oos6Cut = (oos6 > 0 ? monthCutoff(endTs, oos6) : Long.MIN_VALUE);
        long oos12Cut = (oos12 > 0 ? monthCutoff(endTs, oos12) : Long.MIN_VALUE);

        for (ComboSpec c : combos) {
            boolean[] include = new boolean[events.size()];
            int agreeN = 0;

            for (int i = 0; i < events.size(); i++) {
                BarEvent e = events.get(i);
                int netVotes = 0;
                for (int j : c.idx) {
                    netVotes += series.get(j).strengthAt(e.k0, lookback);
                }
                boolean agree = (e.dir == Dir.LONG) ? (netVotes >= needVotes) : (netVotes <= -needVotes);
                if (agree) {
                    include[i] = true;
                    agreeN++;
                }
            }

            int totalN = events.size();
            double cov = totalN == 0 ? 0.0 : (agreeN * 1.0 / totalN);

            // compounding
            double baseFinal = replayEquity(events, initEq, null);
            double filtFinal = replayEquity(events, initEq, include);
            double baseRet = (baseFinal / initEq - 1.0) * 100.0;
            double filtRet = (filtFinal / initEq - 1.0) * 100.0;
            double impPct = (baseFinal > 0 ? (filtFinal / baseFinal - 1.0) * 100.0 : 0.0);
            double eff = (impPct / 100.0) * Math.sqrt(Math.max(0.0, cov));

            // fixed-R（每笔固定用 initEq 做单位名义）
            FixedStats fsBase = replayFixedR(events, initEq, null);
            FixedStats fsFilt = replayFixedR(events, initEq, include);
            double baseFixed = fsBase.finalEq;
            double filtFixed = fsFilt.finalEq;
            double impFixed = (baseFixed > 0 ? (filtFixed / baseFixed - 1.0) * 100.0 : 0.0);

            // OOS6/OOS12（fixed-R）
            OosStats o6 = (oos6 > 0) ? evalOos(events, include, initEq, oos6Cut) : OosStats.na();
            OosStats o12 = (oos12 > 0) ? evalOos(events, include, initEq, oos12Cut) : OosStats.na();

            out.add(new ImpactRow(
                    c.id, c.group,
                    totalN, agreeN, cov,
                    baseFinal, filtFinal, baseRet, filtRet, impPct, eff,
                    baseFixed, filtFixed, impFixed,
                    fsBase.pf, fsFilt.pf, fsBase.maxDdPct, fsFilt.maxDdPct,
                    o6.winLiftPct, o6.pfBase, o6.pfFilt, o6.ddBasePct, o6.ddFiltPct, o6.profitImpactPct,
                    o12.winLiftPct, o12.pfBase, o12.pfFilt, o12.ddBasePct, o12.ddFiltPct, o12.profitImpactPct
            ));
        }
        return out;
    }

    private static double replayEquity(List<BarEvent> events, double initEq, boolean[] include) {
        double eq = initEq;
        for (int i = 0; i < events.size(); i++) {
            if (include == null || include[i]) eq *= events.get(i).factor;
        }
        return eq;
    }

    static final class FixedStats {
        final double finalEq;
        final double pf;
        final double maxDdPct;
        FixedStats(double finalEq, double pf, double maxDdPct) {
            this.finalEq = finalEq;
            this.pf = pf;
            this.maxDdPct = maxDdPct;
        }
    }

    private static FixedStats replayFixedR(List<BarEvent> events, double unitEq, boolean[] include) {
        double eq = unitEq;
        double peak = unitEq;
        double maxDd = 0.0;

        double pos = 0.0, neg = 0.0;

        for (int i = 0; i < events.size(); i++) {
            if (include != null && !include[i]) continue;
            double pnl = unitEq * events.get(i).netRet; // netRet 已含手续费
            if (pnl >= 0) pos += pnl; else neg += -pnl;
            eq += pnl;
            if (eq > peak) peak = eq;
            double dd = (peak > 0 ? (peak - eq) / peak : 0.0);
            if (dd > maxDd) maxDd = dd;
        }

        double pf = (neg <= 1e-12 ? (pos > 0 ? 9.99 : 0.0) : (pos / neg));
        return new FixedStats(eq, pf, maxDd * 100.0);
    }

    static final class OosStats {
        final double winLiftPct;        // filteredWR - baseWR
        final double pfBase;
        final double pfFilt;
        final double ddBasePct;
        final double ddFiltPct;
        final double profitImpactPct;   // (filt/base -1)*100
        OosStats(double winLiftPct, double pfBase, double pfFilt, double ddBasePct, double ddFiltPct, double profitImpactPct) {
            this.winLiftPct = winLiftPct;
            this.pfBase = pfBase;
            this.pfFilt = pfFilt;
            this.ddBasePct = ddBasePct;
            this.ddFiltPct = ddFiltPct;
            this.profitImpactPct = profitImpactPct;
        }
        static OosStats na() { return new OosStats(0,0,0,0,0,0); }
    }

    private static OosStats evalOos(List<BarEvent> events, boolean[] include, double unitEq, long cutTs) {
        ArrayList<BarEvent> base = new ArrayList<>();
        ArrayList<BarEvent> filt = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            BarEvent e = events.get(i);
            if (e.entryTsMs < cutTs) continue;
            base.add(e);
            if (include != null && include[i]) filt.add(e);
        }
        if (base.isEmpty()) return OosStats.na();

        int baseWin=0, filtWin=0;
        for (BarEvent e: base) if (e.win) baseWin++;
        for (BarEvent e: filt) if (e.win) filtWin++;

        double baseWR = baseWin * 100.0 / base.size();
        double filtWR = (filt.isEmpty()? 0.0 : filtWin * 100.0 / filt.size());
        double lift = filtWR - baseWR;

        FixedStats fsBase = replayFixedR(base, unitEq, null);
        FixedStats fsFilt = replayFixedR(filt, unitEq, null);

        double imp = (fsBase.finalEq > 0 ? (fsFilt.finalEq / fsBase.finalEq - 1.0) * 100.0 : 0.0);
        return new OosStats(lift, fsBase.pf, fsFilt.pf, fsBase.maxDdPct, fsFilt.maxDdPct, imp);
    }

    private static long monthCutoff(long endTsMs, int monthsBack) {
        Instant end = Instant.ofEpochMilli(endTsMs);
        ZonedDateTime z = end.atZone(TOKYO);
        ZonedDateTime cut = z.minusMonths(monthsBack);
        return cut.toInstant().toEpochMilli();
    }

    // ======================== output ========================
    private static void printTopProfit(String title, List<ImpactRow> rows, int topN) {
        System.out.println("\n==================== " + title + " ====================");
        for (int i = 0; i < Math.min(topN, rows.size()); i++) {
            ImpactRow r = rows.get(i);
            String line = String.format(Locale.ROOT,
                    "%2d) %s [%s] profitImpact=%+,.2f%% cov=%.1f%% effect=%.4f  baseFinal=%,.2f filtFinal=%,.2f  baseRet=%+,.2f%% filtRet=%+,.2f%% | " +
                            "fixedRImpact=%+,.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% | " +
                            "OOS6 lift=%+,.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% imp=%+,.2f%% | " +
                            "OOS12 lift=%+,.2f%% pf=%.2f->%.2f dd=%.2f%%->%.2f%% imp=%+,.2f%%",
                    (i + 1),
                    r.id,
                    r.group,
                    r.profitImpactPct,
                    r.coverage * 100.0,
                    r.profitEffectScore,
                    r.baseFinal, r.filtFinal,
                    r.baseRetPct, r.filtRetPct,
                    r.profitImpactPctFixed,
                    r.pfBase, r.pfFilt,
                    r.ddBasePct, r.ddFiltPct,
                    r.oos6_winLiftPct, r.oos6_pfBase, r.oos6_pfFilt, r.oos6_ddBasePct, r.oos6_ddFiltPct, r.oos6_profitImpactPct,
                    r.oos12_winLiftPct, r.oos12_pfBase, r.oos12_pfFilt, r.oos12_ddBasePct, r.oos12_ddFiltPct, r.oos12_profitImpactPct
            );
            System.out.println(line);
        }
    }

    private static void writeProfitCsv(String path, List<ImpactRow> rows) {
        try {
            File f = new File(path);
            if (f.exists()) f.delete();
            StringBuilder sb = new StringBuilder();
            sb.append("id,group,totalN,agreeN,coverage,profitImpactPct,profitEffectScore,baseFinal,filtFinal,baseRetPct,filtRetPct,");
            sb.append("fixedImpactPct,pfBase,pfFilt,ddBasePct,ddFiltPct,");
            sb.append("oos6_winLiftPct,oos6_pfBase,oos6_pfFilt,oos6_ddBasePct,oos6_ddFiltPct,oos6_profitImpactPct,");
            sb.append("oos12_winLiftPct,oos12_pfBase,oos12_pfFilt,oos12_ddBasePct,oos12_ddFiltPct,oos12_profitImpactPct\n");
            for (ImpactRow r : rows) {
                sb.append(csv(r.id)).append(",").append(csv(r.group)).append(",");
                sb.append(r.totalN).append(",").append(r.agreeN).append(",").append(fmt(r.coverage)).append(",");
                sb.append(fmt(r.profitImpactPct)).append(",").append(fmt(r.profitEffectScore)).append(",");
                sb.append(fmt(r.baseFinal)).append(",").append(fmt(r.filtFinal)).append(",").append(fmt(r.baseRetPct)).append(",").append(fmt(r.filtRetPct)).append(",");
                sb.append(fmt(r.profitImpactPctFixed)).append(",").append(fmt(r.pfBase)).append(",").append(fmt(r.pfFilt)).append(",").append(fmt(r.ddBasePct)).append(",").append(fmt(r.ddFiltPct)).append(",");
                sb.append(fmt(r.oos6_winLiftPct)).append(",").append(fmt(r.oos6_pfBase)).append(",").append(fmt(r.oos6_pfFilt)).append(",").append(fmt(r.oos6_ddBasePct)).append(",").append(fmt(r.oos6_ddFiltPct)).append(",").append(fmt(r.oos6_profitImpactPct)).append(",");
                sb.append(fmt(r.oos12_winLiftPct)).append(",").append(fmt(r.oos12_pfBase)).append(",").append(fmt(r.oos12_pfFilt)).append(",").append(fmt(r.oos12_ddBasePct)).append(",").append(fmt(r.oos12_ddFiltPct)).append(",").append(fmt(r.oos12_profitImpactPct)).append("\n");
            }
            java.nio.file.Files.write(f.toPath(), sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.out.println("⚠️ CSV写入失败：" + path + " err=" + e.getMessage());
        }
    }

    private static String csv(String s) {
        if (s == null) return "";
        String t = s.replace("\"", "\"\"");
        return "\"" + t + "\"";
    }

    private static String fmtTs(long ms) {
        return FMT.format(Instant.ofEpochMilli(ms));
    }

    private static String fmt(double v) {
        if (!Double.isFinite(v)) return "0";
        return String.format(Locale.ROOT, "%.6f", v);
    }

    private static String parseFamilyFromId(String id) {
        if (id == null) return "UNKNOWN";
        if (id.startsWith("F=")) {
            int p = id.indexOf('|');
            if (p > 2) return id.substring(2, p);
        }
        return "UNKNOWN";
    }
}
