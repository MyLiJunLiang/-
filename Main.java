
package eval;

import eval.config.CandidateConfig;
import eval.engine.*;
import eval.engine.external.CsvExternalFeatureProvider;
import eval.engine.external.ExternalFeatureProviderHolder;
import eval.engine.candidates.Candidate;
import eval.okx.OkxCandleCacheEthOsc;
import eval.okx.OkxKlineUtils;
import eval.model.Candle;

import java.io.File;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);

        long now = System.currentTimeMillis();
        long barMs = OkxKlineUtils.barMs(a.bar);
        long safeCloseMs = a.safeCloseMs;

        System.out.println("========================================================");
        System.out.println("均匀盈利评估引擎（Phase-1）");
        System.out.println("数据源：OkxCandleCacheEthOsc + " + a.db + "  | instId=" + a.instId + " | bar=" + a.bar);
        System.out.println("成本：双边(手续费+滑点)=" + a.costRoundTrip + " （内部按单边=一半计）");
        System.out.println("Walk-forward：" + a.wf);
        System.out.println("输出目录：" + a.outDir);
        System.out.println("========================================================");

        new File(a.outDir).mkdirs();

        // 外部特征（Funding / OI / CVD / DepthRatio / OrderImbalance 等）：默认从 ./external_features 读取 CSV
        ExternalFeatureProviderHolder.set(new CsvExternalFeatureProvider(a.featureDir));

        // 1) 加载K线（优先SQLite，不足会自动拉网络补齐：你只需要接上 OkxKlineUtils.fetch...）
        OkxCandleCacheEthOsc cache = OkxCandleCacheEthOsc.get(a.db, a.years);

        long start = TimeUtil.nowMinusYears(now, a.years);
        long end = now;

        List<Candle> candles = cache.getOrFetchRange(a.instId, a.bar, start, end, now, barMs, safeCloseMs);
        if (candles == null || candles.size() < 1000) {
            throw new IllegalStateException("K线数量太少：" + (candles == null ? 0 : candles.size()) +
                    "。请确认 SQLite 是否已有数据，或把 OkxKlineUtils.fetch... 接入到你的真实OKX实现。");
        }

        // 2) 候选集合（单独测 + 组合测）
        List<Candidate> candidates = CandidateConfig.defaultCandidates(a);

        // 3) 跑评估（Walk-forward + 多尺度均匀性 + CSV）
        Evaluator evaluator = new Evaluator(a, candles);
        evaluator.run(candidates);

        System.out.println("\n✅ 完成：CSV 已输出到 " + a.outDir + "/results.csv");
        System.out.println("如果你要给 Optuna/NSGA-II：直接用 results.csv 做目标/约束即可。");
    }
}
