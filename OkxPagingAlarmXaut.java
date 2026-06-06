/*
package com.xl.XAUT趋势系统;

import java.awt.*;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static com.xl.XAUT趋势系统.OkxSignalAlarmOnlyXAUT.*;

*/
/**
 * 抽离：OKX K线分页拉取 + 闹铃
 *
 * 说明：
 * - 本类不包含任何 ADX / 分段止盈 / 信号强度评分逻辑。
 * - 闹铃：仅保留 alarmOnce(tag)，不再提供“同一根K线只响一次”的去重方法（按你的要求删除）。
 *//*

public class OkxPagingAlarmXaut {


    // ===================== 1Y 回测（启动一次） =====================
    static final int BACKTEST_DAYS = 365; // ✅可调：历史回测天数
    static final Duration BACKTEST_LOOKBACK = Duration.ofDays(BACKTEST_DAYS);

    static void runBacktestOneYearOnBoot() {
        try {
            Instant end = Instant.now();
            Instant start = end.minus(BACKTEST_LOOKBACK);

            System.out.println();
            System.out.println("###############################");
            System.out.println("## BACKTEST 1Y（按闹钟结构） ##");
            System.out.println("###############################");
            System.out.println("区间: " + start + " -> " + end);
            System.out.println("INST=" + INST_ID + " | SL=" + (int)SL_POINTS
                    + " | 2H_FILTER=" );
            System.out.println("日志增强：每笔增加 MFE/MAE（最大浮盈/最大浮亏，点数）");
            System.out.println("进场过滤新增：亚洲盘前半=" + FILTER_ASIA_LOW_LIQ + "（JST " + NO_TRADE_1_START + ":00-" + NO_TRADE_1_END + ":00）"
                    + "（JST " + NO_TRADE_2_START + ":00-" + NO_TRADE_2_END + ":00）");
            System.out.println();

            // 1) 拉一年 1H / 4H
            List<Candle> m30 = fetchRangeCandles(INST_ID, "1H", start.toEpochMilli(), end.toEpochMilli());
            List<Candle> h2  = fetchRangeCandles(INST_ID, "4H",  start.toEpochMilli(), end.toEpochMilli());

            // 只用已收
            m30 = m30.stream().filter(c -> c.confirm == 1).toList();
            long now = end.toEpochMilli(); // ✅ 补上 now（回测用区间尾对齐）
            h2 = filterClosedByTime(h2, BAR4H_MS, now, SAFE_CLOSE_MS);
            if (m30.size() < 800) {
                System.out.println("[ERROR] 数据量不足: 1H=" + m30.size() + " 4H=" + h2.size());
                System.out.println("提示：多半是分页/限流/网络。把 SLEEP_MS 提到 450~650 再试。");
                System.out.println("###############################");
                System.out.println();
                return;
            }

            System.out.println("1H size=" + m30.size()
                    + " first=" + Instant.ofEpochMilli(m30.get(0).ts)
                    + " last=" + Instant.ofEpochMilli(m30.get(m30.size()-1).ts));
            System.out.println("4H size=" + h2.size()
                    + " first=" + (h2.isEmpty() ? "-" : Instant.ofEpochMilli(h2.get(0).ts))
                    + " last="  + (h2.isEmpty() ? "-" : Instant.ofEpochMilli(h2.get(h2.size()-1).ts)));
            System.out.println();

            // 2) 计算 MACD（hist = DIF-DEA，不乘2）
            List<MacdPoint> macd30 = calcMacd(m30);
            List<MacdPoint> macd2h = h2.isEmpty() ? Collections.emptyList() : calcMacd(h2);

            // 3) 回测
            List<CompletedTrade> trades = runBacktestAlarmLogic(m30, macd30, h2, macd2h);

            // 4) 输出统计 + 月度胜率
            printBacktestReport(trades);

            // ✅ 回测完成后，打印 SRMF 最终状态（方便你对齐实盘/下一次 scanHistory）
            System.out.println("[SRMF-END] regime=" + SRMF.getRegime() +
                    " trendPts=" + String.format(Locale.US, "%.2f", SRMF.getTrendPts()) +
                    " mult=" + String.format(Locale.US, "%.2f", SRMF.getCurrentMult()) +
                    " equity=" + String.format(Locale.US, "%.2f", equitySim));

            System.out.println("###############################");
            System.out.println();
        } catch (Exception e) {
            System.out.println("[ERROR] 回测异常: " + e.getMessage());
            e.printStackTrace();
            System.out.println();
        }
    }
    // ===================== 闹铃 =====================
    static void alarmOnce(String tag) { alarm(); }

    static void alarm() {
        try { Toolkit.getDefaultToolkit().beep(); } catch (Exception ignore) {}

        if (!USE_MP3) return;
        try {
            File f = new File(ALARM_MP3);
            if (f.exists() && Desktop.isDesktopSupported()) {
                Desktop.getDesktop().open(f);
            }
        } catch (Exception ignore) {}
    }
}
*/
