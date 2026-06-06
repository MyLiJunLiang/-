package eval;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * ExitMiningAutoPicker
 *
 * 读取 exit_mining_overall.csv / exit_mining_long.csv / exit_mining_short.csv，
 * 自动筛选 + 打分 + 输出 picked_*.csv，并在控制台输出 Top20（中文）。
 *
 * ✅ 兼容你 v3.2/v3.3 输出列：
 * id,group,k,lossGate,totalN,triggers,avgEarlierBars,baseFinalEq,newFinalEq,profitImpactPct,baseMaxDD,newMaxDD,ddImprove,baseWinRate,newWinRate,basePF,newPF,blowup,minEquity,peakEquity,sameBarSL
 *
 * 使用示例：
 *   java eval.ExitMiningAutoPicker --inDir=./out --top=50
 *
 * 默认输入文件：
 *   out/exit_mining_overall.csv
 *   out/exit_mining_long.csv
 *   out/exit_mining_short.csv
 */
public class ExitMiningAutoPicker {

    // ----------- args -----------
    private static final String ARG_IN_DIR = "--inDir=";
    private static final String ARG_TOP = "--top=";

    // ----------- filters (稳健默认，可自行改) -----------
    private static final double PF_MIN = 1.05;
    private static final double DD_MIN = -0.80;            // newMaxDD 必须 > -0.80
    private static final double TRIG_RATE_MAX = 0.80;      // triggers/totalN <= 0.80
    private static final int TOP_PRINT = 20;

    // ----------- scoring weights -----------
    // 均衡榜：更偏 PF + DD
    private static final double W_PF_BAL = 0.45;
    private static final double W_DD_BAL = 0.35;
    private static final double W_PCT_BAL = 0.15;
    private static final double W_TRIG_PEN_BAL = 0.20;
    private static final double LG_BONUS_BAL = 0.05; // LG1 bonus

    // 防守榜：更偏 DD
    private static final double W_PF_DEF = 0.35;
    private static final double W_DD_DEF = 0.45;
    private static final double W_PCT_DEF = 0.10;
    private static final double W_TRIG_PEN_DEF = 0.25;
    private static final double LG_BONUS_DEF = 0.07;

    // 进攻榜：更偏收益，但更严 trig 惩罚，防止乱砍
    private static final double W_PF_ATK = 0.35;
    private static final double W_DD_ATK = 0.25;
    private static final double W_PCT_ATK = 0.25;
    private static final double W_TRIG_PEN_ATK = 0.30;
    private static final double LG_BONUS_ATK = 0.02;

    // ----------- model -----------
    static final class Row {
        String id;
        String group;
        int k;
        String lossGate; // "LG0" / "LG1"
        int totalN;
        int triggers;
        double avgEarlierBars;

        double baseFinalEq;
        double newFinalEq;
        double profitImpactPct;

        double baseMaxDD;
        double newMaxDD;
        double ddImprove;

        double baseWinRate;
        double newWinRate;

        double basePF;
        double newPF;

        int blowup;
        double minEquity;
        double peakEquity;

        int sameBarSL;

        // computed
        double trigRate;
        double scoreBal;
        double scoreDef;
        double scoreAtk;
        boolean dominated;
    }

    public static void main(String[] args) throws Exception {
        String inDir = ".";
        int top = 50;

        for (String a : args) {
            if (a.startsWith(ARG_IN_DIR)) inDir = a.substring(ARG_IN_DIR.length());
            else if (a.startsWith(ARG_TOP)) top = Integer.parseInt(a.substring(ARG_TOP.length()));
        }

        String overall = inDir + "/out/exit_mining_overall.csv";
        String longs = inDir + "/out/exit_mining_long.csv";
        String shorts = inDir + "/out/exit_mining_short.csv";

        runOne("OVERALL（全体）", overall, inDir + "/picked_overall.csv", top);
        runOne("LONG_ONLY（仅做多）", longs, inDir + "/picked_long.csv", top);
        runOne("SHORT_ONLY（仅做空）", shorts, inDir + "/picked_short.csv", top);

        System.out.println("\n✅ 自动挑选完成：");
        System.out.println(" - " + inDir + "/picked_overall.csv");
        System.out.println(" - " + inDir + "/picked_long.csv");
        System.out.println(" - " + inDir + "/picked_short.csv");
    }

    private static void runOne(String title, String inPath, String outPath, int top) throws Exception {
        File f = new File(inPath);
        if (!f.exists()) {
            System.out.println("\n[" + title + "] 跳过：找不到文件 " + inPath);
            return;
        }

        List<Row> rows = readCsv(inPath);

        // compute trigRate
        for (Row r : rows) {
            r.trigRate = (r.totalN <= 0) ? 0.0 : (r.triggers * 1.0 / r.totalN);
        }

        // hard filter
        List<Row> filtered = new ArrayList<>();
        for (Row r : rows) {
            if (r.blowup != 0) continue;
            if (r.newPF < PF_MIN) continue;
            if (!(r.newMaxDD > DD_MIN)) continue;
            if (!(r.trigRate <= TRIG_RATE_MAX)) continue;
            filtered.add(r);
        }

        // Pareto remove dominated (optional but helpful)
        markDominated(filtered);

        List<Row> kept = new ArrayList<>();
        for (Row r : filtered) if (!r.dominated) kept.add(r);

        // score
        for (Row r : kept) {
            r.scoreBal = score(r, W_PF_BAL, W_DD_BAL, W_PCT_BAL, W_TRIG_PEN_BAL, LG_BONUS_BAL, false);
            r.scoreDef = score(r, W_PF_DEF, W_DD_DEF, W_PCT_DEF, W_TRIG_PEN_DEF, LG_BONUS_DEF, true);  // defense prefers LG1 but not forced
            r.scoreAtk = score(r, W_PF_ATK, W_DD_ATK, W_PCT_ATK, W_TRIG_PEN_ATK, LG_BONUS_ATK, false);
        }

        // sort and take
        List<Row> bal = new ArrayList<>(kept);
        List<Row> def = new ArrayList<>(kept);
        List<Row> atk = new ArrayList<>(kept);

        bal.sort((a,b)->Double.compare(b.scoreBal, a.scoreBal));
        def.sort((a,b)->Double.compare(b.scoreDef, a.scoreDef));
        atk.sort((a,b)->Double.compare(b.scoreAtk, a.scoreAtk));

        int lim = Math.min(top, bal.size());

        // write out (three sections)
        writePicked(outPath, title, bal, def, atk, lim);

        // console Top20
        System.out.println("\n==================== 自动挑选：" + title + " ====================");
        System.out.println("输入总行数=" + rows.size()
                + " | 过滤后=" + filtered.size()
                + " | 去劣后=" + kept.size()
                + " | 输出Top=" + lim
                + " | 过滤阈值：PF>=" + PF_MIN + " DD>" + DD_MIN + " trigRate<=" + TRIG_RATE_MAX);

        printTop("均衡榜（PF+DD优先）", bal, Math.min(TOP_PRINT, bal.size()), "BAL");
        printTop("防守榜（更看重DD）", def, Math.min(TOP_PRINT, def.size()), "DEF");
        printTop("进攻榜（更看重收益）", atk, Math.min(TOP_PRINT, atk.size()), "ATK");
    }

    private static void printTop(String title, List<Row> list, int n, String mode) {
        System.out.println("\n--- " + title + " Top" + n + " ---");
        for (int i = 0; i < n; i++) {
            Row r = list.get(i);
            double sc = "DEF".equals(mode) ? r.scoreDef : ("ATK".equals(mode) ? r.scoreAtk : r.scoreBal);
            System.out.printf(Locale.ROOT,
                    "#%02d score=%.4f | %s | trig=%d(%.0f%%) 提前=%.2f根 | Eq=%.3f | DD=%.2f%% | WR=%.2f%% | PF=%.3f | sameBarSL=%d | %s%n",
                    i+1, sc,
                    r.lossGate,
                    r.triggers, r.trigRate*100.0,
                    r.avgEarlierBars,
                    r.newFinalEq,
                    r.newMaxDD*100.0,
                    r.newWinRate*100.0,
                    r.newPF,
                    r.sameBarSL,
                    r.id);
        }
    }

    // -------- scoring --------
    private static double score(Row r,
                                double wPf, double wDd, double wPct, double wTrigPen, double lgBonus,
                                boolean defenseMode) {

        // S(pf) : PF=1 -> 0, PF=2 -> 1
        double sPf = clamp((r.newPF - 1.0) / 1.0, 0, 1);

        // S(-dd) : dd = -newMaxDD (0~1), dd<=0.70 => good, dd=0.70 -> 0, dd=0 -> 1
        double dd = -r.newMaxDD;
        double sDd = clamp((0.70 - dd) / 0.70, 0, 1);

        // S(pct) : -10% -> 0, +10% -> 1
        double sPct = clamp((r.profitImpactPct + 10.0) / 20.0, 0, 1);

        // P(trigRate) : <=30% no penalty, >=80% full penalty
        double pTrig = clamp((r.trigRate - 0.30) / 0.50, 0, 1);

        double lg = "LG1".equalsIgnoreCase(r.lossGate) ? lgBonus : 0.0;

        // 防守榜对 LG0 稍微扣分（不强制过滤，避免错过一些好方案）
        if (defenseMode && "LG0".equalsIgnoreCase(r.lossGate)) lg -= 0.03;

        return wPf * sPf + wDd * sDd + wPct * sPct - wTrigPen * pTrig + lg;
    }

    private static double clamp(double x, double lo, double hi) {
        if (x < lo) return lo;
        if (x > hi) return hi;
        return x;
    }

    // -------- pareto --------
    private static void markDominated(List<Row> rows) {
        // A dominates B if: Eq>=, DD>= (less negative is bigger), PF>= and at least one strictly better
        int n = rows.size();
        for (int i = 0; i < n; i++) rows.get(i).dominated = false;

        for (int i = 0; i < n; i++) {
            Row a = rows.get(i);
            if (a.dominated) continue;
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                Row b = rows.get(j);
                if (b.dominated) continue;

                if (dominates(a, b)) {
                    b.dominated = true;
                }
            }
        }
    }

    private static boolean dominates(Row a, Row b) {
        boolean geEq = a.newFinalEq >= b.newFinalEq;
        boolean geDd = a.newMaxDD >= b.newMaxDD;
        boolean gePf = a.newPF >= b.newPF;
        if (!(geEq && geDd && gePf)) return false;

        boolean strict = (a.newFinalEq > b.newFinalEq) || (a.newMaxDD > b.newMaxDD) || (a.newPF > b.newPF);
        return strict;
    }

    // -------- csv io --------
    private static List<Row> readCsv(String path) throws Exception {
        List<Row> out = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String line;
            int headerIdx = -1;
            List<String> headers = null;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.startsWith("#")) continue;

                if (headers == null) {
                    headers = parseCsvLine(line);
                    headerIdx = 0;
                    continue;
                }
                Row r = parseRow(headers, line);
                if (r != null) out.add(r);
            }
        }
        return out;
    }

    private static Row parseRow(List<String> headers, String line) {
        List<String> cols = parseCsvLine(line);
        if (cols.size() < headers.size()) {
            // tolerate missing tail
            while (cols.size() < headers.size()) cols.add("");
        }
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            m.put(headers.get(i), i < cols.size() ? cols.get(i) : "");
        }

        Row r = new Row();
        r.id = m.getOrDefault("id", "");
        r.group = m.getOrDefault("group", "");
        r.k = toInt(m.get("k"));
        r.lossGate = m.getOrDefault("lossGate", "");

        r.totalN = toInt(m.get("totalN"));
        r.triggers = toInt(m.get("triggers"));
        r.avgEarlierBars = toDouble(m.get("avgEarlierBars"));

        r.baseFinalEq = toDouble(m.get("baseFinalEq"));
        r.newFinalEq = toDouble(m.get("newFinalEq"));
        r.profitImpactPct = toDouble(m.get("profitImpactPct"));

        r.baseMaxDD = toDouble(m.get("baseMaxDD"));
        r.newMaxDD = toDouble(m.get("newMaxDD"));
        r.ddImprove = toDouble(m.get("ddImprove"));

        r.baseWinRate = toDouble(m.get("baseWinRate"));
        r.newWinRate = toDouble(m.get("newWinRate"));

        r.basePF = toDouble(m.get("basePF"));
        r.newPF = toDouble(m.get("newPF"));

        r.blowup = toInt(m.get("blowup"));
        r.minEquity = toDouble(m.get("minEquity"));
        r.peakEquity = toDouble(m.get("peakEquity"));

        r.sameBarSL = toInt(m.getOrDefault("sameBarSL", "0"));

        return r;
    }

    private static void writePicked(String outPath, String title,
                                    List<Row> bal, List<Row> def, List<Row> atk, int top) throws Exception {
        File out = new File(outPath);
        File parent = out.getParentFile();
        if (parent != null) parent.mkdirs();

        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8))) {
            pw.println("# AutoPicked " + title);
            pw.println("# Filters: blowup=0, newPF>=" + PF_MIN + ", newMaxDD>" + DD_MIN + ", trigRate<=" + TRIG_RATE_MAX);
            pw.println("# Sections: BALANCE / DEFENSE / ATTACK");
            pw.println();

            writeSection(pw, "BALANCE（均衡榜：PF+DD优先）", bal, top, "BAL");
            pw.println();
            writeSection(pw, "DEFENSE（防守榜：更看重DD，偏LG1）", def, top, "DEF");
            pw.println();
            writeSection(pw, "ATTACK（进攻榜：更看重收益，但惩罚高触发）", atk, top, "ATK");
        }
    }

    private static void writeSection(PrintWriter pw, String sectionTitle, List<Row> list, int top, String mode) {
        pw.println("## " + sectionTitle);
        pw.println("rank,score,lossGate,triggers,trigRate,avgEarlierBars,newFinalEq,newMaxDD,newWinRate,newPF,sameBarSL,id");
        int lim = Math.min(top, list.size());
        for (int i = 0; i < lim; i++) {
            Row r = list.get(i);
            double sc = "DEF".equals(mode) ? r.scoreDef : ("ATK".equals(mode) ? r.scoreAtk : r.scoreBal);
            pw.printf(Locale.ROOT,
                    "%d,%.6f,%s,%d,%.6f,%.4f,%.8f,%.6f,%.6f,%.6f,%d,%s%n",
                    i + 1, sc, r.lossGate, r.triggers, r.trigRate, r.avgEarlierBars,
                    r.newFinalEq, r.newMaxDD, r.newWinRate, r.newPF, r.sameBarSL,
                    csvEscape(r.id));
        }
    }

    // -------- csv parsing (simple RFC4180-ish) --------
    private static List<String> parseCsvLine(String line) {
        ArrayList<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQ = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (inQ) {
                if (ch == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQ = false;
                    }
                } else {
                    cur.append(ch);
                }
            } else {
                if (ch == ',') {
                    out.add(cur.toString());
                    cur.setLength(0);
                } else if (ch == '"') {
                    inQ = true;
                } else {
                    cur.append(ch);
                }
            }
        }
        out.add(cur.toString());
        return out;
    }

    private static String csvEscape(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static int toInt(String s) {
        if (s == null) return 0;
        s = s.trim();
        if (s.isEmpty()) return 0;
        try { return Integer.parseInt(s); } catch (Exception e) { return 0; }
    }

    private static double toDouble(String s) {
        if (s == null) return 0.0;
        s = s.trim();
        if (s.isEmpty()) return 0.0;
        try { return Double.parseDouble(s); } catch (Exception e) { return 0.0; }
    }
}
