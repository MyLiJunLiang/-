package eval;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * CoinalyzeFindOkxEthPerpSymbol_v3
 *
 * Goal:
 *  - Coinalyze "future-markets" sometimes returns exchange as a CODE (e.g. "3", "T", "H"),
 *    not a human name like "OKX". You must map code -> name using /v1/exchanges.
 *
 * What this tool does:
 *  1) Call /v1/exchanges and build map: exchange_code -> exchange_name
 *  2) Call /v1/future-markets and filter candidates by:
 *        - base_asset=ETH
 *        - quote_asset in (USDT, USD) (configurable)
 *        - is_perpetual = true (configurable)
 *        - exchange_name contains "OKX" or "OKEX" (configurable)
 *  3) Print best matches and a diagnostic summary.
 *
 * Run (IDEA / CLI):
 *  -Dcoinalyze.apiKey=YOUR_API_KEY
 *  Optional:
 *  -Dcoinalyze.exchangeNeed=OKX          (default OKX)
 *  -Dcoinalyze.altExchangeNeed=OKEX      (default OKEX)
 *  -Dcoinalyze.base=ETH                 (default ETH)
 *  -Dcoinalyze.quote=USDT               (default USDT)
 *  -Dcoinalyze.quote2=USD               (default USD)
 *  -Dcoinalyze.perpetualOnly=true       (default true)
 *  -Dcoinalyze.maxPrint=30              (default 30)
 *
 * Tips:
 *  - If you get "Invalid/Missing API key": your key is wrong or not provided.
 *  - If exchanges endpoint returns but OKX is missing: your plan/key may not include OKX data.
 */
public class CoinalyzeFindOkxEthPerpSymbol{

    static String sys(String k, String def) {
        String v = System.getProperty(k);
        return (v == null || v.trim().isEmpty()) ? def : v.trim();
    }

    static boolean sysBool(String k, boolean def) {
        String v = System.getProperty(k);
        if (v == null || v.trim().isEmpty()) return def;
        v = v.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("y");
    }

    static int sysInt(String k, int def) {
        String v = System.getProperty(k);
        if (v == null || v.trim().isEmpty()) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    static String httpGet(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setConnectTimeout(15000);
        con.setReadTimeout(15000);

        int code = con.getResponseCode();
        BufferedReader br = new BufferedReader(new InputStreamReader(
                (code >= 200 && code < 300) ? con.getInputStream() : con.getErrorStream(),
                StandardCharsets.UTF_8
        ));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line);
        br.close();

        if (code < 200 || code >= 300) {
            throw new RuntimeException("HTTP " + code + " for " + urlStr + " body=" + sb);
        }
        return sb.toString();
    }

    static Map<String, String> fetchExchangeMap(String apiKey) throws Exception {
        String url = "https://api.coinalyze.net/v1/exchanges?api_key=" + apiKey;
        String body = httpGet(url);
        JSONArray arr = new JSONArray(body);
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject o = arr.getJSONObject(i);
            // fields may vary, but typically: { "code": "...", "name": "..." }
            String code = o.optString("code", o.optString("id", o.optString("exchange", "")));
            String name = o.optString("name", o.optString("exchange", ""));
            if (!code.isEmpty()) map.put(code, name);
        }
        return map;
    }

    static JSONArray fetchFutureMarkets(String apiKey) throws Exception {
        String url = "https://api.coinalyze.net/v1/future-markets?api_key=" + apiKey;
        String body = httpGet(url);
        return new JSONArray(body);
    }

    static String exchangeName(Map<String, String> exMap, String exchangeCode) {
        if (exchangeCode == null) return "";
        String name = exMap.get(exchangeCode);
        return (name == null) ? exchangeCode : name;
    }

    public static void main(String[] args) throws Exception {
        String apiKey = sys("coinalyze.apiKey", "");
        if (apiKey.isEmpty()) {
            System.out.println("Missing -Dcoinalyze.apiKey=YOUR_API_KEY");
            return;
        }

        String exchangeNeed = sys("coinalyze.exchangeNeed", "OKX");
        String altExchangeNeed = sys("coinalyze.altExchangeNeed", "OKEX");
        String baseNeed = sys("coinalyze.base", "ETH");
        String quoteNeed1 = sys("coinalyze.quote", "USDT");
        String quoteNeed2 = sys("coinalyze.quote2", "USD");
        boolean perpetualOnly = sysBool("coinalyze.perpetualOnly", true);
        int maxPrint = sysInt("coinalyze.maxPrint", 30);

        System.out.println("Fetching: /v1/exchanges");
        Map<String, String> exMap = fetchExchangeMap(apiKey);
        System.out.println("Exchanges returned: " + exMap.size());

        // quick diagnostic: list exchanges that contain "OK"
        List<Map.Entry<String, String>> okLike = exMap.entrySet().stream()
                .filter(e -> (e.getValue() != null && e.getValue().toUpperCase().contains("OK"))
                        || (e.getKey() != null && e.getKey().toUpperCase().contains("OK")))
                .collect(Collectors.toList());

        System.out.println("Exchanges containing 'OK' (code -> name):");
        if (okLike.isEmpty()) {
            System.out.println("  (none)  -> Your plan/key may not include OKX/OKEX, or Coinalyze uses a different naming.");
        } else {
            for (Map.Entry<String, String> e : okLike) {
                System.out.println("  " + e.getKey() + " -> " + e.getValue());
            }
        }

        System.out.println("\nFetching: /v1/future-markets");
        JSONArray markets = fetchFutureMarkets(apiKey);
        System.out.println("Total markets: " + markets.length());
        System.out.println("Filters: exchange contains (" + exchangeNeed + " or " + altExchangeNeed + "), base=" + baseNeed
                + ", quote in (" + quoteNeed1 + "," + quoteNeed2 + "), perpetualOnly=" + perpetualOnly);

        class Cand {
            JSONObject o;
            String exCode;
            String exName;
            String symbol;
            String symbolOnExchange;
            String base;
            String quote;
            boolean perp;
        }

        List<Cand> all = new ArrayList<>();
        for (int i = 0; i < markets.length(); i++) {
            JSONObject o = markets.getJSONObject(i);

            Cand c = new Cand();
            c.o = o;
            c.exCode = o.optString("exchange", "");
            c.exName = exchangeName(exMap, c.exCode);
            c.symbol = o.optString("symbol", "");
            c.symbolOnExchange = o.optString("symbol_on_exchange", "");
            c.base = o.optString("base_asset", "");
            c.quote = o.optString("quote_asset", "");
            c.perp = o.optBoolean("is_perpetual", false);
            all.add(c);
        }

        String exNeedU = exchangeNeed.toUpperCase();
        String altNeedU = altExchangeNeed.toUpperCase();

        List<Cand> matched = all.stream()
                .filter(c -> c.base.equalsIgnoreCase(baseNeed))
                .filter(c -> c.quote.equalsIgnoreCase(quoteNeed1) || c.quote.equalsIgnoreCase(quoteNeed2))
                .filter(c -> !perpetualOnly || c.perp)
                .filter(c -> {
                    String n = (c.exName == null ? "" : c.exName.toUpperCase());
                    return n.contains(exNeedU) || n.contains(altNeedU);
                })
                .collect(Collectors.toList());

        System.out.println("\nMatched: " + matched.size());
        if (matched.isEmpty()) {
            System.out.println("No matches found. Next steps:");
            System.out.println("  1) Print top ETH perpetual candidates across ALL exchanges so you can see what exchange names look like.");
            System.out.println("  2) Verify your exchangeNeed naming by looking at the 'Exchanges containing OK' list above.");
            System.out.println("  3) Your plan may not include OKX data (exchanges endpoint missing OKX).");
        } else {
            matched.sort(Comparator.comparing(a -> a.symbol));
            int n = Math.min(maxPrint, matched.size());
            System.out.println("Top " + n + " matches:");
            for (int i = 0; i < n; i++) {
                Cand c = matched.get(i);
                System.out.println("[" + (i + 1) + "] symbol=" + c.symbol
                        + " | exchange=" + c.exName + " (code=" + c.exCode + ")"
                        + " | symbol_on_exchange=" + c.symbolOnExchange
                        + " | base=" + c.base + " quote=" + c.quote
                        + " | is_perpetual=" + c.perp);
            }
        }

        // Always print a fallback: ETH perpetual markets across all exchanges, useful to see what to filter by
        List<Cand> ethPerpAll = all.stream()
                .filter(c -> c.base.equalsIgnoreCase(baseNeed))
                .filter(c -> !perpetualOnly || c.perp)
                .filter(c -> c.quote.equalsIgnoreCase(quoteNeed1) || c.quote.equalsIgnoreCase(quoteNeed2))
                .collect(Collectors.toList());

        // group by exchange name
        Map<String, Long> byEx = ethPerpAll.stream()
                .collect(Collectors.groupingBy(c -> (c.exName == null || c.exName.isEmpty()) ? c.exCode : c.exName,
                        LinkedHashMap::new, Collectors.counting()));

        System.out.println("\nETH perp candidates by exchange (count):");
        byEx.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(25)
                .forEach(e -> System.out.println("  " + e.getKey() + " -> " + e.getValue()));

        System.out.println("\nSample ETH perp candidates (first 15):");
        ethPerpAll.stream().limit(15).forEach(c -> {
            System.out.println("  symbol=" + c.symbol + " | ex=" + c.exName + " (code=" + c.exCode + ") | onEx=" + c.symbolOnExchange);
        });

        System.out.println("\nDONE.");
    }
}
