package pgdp.freiwillig;

// TODO Imports

import javax.imageio.IIOException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    // TODO have fun :)

    static private ConcurrentMap<String, Long> averageQuantityPerMarketSegment;
    private static boolean cache = false;

    private static Path baseDataDirectory = Paths.get("data");


    public static void setBaseDataDirectory(Path _baseDataDirectory) {

        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;


    }




    public static Map<String, Integer> custPerOrder() {
        try {
            return Files.lines(baseDataDirectory.resolve("orders.tbl"))
                    .unordered()
                    .parallel()
                    .map(x -> x.split("\\|"))
                    .collect(Collectors.toConcurrentMap(x -> x[0], x -> parseInt(x[1]), (x, v) -> v,
                            () -> new ConcurrentHashMap<String, Integer>(1 << 23)));


        } catch (IOException e) {

            throw new RuntimeException("file not found");
        }
        // TODO
    }

    public static Map<Integer, String> segPerCust() {
        try {
            return Files.lines(baseDataDirectory.resolve("customer.tbl"))
                    .unordered()
                    .parallel()
                    .map(x -> {
                        String[] answer = new String[2];
                        int j = 0;
                        int o = 0;
                        for (int i = 0; i < x.length(); i++) {
                            char c = x.charAt(i);
                            if (c == '|') {
                                if (j == 1) {
                                    answer[0] = x.substring(o, i);
                                } else if (j == 6) {
                                    answer[1] = x.substring(o, i);
                                    break;
                                }
                                o = i + 1;
                                ++j;

                            }


                        }
                        //System.out.println(answer[0] + " " + answer[1]);
                        return answer;
                    })
                    .collect(Collectors.toConcurrentMap(x -> parseInt(x[0]), x -> x[1], (x, v) -> v,
                            () -> new ConcurrentHashMap<Integer, String>(1 << 23)));


        } catch (IOException e) {

            throw new RuntimeException("file not found");
        }
        // TODO
    }


    public Database() {
    }

    public static void main(String[] args) {
        //processInputFileOrders().forEach(System.out::println);
        var s = System.nanoTime();
        var db = new Database();
        System.out.println(db.getAverageQuantityPerMarketSegment("AUTOMOBILE"));
        System.out.println(db.getAverageQuantityPerMarketSegment("AUTOMOBILE"));
        System.out.println(db.getAverageQuantityPerMarketSegment("BUILDING"));
        System.out.println(db.getAverageQuantityPerMarketSegment("BUILDING"));
        var e = System.nanoTime();
        System.out.println((e - s) / 1_000_000);
    }

    static int parseInt(String s) {
        int i = 0;
        int q = 0;
        for (; s.charAt(i) > '9' || s.charAt(i) < '1'; ++i) ;
        for (; i < s.length(); ++i) {

            if (s.charAt(i) <= '9' || s.charAt(i) >= '1') {
                q *= 10;
                q += s.charAt(i) - '0';
            }

        }
        return q;


    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        if (!cache) {
            var a = custPerOrder();
            //System.out.println(a.size());
            var b = segPerCust();
            // System.out.println(b.size());
            try {
                averageQuantityPerMarketSegment = Files.lines(baseDataDirectory.resolve("lineitem.tbl"))
                        .unordered()
                        .parallel()
                        .map(x -> {
                            String[] answer = new String[2];
                            int j = 0;
                            int o = 0;
                            for (int i = 0, n = x.length(); i < n; i++) {

                                if (x.charAt(i) == '|') {
                                    if (j == 0) {
                                        answer[0] = x.substring(o, i);
                                    } else if (j == 4) {
                                        answer[1] = x.substring(o, i);
                                        break;
                                    }
                                    o = i + 1;
                                    ++j;

                                }


                            }
                            //System.out.println(answer[0] + " " + answer[1]);
                            return answer;
                        }).collect(Collectors.groupingByConcurrent(x -> b.get(a.get(x[0])),
                                () -> new ConcurrentHashMap<>(1_000_000),
                                Collectors.teeing(
                                        Collectors.summingLong(x -> parseInt(x[1]) * 100),
                                        Collectors.counting(),
                                        (s, c) -> s / c

                                )
                        ));
            } catch (IOException e) {
                throw new RuntimeException("no file");
            }

            cache = true;
        }
        //System.out.println(averageQuantityPerMarketSegment.size());
        return averageQuantityPerMarketSegment.get(marketsegment);
    }
}
