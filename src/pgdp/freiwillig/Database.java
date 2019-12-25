package pgdp.freiwillig;

// TODO Imports

import javax.imageio.IIOException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    // TODO have fun :)


    static private ConcurrentHashMap<String, LongAdder> quantPerMarketSegment;
    static private ConcurrentHashMap<String, LongAdder> countPerMarketSegment;
    private static boolean cache = false;

    private static Path baseDataDirectory = Paths.get("C:\\Users\\ACER\\Downloads\\data");


    public static void setBaseDataDirectory(Path _baseDataDirectory) {

        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;


    }

    public static ConcurrentHashMap<Integer, String> segPerCust() {
        try {
            ConcurrentHashMap<Integer, String> a = new ConcurrentHashMap<>(1 << 24);
            Files.lines(baseDataDirectory.resolve("customer.tbl"))
                    .unordered()
                    .parallel()
                    .forEach(x -> {
                        int key = 0;
                        int j = 0;
                        int o = 0;
                        for (int i = 0; ; i++) {

                            if (x.charAt(i) == '|') {
                                if (j == 1) {
                                    key = parseInt(x, o, i);
                                } else if (j == 6) {
                                    a.put(key, x.substring(o, i));
                                    break;
                                }
                                o = i + 1;
                                ++j;

                            }


                        }

                    });
            /**.collect(Collectors.toConcurrentMap(x -> parseInt(x[0], 0, x[0].length()),
             x -> x[1], (x, v) -> v,
             () -> new ConcurrentHashMap<Integer, String>(1 << 24)));**/

            return a;


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

    static int parseInt(String s, int start, int end) {
        int i = start;
        int q = 0;

        for (char c = s.charAt(i); c > '9' || c < '1'; c = s.charAt(++i)) ;
        for (; i < end; ++i) {
            q *= 10;
            q += s.charAt(i) - '0';
        }
        return q;
    }


    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        if (!cache) {
            //var a = custPerOrder();
            //System.out.println(a.size());
            var sPC = segPerCust();
            // System.out.println(b.size());
            try {

                ConcurrentHashMap<Integer, String> a = new ConcurrentHashMap<>(1 << 24);
                Files.lines(baseDataDirectory.resolve("orders.tbl"))
                        .unordered()
                        .parallel()
                        .forEach(x -> {
                            int key = 0;
                            int j = 0;
                            int o = 0;
                            for (int i = 0, n = x.length(); i < n; i++) {

                                if (x.charAt(i) == '|') {
                                    if (j == 0) {
                                        key = parseInt(x, o, i);
                                    } else if (j == 1) {
                                        a.put(key, sPC.get(parseInt(x, o, i)));
                                        break;
                                    }
                                    o = i + 1;
                                    ++j;

                                }


                            }

                        });


                countPerMarketSegment = new ConcurrentHashMap<>(1_000_000);
                quantPerMarketSegment = new ConcurrentHashMap<>(1_000_000);
                Files.lines(baseDataDirectory.resolve("lineitem.tbl"))
                        .unordered()
                        .parallel()
                        .forEach(x -> {
                            //custom split
                            String key = null;
                            int j = 0;
                            int o = 0;
                            for (int i = 0, n = x.length(); i < n; i++) {

                                if (x.charAt(i) == '|') {
                                    if (j == 0) {
                                        key = a.get(parseInt(x, o, i));
                                    } else if (j == 4) {
                                        quantPerMarketSegment.computeIfAbsent(key, y -> new LongAdder()).add(
                                                parseInt(x, o, i)
                                        );
                                        countPerMarketSegment.computeIfAbsent(key, y -> new LongAdder()).increment();
                                        break;
                                    }
                                    o = i + 1;
                                    ++j;

                                }


                            }
                            //System.out.println(answer[0] + " " + answer[1]);

                        });
            } catch (IOException e) {
                throw new RuntimeException("no file");
            }

            cache = true;
        }
        //System.out.println(averageQuantityPerMarketSegment.size());
        return (100 * quantPerMarketSegment.get(marketsegment).longValue()) /
                countPerMarketSegment.get(marketsegment).longValue();
    }
}
