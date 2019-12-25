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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    // TODO have fun :)


    private static ConcurrentMap<String, Pair> avg;

    static class Pair {
        LongAdder fst, snd;

        Pair() {
            fst = new LongAdder();
            snd = new LongAdder();
        }

        long ans() {
            return (100 * fst.longValue()) / snd.longValue();
        }
    }

    private static boolean cache = false;

    private static Path baseDataDirectory = Paths.get("C:\\Users\\ACER\\Downloads\\data");


    public static void setBaseDataDirectory(Path _baseDataDirectory) {

        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;


    }

    public static ConcurrentHashMap<Integer, Pair> segPerCust() {
        try {
            ConcurrentHashMap<Integer, Pair> a = new ConcurrentHashMap<>();
            //avg = new ConcurrentHashMap<>(1_000_000);
            avg = new ConcurrentSkipListMap<>();

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
                                    a.put(key, avg.computeIfAbsent(x.substring(o, i), y -> new Pair()));
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

                ConcurrentHashMap<Integer, Pair> a = new ConcurrentHashMap<>();
                Files.lines(baseDataDirectory.resolve("orders.tbl"))
                        .unordered()
                        .parallel()
                        .forEach(x -> {
                            int key = 0;
                            int j = 0;
                            int o = 0;
                            for (int i = 0; ; i++) {

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


                Files.lines(baseDataDirectory.resolve("lineitem.tbl"))
                        .unordered()
                        .parallel()
                        .forEach(x -> {
                            //custom split
                            Pair key = null;
                            int j = 0;
                            int o = 0;
                            for (int i = 0; ; i++) {

                                if (x.charAt(i) == '|') {
                                    if (j == 0) {
                                        key = a.get(parseInt(x, o, i));
                                    } else if (j == 4) {
                                        key.fst.add(parseInt(x, o, i));
                                        key.snd.increment();
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
        return avg.get(marketsegment).ans();
}
}
