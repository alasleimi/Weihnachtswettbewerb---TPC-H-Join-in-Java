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

    static private Map<String, Long> averageQuantityPerMarketSegment;
    static private ConcurrentHashMap<String, LongAdder> quantPerMarketSegment;
    static private ConcurrentHashMap<String, LongAdder> countPerMarketSegment;
    private static boolean cache = false;

    private static Path baseDataDirectory = Paths.get("C:\\Users\\ACER\\Downloads\\data");


    public static void setBaseDataDirectory(Path _baseDataDirectory) {

        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;


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

                            if (x.charAt(i) == '|') {
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
                    .collect(Collectors.toConcurrentMap(x -> parseInt(x[0], 0, x[0].length()),
                            x -> x[1], (x, v) -> v,
                            () -> new ConcurrentHashMap<Integer, String>(1 << 24)));


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


                var a = Files.lines(baseDataDirectory.resolve("orders.tbl"))
                        .unordered()
                        .parallel()
                        .map(x -> {
                            int[] answer = new int[2];
                            int j = 0;
                            int o = 0;
                            for (int i = 0, n = x.length(); i < n; i++) {

                                if (x.charAt(i) == '|') {
                                    if (j == 0) {
                                        answer[0] = parseInt(x, o, i);
                                    } else if (j == 1) {
                                        answer[1] = parseInt(x, o, i);
                                        break;
                                    }
                                    o = i + 1;
                                    ++j;

                                }


                            }
                            //System.out.println(answer[0] + " " + answer[1]);
                            return answer;


                        })
                        .collect(Collectors.toConcurrentMap(x -> x[0], x -> sPC.get(x[1]), (x, v) -> v,
                                () -> new ConcurrentHashMap<Integer, String>(1 << 24)));

                countPerMarketSegment = new ConcurrentHashMap<>(1_000_000);
                quantPerMarketSegment = new ConcurrentHashMap<>(1_000_000);
                Files.lines(baseDataDirectory.resolve("lineitem.tbl"))
                        .unordered()
                        .parallel()
                        .map(x -> {
                            //custom split
                            int[] answer = new int[2];
                            int j = 0;
                            int o = 0;
                            for (int i = 0, n = x.length(); i < n; i++) {

                                if (x.charAt(i) == '|') {
                                    if (j == 0) {
                                        answer[0] = parseInt(x, o, i);
                                    } else if (j == 4) {
                                        answer[1] = parseInt(x, o, i);
                                        break;
                                    }
                                    o = i + 1;
                                    ++j;

                                }


                            }
                            //System.out.println(answer[0] + " " + answer[1]);
                            return answer;
                        })/**.collect(Collectors.groupingBy(x -> a.get(x[0]),
                 () -> new HashMap<>(1_000_000),
                 Collector.of(() -> new long[2],
                 (u, t) -> {
                 u[0] += t[1];
                 ++u[1];
                 },
                 (u, b) -> {
                 u[0] += b[0];
                 u[1] += b[1];
                 return u;
                 },
                 u -> (100 * u[0]) / u[1])

                 )
                 );**/
                        .forEach(x -> {
                            var tmp = a.get(x[0]);
                            quantPerMarketSegment.computeIfAbsent(tmp, y -> new LongAdder()).add(x[1]);
                            countPerMarketSegment.computeIfAbsent(tmp, y -> new LongAdder()).increment();

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
