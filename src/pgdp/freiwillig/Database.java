package pgdp.freiwillig;

// TODO Imports

import javax.imageio.IIOException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    // TODO have fun :)


    private static HashMap<String, Pair> avg;

    static class Pair {
        long fst, snd;

        Pair() {
            fst = 0;
            snd = 0;
        }

        long ans() {
            return (100 * fst) / snd;
        }
    }

    private static boolean cache = false;

    private static Path baseDataDirectory = Paths.get("C:\\Users\\ACER\\Downloads\\data");


    public static void setBaseDataDirectory(Path _baseDataDirectory) {

        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;


    }


    public static Pair[] segPerCust() {
        try {
            Pair[] a = new Pair[1 << 24];
            //avg = new ConcurrentHashMap<>(1_000_000);
            avg = new HashMap<String, Pair>();
            byte[] b = Files.readAllBytes(baseDataDirectory.resolve("customer.tbl"));

            int j1 = 0;
            int o1 = 0;

            int key = 0;
            //System.out.println(new String(Arrays.copyOfRange(b,0,b.length)));
            for (int i = 0; i < b.length; i++) {

                //System.out.print(ch);

                if (b[i] == '|') {

                    if (j1 == 1) {
                        key = parseInt(b, o1, i);

                    } else if (j1 == 6) {


                        //System.out.println(key + " "+ new String(Arrays.copyOfRange(b,o1,i)));
                        a[key] = avg.computeIfAbsent(new String(Arrays.copyOfRange(b, o1, i)), y -> new Pair());
                        while (b[i] != '\n') ++i;
                        j1 = 0;
                        o1 = i + 1;
                        continue;
                    }
                    ++j1;
                    o1 = i + 1;


                }


            }
            /**
             System.out.println(line);

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
            q += s.charAt(i) - '0'; // - 0
        }
        return q;
    }

    static int parseInt(byte[] s, int start, int end) {
        int i = start;
        int q = 0;

        for (byte c = s[i]; c > '9' || c < '1'; c = s[++i]) ;
        for (; i < end; ++i) {
            q *= 10;
            q += s[i] - '0'; // - 0
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

                Pair[] a = new Pair[1 << 24];


                byte[] b = Files.readAllBytes(baseDataDirectory.resolve("orders.tbl"));

                int j1 = 0;
                int o1 = 0;

                int key1 = 0;
                //System.out.println(new String(Arrays.copyOfRange(b,0,b.length)));
                for (int i = 0; i < b.length; i++) {

                    //System.out.print(ch);

                    if (b[i] == '|') {

                        if (j1 == 0) {
                            key1 = parseInt(b, o1, i);
                            //System.out.println(key1);

                        } else if (j1 == 1) {
                            //System.out.println(key + " "+ new String(Arrays.copyOfRange(b,o1,i)));
                            a[key1] = sPC[parseInt(b, o1, i)];
                            while (b[i] != '\n') ++i;
                            j1 = 0;
                            o1 = i + 1;
                            continue;
                        }
                        ++j1;
                        o1 = i + 1;

                    }
                }

                //var start = System.nanoTime();
                b = Files.readAllBytes(baseDataDirectory.resolve("lineitem.tbl"));
                //var end = System.nanoTime();
                //System.out.println((end - start)/1_000_000);
                //start = System.nanoTime();
                int j2 = 0;
                int o2 = 0;

                Pair key2 = null;
                //System.out.println(new String(Arrays.copyOfRange(b,0,b.length)));
                for (int i = 0; i < b.length; i++) {
                    //char ch = (char) b[i];
                    //System.out.print(ch);

                    if (b[i] == '|') {

                        if (j2 == 0) {
                            key2 = a[parseInt(b, o2, i)];
                            //System.out.println(key2);

                        } else if (j2 == 4) {
                            //System.out.println(key + " "+ new String(Arrays.copyOfRange(b,o1,i)));

                            //System.out.println(tmp);
                            key2.fst += parseInt(b, o2, i);
                            ++key2.snd;
                            while (b[i] != '\n') ++i;
                            j2 = 0;
                            o2 = i + 1;
                            continue;
                        }
                        ++j2;
                        o2 = i + 1;

                    }
                }
                //end = System.nanoTime();
                //System.out.println((end - start)/1_000_000);
            } catch (IOException e) {
                throw new RuntimeException("no file");
            }

            cache = true;
        }
        //System.out.println(averageQuantityPerMarketSegment.size());
        return avg.get(marketsegment).ans();
}
}
