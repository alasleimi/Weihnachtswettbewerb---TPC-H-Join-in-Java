package pgdp.freiwillig;

// TODO Imports

import java.io.FileInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class Database {
    // TODO have fun :)

    private static final ExecutorService executor = Executors.newFixedThreadPool(3);
    private static HashMap<String, Pair> avgSegment;
    private static boolean cache = false;
    private static Path baseDataDirectory = Paths.get("C:\\Users\\ACER\\Downloads\\data");

    public Database() {
    }

    public static void setBaseDataDirectory(Path _baseDataDirectory) {
        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;
    }


    public static PairArr getCustomerToSegment(byte[] customer) {

        PairArr customerToSegment = new PairArr(150_005);
        avgSegment = new HashMap<>();

        int col = 0;
        int colStart = 0;

        int key = 0;

        for (int i = 0; i < customer.length; i++) {

            if (customer[i] == '|') {

                if (col == 1) {

                    key = parseInt(customer, colStart, i);
                } else if (col == 6) {
                    customerToSegment
                            .put(key,
                                    avgSegment.computeIfAbsent(
                                            new String(Arrays.copyOfRange(customer, colStart, i)),
                                            y -> new Pair()));

                    while (customer[i] != '\n') ++i;
                    col = 0;
                    colStart = i + 1;
                    continue;
                }
                ++col;
                colStart = i + 1;

            }

        }

        return customerToSegment;
    }

    public static void main(String[] args) {
        var s = System.nanoTime();
        var db = new Database();
        System.out.println(db.getAverageQuantityPerMarketSegment("AUTOMOBILE"));
        System.out.println(db.getAverageQuantityPerMarketSegment("AUTOMOBILE"));
        System.out.println(db.getAverageQuantityPerMarketSegment("BUILDING"));
        System.out.println(db.getAverageQuantityPerMarketSegment("BUILDING"));
        var e = System.nanoTime();
        System.out.println((e - s) / 1_000_000);
    }


    static int parseInt(byte[] s, int i, int end) {

        int q = 0;

        for (byte c = s[i]; c > '9' || c < '1'; c = s[++i]) ;
        for (; i < end; ++i) {
            q *= 10;
            q += s[i] - '0'; // - 0
        }
        return q;
    }

    PairArr getOrderToSegment(PairArr customerToSegment, byte[] orders) {
        PairArr orderToSegment = new PairArr(6_000_005);

        int col = 0;
        int colStart = 0;

        int key = 0;

        for (int i = 0; i < orders.length; i++) {

            if (orders[i] == '|') {

                if (col == 0) {
                    key = parseInt(orders, colStart, i);
                } else if (col == 1) {

                    orderToSegment.put(key, customerToSegment.get(parseInt(orders, colStart, i)));
                    while (orders[i] != '\n') ++i;
                    col = 0;
                    colStart = i + 1;
                    continue;
                }
                ++col;
                colStart = i + 1;

            }
        }

        return orderToSegment;


    }

    void calculatePerSegment(PairArr orderToSegment, byte[] lineitem) {


        int ft = lineitem.length / 3;
        for (; ft < lineitem.length && lineitem[ft] != '\n'; ++ft) ;
        int tt = ft + (lineitem.length - ft) / 2;
        for (; tt < lineitem.length && lineitem[tt] != '\n'; ++tt) ;


        BiFunction<Integer, Integer, Callable<Object>> f = (Integer s, Integer e) -> () -> {
            int col = 0x0;
            int colStart = s;
            Pair key = null;
            for (int i = s; i < e; i++) {

                if (lineitem[i] == '|') {

                    if (col == 0) {
                        key = orderToSegment.get(parseInt(lineitem, colStart, i));
                    } else if (col == 4) {
                        key.fst.add(parseInt(lineitem, colStart, i));
                        key.snd.increment();
                        while (lineitem[i] != '\n') ++i;
                        col = 0;
                        colStart = i + 1;
                        continue;
                    }
                    ++col;
                    colStart = i + 1;

                }
            }
            return null;
        };


        try {
            executor.invokeAll(List.of(f.apply(0, ft),
                    f.apply(ft, tt),
                    f.apply(tt, lineitem.length)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    Callable<byte[]> readTable(String tableName) {

        return () -> {

            var ch = new FileInputStream(baseDataDirectory.resolve(tableName).toFile()).getChannel();
            int size = (int) ch.size();
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);
            byte[] table = new byte[size];
            buf.get(table);
            return table;

        };
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        if (!cache) {

            var customer = executor.submit(readTable("customer.tbl"));
            var orders = executor.submit(readTable("orders.tbl"));
            var lineitem = executor.submit(readTable("lineitem.tbl"));

            try {
                PairArr customerToSegement = getCustomerToSegment(customer.get());
                PairArr orderToSegment = getOrderToSegment(customerToSegement, orders.get());
                calculatePerSegment(orderToSegment, lineitem.get());

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            cache = true;
        }

        return avgSegment.get(marketsegment).ans();
    }

    static class Pair {
        LongAdder fst = new LongAdder(), snd = new LongAdder();

        long ans() {
            return (100 * fst.longValue()) / snd.longValue();
        }
    }

    static class PairArr {
        final Pair[] fst;
        Pair[] snd;

        // 2 < initSize < INTEGER.MAX_VALUE  - 2
        // because we can't have an array of size > Max_VALUE - 2;
        // this class is  made to handle the cases where a key is > Max_VALUE - 3
        PairArr(int initSz) {
            fst = new Pair[initSz];
        }

        void put(int key, Pair value) {
            if (key < fst.length) {
                fst[key] = value;
            } else {
                key -= fst.length;
                if (snd == null) {
                    //System.out.println("allocate second");
                    snd = new Pair[Integer.MAX_VALUE - fst.length];
                }
                snd[key] = value;
            }
        }

        Pair get(int key) {
            return key < fst.length ? fst[key] : snd[key - fst.length];
        }

    }
}
