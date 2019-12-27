package pgdp.freiwillig;

// TODO Imports

import sun.misc.Unsafe;

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;


public class Database {
    // TODO have fun :)
    private static final int nCPU = Runtime.getRuntime().availableProcessors();
    //4;
    private static final ExecutorService executor = Executors.newFixedThreadPool(nCPU);
    private static final int nSolts = 2 * nCPU;
    private static HashMap<String, long[]> avgSegment;

    private static boolean cache = false;
    private static Path baseDataDirectory = Paths.get("C:\\Users\\ACER\\Downloads\\data");

    public Database() {
    }

    public static void setBaseDataDirectory(Path _baseDataDirectory) {
        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;
    }


    public static PairArr getCustomerToSegment(MappedByteBuffer customer) {


        PairArr customerToSegment = new PairArr(150_005);
        Unsafe unsafe;
        long a;
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            var f = customer.getClass().getMethod("address");
            f.setAccessible(true);
            a = (long) f.invoke(customer);


        } catch (Exception e) {
            throw new AssertionError(e);
        }

        avgSegment = new HashMap<>();

        int col = 0;
        int colStart = 0;

        int key = 0;

        for (int i = 0; i < customer.capacity(); i++) {

            if (unsafe.getByte(a + i) == '|') {

                if (col == 1) {

                    key = parseInt(customer, colStart, i);

                } else if (col == 6) {
                    byte[] x = new byte[i - colStart];
                    //customer.get(colStart, x, 0, x.length);
                    for (int j = 0; j < x.length; ++j)
                        x[j] = customer.get(colStart + j);


                    customerToSegment
                            .put(key,
                                    avgSegment.computeIfAbsent(
                                            new String(x),
                                            y -> new long[nSolts]));

                    while (customer.get(i) != '\n') ++i;
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


    static int parseInt(MappedByteBuffer s, int i, int end) {

        int q = 0;

        for (byte c = s.get(i); c > '9' || c < '1'; c = s.get(++i)) ;
        for (; i < end; ++i) {
            q *= 10;
            q += s.get(i) - '0'; // - 0
        }
        return q;
    }

    PairArr getOrderToSegment(PairArr customerToSegment, MappedByteBuffer orders) {

        PairArr orderToSegment = new PairArr(6_000_005);


        BiFunction<Integer, Integer, Callable<Object>> f = (s, e) -> () -> {
            int col = 0;
            int colStart = s;

            int key = 0;

            for (int i = s; i < e; i++) {

                if (orders.get(i) == '|') {

                    if (col == 0) {
                        //System.out.println((char)orders[colStart]);
                        key = parseInt(orders, colStart, i);
                    } else if (col == 1) {

                        orderToSegment.put(key, customerToSegment.get(parseInt(orders, colStart, i)));
                        while (orders.get(i) != '\n') ++i;
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
        final ArrayList<Callable<Object>> tasks = new ArrayList<>(nCPU);


        int old = 0;
        for (int i = nCPU; i > 0; --i) {
            int currEnd = old + (orders.capacity() - old) / i;
            for (; currEnd < orders.capacity() && orders.get(currEnd) != '\n'; ++currEnd) ;
            tasks.add(f.apply(old, currEnd));
            old = currEnd + 1;
        }
        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        return orderToSegment;
    }

    Callable<Object> generateCalculateTask(MappedByteBuffer lineitem, PairArr orderToSegment, int s, int e, int index) {
        return () -> {
            int col = 0x0;
            int colStart = s;
            long[] key = null;
            for (int i = s; i < e; i++) {

                if (lineitem.get(i) == '|') {

                    if (col == 0) {
                        key = orderToSegment.get(parseInt(lineitem, colStart, i));
                    } else if (col == 4) {
                        key[index - 1] += parseInt(lineitem, colStart, i);
                        ++key[index];
                        while (lineitem.get(i) != '\n') ++i;
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


    }

    void calculatePerSegment(PairArr orderToSegment, MappedByteBuffer lineitem) {

        final ArrayList<Callable<Object>> tasks = new ArrayList<>(nCPU);

        int old = 0;
        for (int i = nCPU; i > 0; --i) {
            int currEnd = old + (lineitem.capacity() - old) / i;
            for (; currEnd < lineitem.capacity() && lineitem.get(currEnd) != '\n'; ++currEnd) ;

            tasks.add(generateCalculateTask(lineitem, orderToSegment, old, currEnd, 2 * i - 1));
            old = currEnd + 1;
        }


        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    Callable<MappedByteBuffer> readTable(String tableName) {

        return () -> {

            var ch = new FileInputStream(baseDataDirectory.resolve(tableName).toFile()).getChannel();
            var buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
            //buf.load(); ???
            return buf;

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

        return ans(avgSegment.get(marketsegment));
    }

    static long ans(long[] avg) {
        long[] count = new long[2];
        for (int i = 0; i < avg.length; ++i)
            count[i & 1] += avg[i];
        return (100 * count[0]) / count[1];
    }

    static class PairArr {
        final long[][] fst;
        long[][] snd;

        // 2 < initSize < INTEGER.MAX_VALUE  - 2
        // because we can't have an array of size > Max_VALUE - 2;
        // this class is  made to handle the cases where a key is > Max_VALUE - 3
        PairArr(int initSz) {
            fst = new long[initSz][];
        }

        void put(int key, long[] value) {
            if (key < fst.length) {
                fst[key] = value;
            } else {

                if (snd == null) {
                    //System.out.println("allocate second");
                    snd = new long[Integer.MAX_VALUE - fst.length][];
                }
                snd[key - fst.length] = value;
            }

        }


        long[] get(int key) {
            return key < fst.length ? fst[key] : snd[key - fst.length];
        }

    }
}
