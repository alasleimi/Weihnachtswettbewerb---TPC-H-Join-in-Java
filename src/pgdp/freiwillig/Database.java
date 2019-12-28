package pgdp.freiwillig;

// TODO Imports

import sun.misc.Unsafe;

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
    public static Unsafe unsafe;
    private static Method getAddress;

    public Database() {

        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);

        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static void setBaseDataDirectory(Path _baseDataDirectory) {
        Database.baseDataDirectory = _baseDataDirectory;
        cache = false;
    }


    public static PairArr getCustomerToSegment(MappedByteBuffer customer) {


        PairArr customerToSegment = new PairArr(150_005);

        long address;
        try {

            getAddress = customer.getClass().getMethod("address");
            getAddress.setAccessible(true);
            address = (long) getAddress.invoke(customer);

        } catch (Exception e) {
            throw new AssertionError(e);
        }

        avgSegment = new HashMap<>();


        long end = customer.capacity() + address;
        for (long i = address; i < end; ) {


            while (unsafe.getByte(i++) != '|') ; //skip 1st
            i += 9; //skip Customer#

            int tmp;
            int key = 0;
            while (unsafe.getByte(i++) != '0') ;
            while ((tmp = unsafe.getByte(i++)) != '|') {
                key *= 10;
                key += tmp - '0';
            }


            // skip next  4 fields
            for (int u = 0; u < 4; ++u) {
                while (unsafe.getByte(i++) != '|') ;
            }

            //get the market segment and store it in x
            long j = i;
            while (unsafe.getByte(j) != '|') ++j;
            var x = new byte[(int) (j - i)];
            for (int u = 0; i < j; ) x[u++] = unsafe.getByte(i++);

            customerToSegment
                    .put(key,
                            avgSegment.computeIfAbsent(
                                    new String(x),
                                    y -> new long[nSolts]));

            while (unsafe.getByte(i++) != '\n') ;// skip rest

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
        db.getAverageQuantityPerMarketSegment("AUTOMOBILE");
        var e = System.nanoTime();
        System.out.println((e - s) / 1_000_000);

    }


    static int parseInt(long i, long end) {

        int q = 0;

        for (byte c = unsafe.getByte(i); c > '9' || c < '1'; c = unsafe.getByte(++i)) ;
        for (; i < end; ++i) {
            q *= 10;
            q += unsafe.getByte(i) - '0'; // - 0
        }
        return q;
    }

    PairArr getOrderToSegment(PairArr customerToSegment, MappedByteBuffer orders) {

        PairArr orderToSegment = new PairArr(6_000_005);
        long address;
        try {

            address = (long) getAddress.invoke(orders);

        } catch (Exception e) {
            throw new AssertionError(e);
        }


        BiFunction<Long, Long, Callable<Object>> f = (s, e) -> () -> {


            for (long i = s; i < e; ) {
                byte tmp;
                int key = 0;
                while ((tmp = unsafe.getByte(i++)) != '|') {
                    key *= 10;
                    key += tmp - '0';
                }
                int q = 0;
                while ((tmp = unsafe.getByte(i++)) != '|') {
                    q *= 10;
                    q += tmp - '0';
                }
                orderToSegment.put(key, customerToSegment.get(q));
                while (unsafe.getByte(i++) != '\n') ;
            }
            return null;
        };
        final ArrayList<Callable<Object>> tasks = new ArrayList<>(nCPU);


        int old = 0;
        for (int i = nCPU; i > 0; --i) {
            int currEnd = old + (orders.capacity() - old) / i;
            for (; currEnd < orders.capacity() && orders.get(currEnd) != '\n'; ++currEnd) ;
            tasks.add(f.apply(address + old, currEnd + address));
            old = currEnd + 1;
        }
        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        return orderToSegment;
    }

    Object calculateSubTask(PairArr orderToSegment, long startAddress, long end, int index) {

        long[] key;
        for (long i = startAddress; i < end; ) {


            byte tmp;
            int q = 0;
            while ((tmp = unsafe.getByte(i++)) != '|') {
                q *= 10;
                q += tmp - '0';
            }
            // System.out.println("key" + q);
            key = orderToSegment.get(q);
            while (unsafe.getByte(i++) != '|') ;//skip 2nd
            while (unsafe.getByte(i++) != '|') ;//skip 3rd
            while (unsafe.getByte(i++) != '|') ;//skip 4th
            q = 0;
            while ((tmp = unsafe.getByte(i++)) != '|') {
                q *= 10;
                q += tmp - '0';
            }
            key[index - 1] += q;
            ++key[index];
            while (unsafe.getByte(i++) != '\n') ; //skip rest
        }

        return null;


    }


    void calculatePerSegment(PairArr orderToSegment, MappedByteBuffer lineitem) {

        final ArrayList<Callable<Object>> tasks = new ArrayList<>(nCPU);

        long address;
        try {

            address = (long) getAddress.invoke(lineitem);

        } catch (Exception e) {
            throw new AssertionError(e);
        }

        int old = 0;
        for (int i = nCPU; i > 0; --i) {
            int currEnd = old + (lineitem.capacity() - old) / i;
            for (; currEnd < lineitem.capacity() && lineitem.get(currEnd) != '\n'; ++currEnd) ;
            var startAddress = old + address;
            var end = currEnd + address;
            var num = 2 * i - 1;
            tasks.add(() -> calculateSubTask(orderToSegment, startAddress, end, num));
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
            //buf.load(); ???
            var buff = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
            buff.load();
            return buff;

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
