package pgdp.freiwillig;

// TODO Imports

import javax.imageio.IIOException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    // TODO have fun :)

    static private Map<String, Long> averageQuantityPerMarketSegment;
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
                    .collect(Collectors.toConcurrentMap(x -> x[0], x -> Integer.parseInt(x[1])));


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
                    .map(x -> x.split("\\|"))
                    .collect(Collectors.toConcurrentMap(x -> Integer.parseInt(x[1]
                            .replaceAll("\\D", "")), x -> x[6]));


        } catch (IOException e) {

            throw new RuntimeException("file not found");
        }
        // TODO
    }


    public Database() {
    }

    public static void main(String[] args) {
        //processInputFileOrders().forEach(System.out::println);
        System.out.println(new Database().getAverageQuantityPerMarketSegment("AUTOMOBILE"));
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        if (!cache) {
            var a = custPerOrder();
            var b = segPerCust();
            try {
                averageQuantityPerMarketSegment = Files.lines(baseDataDirectory.resolve("lineitem.tbl"))
                        .unordered()
                        .parallel()
                        .map(x -> x.split("\\|")).collect(Collectors.groupingByConcurrent(x -> b.get(a.get(x[0])),
                                Collectors.teeing(
                                        Collectors.summingLong(x -> Integer.parseInt(x[4]) * 100),
                                        Collectors.counting(),
                                        (s, c) -> s / c

                                )
                        ));
            } catch (IOException e) {
                throw new RuntimeException("no file");
            }

            cache = true;
        }
        return averageQuantityPerMarketSegment.get(marketsegment);
    }
}
