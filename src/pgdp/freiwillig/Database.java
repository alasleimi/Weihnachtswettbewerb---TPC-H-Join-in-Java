package pgdp.freiwillig;

// TODO Imports

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


    public static Stream<Customer> processInputFileCustomer() {
        try {
            return Files.lines(baseDataDirectory.resolve("customer.tbl"))
                    .parallel()
                    .map(x -> {
                        String[] d = x.split("\\|");


                        return new Customer(Integer.parseInt(d[1].replaceAll("\\D", ""))
                                , d[6]
                        );
                    });
        } catch (Exception e) {

            throw new RuntimeException("file not found");
        }
        // TODO
    }

    public static Stream<LineItem> processInputFileLineItem() {
        try {
            return Files.lines(baseDataDirectory.resolve("lineitem.tbl"))
                    .parallel()
                    .map(x -> {
                        String[] d = x.split("\\|");


                        return new LineItem(Integer.parseInt(d[0]),
                                Integer.parseInt(d[4]) * 100
                                );
                            }
                    );
        } catch (Exception e) {

            throw new RuntimeException("file not found");

        }
        // Für die Quantity der Lineitems bitte Integer.parseInt(str) * 100 verwenden !
    }

    public static Stream<Order> processInputFileOrders() {
        try {
            return Files.lines(baseDataDirectory.resolve("orders.tbl")).
                    parallel()
                    .map(x -> {
                        String[] d = x.split("\\|");

                        return new Order(Integer.parseInt(d[0]), Integer.parseInt(d[1].replaceAll("\\D", "")));
                    });
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
            var mktsegmentPerCustomer = processInputFileCustomer().collect(
                    Collectors.groupingByConcurrent(x -> x.custKey, Collectors.mapping(x -> x.mktsegment, Collectors.joining())
                    ));

            var mktsegmentPerOrder = processInputFileOrders().collect(
                    Collectors.groupingByConcurrent(x -> x.orderKey,
                            Collectors.mapping(x -> mktsegmentPerCustomer.get(x.custKey),
                                    Collectors.joining())
                    )
            );

            averageQuantityPerMarketSegment = processInputFileLineItem()
                    .collect(Collectors.groupingByConcurrent(x -> mktsegmentPerOrder.get(x.orderKey),
                            Collectors.teeing(
                                    Collectors.summingLong(x -> x.quantity),
                                    Collectors.counting(),
                                    (s, c) -> s / c

                            )
                    ));
            cache = true;
        }
        return averageQuantityPerMarketSegment.get(marketsegment);
    }
}
