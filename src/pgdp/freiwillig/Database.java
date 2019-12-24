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

    final private Map<String, Long> averageQuantityPerMarketSegment;

    private static Path baseDataDirectory = Paths.get("data");

    /**
     * public static void setBaseDataDirectory(Path baseDataDirectory) {
     * Database.baseDataDirectory = baseDataDirectory;
     * }
     **/

    public static Stream<Customer> processInputFileCustomer() {
        try {
            return Files.lines(baseDataDirectory.resolve("customer.tbl")).
                    map(x -> {
                        String[] d = x.split("\\|");


                        return new Customer(Integer.parseInt(d[1].replaceAll("\\D", ""))
                                , d[2].toCharArray()
                                , Integer.parseInt(d[3])
                                , d[4].toCharArray()
                                , Float.parseFloat(d[5])
                                , d[6]
                                , d[7].toCharArray());
                    });
        } catch (Exception e) {

            throw new RuntimeException("file not found");
        }
        // TODO
    }

    public static Stream<LineItem> processInputFileLineItem() {
        try {
            return Files.lines(baseDataDirectory.resolve("lineitem.tbl")).
                    map(x -> {
                                String[] d = x.split("\\|");


                                return new LineItem(Integer.parseInt(d[0]),
                                        Integer.parseInt(d[1]), Integer.parseInt(d[2]), Integer.parseInt(d[3]),
                                        Integer.parseInt(d[4]) * 100,
                                        Float.parseFloat(d[5]),
                                        Float.parseFloat(d[6]), Float.parseFloat(d[7]), d[8].charAt(0),
                                        d[9].charAt(0), LocalDate.parse(d[10]),
                                        LocalDate.parse(d[11]), LocalDate.parse(d[12]), d[13].toCharArray(),
                                        d[14].toCharArray(), d[15].toCharArray()
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
                    map(x -> {
                        String[] d = x.split("\\|");

                        return new Order(Integer.parseInt(d[0]), Integer.parseInt(d[1].replaceAll("\\D", ""))
                                , d[2].charAt(0),
                                Float.parseFloat(d[3]), LocalDate.parse(d[4]),
                                d[5].toCharArray(), d[6].toCharArray(), Integer.parseInt(d[7]), d[8].toCharArray());
                    });
        } catch (IOException e) {

            throw new RuntimeException("file not found");
        }
        // TODO
    }


    public Database() {

        var mktsegmentPerCustomer = processInputFileCustomer().collect(
                Collectors.groupingBy(x -> x.custKey, Collectors.mapping(x -> x.mktsegment, Collectors.joining())
                ));

        var mktsegmentPerOrder = processInputFileOrders().collect(
                Collectors.groupingBy(x -> x.orderKey,
                        Collectors.mapping(x -> mktsegmentPerCustomer.get(x.custKey),
                                Collectors.joining())
                )
        );

        averageQuantityPerMarketSegment = processInputFileLineItem()
                .collect(Collectors.groupingBy(x -> mktsegmentPerOrder.get(x.orderKey),
                        Collectors.teeing(
                                Collectors.summingLong(x -> x.quantity),
                                Collectors.counting(),
                                (s, c) -> s / c

                        )
                ));


    }

    public static void main(String[] args) {
        //processInputFileOrders().forEach(System.out::println);
        System.out.println(new Database().getAverageQuantityPerMarketSegment("AUTOMOBILE"));
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        return averageQuantityPerMarketSegment.get(marketsegment);
    }
}
