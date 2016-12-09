package marketdata;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Client {

    private static final String NODE_NAME = "CLIENT NODE";
    private static final String CACHE_NAME = "W6Cache";
    // The page size will affect the number of results rendered as part of the test
    private static final int PAGE_SIZE = 50;
    private static volatile Ignite ignite;

    public static void main(String[] args) throws Exception {
        init();
        runTests();
        ignite.close();
        System.exit(0);
    }

    private static void init() {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        // set user attributes
        iCfg.setUserAttributes(Collections.unmodifiableMap(Stream.of(
                        new AbstractMap.SimpleEntry<>("nodeName", NODE_NAME))
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)))
        );

        // set work directory
        String workDirectory = System.getProperty("user.home") + File.separator + "ignite";
        iCfg.setWorkDirectory(workDirectory);
        iCfg.setClientMode(true);
        iCfg.setPeerClassLoadingEnabled(true);

        // start
        System.out.println();
        System.out.println(String.format(">>> Starting client with name %s; work directory %s ...", NODE_NAME, workDirectory));
        System.out.println();
        ignite = Ignition.start(iCfg);
        ClusterNode localNode = ignite.cluster().localNode();
        System.out.println();
        System.out.println(String.format(">>> Cache started on %s (%s) successfully", NODE_NAME, localNode.id()));
        System.out.println();
    }

    private static void runTests() throws IOException {
        // return all entities (paginated)
        System.out.println("==========================================================================");
        System.out.println(">>> Scan query (paginated)");
        executeTimedQuery(CACHE_NAME, new ScanQuery());

        // filter on country only
        System.out.println("==========================================================================");
        System.out.println(">>> Filter on country");
        executeTimedQuery(CACHE_NAME, new SqlFieldsQuery(
                "select * from FSEntity where ISSUECOUNTRY = ?")
                .setArgs(
                        Server.getRandomCountry()));

        // filter on country & currency
        System.out.println("==========================================================================");
        System.out.println(">>> Filter on country & currency");
        executeTimedQuery(CACHE_NAME, new SqlFieldsQuery(
                "select * from FSEntity where ISSUECOUNTRY = ? and CURRENCYCODE = ?")
                .setArgs(
                        Server.getRandomCountry(),
                        Server.getRandomValue(Server.getCurrencies())));

        // filter on currency & group by sector
        System.out.println("==========================================================================");
        System.out.println(">>> Filter on currency & group by sector");
        executeTimedQuery(CACHE_NAME, new SqlFieldsQuery(
                "select SECTOR, count(SECTOR) from FSEntity where CURRENCYCODE = ? group by SECTOR")
                .setArgs(Server.getRandomValue(Server.getCurrencies())));
    }

    private static void executeTimedQuery(String cacheName, Query sqlQuery){
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheName);
        long loadStartTime = System.currentTimeMillis();
        QueryCursor results = cache.withKeepBinary().query(sqlQuery);
        System.out.println(String.format(">>> Executed query in %d ms.",System.currentTimeMillis() - loadStartTime));
        System.out.println(sqlQuery.toString());
        System.out.println();
        Iterator iterator = results.iterator();
        int i = 0;
        while (iterator.hasNext() & i < PAGE_SIZE){
            System.out.println("    " + iterator.next());
            i++;
        }
        System.out.println();
    }
}
