package marketdata;


import marketdata.model.FSEntity;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKey;
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
    private static final int PAGE_SIZE = 2_000_000;
    private static volatile Ignite ignite;

    public static void main(String[] args) throws Exception {
        init();
        scanQuery();
        filterOnCountry();
        filterOnCountryAndCurrency();
        filterOnCurrencyAndGroupBySectorCount();
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

        // start
        System.out.println();
        System.out.println(String.format(">>> Starting cache on %s; work directory %s ...", NODE_NAME, workDirectory));
        System.out.println();
        ignite = Ignition.start(iCfg);
        ClusterNode localNode = ignite.cluster().localNode();
        System.out.println();
        System.out.println(String.format(">>> Cache started on %s (%s) successfully", NODE_NAME, localNode.id()));
        System.out.println();
    }

    private static void scanQuery() {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(CACHE_NAME);
        ScanQuery<AffinityKey<Long>, FSEntity> scan = new ScanQuery<>();
        scan.setPageSize(PAGE_SIZE);
        long loadStartTime = System.currentTimeMillis();
        cache.withKeepBinary().query(scan).iterator();
        System.out.println(String.format(">>> Fetched %d entities in %d ms.", scan.getPageSize(),System.currentTimeMillis() - loadStartTime));
        System.out.println();
    }

    private static void filterOnCountry(){
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(CACHE_NAME);
        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select ID  "
                        + "from FSEntity where "
                        + "ISSUECOUNTRY = ? ");
        sql.setArgs(Server.getRandomCountry());
        sql.setPageSize(PAGE_SIZE);
        long loadStartTime = System.currentTimeMillis();
        QueryCursor<List<?>> results = cache.withKeepBinary().query(sql);
        System.out.println(String.format(">>> Executed query in %d ms.",System.currentTimeMillis() - loadStartTime));
        System.out.println(String.format("  > total entry count: %d", results.getAll().size()));
        System.out.println();
    }

    private static void filterOnCountryAndCurrency() throws IOException {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(CACHE_NAME);
        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select ID  "
                        + "from FSEntity where "
                        + "ISSUECOUNTRY = ? "
                        + "and CURRENCYCODE = ?");
        sql.setArgs(
                Server.getRandomCountry(),
                Server.getRandomValue(Server.getCurrencies()));
        sql.setPageSize(PAGE_SIZE);

        long loadStartTime = System.currentTimeMillis();
        QueryCursor<List<?>> results = cache.withKeepBinary().query(sql);
        System.out.println(String.format(">>> Executed query in %d ms.",System.currentTimeMillis() - loadStartTime));
        System.out.println(String.format("  > total entry count: %d", results.getAll().size()));
        System.out.println();
    }

    private static void filterOnCurrencyAndGroupBySectorCount() throws IOException {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(CACHE_NAME);
        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select SECTOR, count (SECTOR)  "
                        + "from FSEntity where "
                        + "CURRENCYCODE = ? "
                        + "group by SECTOR");
        sql.setArgs(
                Server.getRandomValue(Server.getCurrencies()));
        sql.setPageSize(PAGE_SIZE);

        long loadStartTime = System.currentTimeMillis();
        QueryCursor<List<?>> results = cache.withKeepBinary().query(sql);
        System.out.println(String.format(">>> Executed query in %d ms.",System.currentTimeMillis() - loadStartTime));
        for (List<?> result : results) System.out.println(result);
        System.out.println();
    }

}
