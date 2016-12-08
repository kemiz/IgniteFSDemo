package marketdata;


import marketdata.model.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Server {

    private static final String NODE_NAME = "SERVER NODE";
    private static final String W6_CACHE = "W6Cache";
    private static final int CACHE_SIZE = 500_000;
    private static final String CURRENCY_CACHE = "Currencies";
    private static final String SECTORS_CACHE = "Sectors";
    private static volatile Ignite ignite;

    public static void main(String[] args) throws Exception {
        init();
        if (args.length > 0 && args[0].equals("-load")) {
            loadCache();
            loadCurrencyCache();
            loadSectorsCache();
        }
    }

    private static void init() {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        // set user attributes
        iCfg.setUserAttributes(Collections.unmodifiableMap(Stream.of(
                new AbstractMap.SimpleEntry<>("nodeName", NODE_NAME))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)))
        );

        iCfg.setGridName(W6_CACHE);

        // set work directory
        String workDirectory = System.getProperty("user.home") + File.separator + "ignite";
        iCfg.setWorkDirectory(workDirectory);
        iCfg.setPeerClassLoadingEnabled(true);
        // create w6cache configuration
        CacheConfiguration<AffinityKey<Long>, FSEntity> w6Cfg = new CacheConfiguration<>();
        w6Cfg.setCacheMode(CacheMode.PARTITIONED);
        w6Cfg.setName(W6_CACHE);
        w6Cfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        w6Cfg.setCopyOnRead(false);
        w6Cfg.setOffHeapMaxMemory(-1);
        w6Cfg.setBackups(0);
        w6Cfg.setCopyOnRead(false);
        w6Cfg.setIndexedTypes(AffinityKey.class, FSEntity.class);

        // create currency cache configuration
        CacheConfiguration<Long, marketdata.model.Currency> cCfg = new CacheConfiguration<>();
        cCfg.setCacheMode(CacheMode.REPLICATED);
        cCfg.setName(CURRENCY_CACHE);
        cCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cCfg.setCopyOnRead(false);
        cCfg.setOffHeapMaxMemory(-1);
        cCfg.setBackups(0);
        cCfg.setCopyOnRead(false);
        cCfg.setIndexedTypes(Long.class, marketdata.model.Currency.class);

        // create sectors cache configuration
        CacheConfiguration<Long, marketdata.model.Currency> sCfg = new CacheConfiguration<>();
        sCfg.setCacheMode(CacheMode.REPLICATED);
        sCfg.setName(SECTORS_CACHE);
        sCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        sCfg.setCopyOnRead(false);
        sCfg.setOffHeapMaxMemory(-1);
        sCfg.setBackups(0);
        sCfg.setCopyOnRead(false);
        sCfg.setIndexedTypes(Long.class, Sector.class);

        iCfg.setCacheConfiguration(w6Cfg,cCfg,sCfg);

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

    private static void loadCache() {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(W6_CACHE);
        try {
            ArrayList<String> currencies = getCurrencies();
            ArrayList<String> sectors = getSectors();
            System.out.println(String.format(">>> Loading cache with %d entities...", CACHE_SIZE));
            long loadStartTime = System.currentTimeMillis();
            try (IgniteDataStreamer<AffinityKey<Long>, FSEntity> streamer = ignite.dataStreamer(cache.getName())) {
                for (int i = 0; i < CACHE_SIZE; i++) {
                    FSEntity entity = FSEntity.createNew(
                            (long) i,
                            getRandomCountry(),
                            getRandomValue(currencies),
                            getRandomValue(sectors));
                    streamer.addData(entity.key(), entity);
                }
            }
            System.out.println(String.format(">>> Cache loaded with %d entities in %d ms.",
                    cache.size(CachePeekMode.ALL), System.currentTimeMillis() - loadStartTime));
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getRandomCountry(){
        return Locale.getISOCountries()[new Random().nextInt(Locale.getISOCountries().length - 1)];
    }

    public static String getRandomValue(ArrayList<String> list){
        return list.get(new Random().nextInt(list.size()));
    }

    public static ArrayList<String> getCurrencies() throws IOException {
        FileInputStream fis = new FileInputStream(new File("data/currency_codes.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        ArrayList<String> currencies = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            currencies.add(line);
        }
        br.close();
        return currencies;
    }

    public static ArrayList<String> getSectors() throws IOException {
        FileInputStream fis = new FileInputStream(new File("data/sectors.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        ArrayList<String> sectors = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            sectors.add(line);
        }
        br.close();
        return sectors;
    }

    public static void loadCurrencyCache() throws IOException {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(CURRENCY_CACHE);
        System.out.println(String.format(">>> Loading currency cache with %d currencies...", getCurrencies().size()));
        long loadStartTime = System.currentTimeMillis();
        try (IgniteDataStreamer<Long, marketdata.model.Currency> streamer = ignite.dataStreamer(cache.getName())) {
            int i = 0;
            for (String currencyCode : getCurrencies()) {
                marketdata.model.Currency currency = marketdata.model.Currency.createNew((long) i, currencyCode);
                streamer.addData(currency.getId(), currency);
                i++;
            }
        }
        System.out.println(String.format(">>> Cache loaded with %d entities in %d ms.",
                cache.size(CachePeekMode.ALL), System.currentTimeMillis() - loadStartTime));
        System.out.println();
    }

    public static void loadSectorsCache() throws IOException {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(SECTORS_CACHE);
        System.out.println(String.format(">>> Loading " + SECTORS_CACHE + " cache with %d currencies...", getSectors().size()));
        long loadStartTime = System.currentTimeMillis();
        try (IgniteDataStreamer<Long, Sector> streamer = ignite.dataStreamer(cache.getName())) {
            int i = 0;
            for (String sectorName : getSectors()) {
                Sector sector = Sector.createNew((long) i, sectorName);
                streamer.addData(sector.getId(), sector);
                i++;
            }
        }
        System.out.println(String.format(">>> Cache loaded with %d entities in %d ms.",
                cache.size(CachePeekMode.ALL), System.currentTimeMillis() - loadStartTime));
        System.out.println();
    }

}
