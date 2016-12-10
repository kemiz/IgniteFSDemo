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
import org.gridgain.grid.configuration.GridGainConfiguration;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Server {

    private static final String NODE_NAME = "SERVER NODE";
    private static final String W6_CACHE = "W6Cache";
    private static final int CACHE_SIZE = 2_000_000;
    private static final String CURRENCY_CACHE = "Currencies";
    private static final String SECTORS_CACHE = "Sectors";
    private static volatile Ignite ignite;

    public static void main(String[] args) throws Exception {
        init();
        if (args.length > 0 && args[0].equals("-load")) {
            loadSectorsCache();
            loadW6Cache();
        }
    }

    private static void init() {
        IgniteConfiguration iCfg = new IgniteConfiguration();
        GridGainConfiguration ggCfg = new GridGainConfiguration();
        ggCfg.setLicenseUrl("data/gridgain-license.xml");
        iCfg.setPluginConfigurations(ggCfg);
        // set user attributes
        iCfg.setUserAttributes(Collections.unmodifiableMap(Stream.of(
                new AbstractMap.SimpleEntry<>("nodeName", NODE_NAME))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)))
        );

        iCfg.setGridName(W6_CACHE);

        // set work directory
        String workDirectory = System.getProperty("user.home") + File.separator + "ignite";
        iCfg.setWorkDirectory(workDirectory);
        iCfg.setPeerClassLoadingEnabled(false);

        // get cache configurations
        CacheConfiguration w6Cfg = getFSEntityCacheConfiguration();
        CacheConfiguration cCfg = getCacheConfiguration(CURRENCY_CACHE, Currency.class);
        CacheConfiguration sCfg = getCacheConfiguration(SECTORS_CACHE, Sector.class);
        iCfg.setCacheConfiguration(cCfg, sCfg, w6Cfg);

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

    private static void loadW6Cache() {
        IgniteCache<?, ?> w6Cache = ignite.getOrCreateCache(W6_CACHE);
        int sectorsCacheSize = ignite.getOrCreateCache(SECTORS_CACHE).size(CachePeekMode.ALL);
        try {
            ArrayList<String> currencies = getCurrencies();
            System.out.println(String.format(">>> Loading cache with %d entities...", CACHE_SIZE));
            long loadStartTime = System.currentTimeMillis();
            try (IgniteDataStreamer<AffinityKey<Long>, FSEntity> streamer = ignite.dataStreamer(w6Cache.getName())) {
                for (int i = 0; i < CACHE_SIZE; i++) {
                    FSEntity entity = FSEntity.createNew(
                            (long) i,
                            getRandomCountry(),
                            getRandomValue(currencies),
                            (long) new Random().nextInt(sectorsCacheSize));
                    streamer.addData(entity.key(), entity);
                }
            }
            System.out.println(String.format(">>> Cache loaded with %d entities in %d ms.",
                    w6Cache.size(CachePeekMode.ALL), System.currentTimeMillis() - loadStartTime));
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadSectorsCache() throws IOException {
        IgniteCache<Long, Sector> cache = ignite.getOrCreateCache(SECTORS_CACHE);
        System.out.println(String.format(">>> Loading " + SECTORS_CACHE + " cache with %d entries...", getSectors().size()));
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

    private static CacheConfiguration getCacheConfiguration(String cacheName, Class<?> clazz) {
        // create currency cache configuration
        CacheConfiguration<Long, Currency> cCfg = new CacheConfiguration<>();
        cCfg.setCacheMode(CacheMode.REPLICATED);
        cCfg.setName(cacheName);
        cCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cCfg.setOffHeapMaxMemory(-1);
        cCfg.setBackups(0);
        cCfg.setCopyOnRead(false);
        cCfg.setIndexedTypes(Long.class, clazz);
        return cCfg;
    }

    private static CacheConfiguration getFSEntityCacheConfiguration() {
        // create w6cache configuration
        CacheConfiguration<AffinityKey<Long>, FSEntity> w6Cfg = new CacheConfiguration<>();
        w6Cfg.setCacheMode(CacheMode.PARTITIONED);
        w6Cfg.setName(W6_CACHE);
        w6Cfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        w6Cfg.setOffHeapMaxMemory(-1);
        w6Cfg.setBackups(0);
        w6Cfg.setCopyOnRead(false);
        w6Cfg.setIndexedTypes(AffinityKey.class, FSEntity.class);
        return w6Cfg;
    }

    public static String getRandomCountry(){
        return getRandomValue(new ArrayList<String>(Arrays.asList(Locale.getISOCountries())));
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
}
