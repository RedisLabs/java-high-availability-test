package com.redislabs.groupon.hatests;

import com.google.common.base.Stopwatch;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import redis.clients.jedis.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guylu on 6/15/2016.
 */
public class ConsistencyHATests {

    private final JedisPool pool;
    private final List<Item> keys = new ArrayList<>();
    private final int NUMBER_OF_ITEMS = 1_000_000;
    private final Random rnd = new Random(NUMBER_OF_ITEMS);

    private class FailedOperationOnItemException extends Exception {
        private Item item;

        public FailedOperationOnItemException(Item item, Exception baseException) {
            super(baseException.getMessage());
            this.item = item;
        }

        public Item getItem() {
            return item;
        }
    }


    public ConsistencyHATests(String host, int port) {

        JedisPoolConfig poolConfig = new JedisPoolConfig();

        // defaults to make your life with connection pool easier :)
//        poolConfig.setTestWhileIdle(false);
//        poolConfig.setTestOnBorrow(false);
//        poolConfig.setTestOnReturn(false);
//        poolConfig.setMinEvictableIdleTimeMillis(60000);
//        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
//        poolConfig.setNumTestsPerEvictionRun(-1);
        poolConfig.setMaxTotal(500);

        pool = new JedisPool(poolConfig, host, port, 10000);
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 2)
            System.out.println("Please run this app with host and port ");

        //get arguments of host and port
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        ConsistencyHATests consistencyHATests = new ConsistencyHATests(host, port);
        System.out.println("Generating data ");
        consistencyHATests.generateKeys();
        System.out.println("Doing tests ");
        consistencyHATests.runTest();

    }

    /**
     * this is the main test method
     */
    private void runTest() throws InterruptedException {
        Stopwatch periodicDisplay = Stopwatch.createUnstarted();

        DescriptiveStatistics statistics = new DescriptiveStatistics();
        statistics.setWindowSize(2000);

        AtomicLong failureCount = new AtomicLong();

        long lastDisplay = 0;

        periodicDisplay.start();

        //run as long as the test is running
        while (true) {
            FailedOperationOnItemException recheckException = null;

            try {
                Stopwatch latencyWatch = incAndValidate();
                statistics.addValue(latencyWatch.elapsed(TimeUnit.MICROSECONDS));
            } catch (FailedOperationOnItemException e) {
                recheckException = e;
                failureCount.incrementAndGet();
            } catch (Exception e) {
                failureCount.incrementAndGet();
            }

            if (recheckException != null) {
                recheckItemAfterFailure(recheckException.getItem(), recheckException.getMessage());
            }

            // Periodically display info
            long elapsedTime = periodicDisplay.elapsed(TimeUnit.SECONDS);
            if (elapsedTime % 5 == 0 && elapsedTime != lastDisplay) {
                lastDisplay = elapsedTime;
                printlnPrefixed("Failures: " + failureCount.get() + ". Timings (μs):" +
                        "\n   mean  :" + statistics.getMean() +
                        "\n   p99   :" + statistics.getPercentile(90) +
                        "\n   p99   :" + statistics.getPercentile(99) +
                        "\n   max   :" + statistics.getMax());
            }
        }
    }

    private void recheckItemAfterFailure(Item item, String message) {
        Stopwatch timeUntilReachable = Stopwatch.createStarted();

        long expectedCount = item.getExpectedCount();

        Exception reasonFailedToCheck = null;

        // we'll keep trying to get access to redis for 20 seconds
        while (timeUntilReachable.elapsed(TimeUnit.SECONDS) < 20) {
            try (Jedis jedis = pool.getResource()) {
                long actualCount = jedis.incrBy(item.getKey(), 0);

                if (expectedCount < actualCount) {
                    printlnPrefixed("DEBUG: Recheck of " + item.getKey() + ", thought failure but succeeded . actual=" +
                            actualCount + ", expected= " + expectedCount + " after failure '" + message + "'");
                } else if (expectedCount > actualCount) {
                    printlnPrefixed("WARN: Recheck of " + item.getKey() + ", data loss (expected > actual). actual=" +
                            actualCount + ", expected= " + expectedCount + " after failure '" + message + "'");
                }

                reasonFailedToCheck = null;
                break;

            } catch (Exception e) {
                reasonFailedToCheck = e;
            }

        }

        if (reasonFailedToCheck != null) {
            printlnPrefixed("Failed to recheck item after failure " + reasonFailedToCheck.getMessage());
        }

        timeUntilReachable.stop();
    }

    private Stopwatch incAndValidate() throws FailedOperationOnItemException {
        Stopwatch latencyWatch = Stopwatch.createUnstarted();

        int randomPosition = rnd.nextInt(keys.size());
        Item randomItem = keys.get(randomPosition);


        try (Jedis jedis = pool.getResource()) {
            // get random number between 1 and 100000

            latencyWatch.start();
            Long actualCount = jedis.incr(randomItem.getKey());
            latencyWatch.stop();

            // the current expectedCount should be biggest then the old expectedCount by 1
            if (randomItem.getExpectedCount() != (actualCount - 1)) {
                printlnPrefixed(" Didn't get the expectedCount for key " + randomItem.getKey() +
                        " actual=" + actualCount + " expected=" + randomItem.expectedCount);
            }
            randomItem.setExpectedCount(actualCount);
        } catch (Exception e) {
            if (randomItem != null) throw new FailedOperationOnItemException(randomItem, e);
            else throw e;
        }

        return latencyWatch;
    }

    private SimpleDateFormat iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private void printlnPrefixed(String s) {
        System.out.println(iso8601.format(new Date()) + " " + s);
    }

    /**
     * this method will generate 1 mil keys (uuids) that will be incremented .
     */
    private void generateKeys() {

        List<String> bulkKeys = new ArrayList<>();
        List<Response<Long>> bulkResponse = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ITEMS; i++) {
            if (i % 1_000 == 0) {
                printlnPrefixed("Generating key " + i);
                try (Jedis jedis = pool.getResource()) {

                    Pipeline pipeline = jedis.pipelined();

                    for (String sKey : bulkKeys) {

                        bulkResponse.add(pipeline.incr(sKey));
                    }
                    pipeline.sync();

                    for (int j = 0; j < bulkKeys.size(); j++) {
                        Item item = new Item(bulkKeys.get(j), bulkResponse.get(j).get());
                        keys.add(item);
                    }

                    bulkKeys = new ArrayList<>();
                    bulkResponse = new ArrayList<>();

                } catch (Exception ex) {
                    printlnPrefixed("failed to generate key " + i + " " + ex.getMessage());
                    // no-op
                }
            }
            String key = UUID.randomUUID().toString();
            bulkKeys.add(key);

        }
    }

    private class Item {

        public Item(String key, long currentCount) {
            this.key = key;
            this.expectedCount = currentCount;
        }


        public long getExpectedCount() {
            return expectedCount;
        }

        public void setExpectedCount(long expectedCount) {
            this.expectedCount = expectedCount;
        }

        private String key;
        private long expectedCount;

        public String getKey() {
            return key;
        }
    }
}
