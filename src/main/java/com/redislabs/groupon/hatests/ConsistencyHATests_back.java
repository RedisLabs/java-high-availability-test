package com.redislabs.groupon.hatests;

import com.google.common.base.Stopwatch;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guylu on 6/15/2016.
 */
public class ConsistencyHATests_back {

    private final JedisPool pool;
    private final List<Item> localData = new ArrayList<>();
    private final int NUMBER_OF_ITEMS = 100000;
    private final Random rnd = new Random(NUMBER_OF_ITEMS);


    public ConsistencyHATests_back(String host, int port) {

        //starting a connection pool that will be used
        JedisPoolConfig poolConfig=new JedisPoolConfig();
        poolConfig.setMaxTotal(500);
        pool = new JedisPool(poolConfig, host , port,10000);

        //Pipeline pipe =  pool.getResource().pipelined();

    }

    public static void main(String[] args) throws InterruptedException {

        if ( args.length < 2 )
            System.out.println("Please run this app with host and port ");

        //get arguments of host and port
        String host = args[0];
        int port  = Integer.parseInt(args[1]);

        ConsistencyHATests_back consistencyHATests = new ConsistencyHATests_back(host,port);
        System.out.printf("Generating data ");
        consistencyHATests.generateData();
        System.out.println("Doing tests ");
        consistencyHATests.runTest();

    }

    /**
     * this is the main test method
     */
    private void runTest() throws InterruptedException {

       AtomicLong latencyCounter = new AtomicLong();
       List<Long> latencyList = new ArrayList<>();

       Stopwatch stopwatch = Stopwatch.createUnstarted();
       //run as long as the test is running
       while(true)
       {

           stopwatch.start();
           try {
               //increment the counter and validate
               incAndValidate();
           } catch (Exception e) {
               Thread.currentThread().sleep(1000);
               e.printStackTrace();
               System.out.println("Sleeping for 100 mili ");

           }
           stopwatch.stop();
           latencyList.add(stopwatch.elapsed(TimeUnit.MICROSECONDS));
           //print average  latency just to check this code is still working
           if (  (latencyCounter.incrementAndGet() % 1000) == 0 )
           {
               long latencySum =0;
               for (int i = 0; i < latencyList.size(); i++)
                   latencySum  += latencyList.get(i);

               double avgLatency = (double)latencySum / latencyList.size();
               System.out.println("Average latency for 1000 commands " + avgLatency);
           }

           stopwatch.reset();

       }



    }

    private void incAndValidate() throws InterruptedException {


        Jedis jedis =  pool.getResource();
        //get random number between 1 and 100000
        int randomPosition = rnd.nextInt(NUMBER_OF_ITEMS);
        Item item = localData.get(randomPosition);

        Long currentCounter = null;
        try {
            //inc the counter and get the new counter
            currentCounter = jedis.incr(item.getKey());
        } catch (Exception e) {

            Thread.currentThread().sleep(1000);
            e.printStackTrace();
            System.out.println("Sleeping for 100 mili ");
            jedis.close();
            throw e;
        }


        //the current counter should be biggest then the old counter by 1
        if ( item.counter == (currentCounter - 1) )
            localData.get(randomPosition).setCounter(currentCounter);
        else {
            System.out.println("Didn't get the current counter for key " + item.getKey() + " current " + currentCounter + " items counter " + item.counter);
            localData.get(randomPosition).setCounter(currentCounter);

        }

        jedis.close();
    }

    /**
     * this method will generate 1 mil hash that will be incremented for each run
     * that will be stored in local hash for data validation later on
     */
    private void generateData() {

        Jedis jedis = pool.getResource();

        for (int i = 0; i < NUMBER_OF_ITEMS; i++) {


            String key =  UUID.randomUUID().toString();

            long currentCount = jedis.incr(key);

            Item item  =new Item(key,currentCount);

            localData.add(item);

        }

        jedis.close();


    }

    private class Item {

        public Item(String key, long currentCount) {
            this.key = key;
            this.counter = currentCount;
        }


        public long getCounter() {
            return counter;
        }

        public void setCounter(long counter) {
            this.counter = counter;
        }

        private String key;
        private long counter;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }
}
