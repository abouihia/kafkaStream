package com.withoutspring.mock;
import com.withoutspring.utils.ProducerUtil;

import static com.withoutspring.topology.ExploreWindowTopology.WINDOW_WORDS;
import static java.lang.Thread.sleep;

public class WindowsMockDataProduer {

    public static void main(String[] args) throws InterruptedException {

        //bulkMockDataProducer();
        bulkMockDataProducer_SlidingWindows();

    }

    private static void bulkMockDataProducer() throws InterruptedException {
        var key = "A";
        var word = "Apple";
        int count = 0;
        while(count<100){
            var recordMetaData = ProducerUtil.publishMessageSync(WINDOW_WORDS, key,word);
            System.out.printf("Published the alphabet message : {} ", recordMetaData);
            sleep(1000);
            count++;
        }
    }

    private static void bulkMockDataProducer_SlidingWindows() throws InterruptedException {
        var key = "A";
        var word = "Apple";
        int count = 0;
        while(count<10){
            var recordMetaData = ProducerUtil.publishMessageSync(WINDOW_WORDS, key,word);
            System.out.printf("Published the alphabet message : {} ", recordMetaData);
            sleep(1000);
            count++;
        }
    }

}
