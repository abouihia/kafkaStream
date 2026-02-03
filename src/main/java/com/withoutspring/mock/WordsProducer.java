package com.withoutspring.mock;


import com.withoutspring.utils.ProducerUtil;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class WordsProducer {


    static String WORDS  = "words";

    public static void main(String[] args)  throws  InterruptedException{

        var key = "A";
        var word = "Apple";
        var word1 = "Alligator";
        var word2 = "Ambulance";

        var  recodMetaData  = ProducerUtil.publishMessageSync(WORDS, key, word);
        log.info("Published the alphabet message {}", recodMetaData);

        var  recodMetaData1  = ProducerUtil.publishMessageSync(WORDS, key, word1);
        log.info("Published the alphabet message {}", recodMetaData1);

        var  recodMetaData2  = ProducerUtil.publishMessageSync(WORDS, key, word2);
        log.info("Published the alphabet message {}", recodMetaData2);

        var bKey = "B";

        var bWord1 = "Bus";
        var bWord2 = "Baby";
        var recordMetaData3 = ProducerUtil.publishMessageSync(WORDS, bKey,bWord1);
        log.info("Published the alphabet message : {} ", recordMetaData3);

        var recordMetaData4 = ProducerUtil.publishMessageSync(WORDS, bKey,bWord2);
        log.info("Published the alphabet message : {} ", recordMetaData4);
    }
}
