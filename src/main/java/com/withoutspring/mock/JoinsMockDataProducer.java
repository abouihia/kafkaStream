package com.withoutspring.mock;

import com.withoutspring.utils.ProducerUtil;

import java.util.Map;

import static com.withoutspring.topology.ExploreJoinOperatorsTopology.ALPHABETS_ABBREVATIONS;

public class JoinsMockDataProducer {

    public  static void main(String[] args) {


        var alphabetMap = Map.of(

                //"E", "E is the fifth letter in English Alphabets."
                "A", "A is the First letter in English Alphabets.",
                "B", "B is the Second letter in English Alphabets."
        );

      // publishMessages(alphabetMap, ALPHABETS);
      //  publishMessagesWithDelay(alphabetMap, ALPHABETS, 4);
        // sleep(6000);

        var alphabetAbbrevationMap = Map.of(
                "A", "Apple",
                "B", "Bus"
                ,"C", "Cat"

        );
        ProducerUtil.publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

        alphabetAbbrevationMap = Map.of(
                "A", "Airplane",
                "B", "Baby."

        );

      //  publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);


    }


}
