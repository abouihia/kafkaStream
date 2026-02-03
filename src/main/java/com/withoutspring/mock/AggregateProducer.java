package com.withoutspring.mock;

import com.withoutspring.topology.ExploreAggreagateOperatorsTopology;
import com.withoutspring.utils.ProducerUtil;

import java.util.List;


public class AggregateProducer {

    public static void main() {

        pubblishElement("A", List.of("Apple", "Alligator", "Ambulance"));

        pubblishElement("D", List.of("Apple", "Alligator", "Ambulance"));

        pubblishElement("B", List.of("Bus", "Baby"));


    }

    private static void pubblishElement(String key, List<String> elements
    ) {
        var keyD = key;
        elements.stream()
                .forEach(word -> {
                    var recordMetaData = ProducerUtil.publishMessageSync(ExploreAggreagateOperatorsTopology.AGGEGATE, keyD, word);
                    System.out.println("Published the alphabet message :  " + recordMetaData);
                });
    }

}
