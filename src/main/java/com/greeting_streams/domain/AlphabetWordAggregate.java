package com.greeting_streams.domain;

import java.util.HashSet;
import java.util.Set;

public record AlphabetWordAggregate(String key,
                                    Set<String> valueList,
                                    int runningCount){

    public AlphabetWordAggregate(){
         this("", new HashSet<>(), 0);
    }

    public AlphabetWordAggregate mapToAlphabetWordAggegator(String key, String newValue){
        System.out.println("Before the update :  "+ this );
        System.out.printf("New Record : key : {} , value : {} : "+ key+ newValue );
        var newRunningCount = this.runningCount +1;
        valueList.add(newValue);
        var aggregated = new AlphabetWordAggregate(key, valueList, newRunningCount);
        System.out.printf("aggregated : {}" , aggregated);
        return aggregated;
    }

}
