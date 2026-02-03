package com.greeting;

import com.greeting_streams.domain.GreetingRecord;
import com.greeting_streams.serdes.SerdesFactory;
import com.greeting_streams.topoloy.GreetingsTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GreetingTopologyTest {

    TopologyTestDriver topologyTestDriver = null;

    TestInputTopic<String, GreetingRecord>  inputTopics = null;

    TestOutputTopic<String, GreetingRecord>  outputTopic  = null;

    @BeforeEach
    void setUp(){
     topologyTestDriver   = new TopologyTestDriver(GreetingsTopology.buildToplogy());
     inputTopics  = topologyTestDriver.createInputTopic(GreetingsTopology.GREETINGS,
             Serdes.String().serializer(), SerdesFactory.geetingSerdesGenerics().serializer()
             );

     outputTopic  = topologyTestDriver.createOutputTopic(GreetingsTopology.GREETINGS_UPPERCASE,
                Serdes.String().deserializer(), SerdesFactory.geetingSerdesGenerics().deserializer()
        );

    }

    @AfterEach
    void tearDown(){
        topologyTestDriver.close();
    }

    @Test
    public  void buildTopoloyTest(){
        //given
        inputTopics.pipeInput("Good Morning",  new GreetingRecord("Good Morning", LocalDateTime.now()));


        //then
        var count = outputTopic.getQueueSize();
        assertEquals(1, count);
        var outPutValue =  outputTopic.readKeyValue();
        assertEquals("Good Morning".toUpperCase(), outPutValue.value.message());
        assertNotNull(outPutValue.value.timeStamp());

    }

    @Test
    public  void buildTopoloyMultiTest(){
        //given
        inputTopics.pipeInput("Good Morning",  new GreetingRecord("Good Morning", LocalDateTime.now()));
        var greeting1 = KeyValue.pair("GM", new GreetingRecord("Good Morning", LocalDateTime.now()));
        var greeting2 = KeyValue.pair("GM", new GreetingRecord("Good Night", LocalDateTime.now()));
        inputTopics.pipeKeyValueList(List.of(greeting1, greeting2));

        //then
        var count = outputTopic.getQueueSize();
        assertEquals(3, count);

        List<KeyValue<String, GreetingRecord>> keyValues = outputTopic.readKeyValuesToList();

        var outPutValue1 =  keyValues.get(1);
        assertEquals("Good Morning".toUpperCase(), outPutValue1.value.message());
        assertNotNull(outPutValue1.value.timeStamp());

        var outPutValue2 =  keyValues.get(2);
        assertEquals("Good Night".toUpperCase(), outPutValue2.value.message());
        assertNotNull(outPutValue2.value.timeStamp());
    }

    @Test
    public  void buildTopoloyWithErrorTest(){
        //given
        inputTopics.pipeInput("Good Morning",  new GreetingRecord("Transient Error", LocalDateTime.now()));

        //then
        var count = outputTopic.getQueueSize();
        assertEquals(1, count);
        var outPutValue =  outputTopic.readKeyValue().value;
        assertNull(outPutValue);


    }

}
