package com.order.services;


import com.order.dto.HostInfoDTOWithKey;
import com.order.dto.HostInfoDto;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class MetaDataService {

    private final StreamsBuilderFactoryBean  streamsBuilderFactoryBean;


    public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {

        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }



    public List<HostInfoDto> getStreamMetaData(){
        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .metadataForAllStreamsClients()
                .stream().map(streamsMetadata ->
                        new HostInfoDto( streamsMetadata.hostInfo().host(), streamsMetadata.hostInfo().port()))
                .collect(Collectors.toList());

    }

    public HostInfoDTOWithKey getStreamMetaData(String storeName,  String  locationId){
        var metaDataForKey =
                streamsBuilderFactoryBean
                .getKafkaStreams()
                .queryMetadataForKey(storeName, locationId, Serdes.String().serializer());
         if( metaDataForKey != null){
              var activeHost = metaDataForKey.activeHost();
              return new HostInfoDTOWithKey(activeHost.host(), activeHost.port(), locationId);
         }

         return  null;
    }


}
