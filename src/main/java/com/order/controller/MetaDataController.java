package com.order.controller;


import com.order.dto.HostInfoDto;
import com.order.services.MetaDataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/metadata")
public class MetaDataController {
    private MetaDataService metaDataService;

    public MetaDataController(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    @GetMapping("/all")
    public List<HostInfoDto> getStreamsMetaData(){
        return  metaDataService.getStreamMetaData();
    }

}
