package com.evenost.grib_api.controller;

import com.evenost.grib_api.model.GribResponse;
import com.evenost.grib_api.service.GribService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/weather")
public class GribController {

    private final GribService gribService;
    
    @Autowired
    public GribController(GribService gribService) {
        this.gribService = gribService;
    }
    
    @GetMapping("/waves/{area}")
    public GribResponse getWaveData(@PathVariable String area) throws Exception {
        return gribService.getWaveData(area);
    }
    
    @GetMapping("/wind/{area}")
    public GribResponse[] getWindData(@PathVariable String area) throws Exception {
        return gribService.getWindData(area);
    }
    
    @GetMapping("/current/{area}")
    public GribResponse[] getCurrentData(@PathVariable String area) throws Exception {
        return gribService.getCurrentData(area);
    }
    
    @GetMapping("/precipitation/{area}")
    public GribResponse getPrecipitationData(@PathVariable String area) throws Exception {
        return gribService.getPrecipitationData(area);
    }
}