package com.evenost.grib_api.service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.evenost.grib_api.model.GribDataPoint;
import com.evenost.grib_api.model.GribResponse;

import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

@Service
public class GribService {
    private static final Logger logger = Logger.getLogger(GribService.class.getName());
    private final RestTemplate restTemplate = new RestTemplate();
    
    public GribResponse getWaveData(String area) throws Exception {
        try {
            // 1. Download GRIB file
            String url = "https://api.met.no/weatherapi/gribfiles/1.1/waves?area=" + area;
            logger.info("Downloading wave data from: " + url);
            File gribFile = downloadGribFile(url, "waves_" + area + ".grb");
            logger.info("File downloaded to: " + gribFile.getAbsolutePath() + ", size: " + gribFile.length() + " bytes");
            
            // 2. Parse wave height data
            return parseWaveHeightData(gribFile);
        } catch (Exception e) {
            logger.severe("Error getting wave data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    
    public GribResponse[] getWindData(String area) throws Exception {
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/wind?area=" + area;
        File gribFile = downloadGribFile(url, "wind_" + area + ".grb");
        
        // For wind, we return both speed and direction
        GribResponse[] responses = new GribResponse[2];
        responses[0] = parseWindSpeedData(gribFile);
        responses[1] = parseWindDirectionData(gribFile);
        return responses;
    }
    
    public GribResponse[] getCurrentData(String area) throws Exception {
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/current?area=" + area;
        File gribFile = downloadGribFile(url, "current_" + area + ".grb");
        
        // For current, we return both speed and direction
        GribResponse[] responses = new GribResponse[2];
        responses[0] = parseCurrentSpeedData(gribFile);
        responses[1] = parseCurrentDirectionData(gribFile);
        return responses;
    }
    
    public GribResponse getPrecipitationData(String area) throws Exception {
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/precipitation?area=" + area;
        File gribFile = downloadGribFile(url, "precipitation_" + area + ".grb");
        
        return parsePrecipitationData(gribFile);
    }
    
    private File downloadGribFile(String url, String filename) throws Exception {
        try {
            // Create a temporary file
            File tempFile = File.createTempFile("grib", ".grb");
            logger.info("Created temp file: " + tempFile.getAbsolutePath());
            
            // Set headers for API request
            HttpHeaders headers = new HttpHeaders();
            headers.set("User-Agent", "GribAPI/1.0 (https://github.com/yourusername/grib-api)");
            
            // Download the file content with headers
            HttpEntity<String> entity = new HttpEntity<>(headers);
            byte[] gribBytes = restTemplate.exchange(
                url, 
                HttpMethod.GET, 
                entity, 
                byte[].class
            ).getBody();
            
            logger.info("Downloaded " + (gribBytes != null ? gribBytes.length : 0) + " bytes");
            
            // Write to file
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                if (gribBytes != null) {
                    fos.write(gribBytes);
                }
            }
            
            return tempFile;
        } catch (Exception e) {
            logger.severe("Error downloading GRIB file: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    
    private GribResponse parseWaveHeightData(File file) throws Exception {
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;
        
        try (NetcdfFile ncfile = NetcdfFile.open(file.getAbsolutePath())) {
            // Get wave height variable (usually "hs" or similar)
            var waveVar = ncfile.findVariable("hs");
            if (waveVar == null) waveVar = ncfile.findVariable("swh");
            if (waveVar == null) waveVar = ncfile.findVariable("significant_wave_height");
            
            if (waveVar != null) {
                var timeVar = ncfile.findVariable("time");
                var latVar = ncfile.findVariable("latitude");
                if (latVar == null) latVar = ncfile.findVariable("lat");
                
                var lonVar = ncfile.findVariable("longitude");
                if (lonVar == null) lonVar = ncfile.findVariable("lon");
                
                if (timeVar != null && latVar != null && lonVar != null) {
                    // Read data arrays
                    var waveData = waveVar.read();
                    var timeData = timeVar.read();
                    var latData = latVar.read();
                    var lonData = lonVar.read();
                    
                    // Process the data
                    for (int t = 0; t < timeData.getSize(); t++) {
                        long timestamp = timeData.getLong(t) * 1000; // Convert to milliseconds
                        
                        for (int latIdx = 0; latIdx < latData.getSize(); latIdx++) {
                            for (int lonIdx = 0; lonIdx < lonData.getSize(); lonIdx++) {
                                double lat = latData.getDouble(latIdx);
                                double lon = lonData.getDouble(lonIdx);
                                
                                // Get value based on array dimensions
                                double value;
                                if (waveData.getRank() == 3) {
                                    Index index = waveData.getIndex();
                                    index.set(t, latIdx, lonIdx);
                                    value = waveData.getDouble(index);
                                } else {
                                    Index index = waveData.getIndex();
                                    index.set(latIdx, lonIdx);
                                    value = waveData.getDouble(index);
                                }
                                
                                if (!Double.isNaN(value)) {
                                    dataPoints.add(new GribDataPoint(lat, lon, value, timestamp));
                                    minValue = Math.min(minValue, value);
                                    maxValue = Math.max(maxValue, value);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return new GribResponse(
            dataPoints,
            minValue != Double.MAX_VALUE ? minValue : 0.0,
            maxValue != Double.MIN_VALUE ? maxValue : 0.0,
            "WAVE_HEIGHT"
        );
    }
    
    // Implement similar methods for other data types
    private GribResponse parseWindSpeedData(File file) throws Exception {
        // Similar implementation as parseWaveHeightData but for wind speed
        // For now returning empty data for brevity
        return new GribResponse(new ArrayList<>(), 0, 0, "WIND_SPEED");
    }
    
    private GribResponse parseWindDirectionData(File file) throws Exception {
        // Similar implementation as parseWaveHeightData but for wind direction
        return new GribResponse(new ArrayList<>(), 0, 0, "WIND_DIRECTION");
    }
    
    private GribResponse parseCurrentSpeedData(File file) throws Exception {
        // Similar implementation for current speed
        return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_SPEED");
    }
    
    private GribResponse parseCurrentDirectionData(File file) throws Exception {
        // Similar implementation for current direction
        return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_DIRECTION");
    }
    
    private GribResponse parsePrecipitationData(File file) throws Exception {
        // Similar implementation for precipitation
        return new GribResponse(new ArrayList<>(), 0, 0, "PRECIPITATION");
    }
}