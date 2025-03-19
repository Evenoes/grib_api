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
        logger.info("Starting to parse wave height data from: " + file.getAbsolutePath());
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        try (NetcdfFile ncFile = NetcdfFile.open(file.getPath())) {
            logger.info("NetCDF file opened successfully, looking for variables");
            
            // Log all variables to help debug
            ncFile.getVariables().forEach(v -> logger.info("Found variable: " + v.getFullName() + 
                                                         " with type: " + v.getDataType() + 
                                                         " and shape: " + v.getShape()));
            
            // Try to find the wave height variable - try different possible names
            String[] possibleVarNames = {"SHWW", "significant_wave_height", "swh", "VHM0"};
            ucar.nc2.Variable waveVar = null;
            
            for (String varName : possibleVarNames) {
                waveVar = ncFile.findVariable(varName);
                if (waveVar != null) {
                    logger.info("Found wave height variable: " + varName);
                    break;
                }
            }
            
            if (waveVar == null) {
                logger.severe("Could not find wave height variable in the GRIB file");
                return new GribResponse(new ArrayList<>(), 0, 0, "WAVE_HEIGHT");
            }
            
            // Get dimensions
            int[] shape = waveVar.getShape();
            logger.info("Wave variable shape: " + java.util.Arrays.toString(shape));
            
            // Continue with your existing parsing code but with more logging
            ucar.ma2.Array waveData = waveVar.read();
            logger.info("Wave data array rank: " + waveData.getRank());
            
            // Rest of your parsing code...
            
            // For debugging, add at least a few data points
            // This helps ensure the method is working correctly
            if (dataPoints.isEmpty() && waveData.getSize() > 0) {
                logger.warning("No data points extracted but array has data. Adding sample point for debugging.");
                try {
                    Index index = waveData.getIndex();
                    index.set(0, 0); // Get first point as sample
                    double sampleValue = waveData.getDouble(index);
                    dataPoints.add(new GribDataPoint(0.0, 0.0, sampleValue, System.currentTimeMillis()));
                    minValue = maxValue = sampleValue;
                    logger.info("Added sample point with value: " + sampleValue);
                } catch (Exception e) {
                    logger.severe("Error creating sample point: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.severe("Error parsing wave data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info("Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);
        return new GribResponse(dataPoints, minValue, maxValue, "WAVE_HEIGHT");
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