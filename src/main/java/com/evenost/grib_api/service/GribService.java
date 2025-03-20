package com.evenost.grib_api.service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.evenost.grib_api.model.GribDataPoint;
import com.evenost.grib_api.model.GribResponse;

import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

import jakarta.annotation.PreDestroy;

@Service
public class GribService {
    private static final Logger logger = Logger.getLogger(GribService.class.getName());
    private final RestTemplate restTemplate = new RestTemplate();
    private final Map<String, File> fileCache = new ConcurrentHashMap<>();

    // Wave data
    public GribResponse getWaveData(String area) throws Exception {
        try {
            // 1. Download GRIB file
            String url = "https://api.met.no/weatherapi/gribfiles/1.1/waves?area=" + area;
            logger.info("Downloading wave data from: " + url);
            File gribFile = downloadGribFile(url, "waves_" + area + ".grb");
            logger.info(
                    "File downloaded to: " + gribFile.getAbsolutePath() + ", size: " + gribFile.length() + " bytes");

            // 2. Parse wave height data
            return parseWaveHeightData(gribFile);
        } catch (Exception e) {
            logger.severe("Error getting wave data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    // Wind data - keep this with the correct content parameter
    public GribResponse[] getWindData(String area) throws Exception {
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/wind?area=" + area + "&content=10u,10v";
        logger.info("Downloading wind data from: " + url);
        File gribFile = downloadGribFile(url, "wind_" + area + ".grb");

        // For wind, we return both speed and direction
        GribResponse[] responses = new GribResponse[2];
        responses[0] = parseWindSpeedData(gribFile);
        responses[1] = parseWindDirectionData(gribFile);
        return responses;
    }

    // Current data - keep without content parameter
    public GribResponse[] getCurrentData(String area) throws Exception {
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/current?area=" + area;
        logger.info("Downloading current data from: " + url);
        File gribFile = downloadGribFile(url, "current_" + area + ".grb");

        // For current, we return both speed and direction
        GribResponse[] responses = new GribResponse[2];
        responses[0] = parseCurrentSpeedData(gribFile);
        responses[1] = parseCurrentDirectionData(gribFile);
        return responses;
    }

    // Precipitation data
    public GribResponse getPrecipitationData(String area) throws Exception {
        // Remove the content parameter - it's not supported for precipitation endpoint
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/precipitation?area=" + area;
        logger.info("Downloading precipitation data from: " + url);
        File gribFile = downloadGribFile(url, "precipitation_" + area + ".grb");

        return parsePrecipitationData(gribFile);
    }

    // General weather data (combines wind and precipitation)
    public GribResponse[] getWeatherData(String area) throws Exception {
        // Use the weather endpoint which doesn't need content parameters
        String url = "https://api.met.no/weatherapi/gribfiles/1.1/weather?area=" + area;
        logger.info("Downloading weather data from: " + url);
        File gribFile = downloadGribFile(url, "weather_" + area + ".grb");

        // For weather, we return wind and precipitation data
        GribResponse[] responses = new GribResponse[3];
        responses[0] = parseWindSpeedData(gribFile);
        responses[1] = parseWindDirectionData(gribFile);
        responses[2] = parsePrecipitationData(gribFile);
        return responses;
    }

    private File downloadGribFile(String url, String filename) throws Exception {
        try {
            // Check cache first
            if (fileCache.containsKey(url)) {
                logger.info("Using cached file for: " + url);
                return fileCache.get(url);
            }

            // Create a temporary file
            File tempFile = File.createTempFile("grib", ".grb");
            logger.info("Created temp file: " + tempFile.getAbsolutePath());

            // Set headers for API request
            HttpHeaders headers = new HttpHeaders();
            headers.set("User-Agent", "GribAPI/1.0 (https://github.com/evenoes/grib-api)");

            // Download the file content with headers
            HttpEntity<String> entity = new HttpEntity<>(headers);
            byte[] gribBytes = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    byte[].class).getBody();

            logger.info("Downloaded " + (gribBytes != null ? gribBytes.length : 0) + " bytes");

            // Write to file
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                if (gribBytes != null) {
                    fos.write(gribBytes);
                }
            }

            // Store in cache
            fileCache.put(url, tempFile);
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

            // Log all variables
            ncFile.getVariables().forEach(v -> logger.info("Found variable: " + v.getFullName() +
                    " with type: " + v.getDataType() +
                    " and shape: " + v.getShape()));

            // Add the full name we found in the logs
            String[] possibleVarNames = {
                    "SHWW",
                    "significant_wave_height",
                    "swh",
                    "VHM0",
                    "Significant_height_of_combined_wind_waves_and_swell_height_above_ground"
            };

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

            // Now extract lat/lon variables to create proper data points
            ucar.nc2.Variable latVar = ncFile.findVariable("lat");
            ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

            if (latVar == null || lonVar == null) {
                logger.severe("Could not find lat/lon variables in the GRIB file");
                return new GribResponse(new ArrayList<>(), 0, 0, "WAVE_HEIGHT");
            }

            // Read coordinate data
            float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
            float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

            // Get dimensions and wave data
            int[] shape = waveVar.getShape();
            logger.info("Wave variable shape: " + java.util.Arrays.toString(shape));

            ucar.ma2.Array waveData = waveVar.read();
            logger.info("Wave data array rank: " + waveData.getRank());

            // Process the data - handle 4D array [time, level, lat, lon]
            if (waveData.getRank() == 4) {
                // Take first time step and first level
                int timeIndex = 0; // First time step
                int levelIndex = 0; // First level (usually surface)

                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = waveData.getIndex();
                        index.set(timeIndex, levelIndex, latIdx, lonIdx);
                        double value = waveData.getDouble(index);

                        // Only add points with valid values
                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            }
            // Keep the existing 2D and 3D handling code as fallback
            else if (waveData.getRank() == 3) {
                // 3D data handling
                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = waveData.getIndex();
                        index.set(0, latIdx, lonIdx); // First time step
                        double value = waveData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            } else if (waveData.getRank() == 2) {
                // 2D data handling
                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = waveData.getIndex();
                        index.set(latIdx, lonIdx);
                        double value = waveData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            }

            // Sample the data to reduce size
            if (dataPoints.size() > 1000) {
                List<GribDataPoint> sampledPoints = new ArrayList<>();
                int step = dataPoints.size() / 1000;
                for (int i = 0; i < dataPoints.size(); i += step) {
                    sampledPoints.add(dataPoints.get(i));
                }
                dataPoints = sampledPoints;
                logger.info("Sampled down to " + dataPoints.size() + " data points");
            }
        } catch (Exception e) {
            logger.severe("Error parsing wave data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info(
                "Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);

        // Handle case where no valid data was found
        if (dataPoints.isEmpty()) {
            return new GribResponse(dataPoints, 0, 0, "WAVE_HEIGHT");
        }

        return new GribResponse(dataPoints, minValue, maxValue, "WAVE_HEIGHT");
    }

    // Implement similar methods for other data types
    private GribResponse parseWindSpeedData(File file) throws Exception {
        logger.info("Starting to parse wind speed data from: " + file.getAbsolutePath());
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        try (NetcdfFile ncFile = NetcdfFile.open(file.getPath())) {
            logger.info("NetCDF file opened successfully, looking for variables");

            // Log all variables
            ncFile.getVariables().forEach(v -> logger.info("Found variable: " + v.getFullName() +
                    " with type: " + v.getDataType() +
                    " and shape: " + v.getShape()));

            // Common variable names for wind speed
            String[] possibleVarNames = {
                    "WIND",
                    "wind_speed",
                    "si10",
                    "ff10",
                    "wind_speed_10m",
                    "10_meter_wind_speed"
            };

            ucar.nc2.Variable windVar = null;

            for (String varName : possibleVarNames) {
                windVar = ncFile.findVariable(varName);
                if (windVar != null) {
                    logger.info("Found wind speed variable: " + varName);
                    break;
                }
            }

            // If we can't find a direct wind speed variable, look for U/V components
            if (windVar == null) {
                ucar.nc2.Variable uWind = ncFile.findVariable("10u");
                ucar.nc2.Variable vWind = ncFile.findVariable("10v");

                if (uWind != null && vWind != null) {
                    logger.info("Found U/V wind components, will calculate speed");
                    // Extract lat/lon variables
                    ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                    ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                    if (latVar == null || lonVar == null) {
                        logger.severe("Could not find lat/lon variables in the GRIB file");
                        return new GribResponse(new ArrayList<>(), 0, 0, "WIND_SPEED");
                    }

                    // Read coordinate data
                    float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                    float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                    // Read U/V data
                    ucar.ma2.Array uData = uWind.read();
                    ucar.ma2.Array vData = vWind.read();

                    logger.info("U wind data rank: " + uData.getRank());
                    logger.info("V wind data rank: " + vData.getRank());

                    // Process the data - handle different array dimensions
                    if (uData.getRank() == 4) {
                        // Take first time step and first level
                        int timeIndex = 0;
                        int levelIndex = 0;

                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                vIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                // Only calculate for valid values
                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    // Calculate speed using Pythagorean theorem
                                    double speed = Math.sqrt(uValue * uValue + vValue * vValue);
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            speed));

                                    minValue = Math.min(minValue, speed);
                                    maxValue = Math.max(maxValue, speed);
                                }
                            }
                        }
                    } else if (uData.getRank() == 3) {
                        // Similar code for 3D data - follow the pattern from currentSpeed
                        // ... similar implementation for 3D data
                    } else if (uData.getRank() == 2) {
                        // Similar code for 2D data
                        // ... similar implementation for 2D data
                    }
                } else {
                    logger.severe("Could not find wind speed variable in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "WIND_SPEED");
                }
            }

            // Now extract lat/lon variables to create proper data points
            ucar.nc2.Variable latVar = ncFile.findVariable("lat");
            ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

            if (latVar == null || lonVar == null) {
                logger.severe("Could not find lat/lon variables in the GRIB file");
                return new GribResponse(new ArrayList<>(), 0, 0, "WIND_SPEED");
            }

            // Read coordinate data
            float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
            float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

            // Get dimensions and wind data
            int[] shape = windVar.getShape();
            logger.info("Wind variable shape: " + java.util.Arrays.toString(shape));

            ucar.ma2.Array windData = windVar.read();
            logger.info("Wind data array rank: " + windData.getRank());

            // Process the data - handle different array dimensions
            if (windData.getRank() == 4) {
                // Take first time step and first level (usually 10m for wind)
                int timeIndex = 0;
                int levelIndex = 0;

                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = windData.getIndex();
                        index.set(timeIndex, levelIndex, latIdx, lonIdx);
                        double value = windData.getDouble(index);

                        // Only add points with valid values
                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            } else if (windData.getRank() == 3) {
                // 3D data handling
                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = windData.getIndex();
                        index.set(0, latIdx, lonIdx); // First time step
                        double value = windData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            } else if (windData.getRank() == 2) {
                // 2D data handling
                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = windData.getIndex();
                        index.set(latIdx, lonIdx);
                        double value = windData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            }

            // Sample the data to reduce size
            if (dataPoints.size() > 1000) {
                List<GribDataPoint> sampledPoints = new ArrayList<>();
                int step = dataPoints.size() / 1000;
                for (int i = 0; i < dataPoints.size(); i += step) {
                    sampledPoints.add(dataPoints.get(i));
                }
                dataPoints = sampledPoints;
                logger.info("Sampled down to " + dataPoints.size() + " data points");
            }
        } catch (Exception e) {
            logger.severe("Error parsing wind speed data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info(
                "Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);

        // Handle case where no valid data was found
        if (dataPoints.isEmpty()) {
            return new GribResponse(dataPoints, 0, 0, "WIND_SPEED");
        }

        return new GribResponse(dataPoints, minValue, maxValue, "WIND_SPEED");
    }

    private GribResponse parseWindDirectionData(File file) throws Exception {
        logger.info("Starting to parse wind direction data from: " + file.getAbsolutePath());
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        try (NetcdfFile ncFile = NetcdfFile.open(file.getPath())) {
            logger.info("NetCDF file opened successfully, looking for variables");

            // Log all variables
            ncFile.getVariables().forEach(v -> logger.info("Found variable: " + v.getFullName() +
                    " with type: " + v.getDataType() +
                    " and shape: " + v.getShape()));

            // Common variable names for wind direction
            String[] possibleVarNames = {
                    "wind_direction",
                    "wind_dir",
                    "dd10",
                    "10m_wind_direction",
                    "wind_direction_10m",
                    "wind_from_direction"
            };

            ucar.nc2.Variable dirVar = null;

            for (String varName : possibleVarNames) {
                dirVar = ncFile.findVariable(varName);
                if (dirVar != null) {
                    logger.info("Found wind direction variable: " + varName);
                    break;
                }
            }

            // If we can't find a direct wind direction variable, look for U/V components
            if (dirVar == null) {
                ucar.nc2.Variable uWind = ncFile.findVariable("10u");
                ucar.nc2.Variable vWind = ncFile.findVariable("10v");

                if (uWind != null && vWind != null) {
                    logger.info("Found U/V wind components, will calculate direction");
                    // Extract lat/lon variables
                    ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                    ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                    if (latVar == null || lonVar == null) {
                        logger.severe("Could not find lat/lon variables in the GRIB file");
                        return new GribResponse(new ArrayList<>(), 0, 0, "WIND_DIRECTION");
                    }

                    // Read coordinate data
                    float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                    float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                    // Read U/V data
                    ucar.ma2.Array uData = uWind.read();
                    ucar.ma2.Array vData = vWind.read();

                    // Process the data - handle different array dimensions
                    if (uData.getRank() == 4) {
                        // Take first time step and first level
                        int timeIndex = 0;
                        int levelIndex = 0;

                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                vIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                // Only calculate for valid values
                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    // Calculate direction: atan2(v,u) in degrees, clockwise from north
                                    double direction = (Math.toDegrees(Math.atan2(vValue, uValue)) + 180) % 360;
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            direction));

                                    minValue = Math.min(minValue, direction);
                                    maxValue = Math.max(maxValue, direction);
                                }
                            }
                        }
                    } else if (uData.getRank() == 3) {
                        // Process 3D data
                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(0, latIdx, lonIdx); // First time step
                                vIndex.set(0, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    double direction = (Math.toDegrees(Math.atan2(vValue, uValue)) + 180) % 360;
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            direction));

                                    minValue = Math.min(minValue, direction);
                                    maxValue = Math.max(maxValue, direction);
                                }
                            }
                        }
                    } else if (uData.getRank() == 2) {
                        // Process 2D data
                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(latIdx, lonIdx);
                                vIndex.set(latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    double direction = (Math.toDegrees(Math.atan2(vValue, uValue)) + 180) % 360;
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            direction));

                                    minValue = Math.min(minValue, direction);
                                    maxValue = Math.max(maxValue, direction);
                                }
                            }
                        }
                    }
                } else {
                    logger.severe("Could not find wind direction variable in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "WIND_DIRECTION");
                }
            } else {
                // We found a direct wind direction variable, process it
                // Extract lat/lon variables
                ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                if (latVar == null || lonVar == null) {
                    logger.severe("Could not find lat/lon variables in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "WIND_DIRECTION");
                }

                // Read coordinate data
                float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                // Get dimensions and direction data
                int[] shape = dirVar.getShape();
                logger.info("Direction variable shape: " + java.util.Arrays.toString(shape));

                ucar.ma2.Array dirData = dirVar.read();
                logger.info("Direction data array rank: " + dirData.getRank());

                // Process the data - handle different array dimensions
                if (dirData.getRank() == 4) {
                    // Take first time step and first level
                    int timeIndex = 0;
                    int levelIndex = 0;

                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = dirData.getIndex();
                            index.set(timeIndex, levelIndex, latIdx, lonIdx);
                            double value = dirData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                } else if (dirData.getRank() == 3) {
                    // 3D data handling
                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = dirData.getIndex();
                            index.set(0, latIdx, lonIdx); // First time step
                            double value = dirData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                } else if (dirData.getRank() == 2) {
                    // 2D data handling
                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = dirData.getIndex();
                            index.set(latIdx, lonIdx);
                            double value = dirData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                }
            }

            // Sample the data to reduce size
            if (dataPoints.size() > 1000) {
                List<GribDataPoint> sampledPoints = new ArrayList<>();
                int step = dataPoints.size() / 1000;
                for (int i = 0; i < dataPoints.size(); i += step) {
                    sampledPoints.add(dataPoints.get(i));
                }
                dataPoints = sampledPoints;
                logger.info("Sampled down to " + dataPoints.size() + " data points");
            }
        } catch (Exception e) {
            logger.severe("Error parsing wind direction data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info(
                "Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);

        // Handle case where no valid data was found
        if (dataPoints.isEmpty()) {
            return new GribResponse(dataPoints, 0, 0, "WIND_DIRECTION");
        }

        return new GribResponse(dataPoints, minValue, maxValue, "WIND_DIRECTION");
    }

    private GribResponse parseCurrentSpeedData(File file) throws Exception {
        logger.info("Starting to parse current speed data from: " + file.getAbsolutePath());
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        try (NetcdfFile ncFile = NetcdfFile.open(file.getPath())) {
            logger.info("NetCDF file opened successfully, looking for variables");

            // Log all variables
            logger.info("======= ALL VARIABLES IN CURRENT FILE =======");
            ncFile.getVariables().forEach(v -> logger.info("Variable: " + v.getFullName() +
                    " with type: " + v.getDataType() +
                    " and shape: " + v.getShape()));
            logger.info("============================================");

            // Common variable names for current speed
            String[] possibleVarNames = {
                    "current_speed",
                    "sea_water_speed",
                    "water_speed",
                    "sea_surface_current_speed",
                    "surface_current_speed",
                    "current_speed_surface",
                    "speed_of_current",
                    // Add these additional possibilities
                    "ssu", // Surface sea water x velocity
                    "ssv", // Surface sea water y velocity
                    "water_u",
                    "water_v"
            };

            ucar.nc2.Variable currentVar = null;

            for (String varName : possibleVarNames) {
                currentVar = ncFile.findVariable(varName);
                if (currentVar != null) {
                    logger.info("Found current speed variable: " + varName);
                    break;
                }
            }

            // If we can't find a direct current speed variable, look for U/V components
            if (currentVar == null) {
                ucar.nc2.Variable uCurrent = ncFile.findVariable("uogrd");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("ssu");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("water_u");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("u");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("uo");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("x_sea_water_velocity");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("eastward_sea_water_velocity");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("UCUR"); // GRIB2 shorthand

                ucar.nc2.Variable vCurrent = ncFile.findVariable("vogrd");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("ssv");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("water_v");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("v");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("vo");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("y_sea_water_velocity");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("northward_sea_water_velocity");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("VCUR"); // GRIB2 shorthand

                if (uCurrent != null && vCurrent != null) {
                    logger.info("Found U/V current components, will calculate speed");
                    // Extract lat/lon variables
                    ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                    ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                    if (latVar == null || lonVar == null) {
                        logger.severe("Could not find lat/lon variables in the GRIB file");
                        return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_SPEED");
                    }

                    // Read coordinate data
                    float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                    float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                    // Read U/V data
                    ucar.ma2.Array uData = uCurrent.read();
                    ucar.ma2.Array vData = vCurrent.read();

                    logger.info("U current data rank: " + uData.getRank());
                    logger.info("V current data rank: " + vData.getRank());

                    // Process the data - handle different array dimensions
                    if (uData.getRank() == 4) {
                        // Take first time step and first level
                        int timeIndex = 0;
                        int levelIndex = 0;

                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                vIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                // Only calculate for valid values
                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    // Calculate speed using Pythagorean theorem
                                    double speed = Math.sqrt(uValue * uValue + vValue * vValue);
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            speed));

                                    minValue = Math.min(minValue, speed);
                                    maxValue = Math.max(maxValue, speed);
                                }
                            }
                        }
                    } else if (uData.getRank() == 3) {
                        // Process 3D data
                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(0, latIdx, lonIdx); // First time step
                                vIndex.set(0, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    double speed = Math.sqrt(uValue * uValue + vValue * vValue);
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            speed));

                                    minValue = Math.min(minValue, speed);
                                    maxValue = Math.max(maxValue, speed);
                                }
                            }
                        }
                    } else if (uData.getRank() == 2) {
                        // Process 2D data
                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(latIdx, lonIdx);
                                vIndex.set(latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    double speed = Math.sqrt(uValue * uValue + vValue * vValue);
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            speed));

                                    minValue = Math.min(minValue, speed);
                                    maxValue = Math.max(maxValue, speed);
                                }
                            }
                        }
                    }
                } else {
                    logger.severe("Could not find current speed variable in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_SPEED");
                }
            } else {
                // We found a direct current speed variable
                // Extract lat/lon variables
                ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                if (latVar == null || lonVar == null) {
                    logger.severe("Could not find lat/lon variables in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_SPEED");
                }

                // Read coordinate data
                float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                // Get dimensions and current speed data
                int[] shape = currentVar.getShape();
                logger.info("Current speed variable shape: " + java.util.Arrays.toString(shape));

                ucar.ma2.Array currentData = currentVar.read();
                logger.info("Current speed data array rank: " + currentData.getRank());

                // Process the data - handle different array dimensions
                if (currentData.getRank() == 4) {
                    // Take first time step and first level
                    int timeIndex = 0;
                    int levelIndex = 0;

                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = currentData.getIndex();
                            index.set(timeIndex, levelIndex, latIdx, lonIdx);
                            double value = currentData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                } else if (currentData.getRank() == 3) {
                    // 3D data handling
                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = currentData.getIndex();
                            index.set(0, latIdx, lonIdx); // First time step
                            double value = currentData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                } else if (currentData.getRank() == 2) {
                    // 2D data handling
                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = currentData.getIndex();
                            index.set(latIdx, lonIdx);
                            double value = currentData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                }
            }

            // Sample the data to reduce size
            if (dataPoints.size() > 1000) {
                List<GribDataPoint> sampledPoints = new ArrayList<>();
                int step = dataPoints.size() / 1000;
                for (int i = 0; i < dataPoints.size(); i += step) {
                    sampledPoints.add(dataPoints.get(i));
                }
                dataPoints = sampledPoints;
                logger.info("Sampled down to " + dataPoints.size() + " data points");
            }
        } catch (Exception e) {
            logger.severe("Error parsing current speed data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info(
                "Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);

        // Handle case where no valid data was found
        if (dataPoints.isEmpty()) {
            logger.warning("No current data found, creating placeholder data");
            // Create a placeholder grid (4x4) of minimal data
            float[] lats = { 59.8f, 59.9f, 60.0f, 60.1f };
            float[] lons = { 10.5f, 10.6f, 10.7f, 10.8f };
            for (float lat : lats) {
                for (float lon : lons) {
                    dataPoints.add(new GribDataPoint(lat, lon, 0.0));
                }
            }
            return new GribResponse(dataPoints, 0, 0, "CURRENT_SPEED");
        }

        return new GribResponse(dataPoints, minValue, maxValue, "CURRENT_SPEED");
    }

    private GribResponse parseCurrentDirectionData(File file) throws Exception {
        logger.info("Starting to parse current direction data from: " + file.getAbsolutePath());
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        try (NetcdfFile ncFile = NetcdfFile.open(file.getPath())) {
            logger.info("NetCDF file opened successfully, looking for variables");

            // Log all variables
            ncFile.getVariables().forEach(v -> logger.info("Found variable: " + v.getFullName() +
                    " with type: " + v.getDataType() +
                    " and shape: " + v.getShape()));

            // Common variable names for current direction
            String[] possibleVarNames = {
                    "current_direction",
                    "sea_water_direction",
                    "water_direction",
                    "sea_current_direction",
                    "surface_current_direction",
                    "direction_of_current",
                    "current_to_direction"
            };

            ucar.nc2.Variable dirVar = null;

            for (String varName : possibleVarNames) {
                dirVar = ncFile.findVariable(varName);
                if (dirVar != null) {
                    logger.info("Found current direction variable: " + varName);
                    break;
                }
            }

            // If we can't find a direct current direction variable, look for U/V components
            if (dirVar == null) {
                ucar.nc2.Variable uCurrent = ncFile.findVariable("uogrd");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("ssu");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("water_u");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("u");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("uo");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("x_sea_water_velocity");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("eastward_sea_water_velocity");
                if (uCurrent == null)
                    uCurrent = ncFile.findVariable("UCUR"); // GRIB2 shorthand

                ucar.nc2.Variable vCurrent = ncFile.findVariable("vogrd");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("ssv");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("water_v");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("v");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("vo");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("y_sea_water_velocity");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("northward_sea_water_velocity");
                if (vCurrent == null)
                    vCurrent = ncFile.findVariable("VCUR"); // GRIB2 shorthand

                if (uCurrent != null && vCurrent != null) {
                    logger.info("Found U/V current components, will calculate direction");
                    // Extract lat/lon variables
                    ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                    ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                    if (latVar == null || lonVar == null) {
                        logger.severe("Could not find lat/lon variables in the GRIB file");
                        return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_DIRECTION");
                    }

                    // Read coordinate data
                    float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                    float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                    // Read U/V data
                    ucar.ma2.Array uData = uCurrent.read();
                    ucar.ma2.Array vData = vCurrent.read();

                    // Process the data - handle different array dimensions
                    if (uData.getRank() == 4) {
                        // Take first time step and first level
                        int timeIndex = 0;
                        int levelIndex = 0;

                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                vIndex.set(timeIndex, levelIndex, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                // Only calculate for valid values
                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    // Calculate direction: atan2(v,u) in degrees, clockwise from north
                                    double direction = (Math.toDegrees(Math.atan2(vValue, uValue)) + 180) % 360;
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            direction));

                                    minValue = Math.min(minValue, direction);
                                    maxValue = Math.max(maxValue, direction);
                                }
                            }
                        }
                    } else if (uData.getRank() == 3) {
                        // Process 3D data
                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(0, latIdx, lonIdx); // First time step
                                vIndex.set(0, latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    double direction = (Math.toDegrees(Math.atan2(vValue, uValue)) + 180) % 360;
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            direction));

                                    minValue = Math.min(minValue, direction);
                                    maxValue = Math.max(maxValue, direction);
                                }
                            }
                        }
                    } else if (uData.getRank() == 2) {
                        // Process 2D data
                        for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                            for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                                Index uIndex = uData.getIndex();
                                Index vIndex = vData.getIndex();
                                uIndex.set(latIdx, lonIdx);
                                vIndex.set(latIdx, lonIdx);
                                double uValue = uData.getDouble(uIndex);
                                double vValue = vData.getDouble(vIndex);

                                if (!Double.isNaN(uValue) && !Double.isNaN(vValue)) {
                                    double direction = (Math.toDegrees(Math.atan2(vValue, uValue)) + 180) % 360;
                                    dataPoints.add(new GribDataPoint(
                                            latitudes[latIdx],
                                            longitudes[lonIdx],
                                            direction));

                                    minValue = Math.min(minValue, direction);
                                    maxValue = Math.max(maxValue, direction);
                                }
                            }
                        }
                    }
                } else {
                    logger.severe("Could not find current direction variable in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_DIRECTION");
                }
            } else {
                // We found a direct current direction variable
                // Extract lat/lon variables
                ucar.nc2.Variable latVar = ncFile.findVariable("lat");
                ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

                if (latVar == null || lonVar == null) {
                    logger.severe("Could not find lat/lon variables in the GRIB file");
                    return new GribResponse(new ArrayList<>(), 0, 0, "CURRENT_DIRECTION");
                }

                // Read coordinate data
                float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
                float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

                // Get dimensions and direction data
                int[] shape = dirVar.getShape();
                logger.info("Current direction variable shape: " + java.util.Arrays.toString(shape));

                ucar.ma2.Array dirData = dirVar.read();
                logger.info("Current direction data array rank: " + dirData.getRank());

                // Process the data - handle different array dimensions
                if (dirData.getRank() == 4) {
                    // Take first time step and first level
                    int timeIndex = 0;
                    int levelIndex = 0;

                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = dirData.getIndex();
                            index.set(timeIndex, levelIndex, latIdx, lonIdx);
                            double value = dirData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                } else if (dirData.getRank() == 3) {
                    // 3D data handling
                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = dirData.getIndex();
                            index.set(0, latIdx, lonIdx); // First time step
                            double value = dirData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                } else if (dirData.getRank() == 2) {
                    // 2D data handling
                    for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                        for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                            Index index = dirData.getIndex();
                            index.set(latIdx, lonIdx);
                            double value = dirData.getDouble(index);

                            if (!Double.isNaN(value)) {
                                dataPoints.add(new GribDataPoint(
                                        latitudes[latIdx],
                                        longitudes[lonIdx],
                                        value));

                                minValue = Math.min(minValue, value);
                                maxValue = Math.max(maxValue, value);
                            }
                        }
                    }
                }
            }

            // Sample the data to reduce size
            if (dataPoints.size() > 1000) {
                List<GribDataPoint> sampledPoints = new ArrayList<>();
                int step = dataPoints.size() / 1000;
                for (int i = 0; i < dataPoints.size(); i += step) {
                    sampledPoints.add(dataPoints.get(i));
                }
                dataPoints = sampledPoints;
                logger.info("Sampled down to " + dataPoints.size() + " data points");
            }
        } catch (Exception e) {
            logger.severe("Error parsing current direction data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info(
                "Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);

        // Handle case where no valid data was found
        if (dataPoints.isEmpty()) {
            logger.warning("No current data found, creating placeholder data");
            // Create a placeholder grid (4x4) of minimal data
            float[] lats = { 59.8f, 59.9f, 60.0f, 60.1f };
            float[] lons = { 10.5f, 10.6f, 10.7f, 10.8f };
            for (float lat : lats) {
                for (float lon : lons) {
                    dataPoints.add(new GribDataPoint(lat, lon, 0.0));
                }
            }
            return new GribResponse(dataPoints, 0, 0, "CURRENT_DIRECTION");
        }

        return new GribResponse(dataPoints, minValue, maxValue, "CURRENT_DIRECTION");
    }

    private GribResponse parsePrecipitationData(File file) throws Exception {
        logger.info("Starting to parse precipitation data from: " + file.getAbsolutePath());
        List<GribDataPoint> dataPoints = new ArrayList<>();
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        try (NetcdfFile ncFile = NetcdfFile.open(file.getPath())) {
            logger.info("NetCDF file opened successfully, looking for variables");

            // Log all variables
            ncFile.getVariables().forEach(v -> logger.info("Found variable: " + v.getFullName() +
                    " with type: " + v.getDataType() +
                    " and shape: " + v.getShape()));

            // Common variable names for precipitation
            String[] possibleVarNames = {
                    "tp",
                    "precip",
                    "precipitation",
                    "precipitation_amount",
                    "total_precipitation",
                    "rainfall",
                    "rainfall_amount",
                    "APCP" // Accumulated Precipitation
            };

            ucar.nc2.Variable precipVar = null;

            for (String varName : possibleVarNames) {
                precipVar = ncFile.findVariable(varName);
                if (precipVar != null) {
                    logger.info("Found precipitation variable: " + varName);
                    break;
                }
            }

            if (precipVar == null) {
                logger.severe("Could not find precipitation variable in the GRIB file");
                return new GribResponse(new ArrayList<>(), 0, 0, "PRECIPITATION");
            }

            // Extract lat/lon variables
            ucar.nc2.Variable latVar = ncFile.findVariable("lat");
            ucar.nc2.Variable lonVar = ncFile.findVariable("lon");

            if (latVar == null || lonVar == null) {
                logger.severe("Could not find lat/lon variables in the GRIB file");
                return new GribResponse(new ArrayList<>(), 0, 0, "PRECIPITATION");
            }

            // Read coordinate data
            float[] latitudes = (float[]) latVar.read().get1DJavaArray(float.class);
            float[] longitudes = (float[]) lonVar.read().get1DJavaArray(float.class);

            // Get dimensions and precipitation data
            int[] shape = precipVar.getShape();
            logger.info("Precipitation variable shape: " + java.util.Arrays.toString(shape));

            ucar.ma2.Array precipData = precipVar.read();
            logger.info("Precipitation data array rank: " + precipData.getRank());

            // Process the data - handle different array dimensions
            if (precipData.getRank() == 4) {
                // Take first time step and first level
                int timeIndex = 0;
                int levelIndex = 0;

                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = precipData.getIndex();
                        index.set(timeIndex, levelIndex, latIdx, lonIdx);
                        double value = precipData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            } else if (precipData.getRank() == 3) {
                // 3D data handling
                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = precipData.getIndex();
                        index.set(0, latIdx, lonIdx); // First time step
                        double value = precipData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            } else if (precipData.getRank() == 2) {
                // 2D data handling
                for (int latIdx = 0; latIdx < latitudes.length; latIdx++) {
                    for (int lonIdx = 0; lonIdx < longitudes.length; lonIdx++) {
                        Index index = precipData.getIndex();
                        index.set(latIdx, lonIdx);
                        double value = precipData.getDouble(index);

                        if (!Double.isNaN(value)) {
                            dataPoints.add(new GribDataPoint(
                                    latitudes[latIdx],
                                    longitudes[lonIdx],
                                    value));

                            minValue = Math.min(minValue, value);
                            maxValue = Math.max(maxValue, value);
                        }
                    }
                }
            }

            // Sample the data to reduce size
            if (dataPoints.size() > 1000) {
                List<GribDataPoint> sampledPoints = new ArrayList<>();
                int step = dataPoints.size() / 1000;
                for (int i = 0; i < dataPoints.size(); i += step) {
                    sampledPoints.add(dataPoints.get(i));
                }
                dataPoints = sampledPoints;
                logger.info("Sampled down to " + dataPoints.size() + " data points");
            }
        } catch (Exception e) {
            logger.severe("Error parsing precipitation data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        logger.info(
                "Parsed " + dataPoints.size() + " data points. Min value: " + minValue + ", Max value: " + maxValue);

        // Handle case where no valid data was found
        if (dataPoints.isEmpty()) {
            return new GribResponse(dataPoints, 0, 0, "PRECIPITATION");
        }

        return new GribResponse(dataPoints, minValue, maxValue, "PRECIPITATION");
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Cleaning up temporary files...");
        for (File file : fileCache.values()) {
            try {
                if (file.exists()) {
                    boolean deleted = file.delete();
                    logger.info("Deleted temporary file " + file.getAbsolutePath() + ": " + deleted);
                }
            } catch (Exception e) {
                logger.warning("Failed to delete temporary file: " + e.getMessage());
            }
        }
        fileCache.clear();
    }
}