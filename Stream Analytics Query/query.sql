-- Send raw data to Blob
SELECT
    *
INTO
    [historical-data]
FROM
    [iot-rideau-canal-surt0008]
TIMESTAMP BY timestamp;


-- Send aggregated data to Cosmos 
SELECT
    CONCAT(location, '-', CAST(System.Timestamp AS NVARCHAR(MAX))) AS id,
    location,
    AVG(iceThicknessCm)       AS avgIceThicknessCm,
    AVG(surfaceTempC)         AS avgSurfaceTempC,
    AVG(snowAccumulationCm)   AS avgSnowAccumulationCm,
    AVG(externalTempC)        AS avgExternalTempC,
    System.Timestamp          AS windowEnd
INTO
    [SensorAggregations]
FROM
    [iot-rideau-canal-surt0008]
TIMESTAMP BY timestamp
GROUP BY
    location,
    TumblingWindow(minute, 1);