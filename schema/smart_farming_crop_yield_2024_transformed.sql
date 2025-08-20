-- Create flat table without partitions
CREATE TABLE smart_farming_crop_yield_2024_transformed (
    farm_id TEXT,
    region TEXT,
    crop_type TEXT,
    "soil_moisture_%" REAL,
    soil_pH REAL,
    temperature_C REAL,
    rainfall_mm REAL,
    "humidity_%" REAL,
    sunlight_hours REAL,
    irrigation_type TEXT,
    fertilizer_type TEXT,
    pesticide_usage_ml REAL,
    sowing_date DATE,
    harvest_date DATE,
    total_days INTEGER,
    yield_kg_per_hectare REAL,
    sensor_id TEXT,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    NDVI_index REAL,
    crop_disease_status TEXT,
    -- Additional Spark columns
    file_path TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash TEXT,
    PRIMARY KEY (farm_id, sensor_id, timestamp)
);

-- Recommended indexes
CREATE INDEX idx_smart_farming_crop_yield_2024_transformed_sensor 
    ON smart_farming_crop_yield_2024_transformed (sensor_id);

CREATE INDEX idx_smart_farming_crop_yield_2024_transformed_crop_type 
    ON smart_farming_crop_yield_2024_transformed (crop_type);

CREATE INDEX idx_smart_farming_crop_yield_2024_transformed_region_crop 
    ON smart_farming_crop_yield_2024_transformed (region, crop_type);
