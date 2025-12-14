-- This script must be able to run as is, i.e., psql <etl-incr.sql
-- This script may not be "idempotent", so you may not include commands for "new data cleanup"

BEGIN;

-- This SQL script that will:
-- 1/ update the necessary dimension tables

-- 1a. Update dim_date (Insert any missing dates found in the new April data)
INSERT INTO dim_date (date_id, day, month, year, quarter, day_of_week, date)
SELECT DISTINCT 
    TO_CHAR(time, 'YYYYMMDD')::integer,
    EXTRACT(DAY FROM time),
    EXTRACT(MONTH FROM time),
    EXTRACT(YEAR FROM time),
    EXTRACT(QUARTER FROM time),
    EXTRACT(ISODOW FROM time),
    time::date
FROM staging.import_tracking_upd s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_date d WHERE d.date_id = TO_CHAR(s.time, 'YYYYMMDD')::integer
);

-- 1b. Update dim_car (SCD Type 2: Expire old records)
-- We expire the currently active row ('Y') if the attributes in staging differ.
UPDATE dim_car d
SET 
    valid_to = '2025-04-01 00:00:00',
    current_row = 'N'
FROM staging.car_info_upd s
WHERE d.car_key = s.car_key 
  AND d.current_row = 'Y'
  AND (
       d.license_plate IS DISTINCT FROM s.license_plate OR
       d.make          IS DISTINCT FROM s.make OR
       d.color         IS DISTINCT FROM s.color OR
       d.tonnage       IS DISTINCT FROM s.tonnage OR
       d.type          IS DISTINCT FROM s.type
  );

-- 1c. Update dim_car (SCD Type 2: Insert new versions)
-- Insert the new data as the active row effective from April 1st.
INSERT INTO dim_car (
    car_key, company_key, license_plate, make, color, tonnage, type, 
    valid_from, valid_to, current_row
)
SELECT 
    s.car_key, 
    s.company_key, 
    s.license_plate, 
    s.make, 
    s.color, 
    s.tonnage, 
    s.type,
    '2025-04-01 00:00:00'::timestamp,
    '9999-12-31 00:00:00'::timestamp,
    'Y'
FROM staging.car_info_upd s
WHERE NOT EXISTS (
    -- Prevent inserting duplicates if the row was already inserted
    SELECT 1 FROM dim_car d 
    WHERE d.car_key = s.car_key AND d.current_row = 'Y'
);


-- 2/ inserts new facts into tracking

INSERT INTO fact_tracking (
    company_id, car_id, driver_id, time_id, date_id, 
    pos_key, time, truck_status, pos_gps, speed, distance, driving_time
)
SELECT 
    co.company_id,
    c.car_id,        -- The specific version of the car valid at the time of the trip
    d.driver_id,
    t.time_id,
    dt.date_id,
    s.pos_key,
    s.time,
    s.truck_status,
    s.pos_gps,
    s.speed,
    s.distance,
    s.driving_time
FROM staging.import_tracking_upd s
-- Lookup Car: Match Key AND Time Range (SCD Logic)
JOIN dim_car c 
    ON s.car_key = c.car_key 
    AND s.time >= c.valid_from 
    AND s.time < c.valid_to
-- Lookup Driver: Match Name (Assuming current active driver)
LEFT JOIN dim_driver d 
    ON s.driver_name = d.name 
    AND d.current_row = 'Y'
-- Lookup Company: Match Name (Assuming current active company)
LEFT JOIN dim_company co 
    ON s.company_name = co.company 
    AND co.current_row = 'Y'
-- Lookup Date: Match Date ID
JOIN dim_date dt 
    ON TO_CHAR(s.time, 'YYYYMMDD')::integer = dt.date_id
-- Lookup Time: Match Hour/Min/Sec
JOIN dim_time t 
    ON EXTRACT(HOUR FROM s.time) = t.hour
    AND EXTRACT(MINUTE FROM s.time) = t.minute
    AND EXTRACT(SECOND FROM s.time) = t.second;

COMMIT;
