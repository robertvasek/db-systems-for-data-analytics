BEGIN;


INSERT INTO public.dim_date (day, month, year, quarter, day_of_week, date)
SELECT DISTINCT
    EXTRACT(DAY FROM tu.time)::integer,
    EXTRACT(MONTH FROM tu.time)::integer,
    EXTRACT(YEAR FROM tu.time)::integer,
    EXTRACT(QUARTER FROM tu.time)::integer,
    EXTRACT(ISODOW FROM tu.time)::integer,
    tu.time::date as date
FROM staging.import_tracking_upd tu
WHERE NOT EXISTS (
    SELECT 1 FROM public.dim_date d WHERE d.date = tu.time::date
);

-- ------------------

UPDATE public.dim_car d
SET 
    valid_to = '2025-04-01 00:00:00'::timestamp,
    current_row = 'inactive'
FROM staging.car_info_upd cu
WHERE d.car_key = cu.car_key
  AND d.current_row = 'active'
  AND (
       d.company_key   IS DISTINCT FROM cu.company_key OR
       d.license_plate IS DISTINCT FROM COALESCE(NULLIF(cu.license_plate, ''), 'UNKNOWN') OR
       d.make          IS DISTINCT FROM COALESCE(NULLIF(cu.make, ''), 'UNKNOWN') OR
       d.color         IS DISTINCT FROM COALESCE(NULLIF(cu.color, ''), 'UNKNOWN') OR
       d.tonnage       IS DISTINCT FROM cu.tonnage::real OR
       d.type          IS DISTINCT FROM COALESCE(NULLIF(cu.type, ''), 'UNKNOWN')
  );

-- ------------------

INSERT INTO public.dim_car (
    car_key, company_key, license_plate, make, color, tonnage, type, 
    valid_from, valid_to, current_row
)
SELECT 
    cu.car_key,
    cu.company_key,
    COALESCE(NULLIF(cu.license_plate, ''), 'UNKNOWN') AS license_plate,
    COALESCE(NULLIF(cu.make, ''), 'UNKNOWN') AS make,
    COALESCE(NULLIF(cu.color, ''), 'UNKNOWN') AS color,
    cu.tonnage::real,
    COALESCE(NULLIF(cu.type, ''), 'UNKNOWN') AS type,
    '2025-04-01 00:00:00'::timestamp,
    'infinity'::timestamp,
    'active'
FROM staging.car_info_upd cu
WHERE NOT EXISTS (
    SELECT 1 FROM public.dim_car d 
    WHERE d.car_key = cu.car_key AND d.current_row = 'active'
);

-- ------------------

INSERT INTO public.fact_tracking (
    company_id, car_id, driver_id, time_id, date_id, 
    pos_key, "time", truck_status, pos_gps, speed, distance, driving_time
)
SELECT 
    co.company_id,
    c.car_id,        
    d.driver_id,
    t.time_id,
    dt.date_id,
    s.pos_key,
    s."time",
    s.truck_status,
    s.pos_gps,
    s.speed::numeric,       
    s.distance::numeric,    
    s.driving_time::integer 
FROM staging.import_tracking_upd s
-- JOIN 1: Find the Car (SCD Logic)
JOIN public.dim_car c 
    ON s.car_key = c.car_key 
    AND s."time" >= c.valid_from 
    AND s."time" < c.valid_to
-- JOIN 2: Find the Company (Changed to INNER JOIN to enforce NOT NULL)
JOIN public.dim_company co 
    ON c.company_key = co.company_key 
    AND co.current_row = 'active'
-- JOIN 3: Find the Driver (Changed to INNER JOIN to avoid NULL driver_ids)
JOIN public.dim_driver d 
    ON s.driver_key = d.driver_key  
    AND d.current_row = 'active'
-- JOIN 4: Find the Date
JOIN public.dim_date dt 
    ON TO_CHAR(s."time", 'YYYYMMDD')::integer = dt.date_id
-- JOIN 5: Find the Time
JOIN public.dim_time t 
    ON EXTRACT(HOUR FROM s."time") = t.hour
    AND EXTRACT(MINUTE FROM s."time") = t.minute
    AND EXTRACT(SECOND FROM s."time") = t.second
WHERE NOT EXISTS (
    SELECT 1 FROM public.fact_tracking f 
    WHERE f.pos_key = s.pos_key AND f."time" = s."time"
);

COMMIT;
