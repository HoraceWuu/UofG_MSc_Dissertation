CREATE TABLE dissertation.fhv1712 (
dispatching_base_num VARCHAR(100),
pickup_datetime BIGINT,
dropOff_datetime BIGINT,
PUlocationID DOUBLE,
DOlocationID DOUBLE,
SR_Flag DOUBLE,
Affiliated_base_number VARCHAR(100)
) stored as parquet;



load DATA LOCAL inpath '/Users/horace/Documents/UofG/5080P/UofG_MSc_Dissertation/data/odata/parquet/2017/fhv_tripdata_2017-12.parquet' INTO TABLE fhv1712;




-- 
-- UPDATE fhv_202202 SET pickup_datetime = from_unixtime(CAST (pickup_datetime/1000000 as bigint)) WHERE pickup_datetime IS NOT NULL;

CREATE TABLE dissertation.hvfhv201902 (
	hvfhs_license_num VARCHAR(100),
	dispatching_base_num VARCHAR(100),
	originating_base_num VARCHAR(100),
	request_datetime BIGINT,
	on_scene_datetime BIGINT,
	pickup_datetime BIGINT,
	dropoff_datetime BIGINT,
	PULocationID BIGINT,
	DOLocationID BIGINT,
	trip_miles DOUBLE,
	trip_time BIGINT,
	base_passenger_fare DOUBLE,
	tolls DOUBLE,
	bcf DOUBLE,
	sales_tax DOUBLE,
	congestion_surcharge DOUBLE,
	airport_fee INT,
	tips DOUBLE,
	driver_pay DOUBLE,
	shared_request_flag VARCHAR(100),
	shared_match_flag VARCHAR(100),
	access_a_ride_flag VARCHAR(100),
	wav_request_flag VARCHAR(100),
	wav_match_flag INT
) stored as parquet;


load DATA LOCAL inpath '/Users/horace/Documents/UofG/5080P/UofG_MSc_Dissertation/data/odata/parquet/fhvhv_tripdata_2019-02.parquet' INTO TABLE hvfhv201902;



SELECT COUNT(*) from fhv1712 WHERE pulocationid is not NULL AND dolocationid is NOT NULL; 

SELECT COUNT(*) from fhv1712 WHERE pulocationid != dolocationid ;
drop view hvfhv_201902;

CREATE view hvfhv_201902 as 
select hvfhs_license_num,
	dispatching_base_num,
	originating_base_num ,
	CASE 
        WHEN request_datetime IS NOT NULL THEN from_unixtime(CAST(request_datetime/1000000 as BIGINT))
        ELSE request_datetime
    END as request_datetime,
    CASE 
        WHEN on_scene_datetime IS NOT NULL THEN from_unixtime(CAST(request_datetime/1000000 as BIGINT))
        ELSE on_scene_datetime
    END as on_scene_datetime,
    CASE 
        WHEN pickup_datetime IS NOT NULL THEN from_unixtime(CAST(request_datetime/1000000 as BIGINT))
        ELSE pickup_datetime
        END as pickup_datetime,
    CASE 
        WHEN dropoff_datetime IS NOT NULL THEN from_unixtime(CAST(request_datetime/1000000 as BIGINT))
        ELSE dropoff_datetime
    END as dropoff_datetime,
    pulocationid,
    dolocationid,
    trip_miles,
    trip_time,
    base_passenger_fare,
    tolls,
    bcf,
    sales_tax ,
    congestion_surcharge ,
    airport_fee ,
    tips ,
    driver_pay ,
    shared_request_flag ,
    shared_match_flag ,
    access_a_ride_flag ,
    wav_request_flag ,
    wav_match_flag 
FROM hvfhv201902;



SELECT DAY(pickup_datetime) AS day, HOUR(pickup_datetime) AS hour, pulocationid, dolocationid, COUNT(*) AS count
FROM hvfhv_201902
WHERE MONTH(pickup_datetime) = 2
GROUP BY DAY(pickup_datetime), HOUR(pickup_datetime), pulocationid, dolocationid
HAVING COUNT(*) > 1;


SELECT pulocationid, dolocationid, COUNT(*) AS count
FROM hvfhv_201902
WHERE MONTH(pickup_datetime) = 2
GROUP BY DAY(pickup_datetime), HOUR(pickup_datetime), pulocationid, dolocationid
HAVING COUNT(*) > 1;


SELECT COUNT(*)
From trips_23;




CREATE TABLE dissertation.top50zones (
	LocationID INT
)

load DATA LOCAL inpath '/Users/horace/Documents/UofG/5080P/UofG_MSc_Dissertation/data/count/q2/step1/top50_zones.csv' INTO TABLE top50zones;


