select count(*) 
from nyc_taxi ---limit 5;
where (lpep_pickup_datetime between '2025-11-01' and '2025-12-01') 
    and (trip_distance < 1);

SELECT COUNT(*) AS short_trips
FROM nyc_taxi
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;

SELECT COUNT(*) AS short_trips
 FROM nyc_taxi
 WHERE (lpep_pickup_datetime BETWEEN '2025-11-01'
   AND '2025-12-01')
   AND trip_distance <= 1;
---------------------------------------------------------------------
   
SELECT trip_distance, lpep_pickup_datetime
 FROM nyc_taxi
 WHERE trip_distance <= 100
 ORDER BY trip_distance DESC;
   
----------------------------------------------------------------------

SELECT nt."PULocationID", sum(nt.fare_amount) as total_amount, tl."LocationID", tl.Zone
 FROM nyc_taxi nt
 LEFT JOIN taxi_lookup tl
 ON nt."PULocationID" = tl."LocationID"
WHERE lpep_pickup_datetime >= '2025-11-18'
  AND lpep_pickup_datetime <  '2025-11-19'
GROUP BY "PULocationID"
ORDER BY total_amount DESC;


----------------------------------------------------------------------------------


SELECT PULocationID,
       SUM(fare_amount) AS total_amount
FROM nyc_taxi
WHERE lpep_pickup_datetime >= '2025-11-18'
  AND lpep_pickup_datetime <  '2025-11-19'
GROUP BY PULocationID
ORDER BY total_amount DESC;

-------------------------------------------------------------------------------------

SELECT nt."DOLocationID", sum(nt.tip_amount) as tip_total, tl."LocationID", tl."Zone"
  FROM nyc_taxi nt
  LEFT JOIN taxi_lookup tl
  ON nt."PULocationID" = tl."LocationID"
 WHERE lpep_pickup_datetime >= '2025-11-01'
   AND lpep_pickup_datetime <  '2025-12-01'
   AND tl."Zone" = 'East Harlem North'
 GROUP BY nt."DOLocationID", tl."LocationID",tl."Zone" 
 ORDER BY total_amount DESC;


--------------------------------------------------------------------------------------

SELECT 
  nt."DOLocationID",
  (SELECT "Zone" FROM taxi_lookup WHERE "LocationID" = nt."DOLocationID") as drop_zone, 
  MAX(nt.tip_amount), 
  tl."Zone"
FROM nyc_taxi nt
LEFT JOIN taxi_lookup tl
ON nt."PULocationID" = tl."LocationID"
  WHERE lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime <  '2025-12-01'
    AND nt."PULocationID" = 74
  GROUP BY nt."DOLocationID", tl."LocationID",tl."Zone", drop_zone 
  ORDER BY MAX(nt.tip_amount) DESC;



SELECT 
  nt."DOLocationID",
  dz."Zone" as drop_zone,
  MAX(nt.tip_amount), 
  pu."Zone"
FROM nyc_taxi nt
LEFT JOIN taxi_lookup pu
ON nt."PULocationID" = pu."LocationID"
LEFT JOIN taxi_lookup dz
ON nt."DOLocationID" = dz."LocationID"
  WHERE nt.lpep_pickup_datetime >= '2025-11-01'
    AND nt.lpep_pickup_datetime <  '2025-12-01'
    AND nt."PULocationID" = 74
  GROUP BY nt."DOLocationID", drop_zone, pu."Zone"  
  ORDER BY MAX(nt.tip_amount) DESC;
