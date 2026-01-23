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



SELECT PULocationID,
       SUM(fare_amount) AS total_amount
FROM nyc_taxi
WHERE lpep_pickup_datetime >= '2025-11-18'
  AND lpep_pickup_datetime <  '2025-11-19'
GROUP BY PULocationID
ORDER BY total_amount DESC;