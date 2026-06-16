-- Daily Trip Summary
SELECT year, month, day, total_trips, avg_trip_minutes, avg_fare
FROM daily_summary
ORDER BY year DESC, month DESC, day DESC
LIMIT 100;

-- Monthly Revenue Summary
SELECT year, month, total_revenue, avg_fare
FROM monthly_revenue
ORDER BY year DESC, month DESC
LIMIT 100;

-- Peak Hour Analysis
SELECT hour, trips
FROM peak_hour
ORDER BY trips DESC
LIMIT 24;

-- Driver Ranking (top drivers by trips)
SELECT driver_id, trips, avg_fare
FROM driver_performance
ORDER BY trips DESC
LIMIT 100;

-- Location Popularity (top pickup locations)
SELECT pickup_latitude, pickup_longitude, pickup_count
FROM location_popularity
ORDER BY pickup_count DESC
LIMIT 1000;

-- Monthly KPIs example: trips and revenue per month
SELECT year, month, SUM(total_trips) as month_trips, SUM(total_revenue) as month_revenue
FROM (
  SELECT year, month, total_trips, NULL as total_revenue FROM daily_summary
  UNION ALL
  SELECT year, month, NULL as total_trips, total_revenue FROM monthly_revenue
) t
GROUP BY year, month
ORDER BY year DESC, month DESC;
