SELECT
    CAST(date_of_consultation AS DATE) AS consultation_date,
    COUNT(*) AS total_visits
FROM processed_appointments
GROUP BY CAST(date_of_consultation AS DATE)
ORDER BY consultation_date;
