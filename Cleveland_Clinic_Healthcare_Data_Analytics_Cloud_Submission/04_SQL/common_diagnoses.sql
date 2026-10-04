SELECT
    diagnosis,
    COUNT(*) AS diagnosis_count
FROM processed_appointments
GROUP BY diagnosis
ORDER BY diagnosis_count DESC
LIMIT 10;
