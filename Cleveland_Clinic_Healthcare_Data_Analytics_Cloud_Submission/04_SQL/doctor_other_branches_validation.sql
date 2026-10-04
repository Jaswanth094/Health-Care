ALTER TABLE processed_doctor_other_branches
SET TBLPROPERTIES ('skip.header.line.count'='1');

ALTER TABLE processed_doctor_other_branches
REPLACE COLUMNS (doctor_id string, branch_id string);

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(doctor_id, '|', branch_id)) AS unique_relationships
FROM processed_doctor_other_branches;
