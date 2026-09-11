-- Additional checks to be run after the ETL batch.
SELECT member_id, COUNT(*) AS duplicate_count
FROM stg_member_profiles
GROUP BY member_id
HAVING COUNT(*) > 1;

SELECT member_id, COUNT(*) AS duplicate_count
FROM fct_member_redemptions
GROUP BY member_id, txn_id
HAVING COUNT(*) > 1;

SELECT r.*
FROM fct_member_redemptions r
LEFT JOIN stg_member_profiles m ON r.member_id = m.member_id
WHERE m.member_id IS NULL;

SELECT *
FROM fct_member_redemptions
WHERE miles_redeemed < 0;
