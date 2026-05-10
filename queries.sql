-- ============================================================
-- PhonePe Pulse – SQL Schema & Business Case Study Queries
-- ============================================================

-- ── 1. Top 10 states by total transaction amount ─────────────
SELECT state,
       SUM(transaction_amount) AS total_amount,
       SUM(transaction_count)  AS total_txns
FROM aggregated_transaction
GROUP BY state
ORDER BY total_amount DESC
LIMIT 10;

-- ── 2. Year-over-year transaction growth ─────────────────────
SELECT year,
       SUM(transaction_count)                          AS txn_count,
       SUM(transaction_amount)                         AS txn_amount,
       ROUND(
         (SUM(transaction_amount) - LAG(SUM(transaction_amount)) OVER (ORDER BY year))
         / LAG(SUM(transaction_amount)) OVER (ORDER BY year) * 100, 2
       )                                               AS yoy_growth_pct
FROM aggregated_transaction
GROUP BY year
ORDER BY year;

-- ── 3. Most popular transaction types ────────────────────────
SELECT transaction_type,
       SUM(transaction_count)  AS total_txns,
       SUM(transaction_amount) AS total_amount
FROM aggregated_transaction
GROUP BY transaction_type
ORDER BY total_txns DESC;

-- ── 4. Quarterly trend for a specific state ───────────────────
SELECT year, quarter,
       SUM(transaction_count)  AS txns,
       SUM(transaction_amount) AS amount
FROM aggregated_transaction
WHERE state = 'maharashtra'
GROUP BY year, quarter
ORDER BY year, quarter;

-- ── 5. Top 10 districts by transaction value ─────────────────
SELECT state, district,
       SUM(transaction_amount) AS total_amount
FROM map_transaction
GROUP BY state, district
ORDER BY total_amount DESC
LIMIT 10;

-- ── 6. States with highest registered users ───────────────────
SELECT state,
       MAX(registered_users) AS peak_users
FROM aggregated_user
GROUP BY state
ORDER BY peak_users DESC
LIMIT 10;

-- ── 7. App open engagement rate by state ─────────────────────
SELECT state,
       SUM(registered_users) AS total_users,
       SUM(app_opens)        AS total_opens,
       ROUND(SUM(app_opens) / NULLIF(SUM(registered_users), 0), 2) AS opens_per_user
FROM aggregated_user
GROUP BY state
ORDER BY opens_per_user DESC
LIMIT 10;

-- ── 8. Top 5 pincodes by transaction count ────────────────────
SELECT state, entity_name AS pincode,
       SUM(transaction_count)  AS txns,
       SUM(transaction_amount) AS amount
FROM top_transaction
WHERE entity_type = 'pincode'
GROUP BY state, pincode
ORDER BY txns DESC
LIMIT 5;

-- ── 9. Average transaction value by type ─────────────────────
SELECT transaction_type,
       ROUND(SUM(transaction_amount) / NULLIF(SUM(transaction_count), 0), 2) AS avg_txn_value
FROM aggregated_transaction
GROUP BY transaction_type
ORDER BY avg_txn_value DESC;

-- ── 10. Quarter with highest transactions (all years) ─────────
SELECT quarter,
       SUM(transaction_count)  AS total_txns,
       SUM(transaction_amount) AS total_amount
FROM aggregated_transaction
GROUP BY quarter
ORDER BY total_amount DESC;

-- ── 11. State with fastest user growth ────────────────────────
SELECT state,
       MIN(registered_users) AS users_start,
       MAX(registered_users) AS users_end,
       ROUND((MAX(registered_users) - MIN(registered_users))
             / NULLIF(MIN(registered_users), 0) * 100, 2) AS growth_pct
FROM aggregated_user
GROUP BY state
ORDER BY growth_pct DESC
LIMIT 10;

-- ── 12. Districts with low transactions but high users (opportunity) ──
SELECT mt.state, mt.district,
       SUM(mt.transaction_count)  AS txns,
       SUM(mu.registered_users)   AS users,
       ROUND(SUM(mt.transaction_count)
             / NULLIF(SUM(mu.registered_users), 0), 4) AS txn_per_user
FROM map_transaction mt
JOIN map_user mu
  ON mt.state = mu.state
 AND mt.district = mu.district
 AND mt.year = mu.year
 AND mt.quarter = mu.quarter
GROUP BY mt.state, mt.district
HAVING users > 10000
ORDER BY txn_per_user ASC
LIMIT 10;
