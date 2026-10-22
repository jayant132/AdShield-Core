-- Trino dialect of the key investigations (Hive/Iceberg table `events` with the same columns).
-- Differences from Postgres: date_diff / to_unixtime, approx_percentile, count_if, date_trunc buckets.

-- 1. Reward farming (sliding 5-minute window via range frame on epoch seconds).
SELECT device_id, MAX(rewards_5m) AS peak_rewards_5m
FROM (
    SELECT device_id,
           COUNT(*) OVER (PARTITION BY device_id ORDER BY to_unixtime(event_ts)
                          RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) AS rewards_5m
    FROM events WHERE event_type = 'reward'
)
GROUP BY device_id
HAVING MAX(rewards_5m) > 5
ORDER BY peak_rewards_5m DESC;

-- 2. Click-to-install delay per campaign.
WITH pairs AS (
    SELECT i.campaign_id,
           date_diff('second', MAX(c.event_ts), i.event_ts) AS delay_s
    FROM events i
    JOIN events c ON c.device_id = i.device_id AND c.campaign_id = i.campaign_id
                 AND c.event_type = 'click' AND c.event_ts <= i.event_ts
    WHERE i.event_type = 'install'
    GROUP BY i.event_id, i.campaign_id, i.event_ts
)
SELECT campaign_id, COUNT(*) AS installs,
       approx_percentile(delay_s, 0.5) AS p50_s,
       count_if(delay_s < 10) AS under_10s
FROM pairs GROUP BY campaign_id ORDER BY under_10s DESC;

-- 3. Device farm: synchronized rewards per IP in 2-minute buckets.
SELECT ip,
       from_unixtime(floor(to_unixtime(event_ts) / 120) * 120) AS bucket,
       COUNT(DISTINCT device_id) AS devices
FROM events
WHERE event_type = 'reward' AND ip IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(DISTINCT device_id) >= 5
ORDER BY devices DESC;

-- 4. Conversion-rate drift per campaign and hour.
SELECT campaign_id, date_trunc('hour', event_ts) AS hour,
       count_if(event_type = 'click')   AS clicks,
       count_if(event_type = 'install') AS installs,
       round(100.0 * count_if(event_type = 'install') / nullif(count_if(event_type = 'click'), 0), 1) AS cvr_pct
FROM events GROUP BY 1, 2 ORDER BY 1, 2;
