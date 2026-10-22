-- Investigation queries (PostgreSQL). Trino equivalents are in investigations_trino.sql.
-- Thresholds mirror internal/detect.DefaultConfig(); tune against labelled data.

-- 1. Reward farming: >5 rewards inside any 5-minute window per device (sliding window).
SELECT device_id, MAX(rewards_5m) AS peak_rewards_5m, COUNT(*) AS total_rewards
FROM (
    SELECT device_id,
           COUNT(*) OVER (PARTITION BY device_id ORDER BY event_ts
                          RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW) AS rewards_5m
    FROM events WHERE event_type = 'reward'
) w
GROUP BY device_id
HAVING MAX(rewards_5m) > 5
ORDER BY peak_rewards_5m DESC;

-- 2. Click-to-install delay distribution per campaign. Injection shows up as a spike near 0s.
WITH pairs AS (
    SELECT i.device_id, i.campaign_id,
           EXTRACT(EPOCH FROM (i.event_ts - MAX(c.event_ts))) AS delay_s
    FROM events i
    JOIN events c ON c.device_id = i.device_id AND c.campaign_id = i.campaign_id
                 AND c.event_type = 'click' AND c.event_ts <= i.event_ts
    WHERE i.event_type = 'install'
    GROUP BY i.event_id, i.device_id, i.campaign_id, i.event_ts
)
SELECT campaign_id,
       COUNT(*)                                            AS installs,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY delay_s) AS p50_s,
       COUNT(*) FILTER (WHERE delay_s < 10)                 AS under_10s,
       ROUND(100.0 * COUNT(*) FILTER (WHERE delay_s < 10) / COUNT(*), 1) AS pct_under_10s
FROM pairs GROUP BY campaign_id ORDER BY pct_under_10s DESC;

-- 3. Click spam: devices with many clicks and zero installs on a campaign.
SELECT c.device_id, c.campaign_id, COUNT(*) AS clicks
FROM events c
WHERE c.event_type = 'click'
  AND NOT EXISTS (SELECT 1 FROM events i
                  WHERE i.device_id = c.device_id AND i.campaign_id = c.campaign_id AND i.event_type = 'install')
GROUP BY c.device_id, c.campaign_id
HAVING COUNT(*) >= 20
ORDER BY clicks DESC;

-- 4. Device farm: IPs with many devices earning rewards in the same 2-minute bucket.
--    A shared IP alone is not fraud, so require synchronization.
SELECT ip, date_bin('2 minutes', event_ts, TIMESTAMPTZ '2026-01-01') AS bucket,
       COUNT(DISTINCT device_id) AS devices, SUM(reward_cents) AS cents
FROM events
WHERE event_type = 'reward' AND ip IS NOT NULL
GROUP BY ip, bucket
HAVING COUNT(DISTINCT device_id) >= 5
ORDER BY devices DESC;

-- 5. Device tampering share per campaign (Xposed/Frida/emulator on reward events).
SELECT campaign_id,
       COUNT(*) AS rewards,
       COUNT(*) FILTER (WHERE hooked OR emulator) AS tampered,
       ROUND(100.0 * COUNT(*) FILTER (WHERE hooked OR emulator) / COUNT(*), 2) AS pct_tampered
FROM events WHERE event_type = 'reward'
GROUP BY campaign_id ORDER BY pct_tampered DESC;

-- 6. Duplicate event ids (should be 0 after idempotent ingestion; non-zero in a raw stream table).
SELECT event_id, COUNT(*) AS deliveries FROM events GROUP BY event_id HAVING COUNT(*) > 1;

-- 7. Conversion-rate drift by campaign and hour: spots pipeline malfunctions vs fraud.
--    Caveat: bucketing by event hour lets installs lag clicks, so cvr can exceed 100%.
--    For real baselines, cohort installs by the hour of their *click*.
SELECT campaign_id, date_trunc('hour', event_ts) AS hour,
       COUNT(*) FILTER (WHERE event_type = 'click')   AS clicks,
       COUNT(*) FILTER (WHERE event_type = 'install') AS installs,
       ROUND(100.0 * COUNT(*) FILTER (WHERE event_type = 'install')
             / NULLIF(COUNT(*) FILTER (WHERE event_type = 'click'), 0), 1) AS cvr_pct
FROM events GROUP BY 1, 2 ORDER BY 1, 2;
