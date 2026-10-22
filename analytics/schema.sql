-- PostgreSQL schema for loading the CSV from `go run ./cmd/evaluate -export events.csv`
CREATE TABLE events (
    event_id      text PRIMARY KEY,
    event_type    text        NOT NULL,   -- click | install | session | reward
    device_id     text        NOT NULL,
    campaign_id   text        NOT NULL,
    publisher_id  text,
    ip            text,
    os            text,
    event_ts      timestamptz NOT NULL,
    reward_cents  bigint      NOT NULL DEFAULT 0,
    rooted        boolean     NOT NULL DEFAULT false,
    hooked        boolean     NOT NULL DEFAULT false,
    emulator      boolean     NOT NULL DEFAULT false,
    ground_truth  text,                    -- synthetic label, absent in production
    archetype     text
);
CREATE INDEX ON events (device_id, event_ts);
CREATE INDEX ON events (ip, event_ts);
-- \copy events FROM 'events.csv' CSV HEADER
