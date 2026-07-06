CREATE TABLE IF NOT EXISTS events (
  id          BIGSERIAL   PRIMARY KEY,
  channel_id  TEXT        NOT NULL,
  version     BIGINT      NOT NULL,
  type        TEXT        NOT NULL,
  payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_events_stream UNIQUE (channel_id, version)
);
--;;
CREATE INDEX IF NOT EXISTS idx_events_type
  ON events (channel_id, type, version);
