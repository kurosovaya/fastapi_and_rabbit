CREATE TABLE IF NOT EXISTS clients (
    _id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    client_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS events (
    _id             text COLLATE "C" PRIMARY KEY,
    idempotency_key text        NOT NULL UNIQUE,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    published       boolean     NOT NULL DEFAULT false,
    accepted_at     timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS events_unpublished ON events (published, _id);

CREATE TABLE IF NOT EXISTS subscriptions (
    _id         text COLLATE "C" PRIMARY KEY,
    client_id   uuid        NOT NULL REFERENCES clients(_id),
    url         text        NOT NULL,
    event_types text[]      NOT NULL,
    secret      text        NOT NULL,
    active      boolean     NOT NULL,
    created_at  timestamptz NOT NULL,
    UNIQUE (client_id, url)
);
CREATE INDEX IF NOT EXISTS subscriptions_event_types
    ON subscriptions USING gin (event_types) WHERE active;

CREATE TABLE IF NOT EXISTS deliveries (
    _id             text COLLATE "C" PRIMARY KEY,
    event_id        text NOT NULL REFERENCES events(_id),
    subscription_id text NOT NULL REFERENCES subscriptions(_id),
    status          text        NOT NULL DEFAULT 'pending',
    attempt         int         NOT NULL DEFAULT 0,
    attempt_epoch   int         NOT NULL DEFAULT 0,
    locked_by       text,
    locked_until    timestamptz,
    next_attempt_at timestamptz,
    last_error      text,
    accepted_at     timestamptz NOT NULL,
    delivered_at    timestamptz,
    UNIQUE (event_id, subscription_id)
);

CREATE TABLE IF NOT EXISTS sink_settings (
    client_id   text COLLATE "C" PRIMARY KEY,
    accept_rate int NOT NULL,
    delay_ms    int NOT NULL
);
