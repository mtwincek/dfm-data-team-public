----------------------------------------------------------------------------
-- I am not sure what this database is supposed to represent.
-- My best guess is that this table is for a website trying to sell cars.
-- This table seems to be related to marketing/sales metrics.
----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EXAMPLE_REPORT (
    day                         DATE        PRIMARY KEY,
    customer_id                 INTEGER     NOT NULL,
    campaign_id                 INTEGER     NOT NULL,
    campaign                    TEXT        NOT NULL,
    campaign_state              TEXT        NOT NULL,
    campaign_serving_status     TEXT        NOT NULL,
    clicks                      SMALLINT    NOT NULL,
    start_date                  DATE        NOT NULL,
    end_date                    DATE        NOT NULL,
    budget                      INTEGER     NOT NULL,
    budget_id                   INTEGER     NOT NULL,
    budget_explicitly_shared    BOOLEAN     NOT NULL,
    labels_ids                  TEXT,
    labels                      TEXT,
    invalid_clicks              SMALLINT    NOT NULL,
    conversions                 SMALLINT    NOT NULL,
    conversion_rate             REAL        NOT NULL,
    ctr                         REAL        NOT NULL,
    cost                        INTEGER     NOT NULL,
    impressions                 SMALLINT    NOT NULL,
    search_lost_is_rank         REAL        NOT NULL,
    average_position            REAL        NOT NULL,
    interaction_rate            REAL        NOT NULL,
    interactions                SMALLINT    NOT NULL
);
