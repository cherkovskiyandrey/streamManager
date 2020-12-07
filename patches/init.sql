create table viewers_stats
(
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    account_id BIGINT       NOT NULL,
    region     VARCHAR(256) NOT NULL,
    percentile INT          NOT NULL,
    value      INT          NOT NULL,
    created    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    INDEX acc_ts_index (account_id, percentile, created),
    INDEX ts_index (created)
);