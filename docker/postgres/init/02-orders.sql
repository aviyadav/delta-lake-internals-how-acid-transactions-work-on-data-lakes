-- Operational "orders database" for Shop-A-Zone.
-- Deliberately contains the business-data defects the article calls out:
--   * a duplicated order row (double-published event)
--   * a negative amount (bad refund record)
--   * lower/space-padded and unknown statuses
-- Delta can commit those safely; only a data-quality gate can catch them.

CREATE TABLE orders (
    order_id      BIGINT        NOT NULL,
    customer_id   BIGINT,
    order_amount  NUMERIC(12,2),
    order_status  VARCHAR(32),
    created_at    TIMESTAMP     NOT NULL,
    updated_at    TIMESTAMP     NOT NULL
);

INSERT INTO orders (order_id, customer_id, order_amount, order_status, created_at, updated_at) VALUES
 (1001, 501,  500.00, 'COMPLETED', now() - interval '4 days', now() - interval '3 days'),
 (1002, 502,  800.00, 'COMPLETED', now() - interval '4 days', now() - interval '3 days'),
 (1003, 503,  300.00, 'CANCELLED', now() - interval '4 days', now() - interval '2 days'),
 (1004, 504,  900.00, 'CREATED',   now() - interval '4 days', now() - interval '4 days'),
 (1005, 505, 1200.00, 'COMPLETED', now() - interval '3 days', now() - interval '2 days'),
 (1006, 506,  700.00, 'COMPLETED', now() - interval '3 days', now() - interval '2 days'),
 (1007, 507,  650.00, 'PAID',      now() - interval '3 days', now() - interval '1 day'),
 (1008, 508,  150.00, 'REFUNDED',  now() - interval '3 days', now() - interval '1 day'),
 (1009, 501,  220.00, 'COMPLETED', now() - interval '2 days', now() - interval '2 days'),
 (1010, 502,  340.00, 'PAID',      now() - interval '2 days', now() - interval '1 day'),
 (1011, 503,  410.00, 'COMPLETED', now() - interval '2 days', now() - interval '1 day'),
 (1012, 504,  275.00, 'CREATED',   now() - interval '2 days', now() - interval '2 days'),
 (1013, 505,  980.00, 'COMPLETED', now() - interval '2 days', now() - interval '1 day'),
 (1014, 506,  120.00, 'CANCELLED', now() - interval '1 day', now() - interval '1 day'),
 (1015, 507,  560.00, 'COMPLETED', now() - interval '1 day', now() - interval '1 day'),
 (1016, 508,  730.00, 'PAID',      now() - interval '1 day', now() - interval '1 day'),
 (1017, 501,  190.00, 'COMPLETED', now() - interval '1 day', now() - interval '1 day'),
 (1018, 502,  260.00, 'CREATED',   now() - interval '1 day', now() - interval '1 day'),
 (1019, 503,  640.00, 'COMPLETED', now() - interval '20 hours', now() - interval '20 hours'),
 (1020, 504,  890.00, 'PAID',      now() - interval '18 hours', now() - interval '18 hours'),

 -- defect: a source duplicate that arrives later with a corrected amount
 (1012, 504,  275.00, 'CREATED',   now() - interval '2 days', now() - interval '10 hours'),

 -- defect: negative amount, must be quarantined before Gold
 (1021, 505,  -75.00, 'REFUNDED',  now() - interval '16 hours', now() - interval '16 hours'),

 -- defect: status not in the domain
 (1022, 506,  310.00, 'UNKNOWN',   now() - interval '14 hours', now() - interval '14 hours'),

 -- defect: lower-case + trailing space (normalised in Silver)
 (1023, 507,  450.00, 'completed ', now() - interval '12 hours', now() - interval '12 hours'),

 -- defect: missing customer reference
 (1024, NULL, 520.00, 'PAID',      now() - interval '9 hours', now() - interval '9 hours'),

 (1025, 508,  1150.00, 'COMPLETED', now() - interval '8 hours', now() - interval '8 hours'),
 (1026, 501,   180.00, 'CREATED',   now() - interval '6 hours', now() - interval '6 hours'),
 (1027, 502,   990.00, 'PAID',      now() - interval '5 hours', now() - interval '5 hours'),
 (1028, 503,   410.00, 'COMPLETED', now() - interval '4 hours', now() - interval '4 hours'),
 (1029, 504,   260.00, 'CANCELLED', now() - interval '3 hours', now() - interval '3 hours'),
 (1030, 505,   870.00, 'COMPLETED', now() - interval '2 hours', now() - interval '2 hours'),
 (1031, 506,   330.00, 'PAID',      now() - interval '90 minutes', now() - interval '90 minutes'),
 (1032, 507,   145.00, 'COMPLETED', now() - interval '45 minutes', now() - interval '45 minutes');

CREATE INDEX orders_updated_at_idx ON orders (updated_at);
