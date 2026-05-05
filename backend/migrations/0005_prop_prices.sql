-- Migration 0005: store actual prop prices on edges for correct profit/loss grading
-- Previously all props were graded at -110 flat juice, which is wrong.
-- Now we store the book's actual over_price and under_price per edge.

ALTER TABLE edges
    ADD COLUMN IF NOT EXISTS over_price  INTEGER,   -- e.g. -115, +105
    ADD COLUMN IF NOT EXISTS under_price INTEGER;   -- e.g. -105, -110

-- Also add to edge_results: store the juice used for grading (for audit trail)
ALTER TABLE edge_results
    ADD COLUMN IF NOT EXISTS juice_used  INTEGER;   -- actual price used to compute profit_units
