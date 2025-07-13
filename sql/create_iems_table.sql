-- Create iems (In-Ear Monitors) table in Redshift
-- Based on the IEMModel Pydantic schema

CREATE TABLE IF NOT EXISTS iems (
    -- Primary ranking information
    rank VARCHAR(5),
    value_rating VARCHAR(10),
    model VARCHAR(200) NOT NULL,
    price_msrp INTEGER,
    
    -- Product characteristics
    signature VARCHAR(100),
    comments VARCHAR(500),
    tone_grade VARCHAR(5),
    technical_grade VARCHAR(5),
    setup VARCHAR(100),
    status VARCHAR(50),
    
    -- Sorting fields for performance
    ranksort DECIMAL(10,4),
    tonesort DECIMAL(10,4),
    techsort DECIMAL(10,4),
    pricesort DECIMAL(10,4),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (rank, price_msrp);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_iems_rank ON iems(rank);
CREATE INDEX IF NOT EXISTS idx_iems_price ON iems(price_msrp);
CREATE INDEX IF NOT EXISTS idx_iems_model ON iems(model);
CREATE INDEX IF NOT EXISTS idx_iems_tone_grade ON iems(tone_grade);
CREATE INDEX IF NOT EXISTS idx_iems_technical_grade ON iems(technical_grade);
CREATE INDEX IF NOT EXISTS idx_iems_setup ON iems(setup);
CREATE INDEX IF NOT EXISTS idx_iems_status ON iems(status);

-- Add constraints
ALTER TABLE iems ADD CONSTRAINT chk_iems_rank_values 
CHECK (rank IN ('S', 'S-', 'A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E', 'F'));

ALTER TABLE iems ADD CONSTRAINT chk_iems_tone_grade_values 
CHECK (tone_grade IN ('S', 'S-', 'A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E', 'F'));

ALTER TABLE iems ADD CONSTRAINT chk_iems_technical_grade_values 
CHECK (technical_grade IN ('S', 'S-', 'A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E', 'F'));

ALTER TABLE iems ADD CONSTRAINT chk_iems_price_positive 
CHECK (price_msrp >= 0);