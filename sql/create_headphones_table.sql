-- Create headphones table in Redshift
-- Based on the HeadphonesModel Pydantic schema

CREATE TABLE IF NOT EXISTS headphones (
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
    driver_type VARCHAR(50),
    fit_cup_type VARCHAR(50),
    based_on VARCHAR(100),
    note_weight VARCHAR(100),
    
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
CREATE INDEX IF NOT EXISTS idx_headphones_rank ON headphones(rank);
CREATE INDEX IF NOT EXISTS idx_headphones_price ON headphones(price_msrp);
CREATE INDEX IF NOT EXISTS idx_headphones_model ON headphones(model);
CREATE INDEX IF NOT EXISTS idx_headphones_tone_grade ON headphones(tone_grade);
CREATE INDEX IF NOT EXISTS idx_headphones_technical_grade ON headphones(technical_grade);

-- Add constraints
ALTER TABLE headphones ADD CONSTRAINT chk_rank_values 
CHECK (rank IN ('S', 'S-', 'A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E', 'F'));

ALTER TABLE headphones ADD CONSTRAINT chk_tone_grade_values 
CHECK (tone_grade IN ('S', 'S-', 'A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E', 'F'));

ALTER TABLE headphones ADD CONSTRAINT chk_technical_grade_values 
CHECK (technical_grade IN ('S', 'S-', 'A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E', 'F'));

ALTER TABLE headphones ADD CONSTRAINT chk_price_positive 
CHECK (price_msrp >= 0);