-- Migration: 032_people.sql
-- Description: Create people table for personal memory / CRM system
-- Date: 2026-04-04

CREATE TABLE people (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Name
    first_name         TEXT NOT NULL,
    middle_name        TEXT,
    last_name          TEXT,
    preferred_name     TEXT,
    nickname           TEXT,
    -- Relationship context
    relationship_type  TEXT,
    household_name     TEXT,                -- display/grouping label only, not a join key
    how_you_know_them  TEXT,
    active             BOOLEAN NOT NULL DEFAULT TRUE,
    -- Partner link (self-referencing FK)
    partner_person_id  UUID REFERENCES people(id),
    -- Contact info
    email_1            TEXT,
    email_2            TEXT,
    phone_1            TEXT,
    phone_2            TEXT,
    -- Address
    address_line_1     TEXT,
    address_line_2     TEXT,
    city               TEXT,
    state_province     TEXT,
    postal_code        TEXT,
    country            TEXT,
    -- Personal details
    birthdate              DATE,
    dating_anniversary     DATE,
    marriage_anniversary   DATE,
    dietary_preferences    TEXT[],
    dietary_restrictions   TEXT[],
    favorite_foods         TEXT[],
    favorite_drinks        TEXT[],
    -- Household / family
    spouse_partner_name    TEXT,
    children_names         TEXT[],
    pets                   TEXT[],
    -- Personal memory tags
    interests              TEXT[],
    important_dates        TEXT[],
    gift_ideas             TEXT[],
    avoid_gifts            TEXT[],
    -- Work context
    company                TEXT,
    title                  TEXT,
    -- Follow-up tracking
    last_seen_date         DATE,
    last_contact_date      DATE,
    next_check_in_date     DATE,
    -- Metadata
    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_people_last_name             ON people(last_name);
CREATE INDEX idx_people_relationship_type     ON people(relationship_type);
CREATE INDEX idx_people_birthdate             ON people(birthdate);
CREATE INDEX idx_people_dating_anniversary    ON people(dating_anniversary);
CREATE INDEX idx_people_marriage_anniversary  ON people(marriage_anniversary);
CREATE INDEX idx_people_next_check_in_date    ON people(next_check_in_date);
CREATE INDEX idx_people_partner_person_id     ON people(partner_person_id);
CREATE INDEX idx_people_city_state            ON people(city, state_province);

-- updated_at trigger
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_people_updated_at
BEFORE UPDATE ON people
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
