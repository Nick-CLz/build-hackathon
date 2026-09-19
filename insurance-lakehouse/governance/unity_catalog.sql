-- =====================================================================
-- Unity Catalog governance for the insurance lakehouse
--
-- GENERATED FILE -- do not edit by hand.
--   Regenerate: make governance
--   Source of truth: dbt column `meta` + config/sources.yml
--
-- Generated 2026-09-19 08:04 UTC
-- Catalog: insurance_dev
-- Classified columns: 36
--
-- This does not run against OSS Spark; there is no Unity Catalog outside
-- Databricks. It is generated as a reviewable artefact so the governance
-- posture can be diffed in code review rather than clicked in a UI.
-- =====================================================================

USE CATALOG insurance_dev;

-- ---------------------------------------------------------------------
-- 1. Groups
-- ---------------------------------------------------------------------
-- data_engineers: Build and operate the pipeline. Full access on dev, read on prod.
-- analysts: Read gold and marts only. Never silver, never unmasked PII.
-- restricted_health: The sole group permitted unmasked health declarations.
-- Groups are created at account level (SCIM or Terraform), not in SQL.
-- Listed here so the grants below are readable without cross-referencing.

-- ---------------------------------------------------------------------
-- 2. Column tags
--
-- Tags drive discovery ('where is our national ID data?') and can drive
-- policy. They are the machine-readable form of the classification.
-- ---------------------------------------------------------------------
ALTER TABLE bronze.events_health_declarations ALTER COLUMN bmi_band SET TAGS ('pii' = 'true', 'pii_type' = 'health', 'sensitivity' = 'restricted');
ALTER TABLE bronze.events_health_declarations ALTER COLUMN pre_existing_conditions SET TAGS ('pii' = 'true', 'pii_type' = 'health', 'sensitivity' = 'restricted');
ALTER TABLE bronze.partner_policies_a ALTER COLUMN cust_national_id SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE bronze.partner_policies_a ALTER COLUMN plate_no SET TAGS ('pii' = 'true', 'pii_type' = 'plate', 'sensitivity' = 'medium');
ALTER TABLE bronze.partner_policies_b ALTER COLUMN IDCard SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE bronze.partner_policies_b ALTER COLUMN VehiclePlate SET TAGS ('pii' = 'true', 'pii_type' = 'plate', 'sensitivity' = 'medium');
ALTER TABLE bronze.partner_policies_c ALTER COLUMN no_ktp SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE bronze.partner_policies_c ALTER COLUMN nomor_plat SET TAGS ('pii' = 'true', 'pii_type' = 'plate', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_customers ALTER COLUMN address SET TAGS ('pii' = 'true', 'pii_type' = 'address', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_customers ALTER COLUMN date_of_birth SET TAGS ('pii' = 'true', 'pii_type' = 'dob', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_customers ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_customers ALTER COLUMN first_name SET TAGS ('pii' = 'true', 'pii_type' = 'name', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_customers ALTER COLUMN last_name SET TAGS ('pii' = 'true', 'pii_type' = 'name', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_customers ALTER COLUMN national_id SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE bronze.ref_customers ALTER COLUMN phone SET TAGS ('pii' = 'true', 'pii_type' = 'phone', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_vehicles ALTER COLUMN chassis_no SET TAGS ('pii' = 'true', 'pii_type' = 'chassis', 'sensitivity' = 'medium');
ALTER TABLE bronze.ref_vehicles ALTER COLUMN plate_no SET TAGS ('pii' = 'true', 'pii_type' = 'plate', 'sensitivity' = 'medium');
ALTER TABLE gold.dim_customer ALTER COLUMN address_masked SET TAGS ('pii' = 'true', 'pii_type' = 'address', 'sensitivity' = 'medium');
ALTER TABLE gold.dim_customer ALTER COLUMN birth_year SET TAGS ('pii' = 'true', 'pii_type' = 'dob', 'sensitivity' = 'medium');
ALTER TABLE gold.dim_customer ALTER COLUMN email_masked SET TAGS ('pii' = 'true', 'pii_type' = 'email', 'sensitivity' = 'medium');
ALTER TABLE gold.dim_customer ALTER COLUMN national_id_hash SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE gold.dim_customer ALTER COLUMN phone_masked SET TAGS ('pii' = 'true', 'pii_type' = 'phone', 'sensitivity' = 'medium');
ALTER TABLE gold.dim_health_profile ALTER COLUMN bmi_band SET TAGS ('pii' = 'true', 'pii_type' = 'health', 'sensitivity' = 'restricted');
ALTER TABLE gold.dim_health_profile ALTER COLUMN pre_existing_conditions SET TAGS ('pii' = 'true', 'pii_type' = 'health', 'sensitivity' = 'restricted');
ALTER TABLE gold.dim_vehicle ALTER COLUMN chassis_masked SET TAGS ('pii' = 'true', 'pii_type' = 'chassis', 'sensitivity' = 'medium');
ALTER TABLE gold.dim_vehicle ALTER COLUMN plate_masked SET TAGS ('pii' = 'true', 'pii_type' = 'plate', 'sensitivity' = 'medium');
ALTER TABLE silver.stg_customers ALTER COLUMN address SET TAGS ('pii' = 'true', 'pii_type' = 'address', 'sensitivity' = 'medium');
ALTER TABLE silver.stg_customers ALTER COLUMN date_of_birth SET TAGS ('pii' = 'true', 'pii_type' = 'dob', 'sensitivity' = 'medium');
ALTER TABLE silver.stg_customers ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email', 'sensitivity' = 'medium');
ALTER TABLE silver.stg_customers ALTER COLUMN national_id SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE silver.stg_customers ALTER COLUMN phone SET TAGS ('pii' = 'true', 'pii_type' = 'phone', 'sensitivity' = 'medium');
ALTER TABLE silver.stg_health_declarations ALTER COLUMN bmi_band SET TAGS ('pii' = 'true', 'pii_type' = 'health', 'sensitivity' = 'restricted');
ALTER TABLE silver.stg_health_declarations ALTER COLUMN pre_existing_conditions SET TAGS ('pii' = 'true', 'pii_type' = 'health', 'sensitivity' = 'restricted');
ALTER TABLE silver.stg_partner_policies ALTER COLUMN national_id SET TAGS ('pii' = 'true', 'pii_type' = 'national_id', 'sensitivity' = 'high');
ALTER TABLE silver.stg_vehicles ALTER COLUMN chassis_no SET TAGS ('pii' = 'true', 'pii_type' = 'chassis', 'sensitivity' = 'medium');
ALTER TABLE silver.stg_vehicles ALTER COLUMN plate_no SET TAGS ('pii' = 'true', 'pii_type' = 'plate', 'sensitivity' = 'medium');

-- ---------------------------------------------------------------------
-- 3. Column mask functions
--
-- The mask is applied at QUERY TIME by the engine, so it holds however
-- the table is read -- SQL, notebook, BI tool or JDBC. Masking only in a
-- dbt model protects the model; masking here protects the column.
--
-- The salt comes from a secret scope, never a literal. An unsalted hash
-- of a 13-digit national ID is trivially reversible by enumeration.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mask_address(col STRING)
  RETURNS STRING
  COMMENT 'Mask for address. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE '[redacted]'
  END;

CREATE OR REPLACE FUNCTION mask_chassis(col STRING)
  RETURNS STRING
  COMMENT 'Mask for chassis. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE concat('***', substr(col, -4, 4))
  END;

CREATE OR REPLACE FUNCTION mask_dob(col STRING)
  RETURNS STRING
  COMMENT 'Mask for dob. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE concat(cast(year(col) as string), '-01-01')
  END;

CREATE OR REPLACE FUNCTION mask_email(col STRING)
  RETURNS STRING
  COMMENT 'Mask for email. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE concat(substr(col, 1, 1), '***@', split_part(col, '@', 2))
  END;

CREATE OR REPLACE FUNCTION mask_health(col STRING)
  RETURNS STRING
  COMMENT 'Mask for health. Unmasked for restricted_health.'
  RETURN CASE
    WHEN is_account_group_member('restricted_health') THEN col
    WHEN col IS NULL THEN NULL
    ELSE null
  END;

CREATE OR REPLACE FUNCTION mask_name(col STRING)
  RETURNS STRING
  COMMENT 'Mask for name. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE concat(substr(col, 1, 1), '.')
  END;

CREATE OR REPLACE FUNCTION mask_national_id(col STRING)
  RETURNS STRING
  COMMENT 'Mask for national_id. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE sha2(concat(secret('insurance', 'pii_salt'), col), 256)
  END;

CREATE OR REPLACE FUNCTION mask_phone(col STRING)
  RETURNS STRING
  COMMENT 'Mask for phone. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE concat(substr(col, 1, 3), '****', substr(col, -2, 2))
  END;

CREATE OR REPLACE FUNCTION mask_plate(col STRING)
  RETURNS STRING
  COMMENT 'Mask for plate. Unmasked for data_engineers.'
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN col
    WHEN col IS NULL THEN NULL
    ELSE concat('***', substr(col, -2, 2))
  END;

-- Bind masks to columns. Columns already masked in the dbt model are
-- bound anyway: defence in depth, and the model could be changed.
ALTER TABLE bronze.events_health_declarations ALTER COLUMN bmi_band SET MASK mask_health;
ALTER TABLE bronze.events_health_declarations ALTER COLUMN pre_existing_conditions SET MASK mask_health;
ALTER TABLE bronze.partner_policies_a ALTER COLUMN cust_national_id SET MASK mask_national_id;
ALTER TABLE bronze.partner_policies_a ALTER COLUMN plate_no SET MASK mask_plate;
ALTER TABLE bronze.partner_policies_b ALTER COLUMN IDCard SET MASK mask_national_id;
ALTER TABLE bronze.partner_policies_b ALTER COLUMN VehiclePlate SET MASK mask_plate;
ALTER TABLE bronze.partner_policies_c ALTER COLUMN no_ktp SET MASK mask_national_id;
ALTER TABLE bronze.partner_policies_c ALTER COLUMN nomor_plat SET MASK mask_plate;
ALTER TABLE bronze.ref_customers ALTER COLUMN address SET MASK mask_address;
ALTER TABLE bronze.ref_customers ALTER COLUMN date_of_birth SET MASK mask_dob;
ALTER TABLE bronze.ref_customers ALTER COLUMN email SET MASK mask_email;
ALTER TABLE bronze.ref_customers ALTER COLUMN first_name SET MASK mask_name;
ALTER TABLE bronze.ref_customers ALTER COLUMN last_name SET MASK mask_name;
ALTER TABLE bronze.ref_customers ALTER COLUMN national_id SET MASK mask_national_id;
ALTER TABLE bronze.ref_customers ALTER COLUMN phone SET MASK mask_phone;
ALTER TABLE bronze.ref_vehicles ALTER COLUMN chassis_no SET MASK mask_chassis;
ALTER TABLE bronze.ref_vehicles ALTER COLUMN plate_no SET MASK mask_plate;
ALTER TABLE gold.dim_customer ALTER COLUMN address_masked SET MASK mask_address;
ALTER TABLE gold.dim_customer ALTER COLUMN birth_year SET MASK mask_dob;
ALTER TABLE gold.dim_customer ALTER COLUMN email_masked SET MASK mask_email;
ALTER TABLE gold.dim_customer ALTER COLUMN national_id_hash SET MASK mask_national_id;
ALTER TABLE gold.dim_customer ALTER COLUMN phone_masked SET MASK mask_phone;
ALTER TABLE gold.dim_health_profile ALTER COLUMN bmi_band SET MASK mask_health;
ALTER TABLE gold.dim_health_profile ALTER COLUMN pre_existing_conditions SET MASK mask_health;
ALTER TABLE gold.dim_vehicle ALTER COLUMN chassis_masked SET MASK mask_chassis;
ALTER TABLE gold.dim_vehicle ALTER COLUMN plate_masked SET MASK mask_plate;
ALTER TABLE silver.stg_customers ALTER COLUMN address SET MASK mask_address;
ALTER TABLE silver.stg_customers ALTER COLUMN date_of_birth SET MASK mask_dob;
ALTER TABLE silver.stg_customers ALTER COLUMN email SET MASK mask_email;
ALTER TABLE silver.stg_customers ALTER COLUMN national_id SET MASK mask_national_id;
ALTER TABLE silver.stg_customers ALTER COLUMN phone SET MASK mask_phone;
ALTER TABLE silver.stg_health_declarations ALTER COLUMN bmi_band SET MASK mask_health;
ALTER TABLE silver.stg_health_declarations ALTER COLUMN pre_existing_conditions SET MASK mask_health;
ALTER TABLE silver.stg_partner_policies ALTER COLUMN national_id SET MASK mask_national_id;
ALTER TABLE silver.stg_vehicles ALTER COLUMN chassis_no SET MASK mask_chassis;
ALTER TABLE silver.stg_vehicles ALTER COLUMN plate_no SET MASK mask_plate;

-- ---------------------------------------------------------------------
-- 4. Row filter: jurisdiction
--
-- Thai and Indonesian personal data in one warehouse is a CROSS-BORDER
-- TRANSFER question, not merely a storage one. PDPA restricts transfer to
-- jurisdictions without adequate protection; Indonesia's PDP Law is
-- comparable. The default is therefore jurisdiction-restricted access,
-- with cross-border read as an explicit grant carrying a legal basis.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION filter_by_country(country STRING)
  RETURNS BOOLEAN
  COMMENT 'Restrict rows to the reader\'s jurisdiction unless globally cleared.'
  RETURN
    is_account_group_member('data_engineers')
    OR is_account_group_member('analysts_global')
    OR (is_account_group_member('analysts_th') AND country = 'TH')
    OR (is_account_group_member('analysts_id') AND country = 'ID');

ALTER TABLE silver.silver_policies SET ROW FILTER filter_by_country ON (country);
ALTER TABLE gold.fct_claims SET ROW FILTER filter_by_country ON (country);
ALTER TABLE gold.fct_written_premium SET ROW FILTER filter_by_country ON (country);
ALTER TABLE gold.dim_customer SET ROW FILTER filter_by_country ON (country);

-- ---------------------------------------------------------------------
-- 5. Grants
--
-- Least privilege, and note what is NOT granted: analysts have no access
-- to silver at all. Silver holds unmasked PII, and the boundary between
-- silver and gold is the primary control. Masks are the second line.
-- ---------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG insurance_dev TO `data_engineers`;
GRANT USE CATALOG ON CATALOG insurance_dev TO `analysts`;
GRANT USE CATALOG ON CATALOG insurance_dev TO `restricted_health`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA bronze TO `data_engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA silver TO `data_engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA gold TO `data_engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA marts TO `data_engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA observability TO `data_engineers`;

GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA marts TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA observability TO `analysts`;

-- restricted_health reads gold like any analyst; its privilege is that
-- mask_health returns the real value for it.
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `restricted_health`;

-- Explicitly NOT granted, and the reason:
--   analysts -> silver           : unmasked PII lives there
--   analysts -> bronze           : raw partner files, unvalidated
--   restricted_health -> silver  : health access does not imply raw access

-- ---------------------------------------------------------------------
-- 6. Verification
--
-- Run these after applying. A mask that was never verified against a
-- real principal is an assumption, not a control.
-- ---------------------------------------------------------------------
-- SELECT * FROM information_schema.column_tags WHERE tag_name = 'pii';
-- DESCRIBE EXTENDED gold.dim_customer national_id_hash;  -- shows the mask
-- SELECT count(*) FROM gold.fct_claims;  -- as each analyst group
