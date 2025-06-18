# docket_sql.py

"""
SQL definitions for the docket portion of Records DB.
"""

# ————————————————————————————————————————————————————————————————
# Create the main `docket` table
# ————————————————————————————————————————————————————————————————
CREATE_DOCKET_TABLE = """
CREATE TABLE IF NOT EXISTS docket (
    case_id        TEXT PRIMARY KEY,
    court_id       TEXT,
    docket_number  TEXT,
    filing_date    TEXT,
    status         TEXT,
    modified_date  TEXT
);
"""

# ————————————————————————————————————————————————————————————————
# (Removed) The `docket_charges` table has been deleted entirely.
# No CREATE or DROP statements for docket_charges remain.
# ————————————————————————————————————————————————————————————————

# ————————————————————————————————————————————————————————————————
# Index to speed up court lookups
# ————————————————————————————————————————————————————————————————
CREATE_DOCKET_INDEX = """
CREATE INDEX IF NOT EXISTS idx_docket_court
    ON docket(court_id);
"""

# ————————————————————————————————————————————————————————————————
# Insert / Replace into docket
# ————————————————————————————————————————————————————————————————
INSERT_DOCKET = """
INSERT OR REPLACE INTO docket (
    case_id,
    court_id,
    docket_number,
    filing_date,
    status,
    modified_date
) VALUES (?, ?, ?, ?, ?, ?);
"""

# ————————————————————————————————————————————————————————————————
# Selectors
# ————————————————————————————————————————————————————————————————
SELECT_DOCKET_BY_CASE_ID = """
SELECT
    case_id,
    court_id,
    docket_number,
    filing_date,
    status,
    modified_date
FROM docket
WHERE case_id = ?;
"""


DOCKET_TABLES = [
    CREATE_DOCKET_TABLE,
    CREATE_DOCKET_INDEX,
]
