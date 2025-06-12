from docket_sql import *

# Pending uses same structure as docket but with its own table names
pending_case_table = docket_case_table.copy()
pending_charge_table = docket_charge_table.copy()

pending_case_order = docket_case_order
PENDING_CASE_ORDER = DOCKET_CASE_ORDER

pending_charge_order = docket_charge_order
PENDING_CHARGE_ORDER = DOCKET_CHARGE_ORDER

CREATE_PENDING_TABLE = f'''
    CREATE TABLE IF NOT EXISTS pending (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in pending_case_table.items()])},
        UNIQUE (docket_no, version));'''

CREATE_PENDING_CHARGES_TABLE = f'''
    CREATE TABLE IF NOT EXISTS pending_charges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES pending(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in pending_charge_table.items()])});'''

PENDING_TABLES = [CREATE_PENDING_TABLE, CREATE_PENDING_CHARGES_TABLE]

INSERT_PENDING = f'''                   
    INSERT OR IGNORE INTO pending ({', '.join(pending_case_table)})
    VALUES ({', '.join('?' * len(pending_case_table))})'''

INSERT_PENDING_CHARGE = f'''
    INSERT INTO pending_charges (case_id, {', '.join(pending_charge_table)})
    VALUES ({', '.join('?' * (len(pending_charge_table) + 1))})'''
