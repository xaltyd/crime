# pending_sql.py
"""
SQL definitions for the ‘pending’ portion of Records DB.
This file defines its own schema—no longer importing from docket_sql.
"""

# ————————————————————————————————————————————————————————————————
# Column definitions for pending cases (mirrors former docket_case_table)
# ————————————————————————————————————————————————————————————————
pending_case_table = {
    'docket_no':     'TEXT UNIQUE',
    'version':       'INTEGER',
    'last_first_name': 'TEXT',
    'represented_by':   'TEXT',
    'birth_year':       'TEXT',
    'times_on_docket':  'TEXT',
    'arresting_agency': 'TEXT',
    'arrest_date':      'DATE',
    'companion':        'TEXT',
    'docket_type':      'TEXT',
    'court':            'TEXT',
    'bond_amount':      'TEXT',
    'bond_type':        'TEXT',
    'custody':          'TEXT',
    'misc':             'TEXT',
    'purpose':          'TEXT',
    'hearing_date':     'DATE',
    'reason':           'TEXT'
}

# ————————————————————————————————————————————————————————————————
# Column definitions for pending charges (mirrors former docket_charge_table)
# ————————————————————————————————————————————————————————————————
pending_charge_table = {
    'statute':         'TEXT',
    'description':     'TEXT',
    'class':           'TEXT',
    'type':            'TEXT',
    'occ':             'TEXT',
    'offense_date':    'DATE',
    'plea':            'TEXT',
    'verdict_finding': 'TEXT'
}

# ————————————————————————————————————————————————————————————————
# Order lists / lookup maps (if you parse in a fixed order elsewhere)
# ————————————————————————————————————————————————————————————————
pending_case_order = [
    'cphBody_lblDocketNo',
    'cphBody_lblDefendant',
    'cphBody_lblDefendantAttorney',
    'cphBody_lblDefendantBirthDate',
    'cphBody_lblTimesInCourt',
    'cphBody_lblArrestingAgency',
    'cphBody_lblArrestDate',
    'cphBody_lblCompanionDocketNo',
    'cphBody_lblDocketType',
    'cphBody_lblCourt',
    'cphBody_lblBondAmount',
    'cphBody_lblBondTypeDesc',
    'cphBody_lblBondTypeDescHelp',
    'cphBody_lblSidebarFlag',
    'cphBody_lblPurposeDesc',
    'cphBody_lblHearingDate',
    'cphBody_lblReasonDesc'
]
PENDING_CASE_ORDER   = {k: i for i, k in enumerate(pending_case_order)}

pending_charge_order = [
    'Statute',
    'Description',
    'Class',
    'Type',
    'Occ',
    'Offense Date',
    'Plea',
    'Verdict Finding'
]
PENDING_CHARGE_ORDER = {k: i for i, k in enumerate(pending_charge_order)}

# ————————————————————————————————————————————————————————————————
# DDL: Create the pending and pending_charges tables
# ————————————————————————————————————————————————————————————————
CREATE_PENDING_TABLE = f'''
CREATE TABLE IF NOT EXISTS pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    {', '.join(f'{col} {typ}' for col, typ in pending_case_table.items())},
    UNIQUE (docket_no, version)
);
'''

CREATE_PENDING_CHARGES_TABLE = f'''
CREATE TABLE IF NOT EXISTS pending_charges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER REFERENCES pending(id) ON DELETE CASCADE,
    {', '.join(f'{col} {typ}' for col, typ in pending_charge_table.items())}
);
'''

PENDING_TABLES = [
    CREATE_PENDING_TABLE,
    CREATE_PENDING_CHARGES_TABLE,
]

# ————————————————————————————————————————————————————————————————
# DML: Insert statements for pending cases & charges
# ————————————————————————————————————————————————————————————————
INSERT_PENDING = f'''
INSERT OR IGNORE INTO pending (
    {', '.join(pending_case_table)}
) VALUES (
    {', '.join(['?'] * len(pending_case_table))}
);
'''

INSERT_PENDING_CHARGE = f'''
INSERT INTO pending_charges (
    case_id,
    {', '.join(pending_charge_table)}
) VALUES (
    {', '.join(['?'] * (len(pending_charge_table) + 1))}
);
'''
