
docket_case_table = {'docket_no': 'TEXT UNIQUE',
                     'version': 'INTEGER',
                     'last_first_name': 'TEXT',
                     'represented_by': 'TEXT',
                     'birth_year': 'TEXT',
                     'times_on_docket': 'TEXT',
                     'arresting_agency': 'TEXT',
                     'arrest_date': 'DATE',  # Changed from TEXT
                     'companion': 'TEXT',
                     'docket_type': 'TEXT',
                     'court': 'TEXT',
                     'bond_amount': 'TEXT',
                     'bond_type': 'TEXT',
                     'custody': 'TEXT',                   
                     'misc': 'TEXT',
                     'purpose': 'TEXT',
                     'hearing_date': 'DATE',  # Changed from TEXT
                     'reason': 'TEXT'}

docket_charge_table = {'statute': 'TEXT',
                       'description': 'TEXT',
                       'class': 'TEXT',
                       'type': 'TEXT',
                       'occ': 'TEXT',
                       'offense_date': 'DATE',  # Changed from TEXT
                       'plea': 'TEXT',
                       'verdict_finding': 'TEXT'}

docket_case_order = ['cphBody_lblDocketNo', 'cphBody_lblDefendant', 'cphBody_lblDefendantAttorney', 'cphBody_lblDefendantBirthDate',
                     'cphBody_lblTimesInCourt', 'cphBody_lblArrestingAgency', 'cphBody_lblArrestDate', 'cphBody_lblCompanionDocketNo',
                     'cphBody_lblDocketType', 'cphBody_lblCourt', 'cphBody_lblBondAmount', 'cphBody_lblBondTypeDesc',
                     'cphBody_lblBondTypeDescHelp', 'cphBody_lblSidebarFlag', 'cphBody_lblPurposeDesc', 'cphBody_lblHearingDate', 'cphBody_lblReasonDesc']
DOCKET_CASE_ORDER = {k:i for i, k in enumerate(docket_case_order)}

docket_charge_order = ['Statute', 'Description', 'Class', 'Type', 'Occ',
                       'Offense Date', 'Plea', 'Verdict Finding']
DOCKET_CHARGE_ORDER = {k:i for i, k in enumerate(docket_charge_order)}

CREATE_DOCKET_TABLE = f'''
    CREATE TABLE IF NOT EXISTS docket (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in docket_case_table.items()])},
        UNIQUE (docket_no, version));'''

CREATE_DOCKET_CHARGES_TABLE = f'''
    CREATE TABLE IF NOT EXISTS docket_charges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES docket(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in docket_charge_table.items()])});'''

DOCKET_TABLES = [CREATE_DOCKET_TABLE, CREATE_DOCKET_CHARGES_TABLE]

INSERT_DOCKET = f'''                   
    INSERT OR IGNORE INTO docket ({', '.join(docket_case_table)})
    VALUES ({', '.join('?' * len(docket_case_table))})'''

INSERT_DOCKET_CHARGE = f'''
    INSERT INTO docket_charges (case_id, {', '.join(docket_charge_table)})
    VALUES ({', '.join('?' * (len(docket_charge_table) + 1))})'''
