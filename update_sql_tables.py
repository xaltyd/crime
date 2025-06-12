# update_all_sql_files.py
# Script to update all SQL definition files to use DATE types

def create_docket_sql():
    """Create docket_sql.py with DATE types"""
    content = '''
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

CREATE_DOCKET_TABLE = f\'\'\'
    CREATE TABLE IF NOT EXISTS docket (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in docket_case_table.items()])},
        UNIQUE (docket_no, version));\'\'\'

CREATE_DOCKET_CHARGES_TABLE = f\'\'\'
    CREATE TABLE IF NOT EXISTS docket_charges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES docket(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in docket_charge_table.items()])});\'\'\'

DOCKET_TABLES = [CREATE_DOCKET_TABLE, CREATE_DOCKET_CHARGES_TABLE]

INSERT_DOCKET = f\'\'\'                   
    INSERT OR IGNORE INTO docket ({', '.join(docket_case_table)})
    VALUES ({', '.join('?' * len(docket_case_table))})\'\'\'

INSERT_DOCKET_CHARGE = f\'\'\'
    INSERT INTO docket_charges (case_id, {', '.join(docket_charge_table)})
    VALUES ({', '.join('?' * (len(docket_charge_table) + 1))})\'\'\'
'''
    with open('docket_sql.py', 'w') as f:
        f.write(content)
    print("✓ Updated docket_sql.py")

def create_pending_sql():
    """Create pending_sql.py with DATE types"""
    content = '''from docket_sql import *

# Pending uses same structure as docket but with its own table names
pending_case_table = docket_case_table.copy()
pending_charge_table = docket_charge_table.copy()

pending_case_order = docket_case_order
PENDING_CASE_ORDER = DOCKET_CASE_ORDER

pending_charge_order = docket_charge_order
PENDING_CHARGE_ORDER = DOCKET_CHARGE_ORDER

CREATE_PENDING_TABLE = f\'\'\'
    CREATE TABLE IF NOT EXISTS pending (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in pending_case_table.items()])},
        UNIQUE (docket_no, version));\'\'\'

CREATE_PENDING_CHARGES_TABLE = f\'\'\'
    CREATE TABLE IF NOT EXISTS pending_charges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES pending(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in pending_charge_table.items()])});\'\'\'

PENDING_TABLES = [CREATE_PENDING_TABLE, CREATE_PENDING_CHARGES_TABLE]

INSERT_PENDING = f\'\'\'                   
    INSERT OR IGNORE INTO pending ({', '.join(pending_case_table)})
    VALUES ({', '.join('?' * len(pending_case_table))})\'\'\'

INSERT_PENDING_CHARGE = f\'\'\'
    INSERT INTO pending_charges (case_id, {', '.join(pending_charge_table)})
    VALUES ({', '.join('?' * (len(pending_charge_table) + 1))})\'\'\'
'''
    with open('pending_sql.py', 'w') as f:
        f.write(content)
    print("✓ Updated pending_sql.py")

def update_storage_files():
    """Update storage files to use date parsing"""
    # Create a storage module update script
    storage_update = '''# Add this to the top of storage.py after other imports:
from date_utils import parse_date

# Then in the set_table_order method, add date parsing:
def set_table_order(self, table, table_order):
    ls = [''] * len(table_order)
    for k, v in table.items():
        if k in table_order:
            try:
                # Parse dates for specific fields
                if k in ['cphBody_lblArrestDate', 'cphBody_lblHearingDate', 'cphBody_lblSentDate']:
                    v = parse_date(v) if v else v
                elif k == 'Offense Date' or k == 'Verdict Date':
                    v = parse_date(v) if v else v
                    
                ls[table_order[k]] = v
            except Exception as e:
                print(e)
                print(table, '\\n\\n', table_order)
                input('paused')
        else:
            log_issue(f'{k} is an unrecognized field. Will not proceed until investigated')
            cinput('Cannot proceed based on type of issue', Fore.RED)
            sys.exit()
    return ls
'''
    
    with open('storage_date_update.txt', 'w') as f:
        f.write(storage_update)
    print("✓ Created storage_date_update.txt - Add this to storage.py")

def create_sql_modules():
    """Create sql_modules.py"""
    content = '''# sql_modules.py
from docket_sql import *
from pending_sql import *
from conviction_sql import *
# Note: sentence_sql.py removed to prevent duplicate tables
'''
    with open('sql_modules.py', 'w') as f:
        f.write(content)
    print("✓ Updated sql_modules.py")

def main():
    print("=== Updating SQL files to use DATE types ===\n")
    
    # Create updated SQL files
    create_docket_sql()
    create_pending_sql()
    # Note: conviction_sql.py already has DATE types from our earlier fix
    
    # Update imports
    create_sql_modules()
    
    # Create storage update instructions
    update_storage_files()
    
    print("\n=== Update complete! ===")
    print("\nNext steps:")
    print("1. Make sure conviction_sql.py has the DATE types (from our earlier fix)")
    print("2. Update storage.py with the date parsing code from storage_date_update.txt")
    print("3. Delete records.db: rm records.db")
    print("4. Run docket.py to create fresh tables with proper DATE types")
    print("\nAll dates will be stored in ISO format (YYYY-MM-DD) and sort correctly!")

if __name__ == "__main__":
    main()
