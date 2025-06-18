import sqlite3

pending_criminal_table = {'docket_no': 'TEXT UNIQUE',
                    'version': 'INTEGER',
                    'last_first': 'TEXT',
                    'represented_by': 'TEXT',
                    'birth_year': 'INTEGER',
                    'times_on_docket': 'INTEGER',
                    'arresting_agency': 'TEXT',
                    'companion': 'TEXT',
                    'docket_type': 'TEXT',
                    'arrest_date': 'TEXT',
                    'court': 'TEXT',
                    'bond_amount': 'TEXT',
                    'bond_type': 'TEXT',
                    'custody': 'TEXT',                   
                    'misc': 'TEXT',
                    'purpose': 'TEXT',
                    'hearing_date': 'TEXT',
                    'reason': 'TEXT'}

daily_charge_table = {'statute': 'TEXT',
                      'description': 'TEXT',
                      'class': 'TEXT',
                      'type': 'TEXT',
                      'occ': 'INTEGER',
                      'offense_date': 'TEXT',
                      'plea': 'TEXT',
                      'verdict_finding': 'TEXT'}

CREATE_DAILY_TABLE = f'''
    CREATE TABLE IF NOT EXISTS daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in daily_case_table.items()])},
        UNIQUE (docket_no, version));'''

CREATE_DAILY_CHARGES_TABLE = f'''
    CREATE TABLE IF NOT EXISTS daily_charges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES daily(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in daily_charge_table.items()])});'''

INSERT_DAILY = f'''                   
    INSERT OR IGNORE INTO daily ({', '.join(daily_case_table)})
    VALUES ({', '.join('?' * len(daily_case_table))})'''

INSERT_DAILY_CHARGE = f'''
    INSERT INTO daily_charges (case_id, {', '.join(daily_charge_table)})
    VALUES ({', '.join('?' * (len(daily_charge_table) + 1))})'''
