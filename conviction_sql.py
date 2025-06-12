# conviction_sql.py
# Fixed version with proper DATE types and no duplicate tables

from docket_sql import *

# Main conviction case table - with proper DATE types
conviction_case_table = {'docket_no': 'TEXT UNIQUE',
                         'version': 'INTEGER',
                         'last_first_name': 'TEXT',
                         'represented_by': 'TEXT',
                         'birth_year': 'TEXT',
                         'arresting_agency': 'TEXT',
                         'arrest_date': 'DATE',  # Changed from TEXT to DATE
                         'sentenced_date': 'DATE',  # Changed from TEXT to DATE
                         'court': 'TEXT',
                         'cost': 'TEXT',
                         'paid': 'TEXT'}

# Conviction charge table - with proper DATE types
conviction_charge_table = {'statute': 'TEXT',
                          'description': 'TEXT',
                          'class': 'TEXT',
                          'type': 'TEXT',
                          'occ': 'TEXT',
                          'offense_date': 'DATE',  # Changed from TEXT to DATE
                          'plea': 'TEXT',
                          'verdict_finding': 'TEXT',
                          'verdict_date': 'DATE',  # Changed from TEXT to DATE
                          'fine': 'TEXT',
                          'fees': 'TEXT',
                          'charge_sequence': 'INTEGER'}  # To maintain order

# Sentence information table - with proper DATE type
conviction_sentence_table = {'sentence_type': 'TEXT',  # 'OVERALL', 'MODIFIED', or 'CHARGE_SPECIFIC'
                            'sentence_text': 'TEXT',
                            'sentence_date': 'DATE',  # Changed from TEXT to DATE
                            'is_active': 'BOOLEAN'}  # Track which sentence is current

# Link table for charges to their specific sentences
charge_sentence_link_table = {'charge_id': 'INTEGER REFERENCES conviction_charges(id)',
                             'sentence_id': 'INTEGER REFERENCES conviction_sentences(id)',
                             'sentence_details': 'TEXT'}  # Additional details if needed

# Order mappings for data insertion
conviction_case_order = ['cphBody_lblDocketNo', 'cphBody_lblDefendant', 'cphBody_lblDefendantAttorney', 
                         'cphBody_lblDefendantBirthDate', 'cphBody_lblArrestDate', 'cphBody_lblSentDate', 
                         'cphBody_lblCourt', 'cphBody_lblCost', 'cphBody_Label4']
CONVICTION_CASE_ORDER = {k:i for i, k in enumerate(conviction_case_order)}

conviction_charge_order = ['Statute', 'Description', 'Class', 'Type', 'Occ', 
                          'Offense Date', 'Plea', 'Verdict Finding', 'Verdict Date', 
                          'Fine', 'Fee(s)', 'charge_sequence']
CONVICTION_CHARGE_ORDER = {k:i for i, k in enumerate(conviction_charge_order)}

# SQL CREATE statements
CREATE_CONVICTION_TABLE = f'''
    CREATE TABLE IF NOT EXISTS conviction (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in conviction_case_table.items()])},
        UNIQUE (docket_no, version));'''

CREATE_CONVICTION_CHARGES_TABLE = f'''
    CREATE TABLE IF NOT EXISTS conviction_charges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES conviction(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in conviction_charge_table.items()])});'''

CREATE_CONVICTION_SENTENCES_TABLE = f'''
    CREATE TABLE IF NOT EXISTS conviction_sentences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER REFERENCES conviction(id) ON DELETE CASCADE,
        {', '.join([f'{k} {v}' for k, v in conviction_sentence_table.items()])});'''

CREATE_CHARGE_SENTENCE_LINK_TABLE = f'''
    CREATE TABLE IF NOT EXISTS charge_sentence_link (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join([f'{k} {v}' for k, v in charge_sentence_link_table.items()])},
        UNIQUE(charge_id, sentence_id));'''

# Create a view for easier LLM access
CREATE_CONVICTION_SUMMARY_VIEW = '''
    CREATE VIEW IF NOT EXISTS conviction_summary AS
    SELECT 
        c.docket_no,
        c.version,
        c.last_first_name as defendant_name,
        c.represented_by as attorney,
        c.birth_year,
        c.arrest_date,
        c.sentenced_date,
        c.court,
        c.cost as total_cost,
        c.paid as amount_paid,
        CASE 
            WHEN c.paid = c.cost AND c.cost > '0' THEN 'PAID IN FULL'
            WHEN CAST(c.paid AS REAL) > 0 THEN 'PARTIALLY PAID'
            ELSE 'UNPAID'
        END as payment_status,
        (SELECT sentence_text 
         FROM conviction_sentences 
         WHERE case_id = c.id AND sentence_type = 'OVERALL' AND is_active = 1
         LIMIT 1) as current_sentence,
        (SELECT GROUP_CONCAT(sentence_text, '; ') 
         FROM conviction_sentences 
         WHERE case_id = c.id AND sentence_type = 'MODIFIED') as modified_sentences,
        (SELECT COUNT(*) 
         FROM conviction_charges 
         WHERE case_id = c.id) as total_charges,
        (SELECT GROUP_CONCAT(statute || ' - ' || description, ', ') 
         FROM conviction_charges 
         WHERE case_id = c.id) as all_charges,
        (SELECT SUM(CAST(NULLIF(fine, '') AS REAL)) 
         FROM conviction_charges 
         WHERE case_id = c.id) as total_fines,
        (SELECT SUM(CAST(NULLIF(fees, '') AS REAL)) 
         FROM conviction_charges 
         WHERE case_id = c.id) as total_fees
    FROM conviction c
    WHERE c.version = (SELECT MAX(version) FROM conviction WHERE docket_no = c.docket_no);'''

# All tables to create - REMOVED the old sentence table creation
CONVICTION_TABLES = [
    CREATE_CONVICTION_TABLE,
    CREATE_CONVICTION_CHARGES_TABLE,
    CREATE_CONVICTION_SENTENCES_TABLE,
    CREATE_CHARGE_SENTENCE_LINK_TABLE,
    CREATE_CONVICTION_SUMMARY_VIEW
]

# SQL INSERT statements
INSERT_CONVICTION = f'''                   
    INSERT OR IGNORE INTO conviction ({', '.join(conviction_case_table)})
    VALUES ({', '.join('?' * len(conviction_case_table))})'''

INSERT_CONVICTION_CHARGE = f'''
    INSERT INTO conviction_charges (case_id, {', '.join(conviction_charge_table)})
    VALUES ({', '.join('?' * (len(conviction_charge_table) + 1))})'''

INSERT_CONVICTION_SENTENCE = f'''
    INSERT INTO conviction_sentences (case_id, {', '.join(conviction_sentence_table)})
    VALUES ({', '.join('?' * (len(conviction_sentence_table) + 1))})'''

INSERT_CHARGE_SENTENCE_LINK = '''
    INSERT OR IGNORE INTO charge_sentence_link (charge_id, sentence_id, sentence_details)
    VALUES (?, ?, ?)'''
