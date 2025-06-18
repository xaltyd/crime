# simplified_sql.py
"""
Simplified database schema with separate charge tables
Optimized for LLM consumption while maintaining relational structure for charges
"""

import json
from datetime import datetime

# CONVICTIONS TABLE
CREATE_CONVICTIONS_TABLE = '''
CREATE TABLE IF NOT EXISTS convictions (
    -- Primary Key and Versioning
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    docket_no TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    record_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Defendant Information
    defendant_name TEXT,
    defendant_attorney TEXT,
    birth_year TEXT,
    
    -- Case Information
    arresting_agency TEXT,
    arrest_date TEXT,
    sentenced_date TEXT,
    court TEXT,
    
    -- Financial Information
    total_cost TEXT,
    amount_paid TEXT,
    payment_status TEXT,  -- 'PAID IN FULL', 'PARTIALLY PAID', 'UNPAID'
    
    -- Original Sentence Information
    overall_sentence TEXT,  -- The complete overall sentence text
    
    -- Metadata
    is_sealed BOOLEAN DEFAULT FALSE,
    data_source_url TEXT,  -- Only the part after 'crdockets/'
    
    -- Indexing
    UNIQUE(docket_no, version)
)'''

# CONVICTION CHARGES TABLE
CREATE_CONVICTION_CHARGES_TABLE = '''
CREATE TABLE IF NOT EXISTS conviction_charges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL REFERENCES convictions(id) ON DELETE CASCADE,
    charge_sequence INTEGER,  -- To maintain order
    
    -- Charge Information
    statute TEXT,
    description TEXT,
    class TEXT,
    type TEXT,
    occ TEXT,
    offense_date TEXT,
    plea TEXT,
    verdict_finding TEXT,
    verdict_date TEXT,
    fine TEXT,
    fees TEXT,
    sentence_text TEXT,  -- Individual charge sentence if applicable
    
    -- Modified Sentence Information (for charges that appear in modified section)
    is_modified BOOLEAN DEFAULT FALSE,
    modified_sentence_finding TEXT,    -- Changed from modified_verdict_finding
    modified_sentence_date TEXT,       -- Changed from modified_verdict_date
    modified_fine TEXT,
    modified_fees TEXT
)'''

# PENDING TABLE
CREATE_PENDING_TABLE = '''
CREATE TABLE IF NOT EXISTS pending (
    -- Primary Key and Versioning
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    docket_no TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    record_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Defendant Information
    defendant_name TEXT,
    defendant_attorney TEXT,
    birth_year TEXT,
    
    -- Case Information
    arresting_agency TEXT,
    arrest_date TEXT,
    court TEXT,
    times_on_docket TEXT,
    companion_cases TEXT,  -- Comma-separated companion docket numbers
    docket_type TEXT,
    
    -- Bond Information
    bond_amount TEXT,
    bond_type TEXT,
    custody_status TEXT,
    
    -- Hearing Information
    next_hearing_date TEXT,
    hearing_purpose TEXT,
    hearing_reason TEXT,
    
    -- Metadata
    is_sealed BOOLEAN DEFAULT FALSE,
    data_source_url TEXT,  -- Only the part after 'crdockets/'
    misc_notes TEXT,
    
    -- Indexing
    UNIQUE(docket_no, version)
)'''

# PENDING CHARGES TABLE
CREATE_PENDING_CHARGES_TABLE = '''
CREATE TABLE IF NOT EXISTS pending_charges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL REFERENCES pending(id) ON DELETE CASCADE,
    charge_sequence INTEGER,  -- To maintain order
    
    -- Charge Information
    statute TEXT,
    description TEXT,
    class TEXT,
    type TEXT,
    occ TEXT,
    offense_date TEXT,
    plea TEXT,
    verdict_finding TEXT
)'''

# Create indexes for main tables
CREATE_CONVICTION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_conviction_docket_no ON convictions(docket_no)",
    "CREATE INDEX IF NOT EXISTS idx_conviction_defendant_name ON convictions(defendant_name)",
    "CREATE INDEX IF NOT EXISTS idx_conviction_sentenced_date ON convictions(sentenced_date)",
    "CREATE INDEX IF NOT EXISTS idx_conviction_charges_case_id ON conviction_charges(case_id)"
]

CREATE_PENDING_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pending_docket_no ON pending(docket_no)",
    "CREATE INDEX IF NOT EXISTS idx_pending_defendant_name ON pending(defendant_name)",
    "CREATE INDEX IF NOT EXISTS idx_pending_hearing_date ON pending(next_hearing_date)",
    "CREATE INDEX IF NOT EXISTS idx_pending_charges_case_id ON pending_charges(case_id)"
]

# All SQL create statements
ALL_TABLES = [
    CREATE_CONVICTIONS_TABLE,
    CREATE_CONVICTION_CHARGES_TABLE,
    CREATE_PENDING_TABLE,
    CREATE_PENDING_CHARGES_TABLE
] + CREATE_CONVICTION_INDEXES + CREATE_PENDING_INDEXES

# Helper functions for data preparation
def prepare_conviction_data(parsed_data, source_url=None):
    """
    Transform parsed conviction data into rows for insertion.

    Returns:
        case_row (dict): Mapping of conviction-level fields.
        charge_rows (list of tuple): List of tuples for conviction_charges.
    """
    # Handle both wrapped and unwrapped data structures
    if 'case_details' in parsed_data:
        # Data is already wrapped
        case_details = parsed_data.get('case_details', {})
        sentences = parsed_data.get('sentences', {})
        charges = parsed_data.get('charges', [])
    else:
        # Data is from parser directly - need to extract
        case_details = {
            'docket_number': parsed_data.get('docket_number'),
            'defendant_name': parsed_data.get('defendant_name'),
            'defendant_attorney': parsed_data.get('defendant_attorney'),
            'birth_year': parsed_data.get('birth_year'),
            'arresting_agency': parsed_data.get('arresting_agency'),
            'arrest_date': parsed_data.get('arrest_date'),
            'sentenced_date': parsed_data.get('disposition_date'),
            'court': parsed_data.get('court'),
            'total_cost': parsed_data.get('total_cost', ''),
            'amount_paid': parsed_data.get('amount_paid', ''),
            'payment_status': parsed_data.get('payment_status', ''),
            'overall_sentence': parsed_data.get('overall_sentence', ''),
            'is_sealed': False,
            'source_url': source_url or parsed_data.get('href', '')
        }
        sentences = {}
        charges = parsed_data.get('charges', [])

    # Core case fields
    docket_no = case_details.get('docket_number', '')
    defendant_name = case_details.get('defendant_name', '')
    defendant_attorney = case_details.get('defendant_attorney') or parsed_data.get('defendant_attorney', '')
    birth_year = case_details.get('birth_year', '')
    arresting_agency = case_details.get('arresting_agency') or parsed_data.get('arresting_agency', '')
    arrest_date = case_details.get('arrest_date', '')
    sentenced_date = case_details.get('sentenced_date', '') or sentences.get('overall_sentence_date', '')
    court = case_details.get('court') or parsed_data.get('court', '')

    # Financial / status fields
    total_cost = case_details.get('total_cost', '')
    amount_paid = case_details.get('amount_paid', '')
    payment_status = case_details.get('payment_status', '')
    overall_sentence = case_details.get('overall_sentence') or parsed_data.get('overall_sentence', '')
    is_sealed = case_details.get('is_sealed', False)
    data_source = source_url or case_details.get('source_url', '') or case_details.get('data_source_url', '') or ''

    # Build the main conviction row dict - WITHOUT total_fines_amount and total_fees_amount
    case_row = {
        'docket_no': docket_no,
        'defendant_name': defendant_name,
        'defendant_attorney': defendant_attorney,
        'birth_year': birth_year,
        'arresting_agency': arresting_agency,
        'arrest_date': arrest_date,
        'sentenced_date': sentenced_date,
        'court': court,
        'record_created': datetime.utcnow(),
        'record_updated': datetime.utcnow(),
        'total_cost': total_cost,
        'amount_paid': amount_paid,
        'payment_status': payment_status,
        'overall_sentence': overall_sentence,
        'is_sealed': is_sealed,
        'data_source_url': data_source,
    }

    # Build charge rows - handle both uppercase and lowercase keys
    charge_rows = []
    for seq, charge in enumerate(charges, start=1):
        # Handle both uppercase (old) and lowercase (new) keys
        charge_rows.append((
            docket_no,
            seq,
            charge.get('statute') or charge.get('Statute', ''),
            charge.get('description') or charge.get('Description', ''),
            charge.get('class') or charge.get('Class', ''),
            charge.get('type') or charge.get('Type', ''),
            charge.get('occ') or charge.get('Occ', ''),
            charge.get('offense_date') or charge.get('Offense Date', ''),
            charge.get('plea') or charge.get('Plea', ''),
            charge.get('verdict_finding') or charge.get('Verdict Finding', ''),
            charge.get('verdict_date') or charge.get('Verdict Date', ''),
            charge.get('fine') or charge.get('Fine', 0),
            charge.get('fees') or charge.get('Fee(s)') or charge.get('Fees', 0),
            charge.get('sentence_text', ''),
            charge.get('is_modified', False),
            charge.get('modified_sentence_finding', ''),
            charge.get('modified_sentence_date', ''),
            charge.get('modified_fine', 0),
            charge.get('modified_fees', 0),
        ))

    return case_row, charge_rows

def prepare_pending_data(parsed_data, source_url=None):
    """
    Prepare pending case data for insertion
    Returns tuple of (main_record, charges_list)
    """
    case_details = parsed_data.get('case_details', {})
    charges = parsed_data.get('charges', [])
    
    # Extract defendant info
    defendant_name = case_details.get('cphBody_lblDefendant', '')
    birth_date = case_details.get('cphBody_lblDefendantBirthDate', '')
    birth_year = ''
    if birth_date and '/' in birth_date:
        birth_year = birth_date.split('/')[-1]
    
    # Process source URL - only keep part after 'crdockets/'
    if source_url and 'crdockets/' in source_url:
        source_url = source_url.split('crdockets/')[-1]
    
    # Build main record
    pending_record = {
        'docket_no': case_details.get('cphBody_lblDocketNo', ''),
        'defendant_name': defendant_name,
        'defendant_attorney': case_details.get('cphBody_lblDefendantAttorney', ''),
        'birth_year': birth_year,
        'arresting_agency': case_details.get('cphBody_lblArrestingAgency', ''),
        'arrest_date': case_details.get('cphBody_lblArrestDate', ''),
        'court': case_details.get('cphBody_lblCourt', ''),
        'times_on_docket': case_details.get('cphBody_lblTimesInCourt', ''),
        'companion_cases': case_details.get('cphBody_lblCompanionDocketNo', ''),
        'docket_type': case_details.get('cphBody_lblDocketType', ''),
        'bond_amount': case_details.get('cphBody_lblBondAmount', ''),
        'bond_type': case_details.get('cphBody_lblBondTypeDesc', ''),
        'custody_status': case_details.get('cphBody_lblBondTypeDescHelp', ''),
        'next_hearing_date': case_details.get('cphBody_lblHearingDate', ''),
        'hearing_purpose': case_details.get('cphBody_lblPurposeDesc', ''),
        'hearing_reason': case_details.get('cphBody_lblReasonDesc', ''),
        'misc_notes': case_details.get('cphBody_lblSidebarFlag', ''),
        'data_source_url': source_url,
        'record_updated': datetime.now().isoformat()
    }
    
    return pending_record, charges

# SQL Insert statements
INSERT_CONVICTION = '''
INSERT INTO convictions (
    docket_no, version, defendant_name, defendant_attorney, birth_year,
    arresting_agency, arrest_date, sentenced_date, court,
    total_cost, amount_paid, payment_status,
    overall_sentence, is_sealed, data_source_url, record_updated
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

INSERT_CONVICTION_CHARGE = """
INSERT INTO conviction_charges (
    case_id, charge_sequence, statute, description, class, type, occ,
    offense_date, plea, verdict_finding, verdict_date, fine, fees, sentence_text,
    is_modified, modified_sentence_finding, modified_sentence_date, modified_fine, modified_fees
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_PENDING = '''
INSERT INTO pending (
    docket_no, version, defendant_name, defendant_attorney, birth_year,
    arresting_agency, arrest_date, court, times_on_docket, companion_cases,
    docket_type, bond_amount, bond_type, custody_status,
    next_hearing_date, hearing_purpose, hearing_reason,
    is_sealed, data_source_url, misc_notes, record_updated
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

INSERT_PENDING_CHARGE = '''
INSERT INTO pending_charges (
    case_id, charge_sequence, statute, description, class, type, occ,
    offense_date, plea, verdict_finding
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
