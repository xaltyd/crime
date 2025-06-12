# Crime Stats Project Recap
*Last Updated: January 2025*

## Project Overview
Web scraping and database system for Connecticut criminal justice data, including:
- Judicial case data (pending, conviction, docket)
- Department of Corrections inmate data

## Database Structure

### Judicial Tables (records.db)
1. **pending** - Active criminal cases
2. **pending_charges** - Charges for pending cases
3. **conviction** - Completed cases with sentences
4. **conviction_charges** - Charges for convictions
5. **conviction_sentences** - Sentence details (replaced conviction_sentence)
6. **docket** - Daily court docket (may be redundant - pending analysis)
7. **docket_charges** - Charges from docket
8. **case_tracking** - Track dropped/aged-out cases
9. **conviction_summary** - VIEW for easy querying

### DOC Tables (records.db)
1. **dept_of_correction** - Active inmates
2. **dept_of_correction_history** - Track changes over time
3. **dept_of_correction_released** - Inmates no longer on site
4. **doc_scrape_failures** - Detailed failure tracking

## Key Accomplishments

### 1. Fixed Date Storage Issues
- All date columns changed from TEXT to DATE type
- Dates stored in ISO format (YYYY-MM-DD) for proper sorting
- Created migration scripts and date_utils.py for consistent handling

### 2. Resolved Duplicate Tables
- Removed duplicate conviction_sentence table
- Now using only conviction_sentences (with 's')

### 3. Enhanced Error Handling
- DOC scraper tracks detailed failure information
- Automatic retry logic for failed URLs
- Comprehensive failure reports generated

### 4. Configurable Processing Limits
- docket.py has configuration variables:
  - DEBUGGING = True (30 records)
  - LIMIT_RECORDS = True (custom limit)
  - Both False = process all records

## Current Status

### Completed
- ✅ Database schema properly structured with DATE types
- ✅ Conviction data parsing with proper sentence handling
- ✅ DOC scraper with enhanced failure tracking
- ✅ Fixed conviction_summary view for SQLite compatibility
- ✅ Date parsing utilities for consistent formatting

### In Progress
- 🔄 DOC scraper running overnight (~9 hours)
- 🔄 Analysis of docket vs pending overlap (needs full dataset)

### Pending Tasks
1. **Analyze docket necessity** - Check if all docket cases exist in pending
2. **Implement case tracking** - Monitor dropped/aged-out cases
3. **Consider removing docket tables** if redundant

## Key Files

### Core Scripts
- **docket.py** - Main judicial data scraper (configurable limits)
- **doc-portal.py** - DOC inmate scraper with failure tracking
- **storage.py** - Database storage logic
- **conviction_storage.py** - Specialized conviction handling
- **date_utils.py** - Date parsing utilities

### SQL Definitions
- **docket_sql.py** - Docket table definitions
- **pending_sql.py** - Pending table definitions  
- **conviction_sql.py** - Conviction table definitions (with fixed view)
- **sentence_sql.py** - REMOVED to prevent duplicates

### Analysis Scripts
- **analyze_docket_pending.py** - Check overlap between tables
- **check_conviction_view.py** - Fix conviction_summary view
- **fix_all_dates.py** - Comprehensive date fix script

## Important Notes

### Date Handling
- All dates must be parsed using parse_date() from date_utils.py
- Converts MM/DD/YYYY to YYYY-MM-DD (ISO format)
- SQLite DATE columns now sort correctly

### Data Versioning
- Cases track version numbers for change history
- conviction_summary view shows only latest version
- Historical changes preserved in separate tables

### Error Recovery
- DOC failures tracked in doc_scrape_failures table
- Failed URLs can be retried up to 3 times
- Detailed failure reports generated automatically

## Next Session Goals
1. Review DOC scraper results and failure report
2. Run docket vs pending analysis with full dataset
3. Decide on docket table retention
4. Implement daily case tracking logic
5. Consider optimization for faster processing

## Configuration Settings
```python
# docket.py settings
DEBUGGING = False      # Set True for 30 record limit
LIMIT_RECORDS = False  # Set True for custom limit
RECORD_LIMIT = 500     # Used when LIMIT_RECORDS = True
```

## Database Queries for Analysis
```sql
-- Check conviction summary
SELECT COUNT(*) FROM conviction_summary;

-- View recent convictions
SELECT * FROM conviction_summary 
ORDER BY sentenced_date DESC 
LIMIT 10;

-- Check DOC failures
SELECT failure_type, COUNT(*) 
FROM doc_scrape_failures 
GROUP BY failure_type;

-- Find dropped cases (after implementation)
SELECT * FROM case_tracking 
WHERE status = 'DROPPED';
```