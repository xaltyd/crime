import warnings
# Suppress the SQLite datetime adapter deprecation warning
warnings.filterwarnings('ignore', category=DeprecationWarning)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import win32gui
import win32con
import sqlite3
from datetime import datetime
import hashlib
import json
import traceback
import os

# Suppress the SQLite datetime adapter deprecation warning
warnings.filterwarnings('ignore', category=DeprecationWarning, module='sqlite3')

def calculate_age(date_of_birth):
    """Calculate age from date of birth string"""
    try:
        # Parse DOB (assuming format like MM/DD/YYYY)
        dob = datetime.strptime(date_of_birth, '%m/%d/%Y')
        today = datetime.now()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age
    except:
        return None

def setup_database():
    """Create the database tables if they don't exist"""
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    # Drop tables if they exist (uncomment these lines if you want to force recreation)
    # cursor.execute('DROP TABLE IF EXISTS dept_of_correction_history')
    # cursor.execute('DROP TABLE IF EXISTS dept_of_correction')
    # cursor.execute('DROP TABLE IF EXISTS dept_of_correction_released')
    
    # Create dept_of_correction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dept_of_correction (
            inmate_number TEXT PRIMARY KEY,
            inmate_name TEXT,
            date_of_birth TEXT,
            latest_admission_date TEXT,
            current_location TEXT,
            status TEXT,
            bond_amount TEXT,
            controlling_offense TEXT,
            date_of_sentence TEXT,
            maximum_sentence TEXT,
            maximum_release_date TEXT,
            estimated_release_date TEXT,
            special_parole_end_date TEXT,
            detainer TEXT,
            last_updated TIMESTAMP,
            data_hash TEXT
        )
    ''')
    
    # Create dept_of_correction_history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dept_of_correction_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inmate_number TEXT,
            inmate_name TEXT,
            date_of_birth TEXT,
            latest_admission_date TEXT,
            current_location TEXT,
            status TEXT,
            bond_amount TEXT,
            controlling_offense TEXT,
            date_of_sentence TEXT,
            maximum_sentence TEXT,
            maximum_release_date TEXT,
            estimated_release_date TEXT,
            special_parole_end_date TEXT,
            detainer TEXT,
            recorded_at TIMESTAMP,
            data_hash TEXT,
            FOREIGN KEY (inmate_number) REFERENCES dept_of_correction(inmate_number)
        )
    ''')
    
    # Create dept_of_correction_released table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dept_of_correction_released (
            inmate_number TEXT PRIMARY KEY,
            inmate_name TEXT,
            date_of_birth TEXT,
            age_at_release INTEGER,
            controlling_offense TEXT,
            latest_admission_date TEXT,
            estimated_release_date TEXT,
            actual_release_detected_date TIMESTAMP,
            last_known_status TEXT,
            last_known_location TEXT
        )
    ''')
    
    # Create failures tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS doc_scrape_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            failure_type TEXT,
            error_message TEXT,
            error_details TEXT,
            page_title TEXT,
            page_source_preview TEXT,
            extracted_data TEXT,
            attempt_number INTEGER,
            failed_at TIMESTAMP,
            retry_scheduled BOOLEAN DEFAULT 0
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database tables created successfully!")

def extract_inmate_data(driver, url, attempt_number=1):
    """Extract inmate data from a detail page with enhanced error tracking"""
    failure_info = {
        'url': url,
        'attempt_number': attempt_number,
        'failed_at': datetime.now()
    }
    
    try:
        driver.get(url)
        time.sleep(1)  # Brief wait for page load
        
        # Capture page info for debugging
        failure_info['page_title'] = driver.title
        
        # Check if page loaded correctly
        if "Page Not Found" in driver.title or "Error" in driver.title:
            failure_info['failure_type'] = 'PAGE_NOT_FOUND'
            failure_info['error_message'] = f'Page title indicates error: {driver.title}'
            failure_info['page_source_preview'] = driver.page_source[:1000]
            save_failure(failure_info)
            return None
        
        # Find all table rows
        rows = driver.find_elements(By.TAG_NAME, "tr")
        
        if not rows:
            failure_info['failure_type'] = 'NO_TABLE_ROWS'
            failure_info['error_message'] = 'No table rows found on page'
            failure_info['page_source_preview'] = driver.page_source[:1000]
            save_failure(failure_info)
            return None
        
        # Initialize data dictionary
        data = {}
        
        # Map of expected labels to database field names
        field_mapping = {
            "Inmate Number:": "inmate_number",
            "Inmate Name:": "inmate_name",
            "Date of Birth:": "date_of_birth",
            "Latest Admission Date:": "latest_admission_date",
            "Current Location:": "current_location",
            "Status:": "status",
            "Bond Amount:": "bond_amount",
            "Controlling Offense*:": "controlling_offense",
            "Date of Sentence:": "date_of_sentence",
            "Maximum Sentence:": "maximum_sentence",
            "Maximum Release Date:": "maximum_release_date",
            "Estimated Release Date:": "estimated_release_date",
            "Special Parole End Date:": "special_parole_end_date",
            "Detainer:": "detainer"
        }
        
        # Track which fields were found
        fields_found = []
        
        # Extract data from each row
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    
                    # Check if this label is one we're interested in
                    for expected_label, field_name in field_mapping.items():
                        if label == expected_label:
                            data[field_name] = value
                            fields_found.append(field_name)
                            break
            except Exception as row_error:
                # Log row-level errors but continue processing
                print(f"  Warning: Error processing row: {row_error}")
        
        # Check if we got the minimum required fields
        required_fields = ['inmate_number', 'inmate_name']
        missing_required = [f for f in required_fields if f not in data]
        
        if missing_required:
            failure_info['failure_type'] = 'MISSING_REQUIRED_FIELDS'
            failure_info['error_message'] = f'Missing required fields: {missing_required}'
            failure_info['error_details'] = f'Fields found: {fields_found}'
            failure_info['extracted_data'] = json.dumps(data)
            failure_info['page_source_preview'] = driver.page_source[:1000]
            save_failure(failure_info)
            return None
        
        # If we got here, extraction was successful
        return data
        
    except Exception as e:
        failure_info['failure_type'] = 'EXTRACTION_ERROR'
        failure_info['error_message'] = str(e)
        failure_info['error_details'] = traceback.format_exc()
        
        # Try to capture page source for debugging
        try:
            failure_info['page_source_preview'] = driver.page_source[:1000]
        except:
            failure_info['page_source_preview'] = 'Unable to capture page source'
        
        save_failure(failure_info)
        print(f"Error extracting data from {url}: {e}")
        return None

def save_failure(failure_info):
    """Save failure information to the database"""
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO doc_scrape_failures
            (url, failure_type, error_message, error_details, page_title, 
             page_source_preview, extracted_data, attempt_number, failed_at, retry_scheduled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            failure_info.get('url', ''),
            failure_info.get('failure_type', 'UNKNOWN'),
            failure_info.get('error_message', ''),
            failure_info.get('error_details', ''),
            failure_info.get('page_title', ''),
            failure_info.get('page_source_preview', ''),
            failure_info.get('extracted_data', ''),
            failure_info.get('attempt_number', 1),
            failure_info.get('failed_at'),
            failure_info.get('retry_scheduled', 0)
        ))
        conn.commit()
    except Exception as e:
        print(f"Error saving failure info: {e}")
    finally:
        conn.close()

def get_failed_urls():
    """Get list of URLs that failed in previous attempts"""
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    try:
        # Get the most recent failure for each URL
        cursor.execute('''
            SELECT url, MAX(attempt_number) as max_attempts, failure_type
            FROM doc_scrape_failures
            WHERE retry_scheduled = 0
            GROUP BY url
            HAVING max_attempts < 3  -- Only retry up to 3 times
        ''')
        
        return [(row[0], row[1] + 1) for row in cursor.fetchall()]
    finally:
        conn.close()

def generate_failure_report():
    """Generate a detailed report of all failures"""
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    try:
        # Get failure statistics
        cursor.execute('''
            SELECT failure_type, COUNT(*) as count
            FROM doc_scrape_failures
            GROUP BY failure_type
            ORDER BY count DESC
        ''')
        
        failure_stats = cursor.fetchall()
        
        # Get detailed failures
        cursor.execute('''
            SELECT url, failure_type, error_message, attempt_number, failed_at
            FROM doc_scrape_failures
            ORDER BY failed_at DESC
            LIMIT 50
        ''')
        
        recent_failures = cursor.fetchall()
        
        # Generate report
        report_lines = [
            "Connecticut DOC Scrape Failure Report",
            "=" * 50,
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "Failure Statistics:",
            "-" * 30
        ]
        
        for failure_type, count in failure_stats:
            report_lines.append(f"{failure_type}: {count}")
        
        report_lines.extend([
            "",
            "Recent Failures (Last 50):",
            "-" * 30
        ])
        
        for url, failure_type, error_msg, attempt, failed_at in recent_failures:
            report_lines.append(f"\nURL: {url}")
            report_lines.append(f"Type: {failure_type}")
            report_lines.append(f"Error: {error_msg[:100]}...")
            report_lines.append(f"Attempt: {attempt}")
            report_lines.append(f"Failed at: {failed_at}")
        
        # Save report
        with open('doc_scrape_failure_report.txt', 'w') as f:
            f.write('\n'.join(report_lines))
        
        # Also generate CSV for analysis
        cursor.execute('''
            SELECT * FROM doc_scrape_failures
            ORDER BY failed_at DESC
        ''')
        
        import csv
        with open('doc_scrape_failures.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([description[0] for description in cursor.description])
            writer.writerows(cursor.fetchall())
        
        print("✅ Failure reports generated: doc_scrape_failure_report.txt and doc_scrape_failures.csv")
        
    finally:
        conn.close()

def calculate_data_hash(data):
    """Calculate a hash of the data to detect changes"""
    # Remove last_updated from hash calculation
    data_for_hash = {k: v for k, v in data.items() if k not in ['last_updated', 'data_hash']}
    data_string = json.dumps(data_for_hash, sort_keys=True)
    return hashlib.md5(data_string.encode()).hexdigest()

def save_inmate_data(data):
    """Save inmate data to database, handling history if data changed"""
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    try:
        # Calculate hash of current data
        current_hash = calculate_data_hash(data)
        
        # Check if inmate exists and get all columns dynamically
        cursor.execute('SELECT * FROM dept_of_correction WHERE inmate_number = ?', 
                      (data['inmate_number'],))
        existing_record = cursor.fetchone()
        
        if existing_record:
            # Get column names
            cursor.execute("PRAGMA table_info(dept_of_correction)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Create dict from existing record
            existing_data = dict(zip(columns, existing_record))
            existing_hash = existing_data.get('data_hash', '')
            
            # If data has changed, save to history first
            if existing_hash != current_hash:
                # Build history insert dynamically based on existing columns
                history_columns = [col for col in columns if col not in ['last_updated']]
                history_values = [existing_data.get(col, '') for col in history_columns]
                
                # Add recorded_at timestamp
                history_columns.append('recorded_at')
                history_values.append(datetime.now())
                
                placeholders = ','.join(['?' for _ in history_values])
                columns_str = ','.join(history_columns)
                
                cursor.execute(f'''
                    INSERT INTO dept_of_correction_history 
                    ({columns_str})
                    VALUES ({placeholders})
                ''', history_values)
                
                print(f"  - Changes detected for inmate {data['inmate_number']}, saved to history")
        
        # Update or insert current data
        data['last_updated'] = datetime.now()
        data['data_hash'] = current_hash
        
        cursor.execute('''
            INSERT OR REPLACE INTO dept_of_correction 
            (inmate_number, inmate_name, date_of_birth, latest_admission_date,
             current_location, status, bond_amount, controlling_offense,
             date_of_sentence, maximum_sentence, maximum_release_date,
             estimated_release_date, special_parole_end_date, detainer,
             last_updated, data_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('inmate_number', ''),
            data.get('inmate_name', ''),
            data.get('date_of_birth', ''),
            data.get('latest_admission_date', ''),
            data.get('current_location', ''),
            data.get('status', ''),
            data.get('bond_amount', ''),
            data.get('controlling_offense', ''),
            data.get('date_of_sentence', ''),
            data.get('maximum_sentence', ''),
            data.get('maximum_release_date', ''),
            data.get('estimated_release_date', ''),
            data.get('special_parole_end_date', ''),
            data.get('detainer', ''),
            data['last_updated'],
            data['data_hash']
        ))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Database error: {e}")
        conn.rollback()
        return False
    
    finally:
        conn.close()

def process_released_inmates(found_inmate_numbers):
    """Identify and process inmates who are no longer on the DOC site"""
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    try:
        # Get all inmate numbers currently in the database
        cursor.execute('SELECT inmate_number FROM dept_of_correction')
        all_current_inmates = set(row[0] for row in cursor.fetchall())
        
        # Find inmates who are no longer on the site
        released_inmates = all_current_inmates - found_inmate_numbers
        
        if released_inmates:
            print(f"\n🔔 Detected {len(released_inmates)} inmates no longer on DOC site")
            
            for inmate_number in released_inmates:
                # Get the inmate's data before moving them
                cursor.execute('''
                    SELECT inmate_number, inmate_name, date_of_birth, 
                           controlling_offense, latest_admission_date,
                           estimated_release_date, status, current_location
                    FROM dept_of_correction 
                    WHERE inmate_number = ?
                ''', (inmate_number,))
                
                inmate_data = cursor.fetchone()
                
                if inmate_data:
                    # Calculate age at release
                    age_at_release = calculate_age(inmate_data[2]) if inmate_data[2] else None
                    
                    # Insert into released table
                    cursor.execute('''
                        INSERT OR REPLACE INTO dept_of_correction_released
                        (inmate_number, inmate_name, date_of_birth, age_at_release,
                         controlling_offense, latest_admission_date, estimated_release_date,
                         actual_release_detected_date, last_known_status, last_known_location)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        inmate_data[0],  # inmate_number
                        inmate_data[1],  # inmate_name
                        inmate_data[2],  # date_of_birth
                        age_at_release,
                        inmate_data[3],  # controlling_offense
                        inmate_data[4],  # latest_admission_date
                        inmate_data[5],  # estimated_release_date
                        datetime.now(),  # actual_release_detected_date
                        inmate_data[6],  # last_known_status
                        inmate_data[7]   # last_known_location
                    ))
                    
                    # Save final state to history before deletion
                    cursor.execute('''
                        INSERT INTO dept_of_correction_history 
                        SELECT NULL as id, *, ? as recorded_at
                        FROM dept_of_correction 
                        WHERE inmate_number = ?
                    ''', (datetime.now(), inmate_number))
                    
                    # Remove from active inmates table
                    cursor.execute('DELETE FROM dept_of_correction WHERE inmate_number = ?', 
                                 (inmate_number,))
                    
                    print(f"  ✅ Moved to released: {inmate_data[1]} (#{inmate_number})")
            
            conn.commit()
            return len(released_inmates)
        else:
            return 0
            
    except Exception as e:
        print(f"Error processing released inmates: {e}")
        conn.rollback()
        return 0
    
    finally:
        conn.close()

def scrape_all_inmates(hide_window=True, retry_failures=True):
    """Main function to scrape all inmate data with enhanced error tracking"""
    
    # Start timer
    start_time = datetime.now()
    print(f"Starting scrape at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Setup database
    setup_database()
    
    # Read URLs from file
    try:
        with open('inmate_urls.txt', 'r') as f:
            urls = [(line.strip(), 1) for line in f if line.strip()]  # (url, attempt_number)
        print(f"Loaded {len(urls)} inmate URLs")
    except FileNotFoundError:
        print("Error: inmate_urls.txt not found. Run the URL extraction first.")
        return
    
    # Add failed URLs for retry if requested
    if retry_failures:
        failed_urls = get_failed_urls()
        if failed_urls:
            print(f"Adding {len(failed_urls)} previously failed URLs for retry")
            urls.extend(failed_urls)
    
    # Initialize Chrome driver with options
    print(f"Setting up Chrome driver (hide_window={hide_window})...")
    
    # Set up Chrome options
    chrome_options = webdriver.ChromeOptions()
    
    # Try to find Chrome in common locations
    chrome_paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%PROGRAMFILES%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%PROGRAMFILES(X86)%\Google\Chrome\Application\chrome.exe"),
    ]
    
    chrome_binary = None
    for path in chrome_paths:
        if os.path.exists(path):
            chrome_binary = path
            print(f"Found Chrome at: {path}")
            break
    
    if chrome_binary:
        chrome_options.binary_location = chrome_binary
    else:
        print("Warning: Could not find Chrome in standard locations. Trying default...")
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    except Exception as e:
        print(f"Error with ChromeDriverManager: {e}")
        print("Trying alternative approach...")
        # Try without ChromeDriverManager
        driver = webdriver.Chrome(options=chrome_options)
    
    # Hide window if requested
    if hide_window:
        print("Hiding browser window using Windows API...")
        time.sleep(0.5)
        
        def enum_windows_callback(hwnd, windows):
            if win32gui.IsWindowVisible(hwnd) and win32gui.GetWindowText(hwnd):
                windows.append((hwnd, win32gui.GetWindowText(hwnd)))
        
        windows = []
        win32gui.EnumWindows(enum_windows_callback, windows)
        
        chrome_window = None
        for hwnd, title in windows:
            if "Chrome" in title or "chrome" in title:
                chrome_window = hwnd
                break
        
        if chrome_window:
            win32gui.ShowWindow(chrome_window, win32con.SW_HIDE)
            print("✅ Browser window hidden successfully!")
    
    try:
        # Track found inmate numbers for release detection
        found_inmate_numbers = set()
        
        # Process each inmate URL
        successful = 0
        failed = 0
        failure_types = {}
        
        for i, (url, attempt_number) in enumerate(urls):
            print(f"\nProcessing {i+1}/{len(urls)}: {url}")
            if attempt_number > 1:
                print(f"  (Retry attempt #{attempt_number})")
            
            data = extract_inmate_data(driver, url, attempt_number)
            
            if data and 'inmate_number' in data:
                found_inmate_numbers.add(data['inmate_number'])
                
                if save_inmate_data(data):
                    successful += 1
                    print(f"  ✅ Saved: {data['inmate_name']} (#{data['inmate_number']})")
                else:
                    failed += 1
                    print(f"  ❌ Failed to save data")
                    # Track database save failures
                    failure_types['DATABASE_SAVE_ERROR'] = failure_types.get('DATABASE_SAVE_ERROR', 0) + 1
            else:
                failed += 1
                print(f"  ❌ Failed to extract data")
                # Failure already logged in extract_inmate_data
            
            # Progress update every 100 records
            if (i + 1) % 100 == 0:
                elapsed = datetime.now() - start_time
                avg_time_per_record = elapsed.total_seconds() / (i + 1)
                remaining_records = len(urls) - (i + 1)
                estimated_remaining = avg_time_per_record * remaining_records
                
                print(f"\n--- Progress: {i+1}/{len(urls)} processed, {successful} successful, {failed} failed ---")
                print(f"--- Elapsed: {elapsed}, Estimated remaining: {estimated_remaining/60:.1f} minutes ---\n")
        
        # Process released inmates
        print("\n🔍 Checking for released inmates...")
        released_count = process_released_inmates(found_inmate_numbers)
        
        # Generate failure report
        generate_failure_report()
        
        # Get failure statistics from database
        conn = sqlite3.connect('records.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT failure_type, COUNT(*) as count
            FROM doc_scrape_failures
            GROUP BY failure_type
        ''')
        failure_stats = dict(cursor.fetchall())
        conn.close()
        
        # Calculate final statistics
        end_time = datetime.now()
        total_elapsed = end_time - start_time
        
        # Create results dictionary
        results = {
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "total_elapsed_seconds": total_elapsed.total_seconds(),
            "total_elapsed_formatted": str(total_elapsed),
            "total_urls": len(urls),
            "successful": successful,
            "failed": failed,
            "failure_breakdown": failure_stats,
            "released_detected": released_count,
            "success_rate": f"{(successful/len(urls)*100):.2f}%" if urls else "0%",
            "average_seconds_per_record": total_elapsed.total_seconds() / len(urls) if urls else 0
        }
        
        # Save results to file
        with open('scrape_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        # Also save a human-readable summary
        with open('scrape_summary.txt', 'w') as f:
            f.write("Connecticut DOC Scrape Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Start Time: {results['start_time']}\n")
            f.write(f"End Time: {results['end_time']}\n")
            f.write(f"Total Elapsed Time: {results['total_elapsed_formatted']}\n")
            f.write(f"Total Records: {results['total_urls']}\n")
            f.write(f"Successful: {results['successful']}\n")
            f.write(f"Failed: {results['failed']}\n")
            f.write(f"Released Detected: {results['released_detected']}\n")
            f.write(f"Success Rate: {results['success_rate']}\n")
            f.write(f"Average Time per Record: {results['average_seconds_per_record']:.2f} seconds\n")
            f.write(f"\nFailure Breakdown:\n")
            for failure_type, count in failure_stats.items():
                f.write(f"  - {failure_type}: {count}\n")
            f.write(f"\nEstimated time for distributed processing:\n")
            f.write(f"- With 5 devices: {results['total_elapsed_seconds']/5/60:.1f} minutes\n")
            f.write(f"- With 10 devices: {results['total_elapsed_seconds']/10/60:.1f} minutes\n")
            f.write(f"- With 20 devices: {results['total_elapsed_seconds']/20/60:.1f} minutes\n")
        
        print(f"\n\n=== FINAL RESULTS ===")
        print(f"Total URLs processed: {len(urls)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Released Detected: {released_count}")
        print(f"Total Time: {total_elapsed}")
        print(f"Average time per record: {results['average_seconds_per_record']:.2f} seconds")
        
        if failure_stats:
            print(f"\nFailure Breakdown:")
            for failure_type, count in failure_stats.items():
                print(f"  - {failure_type}: {count}")
        
        print(f"\n✅ Results saved to:")
        print(f"  - scrape_results.json")
        print(f"  - scrape_summary.txt")
        print(f"  - doc_scrape_failure_report.txt")
        print(f"  - doc_scrape_failures.csv")
        
    except Exception as e:
        print(f"\nFatal error: {e}")
        traceback.print_exc()
        
        # Save error state
        error_time = datetime.now()
        error_elapsed = error_time - start_time
        with open('scrape_error.txt', 'w') as f:
            f.write(f"Error occurred at: {error_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Elapsed before error: {error_elapsed}\n")
            f.write(f"Error details: {e}\n")
            f.write(f"\nFull traceback:\n{traceback.format_exc()}\n")
    
    finally:
        driver.quit()
        print("\nScraping complete!")

if __name__ == "__main__":
    # Run with visible browser (set to True to hide browser)
    # retry_failures=True will retry previously failed URLs up to 3 times
    scrape_all_inmates(hide_window=True, retry_failures=True)
