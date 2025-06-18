# checkpoint.py
# Add this file to enable checkpoint/resume functionality

import json
import os
from datetime import datetime
from color_print import *

class CheckpointManager:
    def __init__(self, checkpoint_file='scraping_checkpoint.json'):
        self.checkpoint_file = checkpoint_file
        self.checkpoint_data = self.load_checkpoint()
    
    def load_checkpoint(self):
        """Load existing checkpoint or create new one"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                cprint(f"Loaded checkpoint from {self.checkpoint_file}", Fore.GREEN)
                return data
            except Exception as e:
                cprint(f"Error loading checkpoint: {e}", Fore.RED)
        
        return {
            'pending': {
                'last_page': 0,
                'last_href': None,
                'completed': False,
                'total_processed': 0
            },
            'conviction': {
                'last_page': 0,
                'last_href': None,
                'completed': False,
                'total_processed': 0
            },
            'daily_docket': {
                'last_court_code': None,
                'completed_courts': [],
                'completed': False
            },
            'last_update': None
        }
    
    def save_checkpoint(self):
        """Save current checkpoint data"""
        self.checkpoint_data['last_update'] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2)
        except Exception as e:
            cprint(f"Error saving checkpoint: {e}", Fore.RED)
    
    def update_search_progress(self, search_type, page, last_href, total_processed):
        """Update progress for pending/conviction searches"""
        self.checkpoint_data[search_type]['last_page'] = page
        self.checkpoint_data[search_type]['last_href'] = last_href
        self.checkpoint_data[search_type]['total_processed'] = total_processed
        
        # Save every 10 pages
        if page % 10 == 0:
            self.save_checkpoint()
    
    def mark_search_complete(self, search_type):
        """Mark a search type as completed"""
        self.checkpoint_data[search_type]['completed'] = True
        self.save_checkpoint()
    
    def mark_court_complete(self, court_code):
        """Mark a court as completed for daily docket"""
        if court_code not in self.checkpoint_data['daily_docket']['completed_courts']:
            self.checkpoint_data['daily_docket']['completed_courts'].append(court_code)
        self.checkpoint_data['daily_docket']['last_court_code'] = court_code
        self.save_checkpoint()
    
    def should_skip_search(self, search_type):
        """Check if we should skip a search type"""
        return self.checkpoint_data[search_type]['completed']
    
    def should_skip_court(self, court_code):
        """Check if we should skip a court"""
        return court_code in self.checkpoint_data['daily_docket']['completed_courts']
    
    def get_resume_page(self, search_type):
        """Get the page to resume from"""
        return self.checkpoint_data[search_type]['last_page']
    
    def reset(self):
        """Reset checkpoint data"""
        response = input("Are you sure you want to reset all checkpoint data? (y/n): ").lower()
        if response == 'y':
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
            self.checkpoint_data = self.load_checkpoint()
            cprint("Checkpoint data reset!", Fore.GREEN)


# Modified Search.get_records method to support checkpoints
def get_records_with_checkpoint(self, payload, search_type, checkpoint_manager):
    """Modified get_records with checkpoint support"""
    # Get resume page from checkpoint
    start_page = checkpoint_manager.get_resume_page(search_type)
    if start_page > 0:
        cprint(f"Resuming {search_type} from page {start_page}", Fore.YELLOW)
        page = start_page
    else:
        page = 1
    
    results = []
    cph_body_id = '_ctl0$cphBody$grdDockets$_ctl54$'
    self.sealed_count = 0
    
    header = self.page.results_header | {'referer': self.page.url[search_type]}
    
    # If resuming, we need to navigate to the correct page
    if start_page > 1:
        # Navigate to the resume page
        resp = ensure_connection(self.session.post, self.page.url[search_type],
                                 header, payload)
        
        # Skip to the correct page
        for skip_page in range(2, start_page + 1):
            payload = self.page.get_payload('search', resp.text)
            span_tag = f'<span>{skip_page-1}</span>'
            
            text = resp.text
            text = text[text.find(span_tag) + len(span_tag):]
            text = text[text.find(cph_body_id) + len(cph_body_id):]
            ctl_id = text[:text.find('&')]
            next_page = f'{cph_body_id}{ctl_id}'
            
            resp = ensure_connection(self.session.post, self.page.next_url[search_type],
                                     header | {'referer': self.page.next_url[search_type]},
                                     payload | {'__EVENTTARGET': next_page})
    else:
        resp = ensure_connection(self.session.post, self.page.url[search_type],
                                 header, payload)
    
    count = checkpoint_manager.checkpoint_data[search_type]['total_processed']
    last_href = None
    
    while f'<span>{page}</span>' in resp.text:
        if DEBUGGING:
            if count >= 30:
                self.page_count = page
                checkpoint_manager.update_search_progress(search_type, page, last_href, count)
                return results
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        grd_header = soup.find('tr', class_=['grdHeader'])
        header_vals = [a.get_text(strip=True) for a in grd_header.find_all('a')]
        
        rows = soup.find_all('tr', class_=['grdRow', 'grdRowAlt'])
        for row in rows:
            a_tag = row.find('a', id=True)
            if a_tag:
                a_href = a_tag.get('href')
                a_title = a_tag.get('title')
                last_href = a_href
            
            if 'Sealed' not in a_title:
                if DEBUGGING:
                    if count >= 30:
                        break
                
                try:
                    # Get the record data
                    record_data = self.record.get(a_href)
                    
                    # Handle different data structures...
                    if search_type == 'conviction' and isinstance(record_data, dict):
                        if 'case_details' in record_data:
                            results.append(record_data)
                        else:
                            results.append({
                                'case_details': record_data[0] if isinstance(record_data, tuple) else record_data,
                                'sentences': record_data[1] if isinstance(record_data, tuple) and len(record_data) > 1 else {},
                                'charges': []
                            })
                    else:
                        results.append(record_data)
                    
                    print(f'Added data for href {a_href}')
                    count += 1
                    
                except Exception as e:
                    log_issue(f"Error processing {a_href}: {str(e)}")
                    # Continue with next record instead of crashing
                    continue
            else:
                self.sealed_count += 1
        
        # Update checkpoint
        checkpoint_manager.update_search_progress(search_type, page, last_href, count)
        
        # Prepare for next page
        payload = self.page.get_payload('search', resp.text)
        span_tag = f'<span>{page}</span>'
        
        text = resp.text
        text = text[text.find(span_tag) + len(span_tag):]
        text = text[text.find(cph_body_id) + len(cph_body_id):]
        ctl_id = text[:text.find('&')]
        next_page = f'{cph_body_id}{ctl_id}'
        
        resp = ensure_connection(self.session.post, self.page.next_url[search_type],
                                 header | {'referer': self.page.next_url[search_type]},
                                 payload | {'__EVENTTARGET': next_page})
        self.last_resp = resp
        
        page += 1
        self.page_count = page
    
    # Mark as complete
    checkpoint_manager.mark_search_complete(search_type)
    return results
