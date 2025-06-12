from datetime import datetime
from color_print import *

def log_issue(msg):
    ts = datetime.today().strftime('%m/%d/%Y %I:%M %p')
    
    cprint(msg, Fore.RED)
    with open('doc-issue.txt', 'a') as f:
        f.write(f'{ts}: {msg}\n')

def log_action(msg, display = True):
    ts = datetime.today().strftime('%m/%d/%Y %I:%M %p')

    if display:
        cprint(msg, Fore.GREEN)
        
    with open('doc-action.txt', 'a') as f:
        f.write(f'{ts}: {msg}\n')
