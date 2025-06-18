from colorama    import init as colorama_init
from colorama    import Fore, Style
import sys

colorama_init()

def cprint(s, color):
    if 'idlelib' in sys.modules:
        print(s)
        return

    print(f'{color}{s}{Style.RESET_ALL}')

def cinput(s, color):
    if 'idlelib' in sys.modules:
        return input(s)

    print(f'{color}{s}{Style.RESET_ALL}', end = '')
    
    return input()
