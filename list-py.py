import os

url = 'https://raw.githubusercontent.com/xaltyd/crime-stats/refs/heads/main/'

for f in os.listdir():
    if f.endswith('.py'):
        print(f'{url}{f}')

input('\n\nFinished')
