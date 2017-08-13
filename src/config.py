import os


PACKAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PACKAGE_DIR, 'data')
HTML_DIR = os.path.join(DATA_DIR, 'html')
JSON_DIR = os.path.join(DATA_DIR, 'json')
ENTRY_DIR = os.path.join(DATA_DIR, 'entry')

for folder in [DATA_DIR, HTML_DIR, JSON_DIR, ENTRY_DIR]:
    if not os.path.exists(folder):
        os.mkdir(folder)
