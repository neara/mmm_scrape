import os

BASE_DIR=os.path.dirname(__file__)
REPORTS_DIR= os.path.abspath(os.path.join(BASE_DIR,"reports"))
DATA_OUTPUT_DIR= os.path.abspath(os.path.join(BASE_DIR,"output_data"))
TEMPLATE_DIR= os.path.abspath(os.path.join(BASE_DIR,"templates"))

COMMITEEJSONFILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"commitees.json"))
IDENTITIESYAMLFILE = os.path.abspath(os.path.join(BASE_DIR,"identities.yaml"))
DATADIR = os.path.abspath(os.path.join(BASE_DIR,'data/'))

LINKSFILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"mmm.json"))
MATCHESFILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"matches.json"))
COUNTS_CSVFILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"counts.csv"))
MATCHES_CSV_FILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"matches.csv"))

NOMATCHESFILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"no_match.json"))
DATE_TXT_FILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"dates.txt"))
TOPIC_TXT_FILE = os.path.abspath(os.path.join(DATA_OUTPUT_DIR,"topics.txt"))
MATCHES_TEMPLATE_FILE = os.path.abspath(os.path.join(TEMPLATE_DIR,"matches_tmpl.html"))
NO_MATCHES_TEMPLATE_FILE = os.path.abspath(os.path.join(TEMPLATE_DIR,"no_matches_tmpl.html"))
MATCHES_HTML_FILE = os.path.abspath(os.path.join(REPORTS_DIR,"matches.html"))
NO_MATCHES_HTML_FILE = os.path.abspath(os.path.join(REPORTS_DIR,"no_matches.html"))
