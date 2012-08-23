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
MATCHES_TEMPLATE_FILE = os.path.abspath(os.path.join(TEMPLATE_DIR,"matches_tmpl.html"))
NO_MATCHES_TEMPLATE_FILE = os.path.abspath(os.path.join(TEMPLATE_DIR,"no_matches_tmpl.html"))
MATCHES_HTML_FILE = os.path.abspath(os.path.join(REPORTS_DIR,"matches.html"))
NO_MATCHES_HTML_FILE = os.path.abspath(os.path.join(REPORTS_DIR,"no_matches.html"))

DELETE_PDF_AFTER_EXTRACTION=True

SCORE_THRESHOLD = 90
COMMITEE_ID_BASE = 10000 # all ids  higher then this in identities.json identify committees, not persons
# for shorter runtime, if we only care about documents published since start of k18
# dd/mm/YYYY
#START_DATE = "24/02/2009"
START_DATE=None

NUMLINES_TO_PROCESS=1000 # process onl the first n lines of a file

