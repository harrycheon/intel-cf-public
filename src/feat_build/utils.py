import os
from sqlalchemy.engine.url import URL
from pathlib import Path

# Directories
abspath = os.path.dirname(os.path.abspath(__file__))
src = Path(abspath).parent
root = src.parent
global_data = root / 'data'

# Database URL (removed for security purposes)
url = URL.create(
    drivername='', # indicate redshift_connector driver and dialect will be used
    host='', # Amazon Redshift host
    port=0, # Amazon Redshift port
    database='', # Amazon Redshift database
    username='', # Amazon Redshift username
    password='' # Amazon Redshift password
)

# Portable, Desktop Chassis Types
portable = ['Notebook', '2 in 1', 'Tablet']
desktop = ['Desktop', 'Server/WS', 'Intel NUC/STK']

# Table Names and File Names (Need to be in same order as raw_queries.sql)
table_names = [
    'sw_usage', # software usage
    'web_usage', # web usage
    'temp', # temperature
    'cpu_util', # cpu utilization
    'power', # power -> predictor variable
]

# Sysinfo Column List
sysinfo_cols = [
    'guid', 
    'countryname_normalized', 
    'modelvendor_normalized', 
    'ram', 
    'os', 
    '#ofcores', 
    'age_category', 
    'graphicsmanuf', 
    'cpu_family', 
    'cpu_suffix', 
    'screensize_category', 
    'persona'
]

# Device age category ordering (for OrdinalEncoder)
age_cat = [
    'Unknown', 
    '0-1 year', 
    '1-2 years', 
    '2-3 years', 
    '3-4 years', 
    '4-5 years', 
    '5-6 years', 
    '6+ years'
]

# Mapping from screensize category to ordinal number
# Note category with < or > are desktops, so they are mapped to -1
# Rest will be integer values in inches (i.e. strip x)
screensize_mapping = {
    '>24': -1,
    '<20': -1, 
    '<21': -1, 
    '>60': -1,
    'Unknown': -1
}
