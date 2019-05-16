import os

try:
    os.rename('temp_cert_db', 'cert_db')
except:
    pass
