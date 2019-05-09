import os

try:
    os.rename('test_cert_db', 'cert_db')
except:
    print('__init__.py ran... all good!')
    pass
