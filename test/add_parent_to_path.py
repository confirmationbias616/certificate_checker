import os, sys, inspect

# The following 3 lines of code add parent directory to path so that the script can import
# from there
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
