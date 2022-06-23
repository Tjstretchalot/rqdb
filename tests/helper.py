import os
import sys

curdir = os.getcwd()

if os.path.split(curdir)[1] == "tests":
    os.chdir(os.path.join(os.path.dirname(curdir), "src"))


if "." not in sys.path:
    sys.path.append(".")
