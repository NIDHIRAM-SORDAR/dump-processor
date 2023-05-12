import sys
from cx_Freeze import setup, Executable

setup(
    name="Down time calculator",
    version="1.0",
    description="Down time calculator for nokia system",
    executables=[Executable("downtime_cal.py", base="Console")]
)
