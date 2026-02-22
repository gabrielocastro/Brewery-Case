import os
import sys

# Simulating what documentation.py does
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(f"Computed BASE_DIR: {BASE_DIR}")

files_to_check = [
    os.path.join(BASE_DIR, "doc_en.md"),
    os.path.join(BASE_DIR, "doc_pt.md"),
    os.path.join(BASE_DIR, "Project_Documentation_EN.pdf"),
    os.path.join(BASE_DIR, "Project_Documentation_PT.pdf"),
]

for f in files_to_check:
    exists = os.path.exists(f)
    print(f"File {f} exists: {exists}")

# Check current directory contents
print(f"Contents of BASE_DIR ({BASE_DIR}):")
try:
    print(os.listdir(BASE_DIR))
except Exception as e:
    print(f"Error listing BASE_DIR: {e}")
