import re
from pathlib import Path

# 1. Define the exact path to your specific .sql file
FILE_PATH = Path(r"C:\Users\punitkumar.more\Documents\gcp_de\GCP_DE\ST_GENESYS_CONVERSATION_SESSION.sql")

# 2. Define the regex pattern to find "STRING(any_number)"
pattern = re.compile(r"STRING\(\d+\)", flags=re.IGNORECASE)

# 3. Check if the file exists before trying to open it
if FILE_PATH.exists() and FILE_PATH.is_file():
    
    # Read the current content of the file
    with open(FILE_PATH, "r", encoding="utf-8") as file:
        content = file.read()

    # Perform the find-and-replace
    new_content = pattern.sub("STRING", content)

    # If changes were made, write the new content back to the file
    if content != new_content:
        with open(FILE_PATH, "w", encoding="utf-8") as file:
            file.write(new_content)
        print(f"✅ Updated: {FILE_PATH.name}")
    else:
        print(f"⏭️ Skipped (no precision found): {FILE_PATH.name}")

else:
    print(f"❌ Error: Could not find the file at {FILE_PATH}")