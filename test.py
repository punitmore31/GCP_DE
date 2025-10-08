import os
import subprocess

# --- Configuration ---
# 1. Set the root directory to start searching from.
ROOT_DIRECTORY = "fdw/sql/"

# 2. Define the key-value pairs for replacement in a dictionary.
#    This makes it easy to add more replacements in the future.
REPLACEMENTS = {
    "dmgcp-del-170": "{GCP_PROJECT_ID}"
    # Example: "another-old-value": "another-new-value",
}

# 3. Define the Git commit message.
COMMIT_MESSAGE = "feat: Replace hardcoded project ID with template variable"


def replace_in_file(file_path, replacements_dict):
    """Reads a file, replaces strings based on a dictionary, and writes it back."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            original_content = content

        # Loop through the dictionary and apply each replacement
        for old_str, new_str in replacements_dict.items():
            content = content.replace(old_str, new_str)
        
        # Only write to the file if content has actually changed
        if content != original_content:
            print(f"  -> Found and replaced content in: {file_path}")
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(content)
            return True # Indicate that a change was made
            
    except Exception as e:
        print(f"  -> Error processing file {file_path}: {e}")
        
    return False # No change was made


def git_commit_and_push(commit_message):
    """Stages, commits, and pushes changes to the Git repository."""
    try:
        print("\n--- Starting Git operations ---")
        
        print("1. Staging changes (git add .)...")
        subprocess.run(["git", "add", "."], check=True)
        
        print(f"2. Committing changes (git commit -m \"{commit_message}\")...")
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        
        print("3. Pushing changes (git push)...")
        subprocess.run(["git", "push"], check=True)
        
        print("\n✅ Successfully committed and pushed changes to Git.")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ An error occurred during Git operations: {e}")
        print("Please check your Git status and repository configuration.")
    except FileNotFoundError:
        print("\n❌ Git command not found. Is Git installed and in your system's PATH?")


def main():
    """Main function to run the replacement and Git logic."""
    print(f"Starting search in directory: '{ROOT_DIRECTORY}'")
    print("Applying the following replacements:")
    for old, new in REPLACEMENTS.items():
        print(f"  - Replacing '{old}' with '{new}'")
    print() # Adds a newline for better formatting
    
    files_changed_count = 0
    
    # Walk through the directory tree
    for dirpath, _, filenames in os.walk(ROOT_DIRECTORY):
        for filename in filenames:
            if filename.endswith(".sql"):
                full_path = os.path.join(dirpath, filename)
                # Pass the entire replacements dictionary to the function
                if replace_in_file(full_path, REPLACEMENTS):
                    files_changed_count += 1

    print("\n--- Search and replace complete ---")
    
    # Only run Git commands if files were actually changed
    if files_changed_count > 0:
        print(f"Total files changed: {files_changed_count}")
        git_commit_and_push(COMMIT_MESSAGE)
    else:
        print("No files needed changes. Nothing to commit.")


if __name__ == "__main__":
    main()


-------------------------------------------------------

import shutil
import os

# Define the source and destination paths
# On Windows, you might use paths like: r'C:\Users\YourUser\Documents\ProjectA'
source_path = '/path/to/your/source_folder' 
destination_path = '/path/to/your/destination_folder'

# Check if the source directory exists before trying to move it
if not os.path.exists(source_path):
    print(f"Error: Source path '{source_path}' does not exist.")
else:
    try:
        # Move the source directory to the destination
        shutil.move(source_path, destination_path)
        print(f"Successfully moved '{source_path}' to '{destination_path}'")
    except shutil.Error as e:
        print(f"Error moving directory: {e}")
    except OSError as e:
        print(f"An OS error occurred: {e}")


----------------------------------------------------

import shutil
import os

source_dir = '/path/to/your/ProjectA'
destination_dir = '/path/to/backups/ProjectA_backup'

try:
    # Recursively copy the entire directory tree from source to destination
    shutil.copytree(source_dir, destination_dir)
    print(f"Successfully copied '{source_dir}' to '{destination_dir}'")
    
except FileExistsError:
    print(f"Error: Destination directory '{destination_dir}' already exists.")
except OSError as e:
    print(f"An OS error occurred: {e}")

    ------------------------------------------

    import re # Make sure to add 'import re' at the top of your script

def remove_sql_comments(file_path):
    """
    Reads an SQL file and removes block (/*...*/) and line (--) comments.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            original_content = content

        # 1. Remove multi-line block comments: /* ... */
        # The re.DOTALL flag is crucial for comments that span multiple lines.
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        # 2. Remove single-line ncomments: -- ...
        content = re.sub(r'--.*', '', content)
        
        # 3. Clean up extra empty lines that might result from comment removal
        # This joins non-empty lines back together.
        content = "\n".join(line for line in content.splitlines() if line.strip())

        # Only write back to the file if a change was made
        if content != original_content:
            print(f"  -> Removed comments from: {file_path}")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True # Indicate that a change was made

    except Exception as e:
        print(f"  -> An error occurred while removing comments from {file_path}: {e}")
    
    return False # No change was made


