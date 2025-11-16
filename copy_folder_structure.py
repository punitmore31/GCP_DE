
import os
def list_directory_structure(start_path):
    for root, dirs, files in os.walk(start_path):
        level = root.replace(start_path, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{subindent}{f}")





import os

def list_directory_structure(startpath, indent=0):
    """
    Print the directory structure starting from startpath.
    indent parameter controls the indentation level.
    """
    # Print the root directory
    print('|--' + os.path.basename(startpath))
    
    # Walk through the directory
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent_str = '|   ' * level + '|--'
        
        # Print current directory name
        folder_name = os.path.basename(root)
        if root != startpath:
            print(indent_str + folder_name)
        
        # Print all files in current directory
        indent_str = '|   ' * (level + 1) + '|--'
        for f in files:
            print(indent_str + f)

# Example usage
# Replace 'path/to/your/directory' with the actual path you want to scan
start_path = r'C:\Users\punitkumar.more\Documents\Elisa\gcp_de\AUTOMATION\retail_db'
list_directory_structure(start_path)




---------
copy pr files.py


import os
import shutil

SOURCE_BASE_DIR = r"C:\Users\punitkumar.more\Documents\Elisa\gcp_de\AUTOMATION\fdw-dev\fdw_dags"
DESTINATION_DIR = r"C:\Users\punitkumar.more\Documents\Elisa\feature_bq_view_cng\fdw-dags"

# List of workflow names to copy (case will be ignored)
WORKFLOWS_TO_COPY = [
    ]

def find_case_insensitive_path(base_path, path_parts):
    current_path = base_path
    if not os.path.isdir(current_path):
        return None

    for part in path_parts:
        found_next = False
        try:
            sub_dirs = os.listdir(current_path)
            for sub_dir in sub_dirs:
                if sub_dir.lower() == part.lower():
                    current_path = os.path.join(current_path, sub_dir)
                    found_next = True
                    break
        except OSError:
            return None            
        if not found_next:
            return None             
    return current_path


def copy_workflows():
    """
    Main function to parse workflow list and copy the directories.
    """
    print(f"Destination folder is: '{os.path.abspath(DESTINATION_DIR)}'\n")

    # Define the three directory structures to search within
    path_templates = [
        os.path.join('fdw', 'config', 'dag_config'),
        os.path.join('fdw', 'config', 'task_config'),
        os.path.join('fdw', 'sql')
    ]

    for workflow_id in WORKFLOWS_TO_COPY:
        print(f"--- Processing '{workflow_id}' ---")
        try:
            # Split "DATASET.wf_name" into ['DATASET', 'wf_name']
            dataset, wf_name = workflow_id.split('.', 1)
        except ValueError:
            print(f"❌ ERROR: Invalid format for '{workflow_id}'. Skipping.")
            continue

        found_and_copied = False
        for template in path_templates:
            base_search_path = os.path.join(SOURCE_BASE_DIR, template)
            
            # Find the full source path with the correct casing
            source_path = find_case_insensitive_path(base_search_path, [dataset, wf_name])

            if source_path and os.path.isdir(source_path):
                # Construct the destination path to mirror the source structure
                relative_path = os.path.relpath(source_path, SOURCE_BASE_DIR)
                destination_path = os.path.join(DESTINATION_DIR, relative_path)

                print(f"  -> Found: '{source_path}'")
                try:
                    # Copy the entire directory tree
                    shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
                    print(f"  ✅ Copied to: '{destination_path}'")
                    found_and_copied = True
                except OSError as e:
                    print(f"  ❌ ERROR copying to destination: {e}")

        if not found_and_copied:
            print(f"  -> No matching folders found for '{workflow_id}' in any of the configured paths.")
        print("-" * (len(workflow_id) + 20))


if __name__ == "__main__":
    copy_workflows()