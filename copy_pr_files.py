import os
import shutil

SOURCE_BASE_DIR = r"D:\ELISA\fdw\fdw-dags"
DESTINATION_DIR = r"D:\ELISA\feature-interface-01\fdw-dags"

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