import os
from pathlib import Path
import shutil

SOURCE_BASED_PATH = r"C:\Users\punitkumar.more\Documents\Elisa\fdw-dev\fdw-dags"
DESTINATION_BASED_PATH = r"C:\Users\punitkumar.more\Documents\Elisa\gcp_de\AUTOMATION\fdw-dev\fdw-dags"

WORKFLOWS_TO_COPY = ['KAIKU.wf_InitPartyIdLkp','KAIKU.wf_dynamo_kaiku_stg']
print(WORKFLOWS_TO_COPY)

def find_case_insensitive_path(search_base_path, path_parts):
    CURRENT_PATH = search_base_path

    for parts in path_parts:
        sub_dirs = os.listdir(CURRENT_PATH)
        print(f"sub_dir : {sub_dirs}")
        for sub_dir in sub_dirs:
            if sub_dir.lower() == parts.lower():
                CURRENT_PATH = os.path.join(CURRENT_PATH,sub_dir)
                break
    return CURRENT_PATH

def copy_workflows():

    path_templates = [
        # Path("fdw") / "config" / "dag_config",
        # Path("fdw") / "config" / "task_config",
        # Path("fdw") / "sql"

        os.path.join("fdw","config","dag_config"),
        os.path.join("fdw","config","task_config"),
        os.path.join("fdw","sql")
    ]

    print(f"List of Path_templates : {path_templates} ")
    print(
        "---------------------------------------------------------------------------------------------------------------------------------"
    )

    for workflow_id in WORKFLOWS_TO_COPY:
        print(f"\nProcessing Wf : {workflow_id}")
        print("******" *18)
        try : 
            DIR_NAME , WF_NAME  = workflow_id.split('.', 1)
        except ValueError:
            print(f"❌ ERROR: Invalid format for '{workflow_id}'. Skipping.")
            continue
        FOUND_AND_COPY = False
        for template in path_templates:
            print(f"template : {template}")
            search_base_path = os.path.join(SOURCE_BASED_PATH, template)
            print(f"search_base_path : {search_base_path}")

            source_path = find_case_insensitive_path(search_base_path, [DIR_NAME,WF_NAME])
            print(f"source_path = {source_path}")

            if os.path.isdir(source_path):
                print("It's dorec")
                print(f"printing the source {source_path} and Destinatioon path is L {search_base_path}")
                relative_workflow_path = Path(source_path).relative_to(search_base_path)
                # 3. Construct the DESTINATION_PATH by joining the DESTINATION_BASED_PATH, template, and relative path
                print(f"relative_workflow_path :{relative_workflow_path}")
                DESTINATION_PATH = Path(DESTINATION_BASED_PATH) / template / relative_workflow_path
                # -------------------------------------------------------------------------------------------------------------
                # DESTINATION_PATH = Path(f"{DESTINATION_BASED_PATH}") / f"{template}" / ""
                print(f"Destination Path : {DESTINATION_PATH}")
                try : 
                    shutil.copytree(source_path, DESTINATION_PATH, dirs_exist_ok=True)
                    print(f"  ✅ Copied to: '{DESTINATION_PATH}'")
                except OSError as e:
                    print(f"❌Error while coping to destination")
                
                FOUND_AND_COPY = True

    if not FOUND_AND_COPY :
        print(f"No Matching Folder found for {workflow_id} in any of the configured path")
    print("-" * (len(workflow_id) + 20))
                

if __name__ == "__main__":
    print("--" * 20, "Code Started to Copy the folders from one directory to another", "--" * 20)
    copy_workflows()
    
