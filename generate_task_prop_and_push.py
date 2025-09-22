import os
import shutil
import subprocess

# import logging

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# f = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# fh = logging.FileHandler('generate_task_prop_and_push.log')
# fh.setformatter(f)
# logger.addHandler(fh)

# --- Configuration ---
INPUT_TABLE_LIST = [
    "MANKELI_OWNER.DIM_BUSINESS_PORTFOLIO_ACCOUNT",
    "MANKELI_OWNER.DIM_BUSINESS_PORTFOLIO_PL",
    "MANKELI_OWNER.DIM_PROFIT_LOSS_HEADER",
    "MANKELI_OWNER.DIM_PROFIT_LOSS_PARENT_CHILD",
    "MANKELI_OWNER.DIM_TELCO_DS",
    "MANKELI_OWNER.DIM_NEW_TABLE_FOR_TESTING" # Example of a new table
]

# Path where the files are initially generated
OUT_PATH = r'D:\python_project\output\history_task_props'

# --- NEW: Git Repository Configuration ---
# IMPORTANT: Update this path to your local Git repository clone
GIT_REPO_PATH = r'D:\python_project\fdw-qa\fdw-dags'
# IMPORTANT: Specify the subdirectory within the repo to place the files
GIT_TARGET_SUBDIR = r'fdw\config\task_config\MANKELI_HISTORY'

# --- Main Logic ---

# Ensure the output directory exists
os.makedirs(OUT_PATH, exist_ok=True)

newly_created_files = []

for full_table_name in INPUT_TABLE_LIST:
    db_name, tb_name = full_table_name.split(".")
    job_table_name = tb_name.lower().replace('_', '-')
    
    # Construct the final destination path inside the Git repository
    git_destination_dir = os.path.join(GIT_REPO_PATH, GIT_TARGET_SUBDIR)
    # print(git_destination_dir)
    final_file_name = f"oracle_{tb_name.lower()}_task_prop.yaml"
    final_file_path = os.path.join(git_destination_dir, final_file_name)

    # 1. Check if the file already exists in the destination Git repo
    if os.path.exists(final_file_path):
        print(f"Skipping: '{final_file_name}' already exists in the Git repository.")
        continue

    # If the file does not exist, generate it in the temporary output path
    output_content = f"""tasks:
  - task_name: oracle-{job_table_name}-load
    job_name: dif-{job_table_name}-load
    input_processor: RdbmsProcessor
    job_prop_file: gs://edw-airflow-{{ENV}}-dags/dags/fdw/config/job_config/jdbc1_job_prop.yaml
    source_db:
      query: SELECT * FROM {db_name}.{tb_name.upper()}
      schema: {db_name}
      rdbms_table: {tb_name.upper()}
      classpath_str: com.oracle.database.jdbc:ojdbc17:23.7.0.25.01
      driver_class_name: oracle.jdbc.driver.OracleDriver
    targets:
      - bigquery:
          target_table: "{{PROJECT_ID}}.FDW_HISTORICAL_STAGE.{tb_name.upper()}"
          write_disposition: WRITE_TRUNCATE
"""

    temp_file_path = os.path.join(OUT_PATH, final_file_name)
    
    print(f"Generating new file: {final_file_name}")
    with open(temp_file_path, "w", encoding="utf-8") as f:
        f.write(output_content)
    
    # Keep track of the newly created file's temporary path
    newly_created_files.append(temp_file_path)


# --- NEW: Move, Commit, and Push Logic ---
if not newly_created_files:
    print("\nNo new files were created. Nothing to commit.")
else:
    print(f"\nFound {len(newly_created_files)} new files to process.")
    
    try:
        # Move each new file to the Git repository directory
        for temp_path in newly_created_files:
            file_name = os.path.basename(temp_path)
            destination_path = os.path.join(GIT_REPO_PATH, git_destination_dir, file_name)
            print(f"Moving '{file_name}' to '{destination_path}'")
            shutil.move(temp_path, destination_path)

        # Run Git commands from within the repository directory
        print("\nAdding, committing, and pushing changes to the repository...")
        
        # Using subprocess.run is safer for executing external commands.
        # check=True will raise an exception if the command fails.
        subprocess.run(['git', 'add', '.'], cwd=GIT_REPO_PATH, check=True)
        
        commit_message = "Automated commit: Add new task property files"
        subprocess.run(['git', 'commit', '-m', commit_message], cwd=GIT_REPO_PATH, check=True)
        
        subprocess.run(['git', 'push'], cwd=GIT_REPO_PATH, check=True)
        
        print("\n✅ Git operations completed successfully.")

    # except FileNotFoundError:
    #     print(f"\n❌ ERROR: The Git repository path was not found: '{GIT_REPO_PATH}'")
    #     print("Please verify the 'GIT_REPO_PATH' variable in the script.")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ERROR: A Git command failed with exit code {e.returncode}.")
        print(f"Command: {' '.join(e.cmd)}")
        print("Please check your Git configuration, permissions, and repository status.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
