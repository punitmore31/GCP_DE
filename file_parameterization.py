import os
import subprocess
import shutil

ROOT_DIR = r'C:\Users\punitkumar.more\Documents\Elisa\gcp_de\AUTOMATION\fdw-dev\fdw-dags\fdw\sql\KAIKU'
GIT_REPO = r"C:\Users\punitkumar.more\Documents\Elisa\fdw-dev\fdw-dags\fdw\sql\KAIKU"


COMMIT_MESSAGE = "Replacing hardcoded PROJECT_ID with PARAMETERIZED variable"
REPLACEMETN_STRING = {
    'dmgcp-del-170' : "{GCP_PROJECT_ID}"
}

def repalce_in_file(full_file_path, REPLACEMETN_STRING) : 
    print(full_file_path)
    print(REPLACEMETN_STRING)

    """ Reads a file replaces string based on a dictionary, and write it back"""
    try :
        with open(full_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            original_content = content

        for old_str, new_str in REPLACEMETN_STRING.items():
            content = content.replace(old_str,new_str)
            
        if content != original_content:
            print(f'Found and replace content in {full_file_path}')
            with open(full_file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
    except Exception as e:
        print(f"Error processing file {full_file_path}")
def move_modified_files(ROOT_DIR, GIT_REPO):
    print(f"ROOT_DIR : {ROOT_DIR}")
    print(f"GIT_REPO : {GIT_REPO}")
    try : 
        shutil.copytree(ROOT_DIR, GIT_REPO)
        print(f"Successfully copied {ROOT_DIR} TO {GIT_REPO}")
    except FileExistsError :
        print(f"Error ")


def git_commit_and_push(COMMIT_MESSAGE) :
    """Stages, Commit and pushes changes to Git Repository"""
    try :
        print("Starting git operations-----")
        print(f"1. Staging changes (git add .)....")
        subprocess.run(["git", "add",'.'], check=True)
        print(f'2. Commiting changes (git commit -m ) \"{COMMIT_MESSAGE}\"')
        subprocess.run(["git", "commit" "-m", COMMIT_MESSAGE], check=True)
        print(f'3. Pushing the changes to clone (git push).... ')
        #subprocess.run(["git","push"],check=True)

        print("Successfully commited and pushed changes to git")
    except subprocess.CalledProcessError as e:
        print(f"Error Occured during git operations")
    except FileNotFoundError:
        print("Git command not found. Is git installed in your system's path")


def main():
    """main fucntion to run replacement and git logic """
    print(f"start search in directory : {ROOT_DIR}")
    print(f"Applying the following replacement")

    for old, new in REPLACEMETN_STRING.items():
        print(f'Replacing {old} with {new}')

    file_changed_count = 0

    for root, dir, file_name in os.walk(ROOT_DIR):
        print(f'ROOT DIR : {root}')
        print(f'DIR : {dir}')
        print(f'files : {file_name}')
        print("--"*30)
        for files in file_name:
            if files.endswith(".sql"):
                full_file_path = os.path.join(root,files)
                print(f'Full file path : {full_file_path}')

                if repalce_in_file(full_file_path, REPLACEMETN_STRING):
                    file_changed_count += 1

    if file_changed_count > 0:
        print(f"print changed file count : {file_changed_count}")
    

    move_modified_files(full_file_path, GIT_REPO)
    git_commit_and_push(COMMIT_MESSAGE)

if __name__ == '__main__' :
    main()