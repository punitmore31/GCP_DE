import os
import subprocess
import shutil # <-- ADDED: Required for copying files
from datetime import datetime
from google.cloud import bigquery
from github import Github, Auth
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. Configuration Variables
# ---------------------------------------------------------
TARGET_REPO_DIR = r"C:\path\to\your\cloned\repo" # Update this to your local Git path
TARGET_SQL_DIR = os.path.join(TARGET_REPO_DIR, "datasets", "qa", "views") 
MAIN_BRANCH = "dev"
REVIEWER = "prashanth-adepu-elisa"
# REPO_NAME = "your-org/your-repo-name"
REPO_NAME = "elisadatalake/cdw-asset-management-iac"

# BigQuery Metadata Table configuration
METADATA_PROJECT = "cdw-dev-6933"
METADATA_TABLE = f"{METADATA_PROJECT}.CDW_TEMP.DDL_CHECK_METADATA"

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------
def run_cmd(cmd, cwd=TARGET_REPO_DIR, ignore_errors=False):
    """Runs a shell command and optionally ignores linting exit codes."""
    print(f"⚙️ Running: {cmd}")
    result = subprocess.run(cmd, cwd=cwd, shell=True, text=True, capture_output=True)
    
    if result.returncode != 0 and not ignore_errors:
        print(f"❌ Error running '{cmd}':\n{result.stderr or result.stdout}")
        exit(1)
    return result.stdout.strip()

def main():
    load_dotenv()
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("CRITICAL: GITHUB_TOKEN environment variable not found.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    feature_branch = f"feature/auto-view-migration-{timestamp}"
    
    print("🚀 Starting BigQuery View Migration Pipeline...")

    # ---------------------------------------------------------
    # 2. Prepare the Git Repository (Create Feature Branch)
    # ---------------------------------------------------------
    print(f"🌿 Preparing branch {feature_branch}...")
    run_cmd("git fetch origin")
    run_cmd(f"git checkout {MAIN_BRANCH}")
    run_cmd(f"git pull origin {MAIN_BRANCH}")
    run_cmd(f"git checkout -b {feature_branch}")
    
    os.makedirs(TARGET_SQL_DIR, exist_ok=True)

    # ---------------------------------------------------------
    # 3. Read Metadata & Download Views from BigQuery
    # ---------------------------------------------------------
    print(f"🔍 Querying BigQuery Metadata Table: {METADATA_TABLE}...")
    bq_client = bigquery.Client()
    
    query = f"""
        SELECT PROJECT_ID, DATASET_ID, TABLE_NAME 
        FROM `{METADATA_TABLE}`
        WHERE TABLE_TYPE = 'view' 
          AND IS_CONVERTED = 'N'
    """
    query_job = bq_client.query(query)
    rows = list(query_job.result())
    
    if not rows:
        print("⚠️ No unconverted views found in metadata table. Exiting.")
        return

    processed_views = []

    for row in rows:
        project_id = row.PROJECT_ID
        dataset_id = row.DATASET_ID
        view_name = row.TABLE_NAME
        
        print(f"📥 Fetching DDL for: {project_id}.{dataset_id}.{view_name}")
        
        table_ref = bq_client.dataset(dataset_id, project=project_id).table(view_name)
        view_obj = bq_client.get_table(table_ref)
        view_sql = view_obj.view_query 
        
        if not view_sql:
            print(f"   ⏭️ Skipping {view_name}: No view definition found.")
            continue
            
        base_folder = r'C:\Users\punitkumar.more\Documents\gcp_de\GCP_DE'
        file_path = os.path.join(base_folder, f"{view_name}.sql")
        
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            f.write(view_sql)
            
        # ---------------------------------------------------------
        # 4. Run SQLFluff Commands on the Downloaded File
        # ---------------------------------------------------------
        print(f"🧹 Running SQLFluff on {view_name}.sql...")
        run_cmd(f"python -m sqlfluff format --disregard-sqlfluffignores {file_path}", ignore_errors=True)
        run_cmd(f"python -m sqlfluff fix --force --disregard-sqlfluffignores {file_path}", ignore_errors=True)
        run_cmd(f"python -m sqlfluff lint --disregard-sqlfluffignores {file_path}", ignore_errors=True)

        # ---------------------------------------------------------
        # 5. COPY TO GIT DIRECTORY (Added Step)
        # ---------------------------------------------------------
        target_file_path = os.path.join(TARGET_SQL_DIR, f"{view_name}.sql")
        print(f"📁 Copying {view_name}.sql to Git directory: {TARGET_SQL_DIR}")
        shutil.copy2(file_path, target_file_path)

        processed_views.append((project_id, dataset_id, view_name))

    # ---------------------------------------------------------
    # Update Metadata Table (IS_CONVERTED = 'Y')
    # ---------------------------------------------------------
    if processed_views:
        print(f"🔄 Updating {len(processed_views)} rows in metadata table to IS_CONVERTED = 'Y'...")
        where_clauses = [
            f"(PROJECT_ID = '{p}' AND DATASET_ID = '{d}' AND TABLE_NAME = '{t}')" 
            for p, d, t in processed_views
        ]
        
        update_query = f"""
            UPDATE `{METADATA_TABLE}`
            SET IS_CONVERTED = 'Y'
            WHERE {" OR ".join(where_clauses)}
        """
        update_job = bq_client.query(update_query)
        update_job.result() 
        print("✅ Metadata table updated successfully.")

    # ---------------------------------------------------------
    # 6. Commit and Push Changes
    # ---------------------------------------------------------
    print("💾 Committing formatted views to Git...")
    run_cmd(f"git add {TARGET_SQL_DIR}")
    
    # ADDED: Idempotency check. If no files changed, don't try to commit and push (which throws an error).
    diff_check = subprocess.run("git diff --staged --quiet", cwd=TARGET_REPO_DIR, shell=True)
    if diff_check.returncode == 0:
        print("⚠️ No valid changes detected in Git after formatting. Exiting without PR.")
        return

    run_cmd('git commit -m "feat(views): migrate and format BQ views from metadata trigger"')
    run_cmd(f"git push -u origin {feature_branch}")
    
    # ---------------------------------------------------------
    # 7. Create GitHub Pull Request
    # ---------------------------------------------------------
    print("📢 Authenticating with GitHub API...")
    auth = Auth.Token(github_token)
    g = Github(auth=auth)
    repo = g.get_repo(REPO_NAME)
    
    pr_title = f"🚀 Automated View Migration & Formatting: {datetime.now().strftime('%Y-%m-%d')}"
    pr_body = (
        f"This PR was generated automatically by the BigQuery Metadata pipeline.\n\n"
        f"**Details:**\n"
        f"- Processed {len(processed_views)} views from `{METADATA_TABLE}`.\n"
        f"- Applied `sqlfluff format` and `sqlfluff fix` to enforce styling guidelines.\n"
        f"- Please review the syntax changes before merging."
    )
    
    print("📝 Creating Pull Request...")
    pr = repo.create_pull(title=pr_title, body=pr_body, head=feature_branch, base=MAIN_BRANCH)
    print(f"✅ PR Created Successfully: {pr.html_url}")
    
    print(f"👀 Requesting review from @{REVIEWER}...")
    pr.create_review_request(reviewers=[REVIEWER])
    
    print("🎉 Pipeline Complete!")

if __name__ == "__main__":
    main()