import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# COMPOSER_ENVIRONMENT_NAME = "edw-prod"
# COMPOSER_LOCATION = "europe-west1"

CHROME_PROFILE_PATH = (
    r"C:\Users\punitkumar.more\AppData\Local\Google\Chrome\User Data\Profile 2"
)
CHROME_PROFILE_DIRECTORY = "Default"


def capture_dag_screenshot(dag_id_list):
    airflow_uri = "https://0951be772db74b0ba516e1926f8dde94-dot-europe-west1.composer.googleusercontent.com"
    if not airflow_uri:
        return
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={CHROME_PROFILE_PATH}")
    options.add_argument(f"--profile-directory={CHROME_PROFILE_DIRECTORY}")
    options.add_argument("--disable-gpu")
    service = ChromeService(ChromeDriverManager().install())
    driver = None

    try:
        print("🚀 Initializing Chrome WebDriver...")
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_window_size(1920, 1200)
        for dag_id in dag_id_list:
            target_url = f"{airflow_uri}/graph?dag_id={dag_id}"

            print(f"Navigating to DAG: {dag_id}...")
            print(f"URL: {target_url}")
            driver.get(target_url)
            wait_time = 5
            print(f"Waiting {wait_time} seconds for the page to render...")
            time.sleep(wait_time)
            output_png_file = rf"C:\Users\punitkumar.more\Documents\Elisa\gcp_de\AUTOMATION\output\screenshot\{dag_id}.png"
            print(f"📸 Capturing screenshot to '{output_png_file}'...")
            driver.save_screenshot(output_png_file)
            print("✅ Screenshot saved successfully!")

    except Exception as e:
        print(f"❌ An error occurred during automation: {e}")
    finally:
        if driver:
            print("Cleaning up and closing WebDriver.")
            driver.quit()


if __name__ == "__main__":
    # dag name list
    input_list = [
        "wf_ar1_to_kaiku_stg",
        "wf_base_to_kaiku_stg",
        "wf_device_to_kaiku_stg",
        # "wf_dynamo_kaiku_stg",
        # "wf_fira_to_kaiku_stg",
        # "wf_fixed_base_to_kaiku_stg",
        # "wf_initkaikumeta",
        # "wf_initpartyidlkp",
        # "wf_invoice_to_kaiku_stg",
        # "wf_invoicedata_tokaikustg",
        # "wf_jpd_to_kaiku_stg",
        # "wf_kaiku_datamart_report",
        # "wf_kaiku_telco_cloud",
        # "wf_mankeli_hierarchy_to_kaiku_stg",
        # "wf_mankeli_to_kaiku_stg",
        # "wf_nirap_tables_to_kaiku_stg",
        # "wf_odw_bustokaikustg",
        # "wf_odw_to_kaiku_stg",
        # "wf_prepaid",
        # "wf_rambo",
        # "wf_roaming",
        # "wf_sales_to_kaiku_stg",
        # "wf_salesforce_kaiku_stg",
        # "wf_sms_in_workaround",
        # "wf_terra_to_kaiku_stg",
        # "wf_usageic",
        # "wf_vas",
    ]
    capture_dag_screenshot(dag_id_list=input_list)
