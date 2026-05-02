import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# COMPOSER_ENVIRONMENT_NAME = "edw-prod"
# COMPOSER_LOCATION = "europe-west1"

CHROME_PROFILE_PATH = (
    r"C:\Users\punitkumar.more\AppData\Local\Google\Chrome\User Data\Profile 4"
)
CHROME_PROFILE_DIRECTORY = "Default"


def capture_dag_screenshot(dag_id_list):
    # airflow_uri = "https://0951be772db74b0ba516e1926f8dde94-dot-europe-west1.composer.googleusercontent.com"
    airflow_uri = "https://a49743f32e61434faf3083b22bd1960d-dot-europe-west1.composer.googleusercontent.com"
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
            wait_time = 50
            print(f"Waiting {wait_time} seconds for the page to render...")
            time.sleep(wait_time)
            output_png_file = rf"C:\Users\punitkumar.more\Documents\gcp_de\GCP_DE\SS\screenshot\{dag_id}.png"
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
        "wf_mipa_init",
        "wf_mipa_oe",
        "wf_mipa_replicator",
        "wf_MIPA_Product_to_SFDC",
        "wf_daily_load_MIPA_TASPAIK1_MITOS",
        "wf_mitos",
        "wf_mitos_init",
        "wf_mitos_order_init",
        "wf_tellusabp_init",
        "wf_tellus_oe",
        "wf_telluscrm_init",
        "wf_tellus_im_init",
        "wf_telluscrm_rdp",
        "wf_tellus_im",
        "wf_tellusabp_rdp",
        "wf_tellusabp_pwx",
        "wf_telluscrm_pwx",
        "wf_tellus_housekeeping"
        
    ]
    capture_dag_screenshot(dag_id_list=input_list)
