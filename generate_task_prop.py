import os

###########################################################
input_table_list = ["MANKELI_OWNER.DIM_BUSINESS_PORTFOLIO_ACCOUNT","MANKELI_OWNER.DIM_BUSINESS_PORTFOLIO_PL","MANKELI_OWNER.DIM_PROFIT_LOSS_HEADER","MANKELI_OWNER.DIM_PROFIT_LOSS_PARENT_CHILD","MANKELI_OWNER.DIM_TELCO_DS"]

###########################################################
out_path = r'D:\python_project\output\history_task_props'
input_template = """tasks:
  - task_name: oracle-table-name-load
    job_name: dif-table-name-load
    input_processor: RdbmsProcessor
    job_prop_file: gs://edw-airflow-{ENV}-dags/dags/fdw/config/job_config/jdbc1_job_prop.yaml
    source_db:
      query: SELECT * FROM DB_NAME.TABLE_NAME
      schema: DB_NAME
      rdbms_table: TABLE_NAME
      classpath_str: com.oracle.database.jdbc:ojdbc17:23.7.0.25.01
      driver_class_name: oracle.jdbc.driver.OracleDriver
    targets:
      - bigquery:
          target_table: "{PROJECT_ID}.FDW_HISTORICAL_STAGE.TABLE_NAME"
          write_disposition: WRITE_TRUNCATE
"""


for tb in input_table_list:
    db_name = tb.split(".")[0]
    tb_name = tb.split(".")[1]

    out_template = input_template.replace("MANKELI_OWNER", db_name).replace("table-name", tb_name.lower().replace('_', '-')).replace("DB_NAME", db_name).replace("TABLE_NAME", tb_name.upper())

    print(out_template)
    out_file_path = os.path.join(out_path, f"oracle_{tb_name.lower()}_task_prop.yaml")
    with open(out_file_path, "w", encoding="utf-8") as f:
        f.write(out_template)
        


