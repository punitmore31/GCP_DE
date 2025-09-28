import pandas as pd
import json
import os
import pprint as pp
import glob

schemas = json.load(open(r'C:\Users\punitkumar.more\Documents\Elisa\gcp_de\GCP_DE\AUTOMATION\db_schemas.json','r'))
# pp.pprint(schemas)
file_path_list = []
for i in glob.iglob('retail_db/*/*.txt', recursive=True):
    file_path_list.append(i)
    file_path_list.sort()
print(f'Extracted the File path : ')
pp.pprint(file_path_list)
print("****"*25)

def get_column_name(schemas,*db_name_list, sorting_key = 'column_position'):
    for i in db_name_list:
        db_name  = i
        print(f'DB_NAME : ',db_name)
        column_name_list = schemas.get(db_name)
        print(f'Columns name list of {db_name} : ')
        print(column_name_list)
        column_names = [col['column_name'] for col in sorted(column_name_list, key= lambda x : x[sorting_key], reverse=False)]
        print(f'{db_name} Column name : ',column_names)
        print('----------'*25)
        
        for file in file_path_list:
            df = pd.read_csv(file, names=column_names)
        pp.pprint(df.head(4))


db_name_list = [i for i in schemas.keys()]
db_name_list.sort()
print(f"Extracted the table name from JSON : ")
pp.pprint(db_name_list)
print('****'*25)

get_column_name(schemas, *db_name_list)

