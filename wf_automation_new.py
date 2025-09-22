import os
import ast
import re
import shutil
import subprocess
from collections import defaultdict
from lxml import etree as ET

#=========================================================#
WF_NAMES = ["MANKELI_NS.WF_AP_JOIKU_MASTERDATA"]
domain = 'fdw'
#=========================================================#

curr_dir = os.path.join(os.path.dirname(__file__.replace("d:", "D:")))
input_dir = os.path.join(curr_dir, 'input')
output_dir = os.path.join(curr_dir, 'output')

temp_dir = os.path.join(output_dir, 'temp')
repo_structure_path = os.path.join(output_dir, 'repo_structure')
input_xml_path = os.path.join(input_dir, 'infa_xml')
converted_sql_path = os.path.join(input_dir, 'converted_sqls')

#=========================================================#

replacement_dict = {"$PMWorkflowLogDir": "/Mankeli/Logs",
"$PMSessionLogDir" : "/Mankeli/Logs",
"$PMSourceFileDir" : "/Mankeli/Anaplan/FromAnaplan",
"$PMTargetFileDir" : "/Mankeli/Anaplan/ToAnaplan",
"$PMLookupFileDir" : "/Mankeli/Anaplan/Mapping_tables",
"$PMBadFileDir" : "/Mankeli/Anaplan/BadFiles"}
#=========================================================#

class InformaticaXMLParser:
    """
    Analyzes an Informatica XML file to extract detailed information, including
    source-to-target lineage and the status of all workflow sessions.
    """

    def __init__(self, xml_file_path):
        """Initializes the parser and pre-caches key XML elements for performance."""
        if not os.path.exists(xml_file_path):
            raise FileNotFoundError(f"The file '{xml_file_path}' was not found.")
            
        try:
            self.tree = ET.parse(xml_file_path)
            self.root = self.tree.getroot()
        except Exception as e:
            raise ValueError(f"Failed to parse XML file: {e}")
            
        self.mappings = {m.get('NAME'): m for m in self.root.findall('.//MAPPING')}
        self.global_sources = {s.get('NAME'): s for s in self.root.findall('.//SOURCE')}
        self.global_targets = {t.get('NAME'): t for t in self.root.findall('.//TARGET')}
        
        self.mapping_cache = defaultdict(dict)
        for name, mapping_elem in self.mappings.items():
            self.mapping_cache[name]['instances'] = {
                i.get('NAME'): i for i in mapping_elem.findall('.//INSTANCE')
            }
    
    def _get_element_attributes(self, element):
        """Helper to convert an element's attributes to a dictionary."""
        return {k: v for k, v in element.attrib.items() if v}

    def _get_reader_details(self, session_element, mapping_name):
        """Extracts source details, including the SQL query for relational readers."""
        sources = {}
        mapping_instances = self.mapping_cache.get(mapping_name, {}).get('instances', {})
        mapping_element = self.mappings.get(mapping_name)

        for reader_ext in session_element.findall(".//SESSIONEXTENSION[@TYPE='READER']"):
            s_instance_name = reader_ext.get('SINSTANCENAME')
            
            source_instance_candidate = mapping_instances.get(s_instance_name)
            if source_instance_candidate is None: 
                continue
            
            source_instance = None
            if source_instance_candidate.get('TYPE') == 'SOURCE':
                source_instance = source_instance_candidate
            elif source_instance_candidate.get('TRANSFORMATION_TYPE') == 'Source Qualifier':
                assoc_inst_tag = source_instance_candidate.find('.//ASSOCIATED_SOURCE_INSTANCE')
                if assoc_inst_tag is not None:
                    source_instance = mapping_instances.get(assoc_inst_tag.get('NAME'))
            
            if source_instance is None: 
                continue

            source_name = source_instance.get('TRANSFORMATION_NAME')
            source_def = self.global_sources.get(source_name)
            if source_def is None: 
                continue

            if source_name not in sources:
                sources[source_name] = {}
            
            current_source_info = sources[source_name]
            
            current_source_info['type'] = reader_ext.get('SUBTYPE')
            current_source_info['source_name'] = source_name

            subtype = reader_ext.get('SUBTYPE')
            if subtype == 'File Reader':
                path_attr = reader_ext.find(".//ATTRIBUTE[@NAME='Source file directory']")
                if path_attr is not None: 
                    current_source_info['file_path'] = path_attr.get('VALUE')
                
                filename_attr = reader_ext.find(".//ATTRIBUTE[@NAME='Source filename']")
                if filename_attr is not None: 
                    current_source_info['filename'] = filename_attr.get('VALUE')
                
                flatfile_def = source_def.find('.//FLATFILE')
                if flatfile_def is not None and flatfile_def.get('DELIMITERS') is not None:
                    current_source_info['delimiter'] = flatfile_def.get('DELIMITERS').replace('&#x5c;', '\\')

                schema = []
                for field in source_def.findall('.//SOURCEFIELD'):
                    field_details = {
                        'name': field.get('NAME'),
                        'datatype': field.get('DATATYPE'),
                        'precision': field.get('PRECISION'),
                        'scale': field.get('SCALE')
                    }
                    schema.append({k: v for k, v in field_details.items() if v is not None})
                
                if schema:
                    current_source_info['schema'] = schema

            elif subtype == 'Relational Reader':
                current_source_info.update({
                    'database_type': source_def.get('DATABASETYPE'),
                    'database_name': source_def.get('DBDNAME')
                })
                conn_ref = reader_ext.find(".//CONNECTIONREFERENCE")
                if conn_ref is not None: 
                    current_source_info['connection_variable'] = conn_ref.get('VARIABLE')
                
                # Extract the SQL query from the Source Qualifier transformation
                if mapping_element is not None:
                    # 's_instance_name' is the name of the Source Qualifier Transformation instance.
                    sq_transformation = mapping_element.find(f".//TRANSFORMATION[@NAME='{s_instance_name}'][@TYPE='Source Qualifier']")
                    if sq_transformation is not None:
                        sql_query_attr = sq_transformation.find(".//TABLEATTRIBUTE[@NAME='Sql Query']")
                        if sql_query_attr is not None:
                            query = sql_query_attr.get('VALUE')
                            # A non-empty VALUE attribute indicates a SQL override is present.
                            if query: 
                                # Clean up common XML entities for readability.
                                query = query.replace('&#xD;&#xA;', '\n').replace('&apos;', "'").replace('\n\r', ' ').replace('\r', ' ').replace('\n', ' ')
                                current_source_info['sql_query'] = query

        return list(sources.values())

    def _get_writer_details(self, session_element, mapping_name):
        """Extracts target details using the cache."""
        targets = {}
        mapping_instances = self.mapping_cache.get(mapping_name, {}).get('instances', {})

        for writer_ext in session_element.findall(".//SESSIONEXTENSION[@TYPE='WRITER']"):
            s_instance_name = writer_ext.get('SINSTANCENAME')
            
            target_instance = mapping_instances.get(s_instance_name)
            if target_instance is None or target_instance.get('TYPE') != 'TARGET': 
                continue

            target_name = target_instance.get('TRANSFORMATION_NAME')
            target_def = self.global_targets.get(target_name)
            if target_def is None: 
                continue

            target_info = {'type': writer_ext.get('SUBTYPE'), 'target_name': target_name}

            if target_info['type'] == 'File Writer':
                path_attr = writer_ext.find(".//ATTRIBUTE[@NAME='Output file directory']")
                if path_attr is not None: 
                    target_info['file_path'] = path_attr.get('VALUE')

                filename_attr = writer_ext.find(".//ATTRIBUTE[@NAME='Output filename']")
                if filename_attr is not None: 
                    target_info['filename'] = filename_attr.get('VALUE')
                
            elif target_info['type'] == 'Relational Writer':
                target_info['database_type'] = target_def.get('DATABASETYPE')
                conn_ref = writer_ext.find(".//CONNECTIONREFERENCE")
                if conn_ref is not None: 
                    target_info['connection_variable'] = conn_ref.get('VARIABLE')
            
            targets[target_name] = target_info
        return list(targets.values())

    def _trace_source_to_target_lineage(self, mapping_element, mapping_cache):
        """
        Determines which targets are reachable from each source using an
        efficient reachability algorithm.
        """
        instances = mapping_cache['instances']
        graph = defaultdict(list)
        for conn in mapping_element.findall('.//CONNECTOR'):
            graph[conn.get('FROMINSTANCE')].append(conn.get('TOINSTANCE'))

        source_names = {name for name, inst in instances.items() if inst.get('TYPE') == 'SOURCE'}
        target_names = {name for name, inst in instances.items() if inst.get('TYPE') == 'TARGET'}
        
        lineage = defaultdict(list)
        for src in source_names:
            q = [src]
            reachable = {src}
            head = 0
            while head < len(q):
                node = q[head]
                head += 1
                for neighbor in graph.get(node, []):
                    if neighbor not in reachable:
                        reachable.add(neighbor)
                        q.append(neighbor)
            
            reachable_targets = reachable.intersection(target_names)
            if reachable_targets:
                source_instance = instances.get(src)
                if source_instance is not None:
                    src_trans_name = source_instance.get('TRANSFORMATION_NAME')
                    target_trans_names = [instances.get(t, {}).get('TRANSFORMATION_NAME') for t in sorted(list(reachable_targets))]
                    lineage[src_trans_name] = [name for name in target_trans_names if name]

        return lineage

    def _extract_lineage_for_session(self, session_element, status):
        """For a given session, finds its mapping, traces the lineage, and includes its status."""
        session_name = session_element.get('NAME')
        mapping_name = session_element.get('MAPPINGNAME')
        
        mapping_element = self.mappings.get(mapping_name)
        if mapping_element is None:
            return None
        
        sources_details = self._get_reader_details(session_element, mapping_name)
        targets_details = self._get_writer_details(session_element, mapping_name)
        lineage_map = self._trace_source_to_target_lineage(mapping_element, self.mapping_cache[mapping_name])
                                                    
        return {
            "session_name": session_name,
            "mapping_name": mapping_name,
            "status": status,
            "sources": sources_details,
            "targets": targets_details,
            "lineage": lineage_map
        }

    def _extract_workflow_flow(self, workflow_element):
        """
        Analyzes links to trace all execution paths, excluding those with disabled tasks.
        """
        def find_all_paths_recursive(graph, start, end, path=[]):
            path = path + [start]
            if start == end:
                return [path]
            if start not in graph:
                return []
            
            paths = []
            for node in graph[start]:
                if node not in path:
                    new_paths = find_all_paths_recursive(graph, node, end, path)
                    for new_path in new_paths:
                        paths.append(new_path)
            return paths
        
        task_instances = {ti.get('NAME'): ti for ti in workflow_element.findall('.//TASKINSTANCE')}
        tasks = set(task_instances.keys())
        disabled_tasks = {name for name, inst in task_instances.items() if inst.get('ISENABLED') == 'NO'}

        adj_list = defaultdict(list)
        from_nodes = set()
        to_nodes = set()

        for link in workflow_element.findall('.//WORKFLOWLINK'):
            from_task = link.get('FROMTASK')
            to_task = link.get('TOTASK')
            
            if from_task in tasks and to_task in tasks:
                adj_list[from_task].append(to_task)
                from_nodes.add(from_task)
                to_nodes.add(to_task)

        start_nodes = list(tasks - to_nodes)
        end_nodes = list(tasks - from_nodes)
        
        if 'Start' in start_nodes:
            start_nodes = ['Start']
        
        if not adj_list and tasks:
            return {"execution_paths": [[task] for task in sorted(list(tasks))]}
        if not end_nodes and from_nodes:
             return {"execution_paths": [], "notes": "Workflow may contain cycles or has no defined end tasks."}

        all_paths_raw = []
        for start_node in start_nodes:
            for end_node in end_nodes:
                paths = find_all_paths_recursive(adj_list, start_node, end_node)
                if paths:
                    all_paths_raw.extend(paths)
        
        # Filter out any path that contains a disabled task
        valid_paths = []
        for path in all_paths_raw:
            if not any(task in disabled_tasks for task in path):
                valid_paths.append(path)

        return {"execution_paths": valid_paths}

    def get_all_session_lineage(self):
        """
        Extracts all workflow, session, and lineage details, including a clear list
        of all execution paths and the status of each session.
        """
        output = {"workflows": []}      
        workflows = list(self.root.findall('.//WORKFLOW'))

        for workflow in workflows:
            workflow_details = self._get_element_attributes(workflow)
            flow_details = self._extract_workflow_flow(workflow)
            workflow_details["execution_paths"] = flow_details.get("execution_paths", [])
            
            task_instances = {ti.get('NAME'): ti for ti in workflow.findall('.//TASKINSTANCE')}
            linked_tasks = set()
            for link in workflow.findall('.//WORKFLOWLINK'):
                linked_tasks.add(link.get('FROMTASK'))
                linked_tasks.add(link.get('TOTASK'))

            workflow_details["sessions"] = []
            
            sessions = list(workflow.findall('.//SESSION'))
            for session in sessions:
                session_name = session.get('NAME')
                task_instance = task_instances.get(session_name)
                
                status = 'active'
                if task_instance is not None:
                    if task_instance.get('ISENABLED') == 'NO':
                        status = 'inactive'
                    elif session_name not in linked_tasks and 'Start' not in workflow_details.get("execution_paths", [[]])[0]:
                        is_in_path = False
                        for path in workflow_details.get("execution_paths", []):
                            if session_name in path:
                                is_in_path = True
                                break
                        if not is_in_path:
                            status = 'unlinked'
                
                session_lineage = self._extract_lineage_for_session(session, status)
                if session_lineage:
                    workflow_details["sessions"].append(session_lineage)
            
            if workflow_details["sessions"]:
                output["workflows"].append(workflow_details)
                
        return output

def get_list_variable(file_path: str, variable_name: str) -> list | None:
    try:
        with open(file_path, "r") as source:
            tree = ast.parse(source.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == variable_name:
                        return ast.literal_eval(node.value)
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_replaced_path(file_path, replacement_dict):
    """Replaces the first matching old value in file_path with a new value."""
    for old_val, new_val in replacement_dict.items():
        if old_val in file_path:
            return new_val
    return file_path

def get_ingestion_task_properties(domain, wf_name, file_name):
    file_name = file_name.split('.')[0]
    task_id = f"{file_name.upper()}_INGESTION"
    task_prop_file_name = f"{file_name.lower()}_task_prop.yaml"
    ingestion_task_cfg = f"""  - task_id: "{task_id}"
    job_type: INGESTION_TASK
    task_prop_file: gs://{{COMPOSER_BUCKET}}/dags/{domain}/config/task_config/{wf_name.split('.')[0].upper()}/{task_prop_file_name}
    job_name: dif-{file_name.replace('_', '-').lower()}-load
    upstream_dependency : "START"
"""
    return task_prop_file_name, task_id, ingestion_task_cfg

def get_sql_task_cfg(domain, wf_name:str, session_name, upstream_dependency, idx):
    task_id = f"{session_name.upper().replace('S_M_', '')}_SQL"
    sql_path = f"gs://{{COMPOSER_BUCKET}}/dags/{domain}/sql/{wf_name.upper().split('.')[0]}/{session_name.lower().replace('s_m_', '')}.sql"
    
    sql_task_cfg = f"""  - task_id : "{task_id}"
    job_type : SQL
    source : "{sql_path}"
    query_params : {{}}
    labels : {{}}
    upstream_dependency : "{upstream_dependency}"
"""
    return task_id, sql_task_cfg

def lowercase_all(root_dir: str):
    if os.name == 'nt' and not root_dir.startswith('\\\\?\\'):
        root_dir = '\\\\?\\' + os.path.abspath(root_dir)

    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        for filename in filenames:
            try:
                old_path = os.path.join(dirpath, filename)
                new_path = os.path.join(dirpath, filename.lower())
                if old_path != new_path:
                    os.rename(old_path, new_path)
            except OSError as e:
                print(f"Error renaming file {old_path}: {e}")

        for dirname in dirnames:
            try:
                old_path = os.path.join(dirpath, dirname)
                new_path = os.path.join(dirpath, dirname.lower())
                if old_path != new_path:
                    os.rename(old_path, new_path)
            except OSError as e:
                print(f"Error renaming directory {old_path}: {e}")

def find_sql_files(converted_sql_paths, wf_name, session_name):
    session_name = session_name.lower()
    wf_sql_path = os.path.join(converted_sql_paths, wf_name.lower().split('.')[0], wf_name.lower().split('.')[1], session_name)
    file_details = {} 
    for dirpath, _, filenames in os.walk(wf_sql_path):
        for filename in filenames:
            if filename.endswith('.sql') and 'ddl' not in filename.lower():
                file_details = {
                        'path': os.path.join(dirpath, filename),
                        'filename': filename
                    }
    return file_details

def find_ddl_files(converted_sql_paths, wf_name, session_name):
    session_name = session_name.lower()
    wf_sql_path = os.path.join(converted_sql_paths, wf_name.lower().split('.')[0], wf_name.lower().split('.')[1], session_name)
    file_details = {} 
    for dirpath, _, filenames in os.walk(wf_sql_path):
        for filename in filenames:
            if filename.endswith('.sql') and 'ddl' in filename.lower() and session_name.replace('s_', '', 1) in filename.lower():
                file_details = {
                        'path': os.path.join(dirpath, filename),
                        'filename': filename
                    }
    return file_details

def create_dag_config(domain, wf_name, yaml_data, last_task):
    dag_name = wf_name.split('.')[1].lower().replace('wf_', '')
    header = f"""app_nm : {domain.upper()}
dag_name : {dag_name}
stg_table: ""
schedule : ""
tasks:
"""

    start_task = """  - task_id : "START"
    job_type : DUMMY
    status : "PROCESSING"
    upstream_dependency : ""
"""

    end_task = f"""  - task_id : "COMPLETED"
    job_type : DUMMY
    status : "COMPLETED"
    upstream_dependency : "{last_task}"
"""
    audit_task = f"""  - task_id : "AUDIT_INSERT"
    job_type : SQL
    source : "INSERT INTO {domain.upper()}_AUDIT.JOB_AUDIT VALUES (GENERATE_UUID(), '{dag_name}', 'SUCCESS', CAST(FORMAT_DATE('%Y%m', CURRENT_DATE('Europe/Helsinki')) AS INT64), NULL, CURRENT_DATETIME('Europe/Helsinki'))"
    query_params : {{}}
    labels : {{}}
    upstream_dependency : "COMPLETED"
"""
    dag_element = wf_name.split('.')[0].upper() +"/" + dag_name
    return dag_element, f"{dag_name}.yaml", header + start_task + yaml_data + end_task + audit_task

def get_ingestion_task_config(domain:str, file_path:str, file_name:str, delimiter:str):
    task_name = f"{file_name.split('.')[0].lower().replace('_', '-')}-data-load"
    job_name = f"dif-{file_name.split('.')[0].lower().replace('_', '-')}-load"
    if delimiter == '\\011':
        delimiter = '\\t'
    task_prop_cfg = f"""tasks:
  - task_name: {task_name}
    job_name: {job_name}
    input_processor: CSVProcessor
    is_filename_req_in_target: True
    job_prop_file: gs://edw-airflow-{{ENV}}-dags/dags/{domain.lower()}/config/job_config/delimited_job_prop.yaml
    data_file: gs://{domain.lower()}-landing-{{ENV}}{file_path}/{file_name}
    archive_path: gs://{domain.lower()}-archive-{{ENV}}/Archive{file_path}
    delimited_file_props:
      delimiter: "{delimiter}"
      header_exists: True
      null_markers: ""
    targets:
      - bigquery:
          target_table: "{{PROJECT_ID}}.{domain.upper()}_TEMP.TEMP_{file_name.split('.')[0].upper()}"
          write_disposition: WRITE_TRUNCATE
    """
    return task_prop_cfg

def copy_and_rename_dirs(source_path, target_path, word):
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source path '{source_path}' does not exist.")
    if os.path.exists(target_path):
        raise FileExistsError(f"Target path '{target_path}' already exists.")

    shutil.copytree(source_path, target_path)

    for dirpath, dirnames, filenames in os.walk(target_path, topdown=False):
        for dirname in dirnames:
            if "domain" in dirname:
                old_dir_full_path = os.path.join(dirpath, dirname)
                new_dirname = dirname.replace("domain", word)
                new_dir_full_path = os.path.join(dirpath, new_dirname)
                os.rename(old_dir_full_path, new_dir_full_path)

def create_temp_repo_structure(temp_dir, repo_structure_path, domain, input_wf_name):
    wf_name = input_wf_name.split('.')[1].lower()
    dataset = input_wf_name.split('.')[0].upper()
    wf_temp_dir = os.path.join(temp_dir, wf_name)
    if os.path.exists(wf_temp_dir):
        shutil.rmtree(wf_temp_dir)
    
    copy_and_rename_dirs(repo_structure_path, wf_temp_dir, domain.lower())
    dag_configs_path = fr'{wf_temp_dir}\{domain.lower()}-dags\{domain.lower()}\config\dag_config\{dataset}'
    task_configs_path = fr'{wf_temp_dir}\{domain.lower()}-dags\{domain.lower()}\config\task_config\{dataset}'
    sql_files_path = fr'{wf_temp_dir}\{domain.lower()}-dags\{domain.lower()}\sql\{dataset}'
    ddl_files_path = fr'{wf_temp_dir}\{domain.lower()}-dags\{domain.lower()}\sql\ddl\{dataset}'
    if not os.path.exists(dag_configs_path):
        os.mkdir(dag_configs_path)
    if not os.path.exists(task_configs_path):
        os.mkdir(task_configs_path)
    if not os.path.exists(sql_files_path):
        os.mkdir(sql_files_path)
    if not os.path.exists(ddl_files_path):
        os.makedirs(ddl_files_path)
    return wf_temp_dir, dag_configs_path, task_configs_path, sql_files_path, ddl_files_path

def generate_temp_table_bq_ddl(table_name: str, schema: list, doamin: str):
    file_name = f"{table_name.lower()}_ddl.sql"
    if not schema:
        return "-- Error: Input schema is empty. Cannot generate DDL."

    column_definitions = []
    for field in schema:
        if not isinstance(field, dict) or 'name' not in field or 'datatype' not in field:
            continue            
        col_name = field.get('name')
        column_definitions.append(f"  {col_name} STRING")

    full_table_name = f"{{GCP_PROJECT_ID}}.{doamin.upper()}_TEMP.TEMP_{table_name}"
    cols = ',\n'.join(column_definitions)
    ddl = (
        f"CREATE TABLE {full_table_name}\n"
        "(\n"
        f"{cols}"
        "\n);"
    )    
    return (file_name, ddl)

def copy_and_overwrite_old(source_dir: str, dest_dir: str):
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    for dirpath, dirnames, filenames in os.walk(source_dir):
        relative_path = os.path.relpath(dirpath, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)        
        os.makedirs(dest_path, exist_ok=True)
        if not filenames:
            continue
        for filename in filenames:
            source_file = os.path.join(dirpath, filename)
            dest_file = os.path.join(dest_path, filename)
            shutil.copy2(source_file, dest_file)

def copy_and_overwrite(source_dir: str, dest_dir: str):
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    for dirpath, dirnames, filenames in os.walk(source_dir):
        relative_path = os.path.relpath(dirpath, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        os.makedirs(dest_path, exist_ok=True)
        if not filenames:
            continue
        for filename in filenames:
            source_file = os.path.join(dirpath, filename)
            dest_file = os.path.join(dest_path, filename)
            if filename.lower().endswith('.sql') and os.path.exists(dest_file):
                response = 'n'                
                if response in ('y', 'yes'):
                    print(f"    -> Overwriting '{dest_file}'...")
                    shutil.copy2(source_file, dest_file)
            else:
                shutil.copy2(source_file, dest_file)

def git_push_if_changes(repo_path: str):
    if not os.path.isdir(repo_path):
        raise FileNotFoundError(f"The specified repository path does not exist: {repo_path}")

    try:
        status_result = subprocess.run(
            ["git", "status", "--porcelain"], 
            cwd=repo_path, 
            check=True, 
            capture_output=True, 
            text=True
        )

        if not status_result.stdout:
            print("No changes to commit. Working tree clean.")
            return

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", "initial commit"], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(["git", "pull"], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(["git", "push"], cwd=repo_path, check=True, capture_output=True)

    except subprocess.CalledProcessError as e:
        print("An error occurred while executing a git command:")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Stderr: {e.stderr.decode()}")
        raise

def git_pull_before_changes(repo_path: str):
    if not os.path.isdir(repo_path):
        raise FileNotFoundError(f"The specified repository path does not exist: {repo_path}")

    try:
        status_result = subprocess.run(
            ["git", "status", "--porcelain"], 
            cwd=repo_path, 
            check=True, 
            capture_output=True, 
            text=True
        )

        if not status_result.stdout:
            print("No changes to commit. Working tree clean.")
            return
        subprocess.run(["git", "pull"], cwd=repo_path, check=True, capture_output=True)
        
    except subprocess.CalledProcessError as e:
        print("An error occurred while executing a git command:")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Stderr: {e.stderr.decode()}")
        raise

def process_sql_file_in_place(file_path: str, domain):
    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
        sql_no_comments = re.sub(r'--.*', '', sql_no_comments)

        cte_pattern = r'\b(\w+)\s+AS\s*\('
        ctes = set(re.findall(cte_pattern, sql_no_comments, re.IGNORECASE))
        
        cte_alias_pattern = r'\b(?:FROM|JOIN)\s+(\w+)\s+AS\s+(\w+)\b'
        for match in re.finditer(cte_alias_pattern, sql_no_comments, re.IGNORECASE):
            if match.group(1) in ctes:
                ctes.add(match.group(2))

        table_pattern = r'(?:FROM|JOIN|INSERT\s+INTO|DELETE\s+FROM)\s+([a-zA-Z0-9_`$.{}-]+)'
        potential_tables = re.findall(table_pattern, sql_no_comments, re.IGNORECASE)
        
        sql_keywords_to_exclude = {'LEFT', 'RIGHT', 'FULL', 'INNER', 'OUTER'}

        final_tables = {
            name.strip().replace('`', '')
            for name in potential_tables
        }
        
        all_tables = {
            table for table in final_tables
            if table not in ctes and table.upper() not in sql_keywords_to_exclude
        }
        
        dataset_resolve_dict = {
            "$$INTRP_SCHEMA": "MANKELI_OWNER", 
            "$$TERAP_SCHEMA": "MANKELI_OWNER",
            "TEST": f"{domain.upper()}_TEMP"
        }

        for table_id in all_tables:
            if table_id.count('.') == 2:
                project_id, dataset, table = table_id.split('.')
                n_project_id = "{GCP_PROJECT_ID}"
                n_dataset = dataset_resolve_dict.get(dataset, dataset)
                n_table = table
                new_table_id = f"{n_project_id}.{n_dataset}.{n_table}"
                sql_script = sql_script.replace(table_id, new_table_id)
        
        with open(file_path, 'w') as f:
            f.write(sql_script)

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def add_dag_element(file_path: str, dag_to_add: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file was not found at: {file_path}")

    with open(file_path, 'r') as f:
        lines = f.readlines()

    list_start_line = -1
    insertion_line = -1
    dag_found = False

    for i, line in enumerate(lines):
        if list_start_line == -1 and line.strip().startswith("DATA_PIPELINE_DAG_LIST"):
            list_start_line = i
            continue
        if list_start_line != -1:
            if re.search(f'["\']{re.escape(dag_to_add)}["\']', line):
                dag_found = True
                break            
            if ']' in line:
                insertion_line = i
                break
    if list_start_line == -1:
        raise ValueError("Could not find 'DATA_PIPELINE_DAG_LIST' in the file.")    
    if dag_found:
        return
    if insertion_line != -1:
        last_item_index = -1
        for i in range(insertion_line - 1, list_start_line, -1):
            if lines[i].strip():
                last_item_index = i
                break
        if last_item_index != -1:
            line_to_modify = lines[last_item_index].rstrip()
            if line_to_modify and not line_to_modify.endswith(','):
                lines[last_item_index] = line_to_modify + ',\n'
        indentation = '    ' 
        if last_item_index != -1:
            indentation = re.match(r'^\s*', lines[last_item_index]).group(0)
        else:
            base_indent = re.match(r'^\s*', lines[list_start_line]).group(0)
            indentation = base_indent + '    '

        new_line = f'{indentation}"{dag_to_add}",\n'
        lines.insert(insertion_line, new_line)

        with open(file_path, 'w') as f:
            f.writelines(lines)
    else:
        raise ValueError("Could not find the closing bracket for the list.")

def get_msssql_task_cfg(wf_name, domain, source_name):
    task_prop_file_name = f"{source_name.lower()}_task_prop.yaml"
    task_id = f"{source_name.upper()}_INGESTION"
    task_str = f"""  - task_id : "{task_id}"
    job_type : INGESTION
    job_name : df-mssql-mds-connectivity-load
    task_prop_file : gs://{{COMPOSER_BUCKET}}/dags/{domain.lower()}/config/task_config/{wf_name.upper().split('.')[0]}/{task_prop_file_name}
    upstream_dependency : "START"
    secret_uri: "projects/{{project_number}}/secrets/SQL_FINANCEMDS_RO_CREDENTIALS
"""
    return task_prop_file_name, task_id, task_str

def get_mssql_task_prop_cfg(source_name, ms_Sql_query):

    task_props = f"""tasks:
  - task_name: mariadb-connectivity-load
    job_name: dif-{source_name.lower().replace('_', '-')}-load
    input_processor: MariaSqlServerProcessor
    job_prop_file:  gs://edw-airflow-{{ENV}}-dags/dags/fdw/config/job_config/jdbc_job_prop.yaml
    source_db:
      query: {ms_Sql_query}
      schema: MDS
      rdbms_table: payment
      sslmode: encrypt=true;trustServerCertificate=true
      classpath_str: com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11
      driver_class_name: com.microsoft.sqlserver.jdbc.SQLServerDriver
    targets:
      - bigquery:
          target_table: "{{PROJECT_ID}}.FDW_TEMP.{source_name.upper()}"
          write_disposition: WRITE_TRUNCATE
"""
    return task_props

developed_workflows_paths = []
dag_elements = []
for idx, wf in enumerate(WF_NAMES):
    print(f"{idx+1}::{wf}")
    wf = wf.upper()
    yaml_data = ''  
    
    xml_file_name = f"{wf.replace('.', '_').lower()}.xml"
    xml_file_path = os.path.join(input_xml_path, xml_file_name)

    wf_temp_dir, dag_configs_path, task_configs_path, sql_files_path, ddl_files_path = create_temp_repo_structure(temp_dir, repo_structure_path, domain, wf)

    parser = InformaticaXMLParser(xml_file_path)
    all_details = parser.get_all_session_lineage()
    all_sessions = all_details['workflows'][0]['sessions']
    active_sessions = []
    
    for s in all_sessions:
        if s['status']=='active':
            active_sessions.append(s)
    execution_paths = all_details['workflows'][0]['execution_paths']
    file_details = dict({})
    session_names = []
    temp_table_ddls = []    
    all_ingestion_tasks = []

    for session in active_sessions:
        session_names.append(session['session_name'])
        for source in session.get('sources', []):
            if source.get('type') == 'File Reader':
                file_details[session['session_name']] = {
                "file_path": get_replaced_path(source['file_path'], replacement_dict),
                "file_name": source['filename'],
                "full_path": f"{get_replaced_path(source['file_path'], replacement_dict)}/{source['filename']}",
                "delimiter": source['delimiter']
                }   
                file_name = source['filename'].split('.')[0].upper()
                file_schema = source.get('schema')
                
                temp_table_ddls.append(generate_temp_table_bq_ddl(file_name, file_schema, domain))
            
            elif source.get('type') == 'Relational Reader' and  source.get("database_type") == 'Microsoft SQL Server':
                ms_sql_tbl = source.get("source_name")
                ms_sql_qry = source.get("sql_query")

                print(ms_sql_tbl)
                print(ms_sql_qry)
                task_prop_file_name, task_id, task_str = get_msssql_task_cfg(wf, domain, ms_sql_tbl)
                yaml_data += task_str
                task_props = get_mssql_task_prop_cfg(ms_sql_tbl, ms_sql_qry)
                task_prop_file_path = os.path.join(task_configs_path, task_prop_file_name)
                open(task_prop_file_path, 'w').write(task_props)
                all_ingestion_tasks.append(task_id)
            
    for temp_table in temp_table_ddls:
        temp_file_name = temp_table[0]
        temp_table_ddl = temp_table[1]
        temp_ddl_file_path = os.path.join(ddl_files_path, temp_file_name)
        open(temp_ddl_file_path, 'w').write(temp_table_ddl)

    c_up_tasks = []
    sql_upsteram_tasks = 'START'
    all_sql_tasks = {}
    for idx, exec_path in enumerate(execution_paths):
        
        if not len(execution_paths) > 1:
            idx = -1
        exec_path.remove('Start')
        path_ingestion_tasks = []
        
        for exec_session in exec_path:
            if  exec_session in  file_details.keys():          
                file_detail = file_details.get(exec_session)
                file_path, file_name, full_path, delimiter = file_detail['file_path'], file_detail['file_name'], file_detail['full_path'], file_detail['delimiter']

                task_prop_file_name, task_id, ingestion_task_cfg = get_ingestion_task_properties(domain, wf, file_name)
                if task_id in all_ingestion_tasks:
                    continue
                task_prop_cfg = get_ingestion_task_config(domain, file_path, file_name, delimiter)        
                open(os.path.join(task_configs_path, task_prop_file_name), 'w').write(task_prop_cfg)
                
                yaml_data += ingestion_task_cfg
                path_ingestion_tasks.append(task_id)
                all_ingestion_tasks.append(task_id)
        
        if path_ingestion_tasks:
            sql_upsteram_tasks = ','.join(path_ingestion_tasks)    

        
        prev_session_task_id = None
        for idx, session in enumerate(exec_path):
            if session in all_sql_tasks:
                prev_session_task_id = all_sql_tasks[session]
                continue
            if idx == 0:
                prev_session_task_id = sql_upsteram_tasks            
            sql_file_details =  find_sql_files(converted_sql_path, wf, session)
            sql_file_path, sql_file_name = sql_file_details['path'], sql_file_details['filename']
            ddl_file_path, ddl_file_name = sql_file_details['path'], sql_file_details['filename']
            try:
                session_final_sql = open(sql_file_path, 'r').read()
            except Exception:
                session_final_sql = open(fr"\\?\{sql_file_path}").read()
            final_sql_path = os.path.join(sql_files_path, f"{session.lower().replace('s_m_', '')}.sql")
            
            open(final_sql_path, 'w').write(session_final_sql)
            process_sql_file_in_place(final_sql_path, domain)
            task_id, sql_task_cfg = get_sql_task_cfg(domain, wf, session, prev_session_task_id, idx)
            
            prev_session_task_id = task_id
            all_sql_tasks[session]=task_id
            yaml_data += sql_task_cfg     
        
    for idx, exec_path in enumerate(execution_paths):
        c_up_tasks.append(all_sql_tasks[exec_path[-1]])
        
    dag_element, dag_config_file_name, dag_config = create_dag_config(domain, wf, yaml_data, ','.join(c_up_tasks))
    open(os.path.join(dag_configs_path, dag_config_file_name), 'w').write(dag_config)
    dag_elements.append(dag_element)
    developed_workflows_paths.append(os.path.join(wf_temp_dir, f'{domain}-dags'))

# push_to_repo = input("!! Push changes to repo (y/n) ::")
# if developed_workflows_paths and push_to_repo=='y':
#     repo_path = os.path.join(output_dir, 'repo_clones', domain, f"{domain}-dags")
#     for wf_path in developed_workflows_paths:
#         copy_and_overwrite(wf_path, repo_path)
#     dag_path = os.path.join(repo_path, domain, f'constants_wf_{domain.lower()}_dynamic_bus_dm_dag.py')
    
#     git_pull_before_changes(repo_path)
#     for dag_element in dag_elements:
#         add_dag_element(dag_path, dag_element)
#     git_push_if_changes(repo_path)