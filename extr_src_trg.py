import sqlglot
from sqlglot import exp


def extract_tables(sql_file_path):
    with open(sql_file_path, "r") as f:
        sql_content = f.read()

    sources = set()
    targets = set()

    # Parse the SQL content (handles multiple statements in one file)
    parsed = sqlglot.parse(sql_content)

    for expression in parsed:
        # 1. IDENTIFY TARGETS
        # Check for INSERT, CREATE, MERGE, UPDATE, DELETE
        if isinstance(expression, exp.Insert):
            targets.add(expression.this.sql())
        elif isinstance(expression, exp.Create):
            targets.add(expression.this.sql())
        elif isinstance(expression, exp.Merge):
            targets.add(expression.this.sql())
        elif isinstance(expression, exp.Update):
            targets.add(expression.this.sql())
        elif isinstance(expression, exp.Delete):
            targets.add(expression.this.sql())

        # 2. IDENTIFY SOURCES
        # Find all tables mentioned in the expression
        for table in expression.find_all(exp.Table):
            table_name = table.sql()

            # Filter out CTEs (Common Table Expressions)
            # If the table name is defined in a WITH clause, it's not a physical source table
            is_cte = False
            if expression.find(exp.With):
                for cte in expression.find(exp.With).find_all(exp.CTE):
                    if cte.alias == table_name:
                        is_cte = True
                        break

            # If it's not a target and not a CTE, it's likely a source
            if table_name not in targets and not is_cte:
                sources.add(table_name)

    return list(sources), list(targets)


# --- Test the script ---
if __name__ == "__main__":
    # Create a dummy SQL file for demonstration
    dummy_sql = """
    CREATE OR REPLACE TABLE my_project.target_dataset.final_table AS 
    SELECT 
        a.id, 
        b.salary 
    FROM my_project.source_dataset.employees a
    JOIN my_project.ref_dataset.departments b ON a.dept_id = b.id
    WHERE a.status = 'ACTIVE';

    INSERT INTO my_project.target_dataset.logs (message)
    SELECT 'Job Complete' FROM my_project.meta_dataset.dual;
    """

    with open("test_query.sql", "w") as f:
        f.write(dummy_sql)

    # Run extraction
    src, tgt = extract_tables("test_query.sql")

    print(f"{'=' * 10} RESULTS {'=' * 10}")
    print(f"TARGETS: {tgt}")
    print(f"SOURCES: {src}")
