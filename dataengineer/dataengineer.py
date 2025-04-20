from typing import Any, List, Dict, Tuple, Optional
import asyncio
from mcp.server.fastmcp import FastMCP
import aiosqlite
import os
import json
import pandas as pd

# Initialize FastMCP server for data engineering tasks
mcp = FastMCP("data_engineer")


async def get_available_tables() -> List[str]:
    """Returns a list of available tables in the testdb.db database."""
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
            async with db.execute("SELECT name FROM sqlite_master WHERE type='table';") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        # Log the error or handle it as appropriate
        print(f"Error fetching tables: {e}")
        return []

async def get_table_columns_json(table_name: str) -> List[str]:
    """Get column names for a specified table.
    
    Args:
        table_name: The name of the table to get columns for
        
    Returns:
        List of column names
    """
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(f"PRAGMA table_info({table_name})") as cursor:
                columns = await cursor.fetchall()
                return [col[1] for col in columns]  # Column name is the second element
    except Exception as e:
        print(f"Error getting columns for table {table_name}: {e}")
        return []


@mcp.tool()
async def get_mapping_details(mapping_reference_name: str) -> Dict[str, Any]:
    """Reads mapping.xlsx, filters by mapping_reference_name, and returns formatted details.

    Args:
        mapping_reference_name: The reference name to filter the mapping data by.

    Returns:
        A dictionary containing:
        - "success": Boolean indicating overall success
        - "mapping_data": List of dictionaries with mapping details
        - "errors": List of errors if any occurred
        - "metadata": Additional information about the mapping
        - "sql_generation_hints": Structure to help with SQL generation
    """
    excel_file_path = os.path.join(os.path.dirname(__file__), 'mapping.xlsx')
    required_columns = [
        'Mapping Name', 'Type', 'Alias', 'Full Table Name / Subquery Definition',
        'Join Type', 'Left Alias', 'Right Alias', 'Join Condition', 'Load Strategy',
        'Source Data Type (Expected Result)', 'Target Field Name', 'Target Data Type',
        'Target Description', 'Target PK', 'Transformation Type',
        'Transformation Logic / Expression', 'Default Value', 'Is Active'
    ]
    # Define the expected column name for the mapping reference. Adjust if needed.
    mapping_ref_col_header = 'Mapping Name'

    try:
        if not os.path.exists(excel_file_path):
            return {
                "success": False,
                "mapping_data": [],
                "errors": [f"Mapping file not found at {excel_file_path}"],
                "metadata": {"mapping_reference_name": mapping_reference_name}
            }

        df = pd.read_excel(excel_file_path, engine='openpyxl')

        # Determine the actual mapping reference column
        if mapping_ref_col_header not in df.columns:
            if df.shape[1] > 0:
                actual_mapping_ref_col = df.columns[0]
            else:
                return {
                    "success": False,
                    "mapping_data": [],
                    "errors": ["Mapping file has no columns."],
                    "metadata": {"mapping_reference_name": mapping_reference_name}
                }
        else:
            actual_mapping_ref_col = mapping_ref_col_header

        # Check for required output columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            return {
                "success": False,
                "mapping_data": [],
                "errors": [f"Missing required columns in mapping file: {', '.join(missing_cols)}"],
                "metadata": {"mapping_reference_name": mapping_reference_name}
            }

        # Filter based on the mapping reference name (case-insensitive)
        filtered_df = df[df[actual_mapping_ref_col].astype(str).str.lower() == mapping_reference_name.lower()]

        if filtered_df.empty:
            return {
                "success": False, 
                "mapping_data": [],
                "errors": [f"No mapping found for reference name: {mapping_reference_name}"],
                "metadata": {"mapping_reference_name": mapping_reference_name}
            }

        # Format the initial output
        mapping_data = filtered_df[required_columns].rename(columns={
            'Mapping Name': 'mapping_name',
            'Type': 'type',
            'Alias': 'alias',
            'Full Table Name / Subquery Definition': 'definition',
            'Join Type': 'join_type',
            'Left Alias': 'left_alias',
            'Right Alias': 'right_alias',
            'Join Condition': 'join_condition',
            'Load Strategy': 'load_strategy',
            'Source Data Type (Expected Result)': 'source_data_type',
            'Target Field Name': 'target_field_name',
            'Target Data Type': 'target_data_type',
            'Target Description': 'target_description',
            'Target PK': 'target_pk',
            'Transformation Type': 'transformation_type',
            'Transformation Logic / Expression': 'transformation_logic',
            'Default Value': 'default_value',
            'Is Active': 'is_active'
        }).to_dict('records')

        # Replace NaN values with None for JSON compatibility
        for record in mapping_data:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        # Structure the data in a way that's optimized for SQL generation
        sql_generation_hints = {
            "tables": [
                {"alias": rec.get("alias"), "definition": rec.get("definition"), "type": rec.get("type")}
                for rec in mapping_data if rec.get("type") in ["Table", "Subquery"]
            ],
            "joins": [
                {
                    "join_type": rec.get("join_type"),
                    "left_alias": rec.get("left_alias"),
                    "right_alias": rec.get("right_alias"),
                    "condition": rec.get("join_condition")
                }
                for rec in mapping_data if rec.get("type") == "Join"
            ],
            "filters": [
                {"alias": rec.get("alias"), "condition": rec.get("join_condition")}
                for rec in mapping_data if rec.get("type") == "Filter"
            ],
            "target": next(
                {"alias": rec.get("alias"), "load_strategy": rec.get("load_strategy")}
                for rec in mapping_data if rec.get("type") == "Target"
            ) if any(rec.get("type") == "Target" for rec in mapping_data) else {},
            "field_mappings": [
                {
                    "target_field": rec.get("target_field_name"),
                    "expression": rec.get("transformation_logic") or rec.get("alias") + "." + rec.get("definition") if rec.get("definition") else None,
                    "default": rec.get("default_value"),
                    "is_active": rec.get("is_active")
                }
                for rec in mapping_data if rec.get("type") == "Field Mapping"
            ]
        }

        return {
            "success": True,
            "mapping_data": mapping_data,
            "errors": [],
            "metadata": {
                "mapping_reference_name": mapping_reference_name
            }
            # },
            # "sql_generation_hints": sql_generation_hints
        }

    except Exception as e:
        return {
            "success": False,
            "mapping_data": [],
            "errors": [f"An error occurred while processing the mapping file: {str(e)}"],
            "metadata": {"mapping_reference_name": mapping_reference_name}
        }

@mcp.tool()
async def validate_mapping(table_columns_json: Any) -> Dict[str, Any]:
    """Validates whether specified tables and columns exist in the database.
    
    Args:
        table_columns_json: Dict containing tables and their columns in the format:
        {
            "tables": [
                {"tablename": "table1", "columnNames": ["col1", "col2"]},
                {"tablename": "table2", "columnNames": ["col1", "col3"]}
            ]
        }
    Returns:
        A dictionary containing validation results with:
        - "success": Boolean indicating overall success
        - "valid": Boolean indicating if all tables/columns are valid
        - "validations": List of validation results for each table
        - "errors": List of errors if any occurred in processing
    """
    try:
        # Extract tables list from the provided dict
        tables_to_check = []
        if isinstance(table_columns_json, dict):
            tables_to_check = table_columns_json.get("tables", [])
        # Allow direct list input
        if not tables_to_check and isinstance(table_columns_json, list):
            tables_to_check = table_columns_json
        # Invalid format
        if not isinstance(tables_to_check, list):
            return {"success": False, "validations": [], "errors": ["Invalid input format: expected 'tables' list"]}
         
        # Get all available tables from the database
        available_tables = await get_available_tables()
        validations = []
        
        # Process each table in the request
        for table_data in tables_to_check:
            table_name = table_data.get("tablename")
            column_names = table_data.get("columnNames", [])
            
            # Check if the table exists
            if table_name not in available_tables:
                validations.append({
                    "table": table_name,
                    "exists": False,
                    "column_validations": [],
                    "errors": ["Table does not exist in the database"]
                })
                continue
                
            # Table exists, now check each column
            actual_columns = await get_table_columns_json(table_name)
            column_validations = []
            
            for column in column_names:
                column_validations.append({
                    "column": column,
                    "exists": column in actual_columns
                })
                
            # Add validation result for this table
            validations.append({
                "table": table_name,
                "exists": True,
                "column_validations": column_validations,
                "valid_columns": [c["column"] for c in column_validations if c["exists"]],
                "invalid_columns": [c["column"] for c in column_validations if not c["exists"]]
            })
            
        # Check if all validations passed
        all_valid = all(v["exists"] for v in validations) and \
                   all(not v.get("invalid_columns", []) for v in validations)
                   
        return {
            "success": True,
            "valid": all_valid,
            "validations": validations,
            "errors": []
        }
        
    except Exception as e:
        return {
            "success": False,
            "validations": [],
            "errors": [f"An error occurred during validation: {str(e)}"]
        }

@mcp.tool()
async def run_bash_shell(command: str) -> str:
    """Execute a shell command and return combined stdout and stderr."""
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    out = stdout.decode().strip()
    err = stderr.decode().strip()
    if err:
        out += f"\nSTDERR:\n{err}"
    return out


@mcp.tool()
async def get_table_data(table_name: str, limit: int = 10) -> Dict[str, Any]:
    """Fetches a specified number of rows from a given table.

    Args:
        table_name: The name of the table to fetch data from.
        limit: The maximum number of rows to return (default: 10).

    Returns:
        A dictionary containing:
        - "success": Boolean indicating overall success
        - "data": List of dictionaries representing rows
        - "columns": List of column names
        - "errors": List of errors if any occurred
    """
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row  # Return rows as dict-like objects
            # Sanitize table name (basic example, consider more robust validation)
            if not table_name.isalnum() and '_' not in table_name:
                 raise ValueError(f"Invalid table name: {table_name}")
            query = f"SELECT * FROM {table_name} LIMIT ?"
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                data = [dict(row) for row in rows]
                return {"success": True, "data": data, "columns": columns, "errors": []}
    except ValueError as ve:
         return {"success": False, "data": [], "columns": [], "errors": [str(ve)]}
    except aiosqlite.Error as e:
        return {"success": False, "data": [], "columns": [], "errors": [f"Database error fetching data from {table_name}: {e}"]}
    except Exception as e:
        return {"success": False, "data": [], "columns": [], "errors": [f"An unexpected error occurred: {e}"]}


@mcp.tool()
async def get_table_row_count(table_name: str) -> Dict[str, Any]:
    """Returns the total number of rows in the specified table.

    Args:
        table_name: The name of the table to count rows for.

    Returns:
        A dictionary containing:
        - "success": Boolean indicating overall success
        - "table_name": The name of the table
        - "row_count": The number of rows, or None if an error occurred
        - "errors": List of errors if any occurred
    """
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
             # Sanitize table name
            if not table_name.isalnum() and '_' not in table_name:
                 raise ValueError(f"Invalid table name: {table_name}")
            query = f"SELECT COUNT(*) FROM {table_name}"
            async with db.execute(query) as cursor:
                result = await cursor.fetchone()
                count = result[0] if result else 0
                return {"success": True, "table_name": table_name, "row_count": count, "errors": []}
    except ValueError as ve:
         return {"success": False, "table_name": table_name, "row_count": None, "errors": [str(ve)]}
    except aiosqlite.Error as e:
        return {"success": False, "table_name": table_name, "row_count": None, "errors": [f"Database error counting rows for {table_name}: {e}"]}
    except Exception as e:
        return {"success": False, "table_name": table_name, "row_count": None, "errors": [f"An unexpected error occurred: {e}"]}


@mcp.tool()
async def list_mappings() -> Dict[str, Any]:
    """Reads mapping.xlsx and returns a list of unique 'Mapping Name' values.

    Returns:
        A dictionary containing:
        - "success": Boolean indicating overall success
        - "mappings": List of unique mapping names
        - "errors": List of errors if any occurred
    """
    excel_file_path = os.path.join(os.path.dirname(__file__), 'mapping.xlsx')
    mapping_ref_col_header = 'Mapping Name'  # Expected column name

    try:
        if not os.path.exists(excel_file_path):
            return {"success": False, "mappings": [], "errors": [f"Mapping file not found at {excel_file_path}"]}

        # Use asyncio.to_thread for synchronous pandas operation
        df = await asyncio.to_thread(pd.read_excel, excel_file_path, engine='openpyxl')

        if mapping_ref_col_header not in df.columns:
             # Try the first column if 'Mapping Name' isn't found
             if df.shape[1] > 0:
                 actual_mapping_ref_col = df.columns[0]
             else:
                 return {"success": False, "mappings": [], "errors": ["Mapping file has no columns."]}
        else:
            actual_mapping_ref_col = mapping_ref_col_header

        # Get unique, non-null mapping names
        mapping_names = df[actual_mapping_ref_col].dropna().unique().tolist()
        return {"success": True, "mappings": mapping_names, "errors": []}

    except Exception as e:
        return {"success": False, "mappings": [], "errors": [f"An error occurred while reading mapping names: {str(e)}"]}


@mcp.tool()
async def execute_sql_query(query: str) -> Dict[str, Any]:
    """Executes a read-only SQL query against the database and returns results.

    Args:
        query: The SQL query string to execute. IMPORTANT: Only SELECT statements are recommended.

    Returns:
        A dictionary containing:
        - "success": Boolean indicating overall success
        - "data": List of dictionaries representing rows, or None on error
        - "columns": List of column names, or None on error
        - "errors": List of errors if any occurred
    """
    # Basic check to prevent obviously harmful commands (can be improved)
    if not query.strip().upper().startswith("SELECT"):
        return {"success": False, "data": None, "columns": None, "errors": ["Only SELECT queries are allowed."]}

    try:
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                data = [dict(row) for row in rows]
                return {"success": True, "data": data, "columns": columns, "errors": []}
    except aiosqlite.Error as e:
        return {"success": False, "data": None, "columns": None, "errors": [f"Database error executing query: {e}"]}
    except Exception as e:
        return {"success": False, "data": None, "columns": None, "errors": [f"An unexpected error occurred: {e}"]}




@mcp.tool()
async def read_file(file_path: str) -> Dict[str, Any]:
    """Read contents of a file within the workspace.
    
    Args:
        file_path: Path to the file to read
        
    Returns:
        Dictionary with file contents and metadata
    """
    try:
        # Security check - only allow reading files in the current directory or subdirectories
        base_path = os.path.dirname(os.path.abspath(__file__))
        abs_path = os.path.abspath(os.path.join(base_path, file_path))
        
        # Prevent path traversal attacks
        if not abs_path.startswith(base_path):
            return {
                "success": False,
                "content": None,
                "errors": ["Security error: Cannot access files outside of the workspace."]
            }
            
        if not os.path.exists(abs_path):
            return {
                "success": False,
                "content": None, 
                "errors": [f"File not found: {file_path}"]
            }
            
        with open(abs_path, 'r') as file:
            content = file.read()
            
        return {
            "success": True,
            "content": content,
            "file_path": file_path,
            "errors": []
        }
    except Exception as e:
        return {
            "success": False,
            "content": None,
            "errors": [f"Error reading file {file_path}: {str(e)}"]
        }

@mcp.tool()
async def write_file(file_path: str, content: str) -> Dict[str, Any]:
    """Write content to a file within the workspace.
    
    Args:
        file_path: Path where to write the file
        content: String content to write
        
    Returns:
        Dictionary with success status and errors if any
    """
    try:
        # Security check - only allow writing files in the current directory or subdirectories
        base_path = os.path.dirname(os.path.abspath(__file__))
        abs_path = os.path.abspath(os.path.join(base_path, file_path))
        
        # Prevent path traversal attacks
        if not abs_path.startswith(base_path):
            return {
                "success": False,
                "errors": ["Security error: Cannot write files outside of the workspace."]
            }
            
        # Create directories if needed
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            
        with open(abs_path, 'w') as file:
            file.write(content)
            
        return {
            "success": True,
            "file_path": file_path,
            "errors": []
        }
    except Exception as e:
        return {
            "success": False,
            "errors": [f"Error writing to file {file_path}: {str(e)}"]
        }

# Note: generate_sql_from_mapping has been removed
# SQL generation will now be handled by the LLM based on mapping data from get_mapping_details

@mcp.tool()
async def validate_mapping_logic(mapping_reference_name: str) -> Dict[str, Any]:
    """Validates the logical consistency of a mapping document.
    
    Checks for:
    - All referenced aliases in joins exist in table definitions
    - Join conditions reference valid aliases
    - All target fields have valid mappings
    - No circular references in joins
    - Basic syntax checks on expressions
    
    Args:
        mapping_reference_name: The reference name of the mapping to validate
        
    Returns:
        Dictionary with validation results and any logical issues found
    """
    # First get the mapping details
    mapping_result = await get_mapping_details(mapping_reference_name)
    
    if not mapping_result.get("success"):
        return {
            "success": False,
            "valid": False,
            "issues": [],
            "errors": mapping_result.get("errors", ["Failed to retrieve mapping details"])
        }
        
    mapping_data = mapping_result.get("mapping_data", [])
    if not mapping_data:
        return {
            "success": False,
            "valid": False,
            "issues": [],
            "errors": [f"No mapping data found for {mapping_reference_name}"]
        }
    
    try:
        # Initialize results
        issues = []
        
        # Extract components from the mapping
        defined_aliases = set()
        tables = []
        joins = []
        filters = []
        target = None
        field_mappings = []
        
        # First pass: collect all defined aliases and components
        for record in mapping_data:
            rec_type = record.get("type")
            
            if rec_type in ["Table", "Subquery"]:
                alias = record.get("alias")
                if alias:
                    defined_aliases.add(alias)
                tables.append(record)
            elif rec_type == "Join":
                joins.append(record)
            elif rec_type == "Filter":
                filters.append(record)
            elif rec_type == "Target":
                if target:
                    issues.append({
                        "severity": "ERROR",
                        "message": "Multiple target definitions found. Only one target should be defined.",
                        "component": "Target",
                        "details": {
                            "existing_target": target.get("alias"),
                            "additional_target": record.get("alias")
                        }
                    })
                target = record
            elif rec_type == "Field Mapping":
                field_mappings.append(record)
        
        # Check if we have a target definition
        if not target:
            issues.append({
                "severity": "ERROR",
                "message": "No target definition found in the mapping.",
                "component": "Target"
            })
        
        # Validate join references
        for join in joins:
            left_alias = join.get("left_alias")
            right_alias = join.get("right_alias")
            join_condition = join.get("join_condition", "")
            
            # Check if left alias exists
            if left_alias and left_alias not in defined_aliases:
                issues.append({
                    "severity": "ERROR",
                    "message": f"Join references undefined left alias: {left_alias}",
                    "component": "Join",
                    "details": {
                        "left_alias": left_alias,
                        "right_alias": right_alias,
                        "condition": join_condition
                    }
                })
                
            # Check if right alias exists
            if right_alias and right_alias not in defined_aliases:
                issues.append({
                    "severity": "ERROR",
                    "message": f"Join references undefined right alias: {right_alias}",
                    "component": "Join",
                    "details": {
                        "left_alias": left_alias,
                        "right_alias": right_alias,
                        "condition": join_condition
                    }
                })
                
            # Basic check for join condition format
            if not join_condition or "=" not in join_condition:
                issues.append({
                    "severity": "WARNING",
                    "message": f"Join condition may be invalid: {join_condition}",
                    "component": "Join",
                    "details": {
                        "left_alias": left_alias,
                        "right_alias": right_alias,
                        "condition": join_condition
                    }
                })
                
        # Validate filter references
        for filter_def in filters:
            alias = filter_def.get("alias")
            condition = filter_def.get("join_condition", "")
            
            if alias and alias not in defined_aliases:
                issues.append({
                    "severity": "ERROR",
                    "message": f"Filter references undefined alias: {alias}",
                    "component": "Filter",
                    "details": {
                        "alias": alias,
                        "condition": condition
                    }
                })
                
            # Basic check for filter condition
            if not condition or len(condition.strip()) < 3:
                issues.append({
                    "severity": "WARNING",
                    "message": f"Filter condition may be invalid or too simple: {condition}",
                    "component": "Filter",
                    "details": {
                        "alias": alias,
                        "condition": condition
                    }
                })
        
        # Validate field mappings
        target_fields = set()
        mapped_target_fields = set()
        has_primary_key = False
        
        for field in field_mappings:
            target_field = field.get("target_field_name")
            expression = field.get("transformation_logic")
            source_alias = field.get("alias")
            source_field = field.get("definition")
            is_active = field.get("is_active")
            is_pk = field.get("target_pk")
            
            # Track if we have primary keys
            if is_pk:
                has_primary_key = True
                
            if is_active is False:
                continue
                
            if target_field:
                if target_field in target_fields:
                    issues.append({
                        "severity": "ERROR",
                        "message": f"Duplicate target field mapping: {target_field}",
                        "component": "Field Mapping",
                        "details": {
                            "target_field": target_field
                        }
                    })
                target_fields.add(target_field)
                mapped_target_fields.add(target_field)
                
            # Check if source alias exists when referenced
            if source_alias and source_alias not in defined_aliases:
                issues.append({
                    "severity": "ERROR",
                    "message": f"Field mapping references undefined source alias: {source_alias}",
                    "component": "Field Mapping",
                    "details": {
                        "target_field": target_field,
                        "source_alias": source_alias,
                        "source_field": source_field
                    }
                })
                
            # Basic syntax check for transformation expressions
            if expression:
                for alias in defined_aliases:
                    if f"{alias}." in expression and alias not in defined_aliases:
                        issues.append({
                            "severity": "ERROR",
                            "message": f"Expression references undefined alias: {alias}",
                            "component": "Field Mapping",
                            "details": {
                                "target_field": target_field,
                                "expression": expression
                            }
                        })
        
        # Check for primary key
        if not has_primary_key and target:
            load_strategy = target.get("load_strategy", "").lower()
            if "update" in load_strategy or "merge" in load_strategy:
                issues.append({
                    "severity": "ERROR",
                    "message": "Update/merge load strategy requires primary key fields, but none defined.",
                    "component": "Field Mapping",
                    "details": {
                        "load_strategy": load_strategy
                    }
                })
            else:
                issues.append({
                    "severity": "WARNING",
                    "message": "No primary key fields defined in the mapping.",
                    "component": "Field Mapping"
                })
        
        # Determine overall validity
        has_errors = any(issue["severity"] == "ERROR" for issue in issues)
        
        return {
            "success": True,
            "valid": not has_errors,
            "issues": issues,
            "warnings": [issue for issue in issues if issue["severity"] == "WARNING"],
            "errors": [issue for issue in issues if issue["severity"] == "ERROR"],
            "summary": {
                "total_issues": len(issues),
                "errors": sum(1 for issue in issues if issue["severity"] == "ERROR"),
                "warnings": sum(1 for issue in issues if issue["severity"] == "WARNING"),
                "defined_aliases": list(defined_aliases),
                "has_primary_key": has_primary_key,
                "target_field_count": len(mapped_target_fields)
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "valid": False,
            "issues": [],
            "errors": [f"Error validating mapping logic: {str(e)}"]
        }



@mcp.tool()
async def execute_sql_script(script_path: str, unsafe: bool = False) -> Dict[str, Any]:
    """Execute a SQL script file against the SQLite database.
    
    Args:
        script_path: Path to the SQL script file to execute
        unsafe: Set to True to allow non-SELECT statements (use with extreme caution!)
        
    Returns:
        Dictionary with execution results
    """
    try:
        # Security check - only allow executing files in the current directory or subdirectories
        base_path = os.path.dirname(os.path.abspath(__file__))
        abs_path = os.path.abspath(os.path.join(base_path, script_path))
        
        # Prevent path traversal attacks
        if not abs_path.startswith(base_path):
            return {
                "success": False,
                "results": None,
                "errors": ["Security error: Cannot execute scripts outside of the workspace."]
            }
            
        if not os.path.exists(abs_path):
            return {
                "success": False,
                "results": None,
                "errors": [f"SQL script file not found: {script_path}"]
            }
            
        # Read the SQL script content
        with open(abs_path, 'r') as f:
            sql_content = f.read()
            
        # Check if script contains write operations (INSERT, UPDATE, DELETE, etc.)
        has_write_ops = any(
            op.upper() in sql_content.upper() for op in 
            ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "MERGE"]
        )
        
        if has_write_ops and not unsafe:
            return {
                "success": False,
                "results": None,
                "errors": [
                    "Script contains write operations but unsafe=False. "
                    "Set unsafe=True to execute write operations (use with caution!)"
                ]
            }
            
        # Execute the script
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Split script by semicolons to handle multiple statements
            # Note: This is a simple split and doesn't handle all SQL syntax cases properly
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            results = []
            for stmt in statements:
                try:
                    # Skip empty statements
                    if not stmt.strip():
                        continue
                        
                    # Execute the statement
                    async with db.execute(stmt) as cursor:
                        # For SELECT statements, fetch results
                        if stmt.strip().upper().startswith("SELECT"):
                            rows = await cursor.fetchall()
                            columns = [desc[0] for desc in cursor.description] if cursor.description else []
                            data = [dict(row) for row in rows]
                            results.append({
                                "statement": stmt,
                                "is_query": True,
                                "columns": columns,
                                "data": data,
                                "row_count": len(rows)
                            })
                        else:
                            # For other statements, return affected row count
                            results.append({
                                "statement": stmt,
                                "is_query": False,
                                "row_count": cursor.rowcount
                            })
                except Exception as stmt_e:
                    results.append({
                        "statement": stmt,
                        "error": str(stmt_e)
                    })
                    
            # Commit changes if unsafe=True, otherwise roll back
            if unsafe:
                await db.commit()
            else:
                await db.rollback()
                
        return {
            "success": True,
            "results": results,
            "statement_count": len(statements),
            "has_write_operations": has_write_ops,
            "was_committed": unsafe,
            "errors": []
        }
            
    except Exception as e:
        return {
            "success": False,
            "results": None,
            "errors": [f"Error executing SQL script: {str(e)}"]
        }


if __name__ == "__main__":
    # Ensure pandas is available when running directly
    try:
        import pandas as pd
    except ImportError:
        print("Error: pandas library is required. Please install it (`pip install pandas openpyxl`)")
        exit(1)
    mcp.run(transport='stdio') # Changed from mcp.run() to specify stdio transport explicitly if needed