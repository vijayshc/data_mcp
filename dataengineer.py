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
async def query_database(query: str) -> Any:
    """Execute a SQL query against testdb.db and return results as a list of dicts."""
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'testdb.db')
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    mcp.run(transport='stdio')