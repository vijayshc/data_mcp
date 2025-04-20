# Data Engineer MCP Tools

## Overview

This project provides data engineering utilities exposed as Model Context Protocol (MCP) tools using the FastMCP framework. It allows you to interact with a SQLite database (`testdb.db`) and an Excel mapping file (`mapping.xlsx`) for tasks such as listing tables, retrieving column metadata, fetching mapping details, and validating mappings.

## Prerequisites

- Python 3.12 or higher
- SQLite
- `mapping.xlsx` file in the project root
- `testdb.db` file in the project root

## Installation

```bash
# Clone the repository and install dependencies
git clone <repo-url>
cd dataengineer
pip install .
```

Dependencies are specified in `pyproject.toml` and include:

- aiosqlite>=0.21.0
- httpx>=0.28.1
- mcp[cli]>=1.6.0

## Configuration

Ensure that the following files are present in the project root and properly populated:

- `testdb.db`: SQLite database containing your tables.
- `mapping.xlsx`: Excel file with mapping definitions. It should include columns such as `Mapping Name`, `Type`, `Alias`, `Full Table Name / Subquery Definition`, and others as required by the dataengineer tool.

## Usage

### Running the MCP Server

Start the MCP server to expose the tools:

```bash
mcp run data_engineer
```

This command starts a server (by default on `http://localhost:3000`).

### Using the MCP CLI

Interact with the tools via the `mcp` CLI:

```bash
# List available tables
mcp call data_engineer get_available_tables

# Get columns for a specific table
mcp call data_engineer get_table_columns_json --args '["my_table"]'

# Get mapping details
mcp call data_engineer get_mapping_details --args '["my_mapping_reference"]'

# Validate mapping against the database
mcp call data_engineer validate_mapping --args '[<table_columns_json>]'  
```

### Python Client Example

```python
from mcp.client import MCPClient
import asyncio

async def main():
    client = MCPClient("http://localhost:3000")
    tables = await client.call("data_engineer", "get_available_tables")
    print(tables)

if __name__ == "__main__":
    asyncio.run(main())
```

## Tool Descriptions

- **get_available_tables()**: Returns a list of table names in the SQLite database.
- **get_table_columns_json(table_name: str)**: Returns column names for the specified table.
- **get_mapping_details(mapping_reference_name: str)**: Reads `mapping.xlsx` and returns mapping details for the given reference name.
- **validate_mapping(table_columns_json: Any)**: Validates that the specified tables and columns exist in the database.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See `LICENSE` for details.
