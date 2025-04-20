import asyncio
from typing import Optional
from contextlib import AsyncExitStack
import sys
import json

#/home/vijay/anaconda3/bin/python openai_mcp_client.py ~/mcp_learn/dataengineer/dataengineer.py

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from openai import OpenAI
from dotenv import load_dotenv
import os
from logging_config import setup_logger

logger = setup_logger()

load_dotenv()  # load environment variables from .env

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash-preview")

class MCPClient:
    def __init__(self):
        # Initialize session and client objects
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.model = OPENROUTER_MODEL
        self.client = OpenAI(
                        base_url=OPENROUTER_BASE_URL,
                        api_key=OPENROUTER_API_KEY,
                    )


    async def connect_to_server(self, server_script_path: str):
        """Connect to an MCP server
        
        Args:
            server_script_path: Path to the server script (.py or .js)
        """
        is_python = server_script_path.endswith('.py')
        is_js = server_script_path.endswith('.js')
        if not (is_python or is_js):
            raise ValueError("Server script must be a .py or .js file")
            
        command = "/home/vijay/anaconda3/bin/python" if is_python else "node"
        server_params = StdioServerParameters(
            command=command,
            args=[server_script_path],
            env=None
        )
        
        stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
        self.stdio, self.write = stdio_transport
        self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))
        
        await self.session.initialize()
        
        # List available tools
        response = await self.session.list_tools()
        tools = response.tools
        logger.info("Connected to server with tools: %s", [tool.name for tool in tools])

    async def process_query(self, query: str) -> str:
        """Process a query using OpenAI and available tools"""
        logger.info("Processing query: %s", query)
        messages = [
            {
                "role": "system",
                "content": """
1. Do not give much explanation. Just provide only answer to the questions
2. SQL query should be created in sqlite SQL format
3. Do not use CTE. Instead use sub query
4. Do not retry a step unless error
5. First create step by step plan before start to work on(feek free adopt plan on the fly)
"""
            }
        ]
        messages.append(
            {
                "role": "user",
                "content": query
            }
        )

        response = await self.session.list_tools()
        available_tools = [{ 
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.inputSchema
            }
        } for tool in response.tools]

        return_text = []
        try:
            # Initial OpenAI API call
            response = self.client.chat.completions.create(
                model=self.model,
                max_tokens=1000,
                messages=messages,
                tools=available_tools
            )

            # Process response and handle tool calls
            tool_results = []
            final_text = []
            loop=1
            logger.info("Initial response: %s", response)
            message = response.choices[0].message
            if message.content:
                final_text.append(message.content)

            if hasattr(message, 'tool_calls') and message.tool_calls:
                loop = 1
                logger.debug("Tool calls detected in initial response")

            while loop>0:
                for tool_call in message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = json.loads(tool_call.function.arguments)
                    logger.debug("Calling tool %s with args %s", tool_name, tool_args)
                    
                    # Execute tool call
                    result = await self.session.call_tool(tool_name, tool_args)
                    tool_results.append({"call": tool_name, "result": result})
                    final_text.append(f"[Calling tool {tool_name} with args {tool_args}]")

                    # Continue conversation with tool results
                    messages.append({
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": tool_call.id,
                                "type": "function",
                                "function": {"name": tool_name, "arguments": tool_call.function.arguments}
                            }
                        ]
                    })
                    
                    messages.append({
                        "role": "assistant",
                        "content": "Response from tool "+tool_name+"\n"+str(result.content)
                    })
                    messages.append({
                        "role": "user",
                        "content": "Go to next step"
                    })
                    #print ("*************** LOOP ",loop,messages)
                   
                    # Get next response from OpenAI
                    response = self.client.chat.completions.create(
                        model=self.model,
                        max_tokens=1000,
                        messages=messages,
                        tools=available_tools
                    )
                    #print("#######################",response)
                    message = response.choices[0].message
                    
                    if hasattr(message, 'tool_calls') and message.tool_calls:
                        loop = loop + 1
                        logger.info("Tool calls detected in response")
                    else:
                        loop = 0

                    if response.choices[0].message.content:
                        final_text.append(response.choices[0].message.content)

            return "\n".join(final_text)
        except Exception as e:
            logger.exception("Error processing query")
            logger.exception(e.body)
            raise

    async def chat_loop(self):
        """Run an interactive chat loop"""
        logger.info("MCP Client Started!")
        logger.info("Type your queries or 'quit' to exit.")
        
        while True:
            try:
                query = input("\nQuery: ").strip()
                
                if query.lower() == 'quit':
                    break
                    
                response = await self.process_query(query)
                logger.info("Response: %s", response)
            except Exception as e:
                logger.exception("Unexpected error in chat loop")
                print(f"\nError: {str(e)}")
    
    async def cleanup(self):
        """Clean up resources"""
        await self.exit_stack.aclose()

async def main():
    if len(sys.argv) < 2:
        print("Usage: python openai_mcp_client.py <path_to_server_script>")
        sys.exit(1)
        
    client = MCPClient()
    try:
        await client.connect_to_server(sys.argv[1])
        await client.chat_loop()
    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
