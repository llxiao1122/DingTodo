#!/usr/bin/env python3
"""Work query MCP: isolated read-only feedback release; shared work adapter remains unchanged."""
import runpy
import sys
sys.path.insert(0, "/root/.hermes/polardb_runtime")
runpy.run_path("/root/.hermes/releases/polardb-feedback-read-20261101-175710/workflows/polardb_work_adapter/mcp_server.py", run_name="__main__")
