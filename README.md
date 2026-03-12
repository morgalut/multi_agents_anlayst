
```bash
cd xlsx
uvicorn Multi_agen.main:app --reload
```

2. Re-test:

```bash
curl http://127.0.0.1:8000/health
```

3. Then run:

```bash
curl -X POST "http://127.0.0.1:8000/run" ^
  -H "Content-Type: application/json" ^
  -d "{\"run_id\":\"run-1\",\"workbook_path\":\"C:\\\\Users\\\\morga\\\\Desktop\\\\xlsx\\\\Assembly - 1.xlsx\"}"
```

## What was done and what needs to be fixed Programmer's notes
1. create for any agen file is show what is get from promt and what is answer like log but need by more Specific to that agent
2. start build valid for any agen for check answer 
3. check with yaniv what anwer is better and how you can improve this 



ערות 
---
* צריך לבנות את הסוכנים שיחזירו תשובה כמו בדוגמא 
* צריך לבדוק את השגיאה שקיימת 
```sh
2026-03-12 15:39:36 | ERROR | multi_agen | ToolRouter:excel_read_sheet_range FAILED sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 83, in excel_read_sheet_range
    result = self._excel.read_sheet_range(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 117, in read_sheet_range
    resp = self._call_tool("excel.read_sheet_range", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.read_sheet_range
ERROR:multi_agen:ToolRouter:excel_read_sheet_range FAILED sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 83, in excel_read_sheet_range
    result = self._excel.read_sheet_range(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 117, in read_sheet_range
    resp = self._call_tool("excel.read_sheet_range", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.read_sheet_range
2026-03-12 15:39:36 | ERROR | multi_agen.agents.sheet_currency_agent | SheetCurrencyAgent:grid_read_failed sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\agents\sheet_currency_agent.py", line 199, in _collect_evidence
    grid = tools.excel_read_sheet_range(
        sheet_name=sheet_name,
    ...<3 lines>...
        ncols=self.max_preview_cols,
    )
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 83, in excel_read_sheet_range
    result = self._excel.read_sheet_range(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 117, in read_sheet_range
    resp = self._call_tool("excel.read_sheet_range", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.read_sheet_range
ERROR:multi_agen.agents.sheet_currency_agent:SheetCurrencyAgent:grid_read_failed sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\agents\sheet_currency_agent.py", line 199, in _collect_evidence
    grid = tools.excel_read_sheet_range(
        sheet_name=sheet_name,
    ...<3 lines>...
        ncols=self.max_preview_cols,
    )
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 83, in excel_read_sheet_range
    result = self._excel.read_sheet_range(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 117, in read_sheet_range
    resp = self._call_tool("excel.read_sheet_range", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.read_sheet_range
2026-03-12 15:39:36 | INFO | multi_agen | ToolRouter:excel_detect_merged_cells sheet=FS
INFO:multi_agen:ToolRouter:excel_detect_merged_cells sheet=FS
2026-03-12 15:39:36 | INFO | multi_agen | [MCP CALL] server=excel-mcp tool=excel.detect_merged_cells
INFO:multi_agen:[MCP CALL] server=excel-mcp tool=excel.detect_merged_cells
2026-03-12 15:40:36 | ERROR | multi_agen | ToolRouter:excel_detect_merged_cells FAILED sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 108, in excel_detect_merged_cells
    result = self._excel.detect_merged_cells(sheet_name)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 129, in detect_merged_cells
    resp = self._call_tool("excel.detect_merged_cells", {"sheet_name": sheet_name})
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.detect_merged_cells
ERROR:multi_agen:ToolRouter:excel_detect_merged_cells FAILED sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 108, in excel_detect_merged_cells
    result = self._excel.detect_merged_cells(sheet_name)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 129, in detect_merged_cells
    resp = self._call_tool("excel.detect_merged_cells", {"sheet_name": sheet_name})
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.detect_merged_cells
2026-03-12 15:40:36 | ERROR | multi_agen.agents.sheet_currency_agent | SheetCurrencyAgent:merged_read_failed sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\agents\sheet_currency_agent.py", line 212, in _collect_evidence
    tools.excel_detect_merged_cells(sheet_name=sheet_name) or []
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 108, in excel_detect_merged_cells
    result = self._excel.detect_merged_cells(sheet_name)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 129, in detect_merged_cells
    resp = self._call_tool("excel.detect_merged_cells", {"sheet_name": sheet_name})
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.detect_merged_cells
ERROR:multi_agen.agents.sheet_currency_agent:SheetCurrencyAgent:merged_read_failed sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\agents\sheet_currency_agent.py", line 212, in _collect_evidence
    tools.excel_detect_merged_cells(sheet_name=sheet_name) or []
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 108, in excel_detect_merged_cells
    result = self._excel.detect_merged_cells(sheet_name)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 129, in detect_merged_cells
    resp = self._call_tool("excel.detect_merged_cells", {"sheet_name": sheet_name})
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.detect_merged_cells
2026-03-12 15:40:36 | INFO | multi_agen | ToolRouter:excel_get_formulas sheet=FS row0=0 col0=0 nrows=5 ncols=10
INFO:multi_agen:ToolRouter:excel_get_formulas sheet=FS row0=0 col0=0 nrows=5 ncols=10
2026-03-12 15:40:36 | INFO | multi_agen | [MCP CALL] server=excel-mcp tool=excel.get_formulas
INFO:multi_agen:[MCP CALL] server=excel-mcp tool=excel.get_formulas
2026-03-12 15:41:36 | INFO | multi_agen | [MCP CALL] server=excel-mcp tool=excel.get_formulas
INFO:multi_agen:[MCP CALL] server=excel-mcp tool=excel.get_formulas
2026-03-12 15:42:36 | ERROR | multi_agen | ToolRouter:excel_get_formulas FAILED first_attempt sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 129, in excel_get_formulas
    result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas
ERROR:multi_agen:ToolRouter:excel_get_formulas FAILED first_attempt sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 129, in excel_get_formulas
    result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas
2026-03-12 15:42:36 | INFO | multi_agen | ToolRouter:excel_get_formulas retry_small sheet=FS nrows=5 ncols=8
INFO:multi_agen:ToolRouter:excel_get_formulas retry_small sheet=FS nrows=5 ncols=8
2026-03-12 15:42:36 | INFO | multi_agen | [MCP CALL] server=excel-mcp tool=excel.get_formulas
INFO:multi_agen:[MCP CALL] server=excel-mcp tool=excel.get_formulas
2026-03-12 15:43:36 | INFO | multi_agen | [MCP CALL] server=excel-mcp tool=excel.get_formulas
INFO:multi_agen:[MCP CALL] server=excel-mcp tool=excel.get_formulas
2026-03-12 15:44:36 | ERROR | multi_agen | ToolRouter:excel_get_formulas FAILED retry_small sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 129, in excel_get_formulas
    result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 151, in excel_get_formulas
    result = self._excel.get_formulas(
        sheet_name=sheet_name,
    ...<3 lines>...
        ncols=small_ncols,
    )
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas
ERROR:multi_agen:ToolRouter:excel_get_formulas FAILED retry_small sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 129, in excel_get_formulas
    result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 151, in excel_get_formulas
    result = self._excel.get_formulas(
        sheet_name=sheet_name,
    ...<3 lines>...
        ncols=small_ncols,
    )
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas
2026-03-12 15:44:36 | ERROR | multi_agen | ToolRouter:excel_get_formulas_safe FAILED sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 179, in excel_get_formulas_safe
    return self.excel_get_formulas(
           ~~~~~~~~~~~~~~~~~~~~~~~^
        sheet_name=sheet_name,
        ^^^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        ncols=ncols,
        ^^^^^^^^^^^^
    )
    ^
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 168, in excel_get_formulas
    raise exc
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 129, in excel_get_formulas
    result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 237, in _call_tool
    raise ExcelClientError(
    ...<5 lines>...
    ) from exc
Multi_agen.packages.mcp_clients.excel_client.ExcelClientError: MCP timeout calling tool: excel.get_formulas
ERROR:multi_agen:ToolRouter:excel_get_formulas_safe FAILED sheet=FS
Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 199, in _call_tool
    resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\transport_http.py", line 62, in call_tool
    with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
         ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 187, in urlopen
    return opener.open(url, data, timeout)
           ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 487, in open
    response = self._open(req, data)
  File "C:\Python314\Lib\urllib\request.py", line 504, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
                              '_open', req)
  File "C:\Python314\Lib\urllib\request.py", line 464, in _call_chain
    result = func(*args)
  File "C:\Python314\Lib\urllib\request.py", line 1350, in http_open
    return self.do_open(http.client.HTTPConnection, req)
           ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Python314\Lib\urllib\request.py", line 1325, in do_open
    r = h.getresponse()
  File "C:\Python314\Lib\http\client.py", line 1450, in getresponse
    response.begin()
    ~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ~~~~~~~~~~~~~~~~~^^
  File "C:\Python314\Lib\http\client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "C:\Python314\Lib\socket.py", line 725, in readinto
    return self._sock.recv_into(b)
           ~~~~~~~~~~~~~~~~~~~~^^^
TimeoutError: timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 179, in excel_get_formulas_safe
    return self.excel_get_formulas(
           ~~~~~~~~~~~~~~~~~~~~~~~^
        sheet_name=sheet_name,
        ^^^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        ncols=ncols,
        ^^^^^^^^^^^^
    )
    ^
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 168, in excel_get_formulas
    raise exc
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\router\tool_router.py", line 129, in excel_get_formulas
    result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
  File "C:\Users\morga\Desktop\xlsx\Multi_agen\packages\mcp_clients\excel_client.py", line 155, in get_formulas
    resp = self._call_tool("excel.get_formulas", args)
```
