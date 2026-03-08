
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
1. The code works and runs
2. The final result needs to run from which SHEET it took the data and from which row it needs to be compared so that the final JSON will also display these results
3. Build a comparison system that displays the results that the presentation will generate and your results (it is possible for now that it was displayed in JSON and then we will perform a manual comparison)
4. Think about a way to validate the information that can be added to the process