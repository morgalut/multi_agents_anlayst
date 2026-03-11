
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



