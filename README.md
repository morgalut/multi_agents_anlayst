
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

