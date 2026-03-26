# Backend

## Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

The backend rebuilds `sap_o2c.sqlite3` from `../sap-o2c-data` on startup and exposes:

- `GET /graph`
- `GET /graph/neighborhood`
- `GET /node/{node_id}`
- `GET /trace`
- `POST /chat`

Set `GROQ_API_KEY` in `.env` to enable bounded LLM SQL generation for non-template in-domain questions.
