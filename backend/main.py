from fastapi import FastAPI,HTTPException,Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional,Any,Dict
import sqlite3,threading

app=FastAPI()
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_methods=['*'],allow_headers=['*'])

class RunAgentRequest(BaseModel):
    agent:str;source:Optional[str]=None;target:Optional[str]=None;mode:Optional[str]='nl';userText:Optional[str]=None;condition:Optional[str]=None
class AgentResponse(BaseModel):
    message:str;payload:Optional[Dict[str,Any]]=None

_db_lock=threading.Lock()
DB_CONN=sqlite3.connect(':memory:',check_same_thread=False)
DB_CONN.row_factory=sqlite3.Row
def _init_db():
    with _db_lock:
        c=DB_CONN.cursor()
        c.execute('CREATE TABLE employees(id INTEGER PRIMARY KEY,name TEXT,age INTEGER)')
        c.executemany('INSERT INTO employees(name,age) VALUES(?,?)',[('Alice',34),('Bob',82),('Carol',45),('Dave',82)])
        DB_CONN.commit()
_init_db()

def execute_sql(q):
    with _db_lock:
        cur=DB_CONN.cursor();cur.execute(q);cols=[c[0] for c in cur.description];return [dict(zip(cols,r)) for r in cur.fetchall()]

@app.post('/run_agent',response_model=AgentResponse)
def run_agent(req:RunAgentRequest=Body(...)):
    if req.agent=='profiling':
        cond=req.condition or '1=1';query=f'SELECT * FROM employees WHERE {cond} LIMIT 10'
        rows=execute_sql(query)
        payload={'generated_query':query,'metadata':{'tables':[{'name':'employees','rows':len(rows)}]},'sample':rows}
        return AgentResponse(message='Profiled dataset successfully.',payload=payload)
    elif req.agent=='reconciliation':
        cond=req.condition or '1=1';query=f'SELECT * FROM employees WHERE {cond}';rows=execute_sql(query)
        payload={'generated_query':query,'diff_summary':{'total_source':len(rows),'total_target':len(rows),'mismatches':0}}
        return AgentResponse(message='Reconciliation complete.',payload=payload)
    raise HTTPException(status_code=400,detail='Unknown agent')

if __name__=='__main__':
    import uvicorn;uvicorn.run('main:app',host='0.0.0.0',port=8000,reload=True)
