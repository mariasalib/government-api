from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os

app = FastAPI()

def get_conn():
    return mysql.connector.connect(
        host="mysql-18922077-mariasalib95-7600.h.aivencloud.com",
        port=28276,
        user="avnadmin",
        password=os.getenv("DB_PASSWORD"),
        database="defaultdb"
    )

class User(BaseModel):
    email: str
    username: str
    gender: str
    birthdate: str
    country: str
    age: int

class Usage(BaseModel):
    user_email: str
    dataset_identifier: str
    project_type: str

@app.get("/")
def home():
    return {"status": "running"}

@app.post("/register")
def register_user(user: User):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users VALUES (%s,%s,%s,%s,%s,%s)",
            (user.email, user.username, user.gender, user.birthdate, user.country, user.age)
        )
        conn.commit()
        return {"message": "user registered"}
    except mysql.connector.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@app.post("/usage")
def add_usage(usage: Usage):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO datasetuser VALUES (%s,%s,%s)",
            (usage.user_email, usage.dataset_identifier, usage.project_type)
        )
        conn.commit()
        return {"message": "usage added"}
    except mysql.connector.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@app.get("/user-usage/{email}")
def get_user_usage(email: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM datasetuser WHERE User_Email=%s",
        (email,)
    )
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/datasets-by-org-type")
def datasets_by_org_type(org_type: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dataset WHERE Publishing_Organization_Type=%s",
        (org_type,)
    )
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/top-orgs")
def top_orgs():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Publishing_Organization_Name, COUNT(*) c
        FROM dataset
        GROUP BY Publishing_Organization_Name
        ORDER BY c DESC
        LIMIT 5
    """)
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/datasets-by-format")
def datasets_by_format(format: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dataset WHERE Format=%s",
        (format,)
    )
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/datasets-by-tag")
def datasets_by_tag(tag: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.*
        FROM dataset d
        JOIN datasettag t ON d.Identifier=t.Dataset_Identifier
        WHERE t.Tag_Name=%s
    """, (tag,))
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/stats")
def get_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            Publishing_Organization_Name,
            Topic,
            Format,
            Publishing_Organization_Type,
            COUNT(*) as total
        FROM dataset
        GROUP BY Publishing_Organization_Name, Topic, Format, Publishing_Organization_Type
    """)
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/top-datasets")
def top_datasets():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Dataset_Identifier, COUNT(*) c
        FROM datasetuser
        GROUP BY Dataset_Identifier
        ORDER BY c DESC
        LIMIT 5
    """)
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/usage-by-project-type")
def usage_by_project_type():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Project_Type, COUNT(*) as count
        FROM datasetuser
        GROUP BY Project_Type
    """)
    result = cur.fetchall()
    conn.close()
    return result

@app.get("/top-tags-by-project-type")
def top_tags_by_project_type():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT du.Project_Type, dt.Tag_Name, COUNT(*) as count
        FROM datasetuser du
        JOIN datasettag dt ON du.Dataset_Identifier = dt.Dataset_Identifier
        GROUP BY du.Project_Type, dt.Tag_Name
        ORDER BY du.Project_Type, count DESC
    """)
    result = cur.fetchall()
    
    organized = {}
    for row in result:
        project_type, tag, count = row
        if project_type not in organized:
            organized[project_type] = []
        if len(organized[project_type]) < 10:
            organized[project_type].append({"tag": tag, "count": count})
    
    conn.close()
    return organized
