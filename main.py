from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os

app = FastAPI()

def get_conn():
        database="defaultdb"
    )

class User(BaseModel):
    email: str
    username: str
    gender: str
    birthdate: str
    country: str
    age: str

class Usage(BaseModel):
    dataset_id: str
    email: str

class OrgType(BaseModel):
    org_type: str

class FormatType(BaseModel):
    fmt: str

class TagName(BaseModel):
    tag: str

class UserEmail(BaseModel):
    email: str

@app.get("/")
def home():
    return {"status": "running"}

@app.post("/register")
def register_user(user: User):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (Email, Username, gender, Birthdate, Country, Age) VALUES (%s,%s,%s,%s,%s,%s)",
            (user.email, user.username, user.gender, user.birthdate, user.country, user.age)
        )
        conn.commit()
        conn.close()
        return {"message": "User registered successfully"}
    except mysql.connector.Error as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/usage")
def add_usage(usage: Usage):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO datasetuser (Dataset_Identifier, User_Email) VALUES (%s,%s)",
            (usage.dataset_id, usage.email)
        )
        conn.commit()
        conn.close()
        return {"message": "Usage added"}
    except mysql.connector.Error as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/view-usage")
def view_usage(user: UserEmail):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Dataset_Identifier
        FROM datasetuser
        WHERE User_Email=%s
    """, (user.email,))
    result = cur.fetchall()
    conn.close()
    return [{"Dataset": r[0]} for r in result]

@app.post("/datasets-by-orgtype")
def datasets_by_orgtype(org: OrgType):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.Identifier, d.Dataset_Name
        FROM dataset d
        JOIN publishingorganization p
        ON d.Publishing_Organization_Name = p.Organization_Name
        WHERE p.Organization_Type=%s
    """, (org.org_type,))
    result = cur.fetchall()
    conn.close()
    return [{"Identifier": r[0], "Dataset_Name": r[1]} for r in result]

@app.get("/top-orgs")
def top_orgs():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Publishing_Organization_Name, COUNT(*)
        FROM dataset
        GROUP BY Publishing_Organization_Name
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    result = cur.fetchall()
    conn.close()
    return [{"Organization": r[0], "Count": r[1]} for r in result]

@app.post("/datasets-by-format")
def datasets_by_format(fmt: FormatType):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.Identifier, d.Dataset_Name
        FROM dataset d
        JOIN datasetformat f ON d.Identifier=f.Dataset_Identifier
        WHERE f.Format_Type=%s
    """, (fmt.fmt,))
    result = cur.fetchall()
    conn.close()
    return [{"Identifier": r[0], "Dataset_Name": r[1]} for r in result]

@app.post("/datasets-by-tag")
def datasets_by_tag(tag: TagName):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.Identifier, d.Dataset_Name
        FROM dataset d
        JOIN datasettag t ON d.Identifier=t.Dataset_Identifier
        WHERE t.Tag_Name=%s
    """, (tag.tag,))
    result = cur.fetchall()
    conn.close()
    return [{"Identifier": r[0], "Dataset_Name": r[1]} for r in result]

@app.get("/contributions")
def contributions():
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT Publishing_Organization_Name, COUNT(*) FROM dataset GROUP BY Publishing_Organization_Name")
    by_org = cur.fetchall()
    
    cur.execute("SELECT Topic, COUNT(*) FROM dataset GROUP BY Topic")
    by_topic = cur.fetchall()
    
    cur.execute("""
        SELECT f.Format_Type, COUNT(*)
        FROM dataset d
        JOIN datasetformat f ON d.Identifier=f.Dataset_Identifier
        GROUP BY f.Format_Type
    """)
    by_format = cur.fetchall()
    
    cur.execute("""
        SELECT p.Organization_Type, COUNT(*)
        FROM dataset d
        JOIN publishingorganization p
        ON d.Publishing_Organization_Name=p.Organization_Name
        GROUP BY p.Organization_Type
    """)
    by_org_type = cur.fetchall()
    
    conn.close()
    return {
        "by_organization": [{"Organization": r[0], "Count": r[1]} for r in by_org],
        "by_topic": [{"Topic": r[0], "Count": r[1]} for r in by_topic],
        "by_format": [{"Format": r[0], "Count": r[1]} for r in by_format],
        "by_organization_type": [{"Type": r[0], "Count": r[1]} for r in by_org_type]
    }

@app.get("/top-datasets")
def top_datasets():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Dataset_Identifier, COUNT(*)
        FROM datasetuser
        GROUP BY Dataset_Identifier
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    result = cur.fetchall()
    conn.close()
    return [{"Dataset": r[0], "Count": r[1]} for r in result]

@app.get("/usage-by-project-type")
def usage_by_project_type():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Project_Category, COUNT(*)
        FROM project
        GROUP BY Project_Category
    """)
    result = cur.fetchall()
    conn.close()
    return [{"Project_Category": r[0], "Count": r[1]} for r in result]

@app.get("/top-tags")
def top_tags():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.Project_Category, t.Tag_Name, COUNT(*) c
        FROM project p
        JOIN datasettag t ON p.Dataset_Identifier=t.Dataset_Identifier
        GROUP BY p.Project_Category, t.Tag_Name
        ORDER BY p.Project_Category, c DESC
    """)
    data = cur.fetchall()
    conn.close()
    
    result = {}
    current = None
    count = 0
    
    for row in data:
        category, tag, c = row
        if category != current:
            current = category
            count = 0
            result[category] = []
        if count < 10:
            result[category].append({"Tag": tag, "Count": c})
            count += 1
    
    return result
