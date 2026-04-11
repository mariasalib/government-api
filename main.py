from fastapi import FastAPI
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

@app.get("/")
def home():
    return {"status": "running"}

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

@app.post("/register")
def register_user(email: str, username: str, gender: str, birthdate: str, country: str, age: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users VALUES (%s,%s,%s,%s,%s,%s)",
        (email, username, gender, birthdate, country, age)
    )
    conn.commit()
    conn.close()
    return {"message": "user registered"}
