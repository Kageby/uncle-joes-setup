"""
Uncle Joe's Coffee Company — FastAPI login example

Demonstrates how to accept credentials over HTTP, hash the submitted
password with bcrypt, and construct a parameterized BigQuery query to
look up the matching member.

Setup:
    poetry install

Run:
    poetry run uvicorn main:app --reload

Then POST to http://127.0.0.1:8000/login
"""

import bcrypt
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from pydantic import BaseModel

app = FastAPI(title="Uncle Joe's Coffee API")

# Replace with your GCP project ID
GCP_PROJECT = "mgmt-545-group-project-493414"
DATASET = "uncle_joes"

client = bigquery.Client(project=GCP_PROJECT)


class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/login")
def login(body: LoginRequest):
    # 1. Hash the submitted password so we never handle it in plain text
    #    beyond this point.  bcrypt.hashpw produces a new hash every call
    #    (random salt), so we can't compare hashes directly — we use
    #    bcrypt.checkpw() against the stored hash retrieved from the DB.
    submitted_bytes = body.password.encode("utf-8")
    _ = bcrypt.hashpw(submitted_bytes, bcrypt.gensalt())  # shown for illustration

    # 2. Build a parameterized query to fetch the member's stored hash.
    #    Never interpolate user input directly into SQL strings.
    query = """
        SELECT id, first_name, last_name, email, password
        FROM `{project}.{dataset}.members`
        WHERE email = @email
        LIMIT 1
    """.format(project=GCP_PROJECT, dataset=DATASET)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", body.email),
        ]
    )

    results = list(client.query(query, job_config=job_config).result())

    if not results:
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    row = results[0]
    stored_hash: str = row["password"]

    # 3. Verify the submitted password against the bcrypt hash from the DB.
    if not bcrypt.checkpw(submitted_bytes, stored_hash.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    return {
        "authenticated": True,
        "member_id": row["id"],
        "name": f"{row['first_name']} {row['last_name']}",
        "email": row["email"],
    }
