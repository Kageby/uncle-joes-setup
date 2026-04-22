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

#================================================================================================#
#--------------------------------------Menu Display Items----------------------------------------#
#================================================================================================#

# GET menu info (all)
@app.get("/api/menu_items")
def get_menu_items():
    """
    Retrieves all menu items from the menu_items table.
    """
    query = f"""
        SELECT
            id,
            name,
            category,
            size,
            calories,
            price
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        ORDER BY id
    """

    try:
        results = client.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    menu_items = [dict(row) for row in results]
    return menu_items



@app.get("/api/menu_items/{id}")
def get_menu_item_by_id(id: str):
    """
    Retrieves the menu item specified by its id.
    """
    query = f"""
        SELECT
            id,
            name,
            category,
            size,
            calories,
            price
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        WHERE id = @id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", id)
        ]
    )

    try:
        results = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"Menu item with id {id} not found."
        )

    return dict(results[0])


@app.get("/api/menu/calories")
def get_menu_item_calories():
    """
    Retrieves item ids, names, and calories.
    """
    query = f"""
        SELECT
            id,
            name,
            calories
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        ORDER BY id
    """

    try:
        results = client.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/api/menu/price")
def get_menu_item_price():
    """
    Retrieves item ids, names, and prices.
    """
    query = f"""
        SELECT
            id,
            name,
            price
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        ORDER BY id
    """

    try:
        results = client.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/api/menu/name")
def get_menu_item_name():
    """
    Retrieves item ids and names.
    """
    query = f"""
        SELECT
            id,
            name
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        ORDER BY id
    """

    try:
        results = client.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/api/menu/category")
def get_menu_item_category():
    """
    Retrieves item ids, names, and categories.
    """
    query = f"""
        SELECT
            id,
            name,
            category
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        ORDER BY id
    """

    try:
        results = client.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/api/menu/size")
def get_menu_item_size():
    """
    Retrieves item ids, names, and sizes.
    """
    query = f"""
        SELECT
            id,
            name,
            size
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        ORDER BY id
    """

    try:
        results = client.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]