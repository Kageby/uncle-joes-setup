"""
Uncle Joe's Coffee Company — FastAPI Backend

Serves menu, locations, members, orders, and loyalty points data from
BigQuery. Deployed to Google Cloud Run.

Setup:
    poetry install

Run locally:
    poetry run uvicorn main:app --reload

Then open http://127.0.0.1:8000/docs
"""

import math
import bcrypt
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Uncle Joe's Coffee API")

# Allow the frontend (on its own Cloud Run URL) to call this API from the browser.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Replace with your GCP project ID
GCP_PROJECT = "mgmt-545-group-project-493414"
DATASET = "uncle_joes"

client = bigquery.Client(project=GCP_PROJECT)


class LoginRequest(BaseModel):
    email: str
    password: str


@app.get("/")
def root():
    """Health check / welcome endpoint."""
    return {"app": "Uncle Joe's Coffee API", "status": "ok", "docs": "/docs"}


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
        "id": row["id"],
        "name": f"{row['first_name']} {row['last_name']}",
        "email": row["email"],
    }


#================================================================================================#
#--------------------------------------Menu Endpoints (GP2 REQUIRED)-----------------------------#
#================================================================================================#

# GP2 REQUIRED: GET /menu — return menu items (Supports filtering)
@app.get("/menu_items_filter")
def get_menu(
    category: Optional[str] = Query(None, description="Filter by category (Coffee, Espresso, etc.)"),
    size: Optional[str] = Query(None, description="Filter by size (Small, Medium, Large)"),
):
    """
    Returns all menu items. Supports optional filtering by category and size.
    """
    where_clauses = []
    params = []

    if category:
        where_clauses.append("LOWER(category) = LOWER(@category)")
        params.append(bigquery.ScalarQueryParameter("category", "STRING", category))
    if size:
        where_clauses.append("LOWER(size) = LOWER(@size)")
        params.append(bigquery.ScalarQueryParameter("size", "STRING", size))

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT
            id,
            name,
            category,
            size,
            calories,
            CAST(price AS FLOAT64) AS price
        FROM `{GCP_PROJECT}.{DATASET}.menu_items`
        {where_sql}
        ORDER BY category, name, size
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        results = client.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


# GP2 REQUIRED: GET /menu_items/{id} — return a single menu item
@app.get("/menu_items/{id}")
def get_menu_item(id: str):
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
            CAST(price AS FLOAT64) AS price
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


@app.get("/menu_items")
def get_menu_items_alias():
    """
    Alias for /menu — retrieves all menu items from the menu_items table.
    """
    query = f"""
        SELECT
            id,
            name,
            category,
            size,
            calories,
            CAST(price AS FLOAT64) AS price
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


@app.get("/menu/calories")
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


@app.get("/menu_items/price")
def get_menu_item_price():
    """
    Retrieves item ids, names, and prices.
    """
    query = f"""
        SELECT
            id,
            name,
            CAST(price AS FLOAT64) AS price
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


@app.get("/menu_items/name")
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


@app.get("/api/menu_items/category")
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


@app.get("/api/menu_items/size")
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


#================================================================================================#
#--------------------------------------Locations Endpoints (GP2 REQUIRED)------------------------#
#================================================================================================#

# GP2 REQUIRED: GET /locations — return all locations
@app.get("/locations")
def get_locations(
    state: Optional[str] = Query(None, description="Filter by state (e.g. 'MI')"),
    city: Optional[str] = Query(None, description="Filter by city (partial match, case-insensitive)"),
    open_only: bool = Query(False, description="Only return locations currently open for business"),
    limit: int = Query(100, ge=1, le=500, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip (for pagination)"),
):
    """
    Returns store locations. Supports filtering by state, city, and open status
    with pagination. The GP2 spec notes that returning all 485 at once is not
    ideal, so filtering is important for frontend usability.
    """
    where_clauses = []
    params = []

    if state:
        where_clauses.append("UPPER(state) = UPPER(@state)")
        params.append(bigquery.ScalarQueryParameter("state", "STRING", state))
    if city:
        where_clauses.append("LOWER(city) LIKE LOWER(@city)")
        params.append(bigquery.ScalarQueryParameter("city", "STRING", f"%{city}%"))
    if open_only:
        where_clauses.append("open_for_business = TRUE")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT *
        FROM `{GCP_PROJECT}.{DATASET}.locations`
        {where_sql}
        ORDER BY state, city
        LIMIT @limit OFFSET @offset
    """

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        results = client.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    rows = [dict(row) for row in results]
    return {"count": len(rows), "limit": limit, "offset": offset, "results": rows}


# GP2 REQUIRED: GET /locations/{id} — return a single location
@app.get("/locations/{id}")
def get_location(id: str):
    """
    Retrieves a single location by its id.
    """
    query = f"""
        SELECT *
        FROM `{GCP_PROJECT}.{DATASET}.locations`
        WHERE id = @id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", location_id)
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
            detail=f"Location with id {location_id} not found."
        )

    return dict(results[0])


#================================================================================================#
#--------------------------------------Members Endpoints (GP3)-----------------------------------#
#================================================================================================#

@app.get("/members/{id}")
def get_member(_id: str):
    """
    Returns a member's public profile (no password or token).
    """
    query = f"""
        SELECT
            id,
            first_name,
            last_name,
            email,
            phone_number,
            home_store
        FROM `{GCP_PROJECT}.{DATASET}.members`
        WHERE id = @id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", member_id)
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
            detail=f"Member with id {id} not found."
        )

    return dict(results[0])


#================================================================================================#
#--------------------------------------Orders & Order History (GP3)------------------------------#
#================================================================================================#

@app.get("/orders/member/{id}")
def get_member_order_history(
    id: str,
    limit: int = Query(50, ge=1, le=500),
):
    """
    Returns a member's order history, newest first, with line items nested
    under each order. Implements Information Architecture section 2.3:
        - filter orders by member_id
        - join order_items
        - sort by order_date DESC
    """
    query = f"""
        SELECT
            o.order_id,
            o.member_id,
            o.store_id,
            o.order_date,
            CAST(o.items_subtotal AS FLOAT64) AS items_subtotal,
            CAST(o.order_discount AS FLOAT64) AS order_discount,
            CAST(o.order_subtotal AS FLOAT64) AS order_subtotal,
            CAST(o.sales_tax      AS FLOAT64) AS sales_tax,
            CAST(o.order_total    AS FLOAT64) AS order_total,
            ARRAY_AGG(
                STRUCT(
                    oi.id          AS line_id,
                    oi.menu_item_id,
                    oi.item_name,
                    oi.size,
                    oi.quantity,
                    CAST(oi.price AS FLOAT64) AS price
                )
                IGNORE NULLS
            ) AS items
        FROM `{GCP_PROJECT}.{DATASET}.orders` o
        LEFT JOIN `{GCP_PROJECT}.{DATASET}.order_items` oi
            ON oi.order_id = o.order_id
        WHERE o.member_id = @mid
        GROUP BY
            o.order_id, o.member_id, o.store_id, o.order_date,
            o.items_subtotal, o.order_discount, o.order_subtotal,
            o.sales_tax, o.order_total
        ORDER BY o.order_date DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mid", "STRING", member_id),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    try:
        results = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    orders = [dict(row) for row in results]
    return {"member_id": member_id, "count": len(orders), "orders": orders}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    """
    Returns a single order (receipt) including all line items.
    """
    header_query = f"""
        SELECT
            order_id,
            member_id,
            store_id,
            order_date,
            CAST(items_subtotal AS FLOAT64) AS items_subtotal,
            CAST(order_discount AS FLOAT64) AS order_discount,
            CAST(order_subtotal AS FLOAT64) AS order_subtotal,
            CAST(sales_tax      AS FLOAT64) AS sales_tax,
            CAST(order_total    AS FLOAT64) AS order_total
        FROM `{GCP_PROJECT}.{DATASET}.orders`
        WHERE order_id = @oid
        LIMIT 1
    """

    items_query = f"""
        SELECT
            id AS line_id,
            order_id,
            menu_item_id,
            item_name,
            size,
            quantity,
            CAST(price AS FLOAT64) AS price
        FROM `{GCP_PROJECT}.{DATASET}.order_items`
        WHERE order_id = @oid
        ORDER BY item_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("oid", "STRING", order_id)
        ]
    )

    try:
        headers = list(client.query(header_query, job_config=job_config).result())
        if not headers:
            raise HTTPException(
                status_code=404,
                detail=f"Order with id {order_id} not found."
            )
        items = list(client.query(items_query, job_config=job_config).result())
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    order = dict(headers[0])
    order["items"] = [dict(item) for item in items]
    return order


#================================================================================================#
#--------------------------------------Loyalty Points (GP3)--------------------------------------#
#================================================================================================#

@app.get("/members/{id}/points")
def get_member_points(id: str):
    """
    Returns a member's total Coffee Club points balance.

    Per the project plan:
        points_for_order = FLOOR(order_total)
        total_points     = SUM(FLOOR(order_total)) across all member orders
    """
    query = f"""
        SELECT
            COUNT(*) AS order_count,
            IFNULL(SUM(FLOOR(order_total)), 0) AS total_points,
            IFNULL(SUM(order_total), 0) AS lifetime_spend
        FROM `{GCP_PROJECT}.{DATASET}.orders`
        WHERE id = @mid
          AND id IS NOT NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mid", "STRING", id)
        ]
    )

    try:
        results = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    row = results[0] if results else {}
    return {
        "id": id,
        "order_count": int(row.get("order_count") or 0),
        "total_points": int(row.get("total_points") or 0),
        "lifetime_spend": float(row.get("lifetime_spend") or 0.0),
    }


@app.get("/orders/{order_id}/points")
def get_points_for_order(order_id: str):
    """
    Returns the points earned on a single order: FLOOR(order_total).
    """
    query = f"""
        SELECT
            order_id,
            CAST(order_total AS FLOAT64) AS order_total
        FROM `{GCP_PROJECT}.{DATASET}.orders`
        WHERE order_id = @oid
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("oid", "STRING", order_id)
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
            detail=f"Order with id {order_id} not found."
        )

    row = dict(results[0])
    total = row["order_total"] or 0.0
    return {
        "order_id": order_id,
        "order_total": total,
        "points_earned": int(math.floor(total)),
    }
