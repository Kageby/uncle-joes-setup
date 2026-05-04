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
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel, Field
from typing import Optional
import uuid

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
    email: str = Field(..., min_length=3, max_length=254)
    # bcrypt only uses the first 72 bytes of input; cap on the API side
    # so we don't silently truncate longer passwords.
    password: str = Field(..., min_length=1, max_length=72)


@app.get("/")
def root():
    """Health check / welcome endpoint."""
    return {"app": "Uncle Joe's Coffee API", "status": "ok", "docs": "/docs"}


@app.post("/login")
def login(body: LoginRequest):
    """
    Authenticate a Coffee Club member by email + password.

    Verification happens entirely on the backend:
      - The submitted password is sent over HTTPS in the request body.
      - We fetch the bcrypt hash from BigQuery using a parameterized query.
      - bcrypt.checkpw() compares the submitted password to the stored hash
        in constant time. The hash never leaves the backend.

    On success we return the member's public profile (no password, no
    api_token) so the frontend can populate the dashboard without an
    extra round-trip to /members/{id}.
    """
    # Normalize the email so casing/whitespace differences don't cause
    # false-negative logins. Passwords are NOT normalized.
    email_normalized = body.email.strip().lower()
    submitted_bytes = body.password.encode("utf-8")

    # Parameterized query — never interpolate user input into SQL.
    query = f"""
        SELECT
            id,
            first_name,
            last_name,
            email,
            phone_number,
            home_store,
            password
        FROM `{GCP_PROJECT}.{DATASET}.members`
        WHERE LOWER(email) = @email
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", email_normalized),
        ]
    )

    try:
        results = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    # Use the SAME generic error for "no such email" and "wrong password"
    # so an attacker can't tell which Coffee Club emails exist.
    INVALID = HTTPException(status_code=401, detail="Invalid email or password.")

    if not results:
        raise INVALID

    row = results[0]
    stored_hash = row["password"]

    if not stored_hash:
        raise INVALID

    try:
        valid = bcrypt.checkpw(submitted_bytes, stored_hash.encode("utf-8"))
    except ValueError:
        # Malformed hash in DB — treat as auth failure rather than 500.
        raise INVALID

    if not valid:
        raise INVALID

    # Success — return the public profile. NEVER return password or api_token.
    return {
        "authenticated": True,
        "id": row["id"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "name": f"{row['first_name']} {row['last_name']}",
        "email": row["email"],
        "phone_number": row["phone_number"],
        "home_store": row["home_store"],
    }


#================================================================================================#
#--------------------------------------Menu Endpoints (GP2 REQUIRED)-----------------------------#
#================================================================================================#

# GP2 REQUIRED: GET /menu — return menu items (Supports filtering)
@app.get("/filter/menu_items")
def get_filtered_menu(
    category: Optional[str] = Query(None, description="Filter by category (Coffee, Espresso, etc.)"),
    size: Optional[str] = Query(None, description="Filter by size (Small, Medium, Large)"),
    min_price: Optional[float] = Query(None, gt=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, gt=0, description="Maximum price"),
    min_calories: Optional[int] = Query(None, ge=0, description="Minimum calories"),
    max_calories: Optional[int] = Query(None, ge=0, description="Maximum calories"),
):
    """
    Returns all menu items by filters.
    """
    where_clauses = []
    params = []

    # String Filters
    if category:
        where_clauses.append("LOWER(category) = LOWER(@category)")
        params.append(bigquery.ScalarQueryParameter("category", "STRING", category))
    if size:
        where_clauses.append("LOWER(size) = LOWER(@size)")
        params.append(bigquery.ScalarQueryParameter("size", "STRING", size))

    # Numerical Filters   
    if min_price is not None:
        where_clauses.append("price >= @min_price")
        params.append(bigquery.ScalarQueryParameter("min_price", "FLOAT64", min_price))
    if max_price is not None:
        where_clauses.append("price <= @max_price")
        params.append(bigquery.ScalarQueryParameter("max_price", "FLOAT64", max_price))
    if min_calories is not None:
        where_clauses.append("calories >= @min_calories")
        params.append(bigquery.ScalarQueryParameter("min_calories", "INT64", min_calories))
    if max_calories is not None:
        where_clauses.append("calories <= @max_calories")
        params.append(bigquery.ScalarQueryParameter("max_calories", "INT64", max_calories))

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
        ORDER BY name
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        query_job = client.query(query, job_config=job_config)
        results = [dict(row) for row in query_job.result()]
        
        # Check if empty
        if not results:
            return [] ## return blank array to fix cold start issue leading to reload necessity
        return results
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


# GP2 REQUIRED: GET /menu_items/{id} — return a single menu item by id (Can include which fields to return by id)
@app.get("/menu_items/{id}")
def get_menu_item_by_id(
    id: str,
    include: Optional[str] = Query(None, description="separate fields by comma")  
):
    field_map = {
        "id": "id",
        "name": "name",
        "category": "category",
        "size": "size",
        "calories": "calories",
        "price": "CAST(price AS FLOAT64) AS price"
    }

    # Default
    if not include:
        selected_fields = ["id"]
    else:
        requested = [f.strip().lower() for f in include.split(",")]

        # Always include id 
        if "id" not in requested:
            requested.append("id")

        selected_fields = []
        for field in requested:
            if field in field_map:
                selected_fields.append(field_map[field])

    select_clause = ",\n    ".join(selected_fields)

    query = f"""
        SELECT
            {select_clause}
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

# GP2 REQUIRED: GET /menu_items — return all the menu items (Can include which fields to show)
@app.get("/menu_items")
def get_all_menu_items(include: Optional[str] = Query(None, description="separate fields by comma")  
):
    field_map = {
        "id": "id",
        "name": "name",
        "category": "category",
        "size": "size",
        "calories": "calories",
        "price": "CAST(price AS FLOAT64) AS price"
    }

    selected_fields = ["id"]
    if include:
        requested = [f.strip().lower() for f in include.split(",")]
        for field in requested:
            if field in field_map:
                selected_fields.append(field_map[field])
    select_clause = ", ".join(selected_fields)

    query = f"""
        SELECT id
            {select_clause}
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
            detail=f"Location with id {id} not found."
        )

    return dict(results[0])


#================================================================================================#
#--------------------------------------Members Endpoints (GP3)-----------------------------------#
#================================================================================================#

@app.get("/members/{id}")
def get_member(id: str):
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
            detail=f"Member with id {id} not found."
        )

    return dict(results[0])


#================================================================================================#
#--------------------------------------Orders & Order History (GP3)------------------------------#
#================================================================================================#

@app.get("/order_history/members/{id}")
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
            bigquery.ScalarQueryParameter("mid", "STRING", id),
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
    return {"id": id, "count": len(orders), "orders": orders}


@app.get("/receipt/orders/{order_id}")
def get_order_receipt(order_id: str):
    """
    Returns a single order receipt including all line items.
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
    order["items"] = [dict(items) for items in items]
    return order


#================================================================================================#
#--------------------------------------Create Order Endpoint (GP3)-------------------------------#
#================================================================================================#

@app.post("/orders")
def create_order(body: dict):
    items = body.get("items", [])
    member_id = body.get("member_id")
    store_id = body.get("store_id")
    fulfillment = body.get("fulfillment", "pickup")

    if not items:
        raise HTTPException(status_code=400, detail="Cart is empty.")

    if not store_id:
        raise HTTPException(status_code=400, detail="Store is required.")

    try:
        order_id = f"ORD-{uuid.uuid4()}"
        full_items = []

        for item in items:
            menu_item_id = item.get("menu_item_id")
            quantity = int(item.get("quantity", 1))

            if not menu_item_id:
                raise HTTPException(status_code=400, detail="Missing menu_item_id.")

            menu_query = f"""
                SELECT
                    id,
                    name,
                    size,
                    CAST(price AS FLOAT64) AS price
                FROM `{GCP_PROJECT}.{DATASET}.menu_items`
                WHERE id = @menu_item_id
                LIMIT 1
            """

            menu_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("menu_item_id", "STRING", menu_item_id)
                ]
            )

            menu_results = list(client.query(menu_query, job_config=menu_config).result())

            if not menu_results:
                raise HTTPException(
                    status_code=404,
                    detail=f"Menu item with id {menu_item_id} not found."
                )

            menu_row = dict(menu_results[0])

            full_items.append({
                "menu_item_id": menu_row["id"],
                "name": menu_row["name"],
                "size": menu_row["size"],
                "price": float(menu_row["price"]),
                "quantity": quantity
            })

        items_subtotal = round(
            sum(item["price"] * item["quantity"] for item in full_items),
            2
        )

        order_discount = 0.0
        order_subtotal = round(items_subtotal - order_discount, 2)
        sales_tax = round(order_subtotal * 0.06, 2)
        order_total = round(order_subtotal + sales_tax, 2)

        order_query = f"""
            INSERT INTO `{GCP_PROJECT}.{DATASET}.orders`
                (order_id, member_id, store_id, order_date,
                 items_subtotal, order_discount, order_subtotal, sales_tax, order_total)
            VALUES
                (@order_id, @member_id, @store_id, CURRENT_TIMESTAMP(),
                 @items_subtotal, @order_discount, @order_subtotal, @sales_tax, @order_total)
        """

        order_params = [
            bigquery.ScalarQueryParameter("order_id", "STRING", order_id),
            bigquery.ScalarQueryParameter("member_id", "STRING", member_id),
            bigquery.ScalarQueryParameter("store_id", "STRING", store_id),
            bigquery.ScalarQueryParameter("items_subtotal", "FLOAT64", items_subtotal),
            bigquery.ScalarQueryParameter("order_discount", "FLOAT64", order_discount),
            bigquery.ScalarQueryParameter("order_subtotal", "FLOAT64", order_subtotal),
            bigquery.ScalarQueryParameter("sales_tax", "FLOAT64", sales_tax),
            bigquery.ScalarQueryParameter("order_total", "FLOAT64", order_total),
        ]

        client.query(
            order_query,
            job_config=bigquery.QueryJobConfig(query_parameters=order_params)
        ).result()

        for item in full_items:
            line_id = f"LINE-{uuid.uuid4()}"

            item_query = f"""
                INSERT INTO `{GCP_PROJECT}.{DATASET}.order_items`
                    (id, order_id, menu_item_id, item_name, size, quantity, price)
                VALUES
                    (@id, @order_id, @menu_item_id, @item_name, @size, @quantity, @price)
            """

            item_params = [
                bigquery.ScalarQueryParameter("id", "STRING", line_id),
                bigquery.ScalarQueryParameter("order_id", "STRING", order_id),
                bigquery.ScalarQueryParameter("menu_item_id", "STRING", item["menu_item_id"]),
                bigquery.ScalarQueryParameter("item_name", "STRING", item["name"]),
                bigquery.ScalarQueryParameter("size", "STRING", item["size"]),
                bigquery.ScalarQueryParameter("quantity", "INT64", item["quantity"]),
                bigquery.ScalarQueryParameter("price", "FLOAT64", item["price"]),
            ]

            client.query(
                item_query,
                job_config=bigquery.QueryJobConfig(query_parameters=item_params)
            ).result()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create order: {str(e)}"
        )

    return {
        "order_id": order_id,
        "member_id": member_id,
        "store_id": store_id,
        "fulfillment": fulfillment,
        "items_subtotal": items_subtotal,
        "order_discount": order_discount,
        "order_subtotal": order_subtotal,
        "sales_tax": sales_tax,
        "order_total": order_total,
        "items": full_items
    }

#================================================================================================#
#--------------------------------------Loyalty Points (GP3)--------------------------------------#
#================================================================================================#

@app.get("/points/members/{id}")
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
        WHERE member_id = @mid
          AND member_id IS NOT NULL
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


@app.get("/points/orders/{order_id}")
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
        "points_earned": int(math.floor(total))
    }


#================================================================================================#
#--------------------------------------Other Endpoints (GP3)--------------------------------------#
#================================================================================================# 


def get_bq_client():
    return bigquery.Client(project="mgmt-545-group-project-493414")

class UpdateHomeStoreRequest(BaseModel):
    home_store: str


@app.put("/update_profile/{id}/home_store")
def update_home_store(
    id: str,
    request: UpdateHomeStoreRequest,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Update member home_store after validating both the member and selected location.
    """

    member_query = f"""
        SELECT id
        FROM `{GCP_PROJECT}.{DATASET}.members`
        WHERE id = @id
        LIMIT 1
    """

    member_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", id)
        ]
    )

    validate_query = f"""
        SELECT id
        FROM `{GCP_PROJECT}.{DATASET}.locations`
        WHERE id = @home_store
        LIMIT 1
    """

    validate_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("home_store", "STRING", request.home_store)
        ]
    )

    try:
        member_result = list(
            bq.query(member_query, job_config=member_config).result()
        )

        if not member_result:
            raise HTTPException(status_code=404, detail="Member not found")

        validation_result = list(
            bq.query(validate_query, job_config=validate_config).result()
        )

        if not validation_result:
            raise HTTPException(status_code=404, detail="Location not found")

        update_query = f"""
            UPDATE `{GCP_PROJECT}.{DATASET}.members`
            SET home_store = @home_store
            WHERE id = @id
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("home_store", "STRING", request.home_store),
                bigquery.ScalarQueryParameter("id", "STRING", id),
            ]
        )

        bq.query(update_query, job_config=update_config).result()

        return {
            "message": "Home store updated successfully",
            "id": id,
            "home_store": request.home_store
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))