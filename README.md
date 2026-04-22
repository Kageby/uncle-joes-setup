# Uncle Joe’s Coffee Company — Internal Pilot Application

## Overview

Uncle Joe’s Coffee Company is a regional coffee chain with nearly 500 locations across the Upper Midwest. Known for its simple menu, fast service, and customer loyalty program — the **Uncle Joe’s Coffee Club** — the company is building an internal pilot to modernize its digital platform.

This project uses real historical data to validate system design before a full-scale rollout.

## Data Model

### `locations`

Detailed store-level information.

**Fields:**
- `id`
- `open_for_business`
- `city`, `state`, `zip_code`
- `address_one`, `address_two`
- `location_map_address`, `location_map_lat`, `location_map_lng`
- `wifi`, `drive_thru`, `door_dash`
- `email`, `phone_number`, `fax_number`
- `near_by`
- Daily hours:
  - `hours_monday_open` → `hours_sunday_close`

### `members`

Coffee Club users.

**Fields:**
- `id`
- `first_name`, `last_name`
- `email`, `phone_number`
- `home_store`
- `password` (bcrypt hash)
- `api_token`

---

### `orders`

Order-level transaction records.

**Fields:**
- `order_id`
- `member_id` (nullable)
- `store_id`
- `order_date`
- `items_subtotal`
- `order_discount`
- `order_subtotal`
- `sales_tax`
- `order_total`

---

### `order_items`

Line items per order.

**Fields:**
- `id`
- `order_id`
- `menu_item_id`
- `item_name`
- `size`
- `quantity`
- `price`

---

### `menu_items`

Menu catalog.

**Fields:**
- `id`
- `name`
- `category`
- `size`
- `calories`
- `price`

---

## Relationships

- `members.home_store → locations.id`
- `orders.member_id → members.id`
- `orders.store_id → locations.id`
- `order_items.order_id → orders.order_id`
- `order_items.menu_item_id → menu_items.id`

---

## Loyalty Points System

Loyalty points are **not stored** — they are calculated dynamically.

### Rule:
- Members earn **1 point per whole dollar spent**
- Cents are ignored
- Numbers are rounded down to the nearest whole integer

### Team Members:

Knittala, AlexaGonzalesTuesta, MaxChen2024 (Max Chen), Kageby (Dharma h)


4/22 - Max Testing commit from bash
