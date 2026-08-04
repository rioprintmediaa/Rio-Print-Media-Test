from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import motor.motor_asyncio
import secrets
from datetime import datetime, date, timedelta
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://rioprintmediaa_db_Mani:Nannavinam%401212@rioprintmedia.6e7l1ak.mongodb.net/?appName=RIOPRINTMEDIA")
DB_NAME = os.environ.get("MONGO_DB", "RioPrintMedia_Test")

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]

HTML_FILE = os.environ.get("HTML_FILE", "Rio_Sales_Tracker_ONLINE.html")

@app.on_event("startup")
async def startup():
    logger.info("RIO ERP NEU v08 starting...")
    try:
        await client.admin.command("ping")
        logger.info("MongoDB connected")
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")

@app.get("/", response_class=HTMLResponse)
async def serve_html():
    paths = [
        HTML_FILE,
        f"/app/{HTML_FILE}",
        f"./{HTML_FILE}",
    ]
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    raise HTTPException(status_code=404, detail="HTML file not found")

# ===== HELPER =====
async def next_id(collection_name):
    result = await db[collection_name].find().sort("Id", -1).limit(1).to_list(1)
    return (result[0]["Id"] + 1) if result else 1

# ===== LOGIN =====
@app.post("/api/login")
async def login(request: Request):
    data = await request.json()
    username = data.get("username", "")
    password = data.get("password", "")
    client_ip = request.client.host if request.client else ""
    user_agent = request.headers.get("user-agent", "")
    async def _log(result: str):
        await db["login_logs"].insert_one({
            "Id": await next_id("login_logs"),
            "Username": username,
            "Result": result,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "IP": client_ip,
            "UserAgent": user_agent,
        })
    user = await db["rio_users"].find_one({"username": username, "password": password})
    if user:
        await _log("Success")
        return {"success": True, "user": {"username": username, "name": user.get("name", username), "role": user.get("role", "user")}}
    # Restricted fallback: only the 'admin' username with this specific password
    # works if rio_users has no matching record. This used to accept ANY
    # non-empty username+password combo, which meant literally anyone could
    # log in as admin - fixed to a single named account with a real check.
    if username == "admin" and password == "RioAdmin@2026":
        await _log("Success (admin fallback)")
        return {"success": True, "user": {"username": "admin", "name": "Admin", "role": "admin"}}
    await _log("Failed")
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/api/login-logs")
async def get_login_logs(page: int = 1, limit: int = 50):
    skip = (page - 1) * limit
    total = await db["login_logs"].count_documents({})
    rows = await db["login_logs"].find({}, {"_id": 0}).sort("Id", -1).skip(skip).limit(limit).to_list(limit)
    return {"data": rows, "total": total, "page": page}

# ===== SALES =====
@app.get("/api/sales")
async def get_sales():
    records = await db["sales_records"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/sales")
async def add_sale(request: Request):
    data = await request.json()
    data["Id"] = await next_id("sales_records")
    await db["sales_records"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/sales/{sale_id}")
async def update_sale(sale_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    # BUG FIX: sales records are only ever inserted with "Id" (see add_sale
    # below) — "SNo" is never set on insert, so querying by SNo alone
    # matched zero documents and every edit silently no-op'd. Match either
    # field so this also covers any legacy data that does have SNo.
    await db["sales_records"].update_one({"$or": [{"Id": sale_id}, {"SNo": sale_id}]}, {"$set": data})
    return {"success": True}

@app.delete("/api/sales/{sale_id}")
async def delete_sale(sale_id: int):
    await db["sales_records"].delete_one({"$or": [{"Id": sale_id}, {"SNo": sale_id}]})
    return {"success": True}

# ===== EXPENSES =====
@app.get("/api/expenses")
async def get_expenses():
    records = await db["daily_expenses"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/expenses")
async def add_expense(request: Request):
    data = await request.json()
    data["Id"] = await next_id("daily_expenses")
    await db["daily_expenses"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/expenses/{expense_id}")
async def update_expense(expense_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["daily_expenses"].update_one({"Id": expense_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/expenses/{expense_id}")
async def delete_expense(expense_id: int):
    await db["daily_expenses"].delete_one({"Id": expense_id})
    return {"success": True}

# ===== CUSTOMERS =====
@app.get("/api/customers")
async def get_customers():
    records = await db["rio_clients"].find({}, {"_id": 0}).sort("Id", 1).to_list(1000)
    return records

@app.post("/api/customers")
async def add_customer(request: Request):
    data = await request.json()
    data["Id"] = await next_id("rio_clients")
    await db["rio_clients"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/customers/{cust_id}")
async def update_customer(cust_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_clients"].update_one({"Id": cust_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/customers/{cust_id}")
async def delete_customer(cust_id: int):
    await db["rio_clients"].delete_one({"Id": cust_id})
    return {"success": True}

# ===== PRODUCTS =====
@app.get("/api/products")
async def get_products():
    records = await db["products"].find({}, {"_id": 0}).sort("Id", 1).to_list(1000)
    return records

@app.post("/api/products")
async def add_product(request: Request):
    data = await request.json()
    data["Id"] = await next_id("products")
    await db["products"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== INVOICES =====
@app.get("/api/invoices")
async def get_invoices():
    records = await db["sales_invoices"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/invoices")
async def add_invoice(request: Request):
    data = await request.json()
    data["Id"] = await next_id("sales_invoices")
    await db["sales_invoices"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.delete("/api/invoices/{inv_id}")
async def delete_invoice(inv_id: int):
    await db["sales_invoices"].delete_one({"Id": inv_id})
    return {"success": True}

# ===== QUOTATIONS =====
@app.get("/api/quotations")
async def get_quotations():
    records = await db["quotations"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/quotations")
async def add_quotation(request: Request):
    data = await request.json()
    data["Id"] = await next_id("quotations")
    await db["quotations"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== LEDGER =====
@app.get("/api/ledger")
async def get_ledger(fy: str = "", account: str = "", month: str = ""):
    # v076: requires FY, fields: CreditAmt, DebitAmt, EntryType, Balance, AccountName, EntryDate
    query = {}
    if fy: query["FY"] = fy
    if account: query["AccountName"] = account
    if month: query["EntryDate"] = {"$regex": f"^{month}"}
    records = await db["account_ledger"].find(query, {"_id": 0}).sort(
        [("EntryDate", 1), ("Id", 1)]
    ).to_list(2000)
    # Normalize field names — support both old (Credit/Debit) and new (CreditAmt/DebitAmt)
    for r in records:
        if "Credit" in r and "CreditAmt" not in r:
            r["CreditAmt"] = r["Credit"]
        if "Debit" in r and "DebitAmt" not in r:
            r["DebitAmt"] = r["Debit"]
        if "CreditAmt" not in r: r["CreditAmt"] = 0
        if "DebitAmt" not in r: r["DebitAmt"] = 0
        if "EntryType" not in r:
            r["EntryType"] = "Credit" if (r.get("CreditAmt",0)>0) else "Debit" if (r.get("DebitAmt",0)>0) else "Manual"
    return records

@app.post("/api/ledger")
async def add_ledger(request: Request):
    data = await request.json()
    data["Id"] = await next_id("account_ledger")
    # Normalize: store as CreditAmt/DebitAmt (v076 field names)
    if "Credit" in data and "CreditAmt" not in data:
        data["CreditAmt"] = data.pop("Credit", 0)
    if "Debit" in data and "DebitAmt" not in data:
        data["DebitAmt"] = data.pop("Debit", 0)
    if "CreditAmt" not in data: data["CreditAmt"] = 0
    if "DebitAmt" not in data: data["DebitAmt"] = 0
    if "EntryType" not in data:
        data["EntryType"] = "Credit" if data["CreditAmt"]>0 else "Debit" if data["DebitAmt"]>0 else "Manual"
    await db["account_ledger"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== JOBS =====
@app.get("/api/jobs")
async def get_jobs():
    records = await db["jobs"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/jobs")
async def add_job(request: Request):
    data = await request.json()
    data["Id"] = await next_id("jobs")
    await db["jobs"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== NOTES =====
@app.get("/api/notes")
async def get_notes():
    records = await db["notes"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/notes")
async def add_note(request: Request):
    data = await request.json()
    data["Id"] = await next_id("notes")
    await db["notes"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== FOLLOWUPS =====
@app.get("/api/followups")
async def get_followups():
    records = await db["followups"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/followups")
async def add_followup(request: Request):
    data = await request.json()
    data["Id"] = await next_id("followups")
    await db["followups"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/followups/{fid}/snooze")
async def snooze_followup(fid: int, request: Request):
    """Pushes SnoozedUntil forward by the requested number of minutes from
    now, so the reminder popup won't re-trigger for this item until that
    time passes - checked client-side alongside FollowupDate/Time."""
    b = await request.json()
    minutes = int(b.get("minutes") or 10)
    snoozed_until = (datetime.now() + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    await db["followups"].update_one({"Id": fid}, {"$set": {"SnoozedUntil": snoozed_until}})
    return {"success": True, "SnoozedUntil": snoozed_until}

# ===== CONTACTS =====
@app.get("/api/contacts")
async def get_contacts(q: str = "", category: str = ""):
    query = {}
    if q:
        query["$or"] = [
            {"Name": {"$regex": q, "$options": "i"}},
            {"Phone": {"$regex": q, "$options": "i"}},
            {"Place": {"$regex": q, "$options": "i"}},
        ]
    if category and category != "all":
        query["Category"] = category
    records = await db["contacts"].find(query, {"_id": 0}).sort("Id", -1).to_list(2000)
    return {"data": records}

@app.post("/api/contacts")
async def add_contact(request: Request):
    data = await request.json()
    data["Id"] = await next_id("contacts")
    await db["contacts"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/contacts/{contact_id}")
async def update_contact(contact_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["contacts"].update_one({"Id": contact_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/contacts/{contact_id}")
async def delete_contact(contact_id: int):
    await db["contacts"].delete_one({"Id": contact_id})
    return {"success": True}

@app.get("/api/contacts/categories")
async def get_contact_categories():
    cats = await db["contacts"].distinct("Category")
    return {"data": sorted([c for c in cats if c])}

# ===== INVESTMENT DETAILS =====
# Stored as a single document with itemized rows per business stage
# (Proposal & Setup, Implementation, Kickoff). Subtotals/grand total are
# computed client-side from the row amounts, same as v213.
@app.get("/api/investment-details")
async def get_investment_details():
    record = await db["investment_details"].find_one({}, {"_id": 0})
    return record or {"stages": {}}

@app.post("/api/investment-details")
async def save_investment_details(request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["investment_details"].update_one({}, {"$set": data}, upsert=True)
    return {"success": True}

# ===== SUPPLIERS =====
@app.get("/api/suppliers")
async def get_suppliers(q: str = ""):
    query = {}
    if q:
        query = {"Name": {"$regex": q, "$options": "i"}}
    records = await db["suppliers"].find(query, {"_id": 0}).sort("Id", -1).to_list(1000)
    return {"data": records}

@app.get("/api/suppliers/{sup_id}")
async def get_supplier(sup_id: int):
    rec = await db["suppliers"].find_one({"Id": sup_id}, {"_id": 0})
    return rec or {}

@app.post("/api/suppliers")
async def add_supplier(request: Request):
    data = await request.json()
    data["Id"] = await next_id("suppliers")
    await db["suppliers"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/suppliers/{sup_id}")
async def update_supplier(sup_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["suppliers"].update_one({"Id": sup_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/suppliers/{sup_id}")
async def delete_supplier(sup_id: int):
    await db["suppliers"].delete_one({"Id": sup_id})
    return {"success": True}

# ===== PURCHASES (auto-writes Stock IN entries per line item) =====
@app.get("/api/purchases/next-no")
async def next_purchase_no():
    n = await next_id("purchases")
    return {"PurchaseNo": f"P{str(n).zfill(3)}"}

@app.get("/api/purchases")
async def get_purchases(q: str = ""):
    query = {}
    if q:
        query = {"$or": [{"PurchaseNo": {"$regex": q, "$options": "i"}}, {"SupplierName": {"$regex": q, "$options": "i"}}]}
    records = await db["purchases"].find(query, {"_id": 0}).sort("Id", -1).to_list(1000)
    return {"data": records}

@app.get("/api/purchases/{pur_id}")
async def get_purchase(pur_id: int):
    rec = await db["purchases"].find_one({"Id": pur_id}, {"_id": 0})
    return rec or {}

async def _write_purchase_stock_in(purchase_id: int, purchase_date: str, items: list):
    """Writes one Stock IN ledger entry per purchase line item. Called on
    both create and update (update first removes the old auto entries via
    _remove_purchase_stock_in, then this rewrites them fresh)."""
    for it in items:
        entry = {
            "Id": await next_id("stock_ledger"),
            "Date": purchase_date,
            "ProductName": it.get("ProductName", ""),
            "Type": "IN",
            "Qty": float(it.get("Qty") or 0),
            "Unit": it.get("Unit", "Nos"),
            "UpdatedBy": "", "Remarks": "Auto from purchase",
            "PurchaseId": purchase_id,
        }
        await db["stock_ledger"].insert_one(entry)

async def _remove_purchase_stock_in(purchase_id: int):
    await db["stock_ledger"].delete_many({"PurchaseId": purchase_id, "Type": "IN"})

@app.post("/api/purchases")
async def add_purchase(request: Request):
    data = await request.json()
    sup = await db["suppliers"].find_one({"Id": data.get("SupplierId")}, {"_id": 0})
    items = data.get("Items", [])
    total = sum(float(it.get("Qty") or 0) * float(it.get("Rate") or 0) for it in items)
    for it in items:
        it["Total"] = float(it.get("Qty") or 0) * float(it.get("Rate") or 0)
    pid = await next_id("purchases")
    doc = {
        "Id": pid,
        "PurchaseNo": f"P{str(pid).zfill(3)}",
        "SupplierId": data.get("SupplierId"),
        "SupplierName": sup.get("Name", "") if sup else "",
        "PurchaseDate": data.get("PurchaseDate", ""),
        "InvoiceRef": data.get("InvoiceRef", ""),
        "Notes": data.get("Notes", ""),
        "Items": items,
        "TotalAmount": total,
    }
    await db["purchases"].insert_one(doc)
    await _write_purchase_stock_in(pid, doc["PurchaseDate"], items)
    doc.pop("_id", None)
    return doc

@app.put("/api/purchases/{pur_id}")
async def update_purchase(pur_id: int, request: Request):
    data = await request.json()
    sup = await db["suppliers"].find_one({"Id": data.get("SupplierId")}, {"_id": 0})
    items = data.get("Items", [])
    total = sum(float(it.get("Qty") or 0) * float(it.get("Rate") or 0) for it in items)
    for it in items:
        it["Total"] = float(it.get("Qty") or 0) * float(it.get("Rate") or 0)
    updates = {
        "SupplierId": data.get("SupplierId"),
        "SupplierName": sup.get("Name", "") if sup else "",
        "PurchaseDate": data.get("PurchaseDate", ""),
        "InvoiceRef": data.get("InvoiceRef", ""),
        "Notes": data.get("Notes", ""),
        "Items": items,
        "TotalAmount": total,
    }
    await db["purchases"].update_one({"Id": pur_id}, {"$set": updates})
    # Rewrite the auto Stock IN entries so quantities/products stay in sync
    await _remove_purchase_stock_in(pur_id)
    await _write_purchase_stock_in(pur_id, updates["PurchaseDate"], items)
    rec = await db["purchases"].find_one({"Id": pur_id}, {"_id": 0})
    return rec or {"success": True}

@app.delete("/api/purchases/{pur_id}")
async def delete_purchase(pur_id: int):
    await db["purchases"].delete_one({"Id": pur_id})
    await _remove_purchase_stock_in(pur_id)
    return {"success": True}

# ===== STOCK LEDGER =====
@app.get("/api/stock/ledger")
async def get_stock_ledger(product: str = "", type_filter: str = "", date_from: str = "", date_to: str = ""):
    query = {}
    if product:
        query["ProductName"] = {"$regex": product, "$options": "i"}
    if type_filter:
        query["Type"] = type_filter
    if date_from or date_to:
        dq = {}
        if date_from: dq["$gte"] = date_from
        if date_to:   dq["$lte"] = date_to
        query["Date"] = dq
    rows = await db["stock_ledger"].find(query, {"_id": 0}).sort([("Date", 1), ("Id", 1)]).to_list(5000)
    # Running balance per product, in chronological order
    balances = {}
    for r in rows:
        p = r.get("ProductName", "")
        delta = r.get("Qty", 0) if r.get("Type") == "IN" else -r.get("Qty", 0)
        balances[p] = balances.get(p, 0) + delta
        r["Balance"] = balances[p]
    rows.reverse()  # newest first for display
    return {"data": rows}

@app.get("/api/stock/summary")
async def get_stock_summary():
    rows = await db["stock_ledger"].find({}, {"_id": 0}).sort([("Date", 1), ("Id", 1)]).to_list(5000)
    balances = {}
    for r in rows:
        p = r.get("ProductName", "")
        delta = r.get("Qty", 0) if r.get("Type") == "IN" else -r.get("Qty", 0)
        balances[p] = balances.get(p, 0) + delta
    return {"data": [{"ProductName": k, "Balance": v} for k, v in balances.items()]}

@app.post("/api/stock/out")
async def add_stock_out(request: Request):
    data = await request.json()
    data["Id"] = await next_id("stock_ledger")
    data["Type"] = "OUT"
    await db["stock_ledger"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/stock/ledger/{entry_id}")
async def update_stock_entry(entry_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    entry = await db["stock_ledger"].find_one({"Id": entry_id}, {"_id": 0})
    if entry and entry.get("PurchaseId"):
        return {"error": "Auto stock-in entries can't be edited directly — edit the linked purchase instead."}
    await db["stock_ledger"].update_one({"Id": entry_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/stock/ledger/{entry_id}")
async def delete_stock_entry(entry_id: int):
    entry = await db["stock_ledger"].find_one({"Id": entry_id}, {"_id": 0})
    if entry and entry.get("PurchaseId"):
        return {"error": "Auto stock-in entries can't be deleted directly — delete the linked purchase instead."}
    await db["stock_ledger"].delete_one({"Id": entry_id})
    return {"success": True}

# ===== ATTENDANCE =====
@app.get("/api/attendance")
async def get_attendance():
    records = await db["attendance"].find({}, {"_id": 0}).sort("Id", -1).to_list(1000)
    return records

@app.post("/api/attendance")
async def add_attendance(request: Request):
    data = await request.json()
    data["Id"] = await next_id("attendance")
    await db["attendance"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== USERS =====
@app.get("/api/users")
async def get_users():
    records = await db["rio_users"].find({}, {"_id": 0, "password": 0}).to_list(1000)
    return records

@app.post("/api/users")
async def add_user(request: Request):
    data = await request.json()
    data["Id"] = await next_id("rio_users")
    await db["rio_users"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

# ===== EXPENSE CATEGORIES =====
@app.get("/api/expense_categories")
async def get_expense_categories():
    records = await db["expense_categories"].find({}, {"_id": 0}).to_list(200)
    if not records:
        # Return default categories if none in DB
        return [
            {"Category": "Purchases & Supplies"},
            {"Category": "Salary & Wages"},
            {"Category": "Admin & Finance"},
            {"Category": "Operations & Maintenance"},
            {"Category": "Transport & Logistics"},
            {"Category": "Rent & Utilities"},
            {"Category": "Other"},
        ]
    return records

# ===== JOBS PUT =====
@app.put("/api/jobs/{job_id}")
async def update_job(job_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["jobs"].update_one({"Id": job_id}, {"$set": data})
    return {"success": True}

# ===== FOLLOWUPS PUT =====
@app.put("/api/followups/{fu_id}")
async def update_followup(fu_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["followups"].update_one({"Id": fu_id}, {"$set": data})
    return {"success": True}


@app.post("/api/change-password")
async def change_password(request: Request):
    data = await request.json()
    username = data.get("username", "")
    current_password = data.get("current_password", "")
    new_password = data.get("new_password", "")
    if not username or not current_password or not new_password:
        raise HTTPException(status_code=400, detail="All fields required")
    user = await db["rio_users"].find_one({"username": username, "password": current_password})
    if not user:
        raise HTTPException(status_code=401, detail="Current password is incorrect")
    await db["rio_users"].update_one({"username": username}, {"$set": {"password": new_password}})
    return {"success": True, "message": "Password updated successfully"}

# ===== COMPANY DETAILS =====
@app.get("/api/company")
async def get_company():
    record = await db["company_details"].find_one({}, {"_id": 0})
    return record or {}

@app.post("/api/company")
async def save_company(request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["company_details"].update_one(
        {}, {"$set": data}, upsert=True
    )
    return {"success": True}


@app.get("/api/balances")
async def get_balances():
    records = await db["account_balances"].find({}, {"_id": 0}).to_list(100)
    return records

@app.post("/api/balances")
async def set_balance(request: Request):
    data = await request.json()
    await db["account_balances"].update_one(
        {"Account": data.get("Account")},
        {"$set": data},
        upsert=True
    )
    return {"success": True}


@app.get("/api/summary")
async def get_summary():
    sales = await db["sales_records"].find({}, {"_id": 0}).to_list(10000)
    expenses = await db["daily_expenses"].find({}, {"_id": 0}).to_list(10000)
    total_sales = sum(r.get("TotalAmount", 0) for r in sales)
    total_received = sum(r.get("AdvanceAmount", 0) for r in sales)
    total_expenses = sum(r.get("Amount", 0) for r in expenses)
    return {
        "total_sales": total_sales,
        "total_received": total_received,
        "pending_balance": total_sales - total_received,
        "total_expenses": total_expenses,
        "net_balance": total_received - total_expenses
    }


# ══════════════════════════════════════════════════════════════
#  OPTION B — MISSING ENDPOINTS ADDED TO v16 API
#  Mapped to v076 MongoDB collection names
# ══════════════════════════════════════════════════════════════

# ── AUTH (v076 paths) ─────────────────────────────────────────
@app.post("/api/auth/login")
async def auth_login(request: Request):
    data = await request.json()
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    user = await db["rio_users"].find_one({"username": username})
    if user:
        stored = user.get("password", "")
        # plain text match (v076 style until bcrypt added)
        if stored == password or password == stored:
            return {"ok": True, "username": user.get("username", username),
                    "name": user.get("name", username),
                    "role": user.get("role", "admin"),
                    "scope": user.get("scope", "all"),
                    "token": secrets.token_hex(16)}
    # dev fallback
    if username and password:
        return {"ok": True, "username": username, "name": username,
                "role": "admin", "scope": "all", "token": secrets.token_hex(16)}
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/auth/users")
async def get_auth_users():
    users = await db["rio_users"].find({}, {"_id": 0, "password": 0}).to_list(200)
    return users

@app.post("/api/auth/users")
async def create_auth_user(request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_users"].insert_one(data)
    return {"success": True}

@app.put("/api/auth/users/{username}")
async def update_auth_user(username: str, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_users"].update_one({"username": username}, {"$set": data})
    return {"success": True}

@app.delete("/api/auth/users/{username}")
async def delete_auth_user(username: str):
    await db["rio_users"].delete_one({"username": username})
    return {"success": True}

# ── ROLES ─────────────────────────────────────────────────────
@app.get("/api/roles")
async def get_roles():
    roles = await db["rio_custom_roles"].find({}, {"_id": 0}).to_list(100)
    return roles

@app.post("/api/roles")
async def create_role(request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_custom_roles"].insert_one(data)
    return {"success": True}

@app.put("/api/roles/{role_id}")
async def update_role(role_id: str, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_custom_roles"].update_one({"RoleId": role_id}, {"$set": data}, upsert=True)
    return {"success": True}

@app.delete("/api/roles/{role_id}")
async def delete_role(role_id: str):
    await db["rio_custom_roles"].delete_one({"RoleId": role_id})
    return {"success": True}

# ── CLIENTS (sales customers, v076: rio_clients) ──────────────
@app.get("/api/clients")
async def get_clients():
    clients = await db["rio_clients"].find({}, {"_id": 0}).to_list(2000)
    return clients

@app.get("/api/rio_clients")
async def get_rio_clients():
    clients = await db["rio_clients"].find({}, {"_id": 0}).to_list(2000)
    return clients

@app.post("/api/clients")
async def create_client(request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_clients"].update_one(
        {"ClientName": data.get("ClientName", "")},
        {"$set": data}, upsert=True)
    return {"success": True}

@app.delete("/api/clients/{client_name:path}")
async def delete_client(client_name: str):
    await db["rio_clients"].delete_one({"ClientName": client_name})
    return {"success": True}

# ── CATEGORIES (sales) ────────────────────────────────────────
@app.get("/api/categories")
async def get_categories():
    # v076: returns distinct CategoryName strings from expense_categories
    rows = await db["expense_categories"].distinct("CategoryName")
    return sorted(rows)

@app.get("/api/categories/all")
async def get_categories_all():
    rows = await db["expense_categories"].find({}, {"_id": 0}).to_list(2000)
    from collections import defaultdict
    mp = defaultdict(list)
    for r in rows:
        cat = r.get("CategoryName", "")
        sub = r.get("SubCategoryName", "")
        if cat and sub:
            mp[cat].append(sub)
    return [{"category": k, "subcats": v} for k, v in mp.items()]

@app.get("/api/categories/subcats")
async def get_subcategories(cat: str = ""):
    # v076: returns plain string array of SubCategoryName
    rows = await db["expense_categories"].find({"CategoryName": cat}, {"_id": 0, "SubCategoryName": 1}).to_list(500)
    subs = [r["SubCategoryName"] for r in rows if r.get("SubCategoryName")]
    return subs if subs else ["Other"]

@app.post("/api/categories")
async def create_category(request: Request):
    data = await request.json()
    data.pop("_id", None)
    cn = (data.get("CategoryName") or data.get("Category") or "").strip()
    sn = (data.get("SubCategoryName") or data.get("SubCategory") or "").strip()
    if cn and sn:
        exists = await db["expense_categories"].find_one({"CategoryName": cn, "SubCategoryName": sn})
        if not exists:
            last = await db["expense_categories"].find_one(sort=[("Id", -1)])
            new_id = (last["Id"] + 1) if last and "Id" in last else 1
            await db["expense_categories"].insert_one({"Id": new_id, "CategoryName": cn, "SubCategoryName": sn})
    return {"success": True}


# ── ACCOUNT BALANCES (v076: account_balances) ─────────────────
@app.get("/api/accountbalances")
async def get_account_balances():
    records = await db["account_balances"].find({}, {"_id": 0}).to_list(100)
    return records

@app.post("/api/accountbalances")
async def save_account_balance(request: Request):
    data = await request.json()
    data.pop("_id", None)
    account = data.get("Account", "")
    await db["account_balances"].update_one(
        {"Account": account}, {"$set": data}, upsert=True)
    return {"success": True}

@app.delete("/api/accountbalances/{ab_id}")
async def delete_account_balance(ab_id: int):
    await db["account_balances"].delete_one({"Id": ab_id})
    return {"success": True}

# ── LEDGER OPENING BALANCES ───────────────────────────────────
@app.get("/api/ledger/opening")
async def get_ledger_opening(fy: str = ""):
    query = {"FY": fy} if fy else {}
    rec = await db["ledger_opening"].find_one(query, {"_id": 0})
    return rec or {}

@app.post("/api/ledger/opening")
async def save_ledger_opening(request: Request):
    data = await request.json()
    data.pop("_id", None)
    fy = data.get("FY", "")
    await db["ledger_opening"].update_one({"FY": fy}, {"$set": data}, upsert=True)
    return {"success": True}

@app.get("/api/ledger/prev-closing")
async def get_prev_closing(fy: str = ""):
    rec = await db["ledger_opening"].find_one({"FY": fy}, {"_id": 0})
    return rec or {}

@app.delete("/api/ledger/{led_id}")
async def delete_ledger_entry(led_id: int):
    await db["account_ledger"].delete_one({"Id": led_id})
    return {"success": True}

# ── BILLING CUSTOMERS (v076: rio_billing_customers) ───────────
@app.get("/api/billing/quotations/{quot_id}/pdf")
async def download_quotation_pdf(quot_id: int):
    """Generate quotation PDF via WeasyPrint."""
    try:
        from weasyprint import HTML as WeasyprintHTML
    except ImportError:
        return JSONResponse({"error": "WeasyPrint not installed"}, status_code=500)

    quot = await db["quotations"].find_one({"Id": quot_id}, {"_id": 0})
    if not quot:
        return JSONResponse({"error": "Quotation not found"}, status_code=404)

    items = quot.get("Items") or quot.get("items") or []
    cust = {}
    if quot.get("CustomerId"):
        c = await db["rio_clients"].find_one({"Id": quot["CustomerId"]}, {"_id": 0})
        if c:
            cust = {"Name":c.get("ClientName",""),"BillToAddress":c.get("BillToAddress",""),"ShipToAddress":c.get("ShipToAddress",c.get("BillToAddress","")),"Mobile":c.get("Mobile",""),"GSTNo":c.get("GSTNo",""),"State":c.get("State",""),"StateCode":c.get("StateCode","33")}

    def fmt(v):
        try: return f"₹ {float(v or 0):,.2f}"
        except: return "₹ 0.00"

    def words(n):
        try: n=int(round(float(n or 0)))
        except: return "Zero"
        ones=["","One","Two","Three","Four","Five","Six","Seven","Eight","Nine","Ten","Eleven","Twelve","Thirteen","Fourteen","Fifteen","Sixteen","Seventeen","Eighteen","Nineteen"]
        tens=["","","Twenty","Thirty","Forty","Fifty","Sixty","Seventy","Eighty","Ninety"]
        def t(n): return ones[n] if n<20 else tens[n//10]+(" "+ones[n%10] if n%10 else "")
        def h(n):
            if n>=10000000: return h(n//10000000)+" Crore"+((" "+h(n%10000000)) if n%10000000 else "")
            if n>=100000: return h(n//100000)+" Lakh"+((" "+h(n%100000)) if n%100000 else "")
            if n>=1000: return h(n//1000)+" Thousand"+((" "+h(n%1000)) if n%1000 else "")
            if n>=100: return ones[n//100]+" Hundred"+((" "+t(n%100)) if n%100 else "")
            return t(n)
        return (h(n) if n else "Zero")+" Rupees Only"

    cust_name=cust.get("Name") or quot.get("CustomerName","")
    cust_addr=cust.get("BillToAddress",""); cust_ship=cust.get("ShipToAddress",cust_addr)
    cust_mobile=cust.get("Mobile",""); cust_gst=cust.get("GSTNo") or quot.get("CustomerGST","")
    cust_state=cust.get("State") or quot.get("PlaceOfSupply","Tamil Nadu"); cust_code=cust.get("StateCode","33")
    sub=float(quot.get("SubTotal",0)); cgst=float(quot.get("CGST",0)); sgst=float(quot.get("SGST",0))
    igst=float(quot.get("IGST",0)); tot=float(quot.get("TotalAmount",quot.get("GrandTotal",0)))

    item_rows_html=""
    for i,it in enumerate(items):
        item_rows_html+=f"<tr style='border-bottom:1px solid #555;'><td class='center'>{i+1}</td><td>{it.get('ProductName','')}</td><td class='center'>{it.get('HSN','')}</td><td class='center'>{it.get('Qty','')} Nos</td><td class='right'>{float(it.get('Rate',0)):.2f}</td><td class='right'>{float(it.get('TaxableValue',0)):.2f}</td><td class='center'>{it.get('GSTRate',0)}</td><td class='right'>{float(it.get('Total',0)):.2f}</td></tr>"
    filler=max(0,7-len(items))
    item_rows_html+="<tr style='height:22px;'><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>"*filler

    html_content=f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><style>*{{box-sizing:border-box;margin:0;padding:0;}}body{{font-family:Arial,sans-serif;font-size:10pt;color:#000;}}.page{{width:200mm;height:287mm;margin:0 auto;padding:3mm 4mm;overflow:hidden;}}.header-table{{width:100%;border-collapse:collapse;border:2px solid #000;}}.co-cell{{vertical-align:middle;text-align:center;padding:6px;}}.co-name{{font-size:19pt;font-weight:900;letter-spacing:1px;}}.rio{{color:#005073;}}.prnt{{color:#6a1b9a;}}.co-det{{font-size:9pt;line-height:1.6;margin-top:4px;}}.title-bar{{background:#6a1b9a;color:#fff;text-align:center;font-size:16pt;font-weight:900;padding:8px;letter-spacing:3px;border:2px solid #000;border-top:none;}}.info-table{{width:100%;border-collapse:collapse;border:2px solid #000;border-top:none;}}.info-table td{{padding:4px 5px;font-size:8.5pt;border-bottom:1px solid #ddd;}}.lbl{{font-weight:bold;white-space:nowrap;}}.addr-table{{width:100%;border-collapse:collapse;border:2px solid #000;border-top:none;}}.addr-table td{{padding:6px 10px;vertical-align:top;font-size:9pt;width:50%;border-right:1px solid #000;height:120px;}}.addr-table td:last-child{{border-right:none;}}.sec-lbl{{font-weight:bold;font-size:8pt;border-bottom:1px solid #bbb;margin-bottom:3px;color:#6a1b9a;}}.cust-name{{font-weight:bold;font-size:10pt;}}.items-table{{width:100%;border-collapse:collapse;border:2px solid #000;border-top:none;}}.items-table thead tr{{background:#6a1b9a;}}.items-table th{{color:#fff;padding:4px;font-size:9pt;text-align:center;border-right:1.5px solid #9c5bb5;border-bottom:2px solid #000;}}.items-table tbody td{{border:none;border-right:1px solid #555;padding:3px 4px;font-size:9pt;}}.footer-table{{width:100%;border-collapse:collapse;border:2px solid #000;border-top:none;}}.rupees-bar{{background:#6a1b9a;color:#fff;font-weight:bold;font-size:10pt;border:2px solid #000;border-top:none;display:flex;}}.rt-words{{flex:1;padding:4px 8px;font-style:italic;}}.rt-label{{padding:4px 8px;text-align:right;white-space:nowrap;border-left:1px solid rgba(255,255,255,0.4);}}.rt-amount{{padding:4px 10px;text-align:right;font-size:11pt;border-left:1px solid rgba(255,255,255,0.4);width:110px;}}.terms-box{{width:100%;border:2px solid #000;border-top:none;padding:4px 8px;font-size:9pt;line-height:1.5;}}.sign-box{{width:100%;border-collapse:collapse;border:2px solid #000;border-top:none;}}.sign-box td{{padding:6px 10px;vertical-align:bottom;font-size:8pt;height:60px;}}.center{{text-align:center;}}.right{{text-align:right;}}@page{{size:A4;margin:5mm 4mm;}}</style></head><body><div class="page">
<table class="header-table"><tr><td style="width:130px;vertical-align:middle;text-align:center;padding:4px;border-right:2px solid #000;"><div style="font-size:22pt;font-weight:900;color:#005073;">R</div><div style="font-size:8pt;color:#6a1b9a;font-weight:700;">PRINT</div></td><td class="co-cell"><div class="co-name"><span class="rio">RIO </span><span class="prnt">PRINT MEDIA</span></div><div class="co-det">RSF No: 304/6, Sembampalayam, Erode – 638107<br>Ph: 9962011515 | GSTIN: 33BCFPR3860P1Z2</div></td></tr></table>
<div class="title-bar">QUOTATION</div>
<table class="info-table"><colgroup><col style="width:16%"><col style="width:2%"><col style="width:32%"><col style="width:1%"><col style="width:18%"><col style="width:2%"><col></colgroup>
<tr><td class="lbl">Quotation No</td><td>:</td><td><strong>{quot.get('QuotationNo','')}</strong></td><td style="border-right:2px solid #000;padding:0;"></td><td class="lbl" style="padding-left:14px;">Payment Terms</td><td>:</td><td>{quot.get('PaymentTerms','')}</td></tr>
<tr><td class="lbl">Quotation Date</td><td>:</td><td>{quot.get('QuotationDate','')}</td><td style="border-right:2px solid #000;padding:0;"></td><td class="lbl" style="padding-left:14px;">Valid Till</td><td>:</td><td>{quot.get('ValidTill','')}</td></tr>
<tr><td class="lbl">State &amp; Code</td><td>:</td><td>Tamil Nadu Code: 33</td><td style="border-right:2px solid #000;padding:0;"></td><td class="lbl" style="padding-left:14px;">Place of Supply</td><td>:</td><td>{cust_state} Code: {cust_code}</td></tr>
</table>
<table class="addr-table"><tr><td><div class="sec-lbl">Bill To:</div><div style="height:4px;"></div><div class="cust-name">{cust_name}</div><div>{cust_addr}</div></td><td><div class="sec-lbl">Ship To:</div><div style="height:4px;"></div><div class="cust-name">{cust_name}</div><div>{cust_ship}</div></td></tr></table>
<table class="items-table"><thead><tr><th style="width:30px;">S.No</th><th style="text-align:left;padding-left:6px;">Product / Description</th><th style="width:78px;">HSN</th><th style="width:60px;">Qty</th><th style="width:52px;">Rate</th><th style="width:80px;text-align:right;">Taxable Value</th><th style="width:48px;">GST%</th><th style="width:72px;text-align:right;">Total</th></tr></thead><tbody>{item_rows_html}</tbody></table>
<table class="footer-table"><tr><td style="vertical-align:middle;padding:4px 10px;width:72%;font-size:9pt;"></td><td style="vertical-align:top;padding:0;width:28%;"><table style="width:100%;border-collapse:collapse;"><tr style="border-bottom:1px solid #bbb;"><td style="padding:3px 8px;text-align:right;font-size:9pt;">Taxable Amount</td><td style="padding:3px 8px;text-align:right;font-size:9pt;font-weight:bold;border-left:1.5px solid #555;width:80px;">{fmt(sub)}</td></tr><tr style="border-bottom:1px solid #bbb;"><td style="padding:3px 8px;text-align:right;font-size:9pt;">CGST</td><td style="padding:3px 8px;text-align:right;border-left:1.5px solid #555;">{fmt(cgst)}</td></tr><tr style="border-bottom:1px solid #bbb;"><td style="padding:3px 8px;text-align:right;font-size:9pt;">SGST</td><td style="padding:3px 8px;text-align:right;border-left:1.5px solid #555;">{fmt(sgst)}</td></tr><tr><td style="padding:3px 8px;text-align:right;font-size:9pt;">IGST</td><td style="padding:3px 8px;text-align:right;border-left:1.5px solid #555;">{fmt(igst)}</td></tr></table></td></tr></table>
<div class="rupees-bar"><span class="rt-words">Rupees: {words(tot)}</span><span class="rt-label">Total Amount</span><span class="rt-amount">{fmt(tot)}</span></div>
<div class="terms-box"><strong>Terms &amp; Conditions :</strong><br>1. This quotation is valid for 7 days from the date of issue. 2. Prices are subject to change without prior notice after validity. 3. Subject to Erode Jurisdiction.</div>
<table class="sign-box"><tr><td style="width:60%;border-right:2px solid #000;font-style:italic;color:#333;">Please verify all details before placing the order</td><td style="text-align:right;">For <strong>RIO PRINT MEDIA</strong><br><br>Authorized Signatory</td></tr></table>
<div style="text-align:center;font-size:8pt;color:#555;padding:4px 0 2px;font-style:italic;border-top:1px dashed #ccc;margin-top:2px;">This is a computer-generated document. No signature is required.</div>
</div></body></html>"""

    try:
        pdf_bytes = WeasyprintHTML(string=html_content, base_url=None).write_pdf()
    except Exception as e:
        return JSONResponse({"error": f"PDF generation failed: {str(e)}"}, status_code=500)

    quot_no   = quot.get("QuotationNo", str(quot_id))
    safe_cust = "".join(c if c.isalnum() or c in "-_ " else "" for c in cust_name).strip().replace(" ","_")[:30]
    filename  = f"Quotation_{safe_cust}_{quot_no}.pdf"

    from fastapi.responses import Response
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.get("/api/billing/customers")
async def get_billing_customers(name: str = "", q: str = ""):
    # v076: reads from rio_clients, returns Name field (not CustomerName)
    search = name or q
    query = {}
    if search:
        query = {"$or": [
            {"ClientName": {"$regex": search, "$options": "i"}},
            {"Mobile": {"$regex": search, "$options": "i"}},
        ]}
    rows = await db["rio_clients"].find(query, {"_id": 0}).sort("ClientName", 1).to_list(2000)
    result = []
    for r in rows:
        result.append({
            "Id": r.get("Id"),
            "Name": r.get("ClientName", ""),
            "CustomerName": r.get("ClientName", ""),  # alias
            "BillToAddress": r.get("BillToAddress", ""),
            "ShipToAddress": r.get("ShipToAddress", ""),
            "State": r.get("State", ""),
            "StateCode": r.get("StateCode", "33"),
            "Mobile": r.get("Mobile", ""),
            "GSTNo": r.get("GSTNo", ""),
            "Email": r.get("Email", ""),
            "CustomerType": r.get("CustomerType", ""),
        })
    return result

@app.get("/api/billing/customers/byname")
async def get_billing_customer_byname(name: str = ""):
    cust = await db["rio_clients"].find_one(
        {"ClientName": {"$regex": f"^{name}$", "$options": "i"}}, {"_id": 0})
    if cust:
        return {"Id": cust.get("Id"), "Name": cust.get("ClientName",""), "CustomerName": cust.get("ClientName",""),
                "State": cust.get("State",""), "StateCode": cust.get("StateCode","33"),
                "Mobile": cust.get("Mobile",""), "GSTNo": cust.get("GSTNo",""), "Email": cust.get("Email","")}
    return {}

@app.get("/api/billing/customers/{cust_id}")
async def get_billing_customer(cust_id: int):
    cust = await db["rio_clients"].find_one({"Id": cust_id}, {"_id": 0})
    if cust:
        return {"Id": cust.get("Id"), "Name": cust.get("ClientName",""), "CustomerName": cust.get("ClientName",""),
                "BillToAddress": cust.get("BillToAddress",""), "ShipToAddress": cust.get("ShipToAddress",""),
                "State": cust.get("State",""), "StateCode": cust.get("StateCode","33"),
                "Mobile": cust.get("Mobile",""), "GSTNo": cust.get("GSTNo",""), "Email": cust.get("Email","")}
    return {}

@app.post("/api/billing/customers")
async def create_billing_customer(request: Request):
    data = await request.json()
    data.pop("_id", None)
    # Auto-increment Id
    last = await db["rio_billing_customers"].find_one(sort=[("Id", -1)])
    data["Id"] = (last["Id"] + 1) if last and "Id" in last else 1
    await db["rio_billing_customers"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/billing/customers/{cust_id}")
async def update_billing_customer(cust_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["rio_billing_customers"].update_one({"Id": cust_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/billing/customers/{cust_id}")
async def delete_billing_customer(cust_id: int):
    await db["rio_billing_customers"].delete_one({"Id": cust_id})
    return {"success": True}

# ── BILLING PRODUCTS (v076: rio_products) ────────────────────
@app.get("/api/billing/products")
async def get_billing_products(q: str = ""):
    # v076: collection = "products", fields: Code, Name, PrintName, HSN, Category, Unit, GSTRate
    if q:
        query = {"$or": [{"Name": {"$regex": q, "$options": "i"}}, {"Code": {"$regex": q, "$options": "i"}}]}
    else:
        query = {}
    prods = await db["products"].find(query, {"_id": 0}).sort("Code", 1).to_list(500)
    return prods

@app.get("/api/billing/products/nextcode")
async def get_next_product_code():
    # v076: Code field, format P001, P002...
    last = await db["products"].find_one(sort=[("Code", -1)])
    if last and last.get("Code", "").startswith("P"):
        try:
            n = int(last["Code"][1:]) + 1
            return {"code": f"P{n:03d}"}
        except: pass
    count = await db["products"].count_documents({})
    return {"code": f"P{count+1:03d}"}

@app.get("/api/billing/products/{prod_id}")
async def get_billing_product(prod_id: int):
    prod = await db["products"].find_one({"Id": prod_id}, {"_id": 0})
    return prod or {}

@app.post("/api/billing/products")
async def create_billing_product(request: Request):
    data = await request.json()
    data.pop("_id", None)
    last = await db["products"].find_one(sort=[("Id", -1)])
    data["Id"] = (last["Id"] + 1) if last and "Id" in last else 1
    await db["products"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.put("/api/billing/products/{prod_id}")
async def update_billing_product(prod_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["products"].update_one({"Id": prod_id}, {"$set": data})
    return {"success": True}

@app.delete("/api/billing/products/{prod_id}")
async def delete_billing_product(prod_id: int):
    await db["products"].delete_one({"Id": prod_id})
    return {"success": True}

# ── BILLING INVOICES (v076: sales_invoices) ───────────────────
@app.get("/api/billing/invoices/next")
async def get_next_invoice_no(type: str = "GST", fy: str = ""):
    import re as _re
    if not fy:
        from datetime import datetime
        y, m = datetime.now().year, datetime.now().month
        s = y if m >= 4 else y - 1
        fy = f"{s}-{str(s+1)[-2:]}"
    # v076 exact logic: GST uses R## prefix, NONGST uses RN## prefix
    is_gst = type.upper() not in ("NONGST", "NON-GST", "NON_GST")
    if is_gst:
        # Find max number among R01, R02... (not RN)
        pipeline = [
            {"$match": {"InvoiceNo": {"$regex": r"^R\d"}}},
            {"$project": {"num": {"$toInt": {"$substr": ["$InvoiceNo", 1, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = await db["sales_invoices"].aggregate(pipeline).to_list(1)
        n = (res[0]["max"] if res else 0) + 1
        inv_no = f"R{n:02d}"
    else:
        # Find max number among RN01, RN02...
        pipeline = [
            {"$match": {"InvoiceNo": {"$regex": r"^RN\d"}}},
            {"$project": {"num": {"$toInt": {"$substr": ["$InvoiceNo", 2, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = await db["sales_invoices"].aggregate(pipeline).to_list(1)
        n = (res[0]["max"] if res else 0) + 1
        inv_no = f"RN{n:02d}"
    return {"InvoiceNo": inv_no, "invoiceNo": inv_no}

# v076 alias
@app.get("/api/invoices/next")
async def get_next_invoice_no_alias(type: str = "GST", fy: str = ""):
    return await get_next_invoice_no(type=type, fy=fy)

@app.get("/api/billing/invoices/peek")
async def peek_invoice_no(type: str = "GST", fy: str = ""):
    return await get_next_invoice_no(type=type, fy=fy)

@app.get("/api/billing/invoices/byno")
async def get_invoice_byno(no: str = ""):
    inv = await db["sales_invoices"].find_one({"InvoiceNo": no}, {"_id": 0})
    if not inv:
        return {}
    items = await db["sales_items"].find(
        {"InvoiceId": inv.get("Id")}, {"_id": 0}).to_list(50)
    inv["items"] = items
    return inv

@app.get("/api/billing/invoices")
async def get_billing_invoices(from_date: str = "", to_date: str = "",
                                inv_type: str = "", customer: str = ""):
    query = {}
    if from_date: query["InvoiceDate"] = {"$gte": from_date}
    if to_date:
        query.setdefault("InvoiceDate", {})
        query["InvoiceDate"]["$lte"] = to_date
    if inv_type and inv_type != "All": query["InvoiceType"] = inv_type
    if customer: query["CustomerName"] = {"$regex": customer, "$options": "i"}
    invs = await db["sales_invoices"].find(query, {"_id": 0}).sort("Id", -1).to_list(500)
    return invs

@app.get("/api/billing/invoices/{inv_id}")
async def get_billing_invoice(inv_id: int):
    inv = await db["sales_invoices"].find_one({"Id": inv_id}, {"_id": 0})
    if not inv: return {}
    items = await db["sales_items"].find({"InvoiceId": inv_id}, {"_id": 0}).to_list(50)
    inv["items"] = items
    return inv

@app.post("/api/billing/invoices")
async def create_billing_invoice(request: Request):
    data = await request.json()
    data.pop("_id", None)
    items = data.pop("items", [])
    last = await db["sales_invoices"].find_one(sort=[("Id", -1)])
    inv_id = (last["Id"] + 1) if last and "Id" in last else 1
    data["Id"] = inv_id
    await db["sales_invoices"].insert_one(data)
    for i, item in enumerate(items):
        item.pop("_id", None)
        item["InvoiceId"] = inv_id
        item["RowNo"] = i + 1
        await db["sales_items"].insert_one(item)
    return {"success": True, "Id": inv_id, "InvoiceNo": data.get("InvoiceNo", "")}

@app.put("/api/billing/invoices/{inv_id}")
async def update_billing_invoice(inv_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    items = data.pop("items", None)
    await db["sales_invoices"].update_one({"Id": inv_id}, {"$set": data})
    if items is not None:
        await db["sales_items"].delete_many({"InvoiceId": inv_id})
        for i, item in enumerate(items):
            item.pop("_id", None)
            item["InvoiceId"] = inv_id
            item["RowNo"] = i + 1
            await db["sales_items"].insert_one(item)
    return {"success": True}

@app.delete("/api/billing/invoices/{inv_id}")
async def delete_billing_invoice(inv_id: int):
    await db["sales_invoices"].delete_one({"Id": inv_id})
    await db["sales_items"].delete_many({"InvoiceId": inv_id})
    return {"success": True}

# ── INVOICE PDF GENERATION (WeasyPrint) ──────────────────────
@app.get("/api/billing/invoices/{inv_id}/pdf")
async def download_invoice_pdf(inv_id: int):
    """Generate invoice PDF via WeasyPrint (v076-exact layout)."""
    try:
        from weasyprint import HTML as WP
    except ImportError:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "WeasyPrint not installed on server. Run: pip install weasyprint==62.3"}, status_code=501)

    inv = await db["sales_invoices"].find_one({"Id": inv_id}, {"_id": 0})
    if not inv:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Invoice not found"}, status_code=404)

    items = inv.get("Items") or inv.get("items") or []
    if not items:
        db_items = await db["sales_items"].find({"InvoiceId": inv_id}, {"_id": 0}).to_list(100)
        items = db_items

    cust = {}
    if inv.get("CustomerId"):
        c = await db["rio_clients"].find_one({"Id": inv["CustomerId"]}, {"_id": 0})
        if c:
            cust = {
                "Name": c.get("ClientName",""),
                "BillToAddress": c.get("BillToAddress",""),
                "ShipToAddress": c.get("ShipToAddress", c.get("BillToAddress","")),
                "Mobile": c.get("Mobile",""),
                "GSTNo": c.get("GSTNo",""),
                "State": c.get("State","Tamil Nadu"),
                "StateCode": c.get("StateCode","33"),
            }

    def fmt2(v):
        try: return f"{float(v or 0):.2f}"
        except: return "0.00"

    def fmtc(v):
        try: return f"&#8377; {float(v or 0):,.2f}"
        except: return "&#8377; 0.00"

    def words(n):
        try: n = int(round(float(n or 0)))
        except: return "Zero"
        ones=["","One","Two","Three","Four","Five","Six","Seven","Eight","Nine","Ten","Eleven","Twelve","Thirteen","Fourteen","Fifteen","Sixteen","Seventeen","Eighteen","Nineteen"]
        tens=["","","Twenty","Thirty","Forty","Fifty","Sixty","Seventy","Eighty","Ninety"]
        def t(n): return ones[n] if n < 20 else tens[n//10] + (" " + ones[n%10] if n%10 else "")
        def h(n):
            if n >= 10000000: return h(n//10000000)+" Crore"+((" "+h(n%10000000)) if n%10000000 else "")
            if n >= 100000:   return h(n//100000)+" Lakh"+((" "+h(n%100000)) if n%100000 else "")
            if n >= 1000:     return h(n//1000)+" Thousand"+((" "+h(n%1000)) if n%1000 else "")
            if n >= 100:      return ones[n//100]+" Hundred"+((" "+t(n%100)) if n%100 else "")
            return t(n)
        return (h(n) if n else "Zero") + " Rupees Only"

    is_non_gst = (inv.get("BillingType","") in ("NON-GST","NONGST")) or (inv.get("InvoiceNo","")).startswith("RN")
    title_bar  = "CASH BILL" if is_non_gst else "TAX INVOICE"

    cust_name  = cust.get("Name") or inv.get("CustomerName","")
    bill_addr  = (cust.get("BillToAddress") or "").replace("\n","<br>")
    ship_addr  = (cust.get("ShipToAddress") or cust.get("BillToAddress") or "").replace("\n","<br>") or bill_addr
    cust_mob   = cust.get("Mobile","")
    cust_gst   = cust.get("GSTNo") or inv.get("CustomerGST","")
    cust_state = cust.get("State") or inv.get("PlaceOfSupply","Tamil Nadu")
    cust_code  = cust.get("StateCode") or inv.get("PlaceOfSupplyCode","33")

    sub  = float(inv.get("SubTotal") or 0)
    cgst = float(inv.get("CGST") or 0)
    sgst = float(inv.get("SGST") or 0)
    igst = float(inv.get("IGST") or 0)
    tot  = float(inv.get("TotalAmount") or inv.get("GrandTotal") or 0)

    # Build item rows (v076-exact: 8 cols, SizeNotes as sub-line)
    item_rows = ""
    for i, it in enumerate(items):
        name   = it.get("ProductName") or it.get("Description","")
        size   = it.get("SizeNotes","") or ""
        size_h = f"<span class='desc-note'>Size: {size}</span>" if size.strip() else ""
        qty    = it.get("Qty",0)
        rate   = float(it.get("Rate",0) or 0)
        tax    = float(it.get("TaxableValue",0) or 0)
        gstr   = it.get("GSTRate",0)
        total  = float(it.get("Total",0) or 0)
        item_rows += f"<tr style='border-bottom:1px solid #555;'><td class='center col-sep'>{i+1}</td><td class='left col-sep'>{name}{size_h}</td><td class='center col-sep'>{it.get('HSN','')}</td><td class='center col-sep'>{qty} Nos</td><td class='right col-sep'>{rate:.2f}</td><td class='right col-sep'>{tax:.2f}</td><td class='center col-sep'>{gstr}</td><td class='right'>{total:.2f}</td></tr>"

    # Filler rows (v076 logic)
    used_px = sum(40 if (it.get("SizeNotes","") or "").strip() else 22 for it in items)
    target_px = 7 * 36
    remaining = max(0, target_px - used_px)
    filler_count = round(remaining / 32) if len(items) <= 3 else 3
    filler_rows = "".join(
        "<tr style='height:25px;'><td class='col-sep'></td><td class='col-sep'></td><td class='col-sep'></td><td class='col-sep'></td><td class='col-sep'></td><td class='col-sep'></td><td class='col-sep'></td><td></td></tr>"
        for _ in range(filler_count)
    )

    # Address helper (v076 exact)
    def addr_cell(lbl, addr, name, mob, gst, state, code):
        return f"""<td><div class='sec-lbl'>{lbl}:</div><div style='height:8px;'></div>
<div class='cust-name'>{name}</div>
<div style='font-size:9pt;line-height:1.5;'>{addr}</div>
<div class='addr-contact'>
  <span class='cline'><span class='clbl'>Mob</span><span class='ccol'>:</span><strong>{mob}</strong></span>
  <span class='cline' style='height:6px;display:block;'></span>
  <span class='cline'><span class='clbl'>GST No</span><span class='ccol'>:</span><strong>{gst}</strong></span>
  <span class='cline'><span class='clbl'>State</span><span class='ccol'>:</span><strong>{state}</strong>&nbsp;&nbsp;Code:&nbsp;<strong>{code}</strong></span>
</div></td>"""

    # Totals helper (v076 exact)
    def tot_row(lbl, val, bold=False):
        b = "font-weight:bold;" if bold else ""
        return f"<tr style='border-bottom:1px solid #bbb;'><td style='padding:4px 10px 4px 16px;font-size:9pt;text-align:right;white-space:nowrap;{b}'>{lbl}</td><td style='padding:4px 8px;font-size:9pt;text-align:right;white-space:nowrap;border-left:1.5px solid #555;width:80px;{b}'>{val}</td></tr>"

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: Arial, sans-serif; font-size:10pt; color:#000; background:#fff; }}
.page {{ width:200mm; margin:0 auto; padding:3mm 4mm; }}
.header-table {{ width:100%; border-collapse:collapse; border:2px solid #000; }}
.logo-cell {{ width:130px; height:118px; vertical-align:middle; text-align:center; padding:2px 2px 2px 8px; border-right:2px solid #000; }}
.logo-txt {{ font-size:36pt; font-weight:900; color:#005073; line-height:1; }}
.logo-sub {{ font-size:9pt; font-weight:700; color:#c2185b; margin-top:2px; }}
.co-cell {{ vertical-align:middle; text-align:center; padding:4px 8px; border-right:2px solid #000; }}
.co-name {{ font-size:19pt; font-weight:900; letter-spacing:1px; line-height:1.1; }}
.rio {{ color:#005073; }}
.print {{ color:#c2185b; }}
.co-det {{ font-size:9pt; line-height:1.5; margin-top:3px; }}
.copy-cell {{ width:162px; vertical-align:middle; padding:5px 7px; font-size:8pt; }}
.copy-line {{ border:1px solid #aaa; padding:3px 7px; margin-bottom:4px; background:#f9f9f9; }}
.title-bar {{ background:#c2185b; color:#fff; text-align:center; font-size:16pt; font-weight:900; padding:8px 4px; letter-spacing:3px; border-left:2px solid #000; border-right:2px solid #000; border-bottom:2px solid #000; }}
.info-table {{ width:100%; border-collapse:collapse; border:2px solid #000; border-top:none; }}
.info-table td {{ padding:4px 5px; font-size:8.5pt; border-bottom:1px solid #ddd; vertical-align:middle; }}
.info-table tr:last-child td {{ border-bottom:none; }}
.info-table .lbl {{ font-weight:bold; white-space:nowrap; }}
.bold {{ font-weight:bold; }}
.addr-table {{ width:100%; border-collapse:collapse; border:2px solid #000; border-top:none; }}
.addr-table td {{ padding:5px 10px; vertical-align:top; font-size:9.5pt; width:50%; border-right:1px solid #000; height:190px; position:relative; }}
.addr-table td:last-child {{ border-right:none; }}
.sec-lbl {{ font-weight:bold; font-size:8pt; border-bottom:1px solid #bbb; margin-bottom:3px; padding-bottom:2px; color:#c2185b; }}
.cust-name {{ font-weight:bold; font-size:10pt; margin-bottom:2px; }}
.addr-contact {{ position:absolute; bottom:5px; left:10px; right:10px; font-size:8.5pt; border-top:1px dashed #ccc; padding-top:4px; }}
.cline {{ display:block; margin-bottom:2px; white-space:nowrap; }}
.clbl {{ display:inline-block; width:54px; }}
.ccol {{ display:inline-block; width:10px; }}
.items-table {{ width:100%; border-collapse:collapse; border:2px solid #000; border-top:none; }}
.items-table thead tr {{ background:#005073; }}
.items-table th {{ color:#fff; padding:3px 2px; font-size:9pt; text-align:center; border-right:1.5px solid #3a6a9a; border-bottom:2px solid #000; font-weight:bold; }}
.items-table th:last-child {{ border-right:none; }}
.items-table tbody td {{ border:none; border-right:1px solid #555; padding:3px 4px; font-size:9.25pt; vertical-align:top; line-height:1.3; }}
.items-table tbody td.col-sep {{ border-right:1px solid #666; }}
.items-table tbody td:last-child {{ border-right:none; }}
.desc-note {{ display:block; font-size:7.5pt; color:#333; font-style:italic; margin-top:1px; }}
.center {{ text-align:center; }} .right {{ text-align:right; }} .left {{ text-align:left; }}
.footer-table {{ width:100%; border-collapse:collapse; border:2px solid #000; border-top:none; }}
.rupees-bar {{ background:#c2185b; color:#fff; font-weight:bold; font-size:10pt; border:2px solid #000; border-top:none; display:table; width:100%; table-layout:fixed; }}
.rt-words {{ display:table-cell; padding:3px 8px; vertical-align:middle; font-style:italic; }}
.rt-label {{ display:table-cell; padding:3px 8px; text-align:right; white-space:nowrap; vertical-align:middle; width:155px; border-left:1px solid rgba(255,255,255,0.4); }}
.rt-amount {{ display:table-cell; padding:3px 10px; text-align:right; white-space:nowrap; font-size:11pt; vertical-align:middle; width:105px; border-left:1px solid rgba(255,255,255,0.4); }}
.terms-box {{ width:100%; border:2px solid #000; border-top:none; padding:4px 8px; font-size:9pt; line-height:1.5; }}
.sign-box {{ width:100%; border-collapse:collapse; border:2px solid #000; border-top:none; }}
.sign-left-cell {{ height:115px; vertical-align:bottom; padding:6px 10px; border-right:2px solid #000; font-size:7.5pt; font-style:italic; color:#333; }}
.sign-right-cell {{ width:260px; height:115px; vertical-align:top; padding:6px 14px 2px 10px; text-align:right; }}
.sign-company {{ font-style:italic; font-weight:bold; font-size:10.5pt; display:block; }}
.sign-auth {{ font-weight:bold; font-size:8.5pt; display:block; padding-bottom:4px; }}
@page {{ size:A4 portrait; margin:3mm 2mm; }}
@media print {{ body {{ margin:0; padding:0; }} .page {{ margin:0 auto; width:200mm; }} }}
</style></head><body><div class="page">

<table class="header-table"><tr>
  <td class="logo-cell"><div class="logo-txt">R</div><div class="logo-sub">PRINT</div></td>
  <td class="co-cell">
    <div class="co-name"><span class="rio">RIO </span><span class="print">PRINT MEDIA</span></div>
    <div class="co-det">RSF No: 304/6, Veppankattu Thottam, Sembampalayam, Nasiyanoor Post, Erode – 638107<br>
    Ph: 9962011515 | Email: rioprintmedia@gmail.com<br>
    <strong>GSTIN: 33BCFPR3860P1Z2</strong></div>
  </td>
  <td class="copy-cell">
    <div class="copy-line">&#9633; Original for Recipient</div>
    <div class="copy-line">&#9633; Duplicate For Transporter</div>
    <div class="copy-line">&#9633; Triplicate for Supplier</div>
  </td>
</tr></table>

<div class="title-bar">{title_bar}</div>

<table class="info-table">
  <colgroup><col style="width:16%"><col style="width:2%"><col style="width:32%"><col style="width:1%"><col style="width:18%"><col style="width:2%"><col></colgroup>
  <tr>
    <td class="lbl">Invoice No</td><td>:</td><td>{inv.get("InvoiceNo","")}</td>
    <td style="border-right:2px solid #000;padding:0;"></td>
    <td class="lbl" style="padding-left:14px;">Payment Terms &amp; Mode</td><td>:</td>
    <td class="bold">{inv.get("PaymentTerms","")}</td>
  </tr>
  <tr>
    <td class="lbl">Invoice Date</td><td>:</td><td>{inv.get("InvoiceDate","")}</td>
    <td style="border-right:2px solid #000;padding:0;"></td>
    <td class="lbl" style="padding-left:14px;">Date of Supply</td><td>:</td>
    <td class="bold">{inv.get("InvoiceDate","")}</td>
  </tr>
  <tr>
    <td class="lbl">State &amp; Code</td><td>:</td><td>Tamil Nadu&nbsp; Code: 33</td>
    <td style="border-right:2px solid #000;padding:0;"></td>
    <td class="lbl" style="padding-left:14px;">Place of Supply</td><td>:</td>
    <td class="bold">{cust_state}&nbsp; Code: {cust_code}</td>
  </tr>
</table>

<table class="addr-table"><tr>
  {addr_cell("Bill To", bill_addr, cust_name, cust_mob, cust_gst, cust_state, cust_code)}
  {addr_cell("Ship To", ship_addr, cust_name, cust_mob, cust_gst, cust_state, cust_code)}
</tr></table>

<table class="items-table">
  <thead><tr>
    <th style="width:30px;border-right:1.5px solid #5090c0;">S.No</th>
    <th style="text-align:left;padding-left:6px;border-right:1.5px solid #5090c0;">Product / Description</th>
    <th style="width:78px;border-right:1.5px solid #5090c0;">HSN</th>
    <th style="width:60px;border-right:1.5px solid #5090c0;">Qty</th>
    <th style="width:52px;border-right:1.5px solid #5090c0;">Rate</th>
    <th style="width:80px;border-right:1.5px solid #5090c0;">Taxable<br>Value</th>
    <th style="width:48px;border-right:1.5px solid #5090c0;">GST %</th>
    <th style="width:72px;">Total</th>
  </tr></thead>
  <tbody>{item_rows}{filler_rows}</tbody>
</table>

<table class="footer-table" style="border-collapse:collapse;width:100%;">
  <colgroup><col style="width:35%;"><col style="width:37%;"><col style="width:28%;"></colgroup>
  <tr>
    <td style="vertical-align:middle;padding:4px 10px;">
      <table style="width:100%;height:100%;border-collapse:collapse;font-size:10pt;">
        <tr><td colspan="3" style="padding:0 0 4px 0;font-weight:bold;font-size:10.5pt;">Bank Details</td></tr>
        <tr><td style="white-space:nowrap;padding:3px 4px 3px 0;">Bank Name</td><td style="padding:3px 8px;">:</td><td style="padding:3px 0;white-space:nowrap;"><strong>KVB BANK</strong></td></tr>
        <tr><td style="white-space:nowrap;padding:3px 4px 3px 0;">Bank Account No</td><td style="padding:3px 8px;">:</td><td style="padding:3px 0;white-space:nowrap;"><strong>1720172000027799</strong></td></tr>
        <tr><td style="white-space:nowrap;padding:3px 4px 3px 0;">IFSC Code</td><td style="padding:3px 8px;">:</td><td style="padding:3px 0;white-space:nowrap;"><strong>KVBL0001720</strong></td></tr>
        <tr><td style="white-space:nowrap;padding:3px 4px 3px 0;">Branch</td><td style="padding:3px 8px;">:</td><td style="padding:3px 0;white-space:nowrap;"><strong>Erode</strong></td></tr>
      </table>
    </td>
    <td style="vertical-align:top;border-right:2px solid #000;padding:0;"></td>
    <td style="vertical-align:top;padding:0;">
      <table style="width:100%;border-collapse:collapse;">
        {tot_row("Taxable Amount", fmt2(sub), True)}
        {tot_row("SGST", fmt2(sgst))}
        {tot_row("CGST", fmt2(cgst))}
        {tot_row("IGST", fmt2(igst))}
        {tot_row("Round Off", "0.00")}
      </table>
    </td>
  </tr>
</table>

<div class="rupees-bar">
  <span class="rt-words">Rupees: {words(tot)}</span>
  <span class="rt-label">Total After Tax</span>
  <span class="rt-amount">{fmt2(tot)}</span>
</div>

<div class="terms-box">
  <strong>Terms &amp; Conditions :</strong><br>
  1. Goods once sold cannot be taken back. Our responsibility ceases as soon as the goods are delivered.<br>
  2. Subject to Erode Jurisdiction.&nbsp; 3. All Claims must be Notified in writing within 8 days.<br>
  4. It will not be considered thereafter in any circumstances.
</div>

<table class="sign-box"><tr>
  <td class="sign-left-cell">Please check the quality and quantity before consumption</td>
  <td class="sign-right-cell">
    <span class="sign-company">For <em>RIO PRINT MEDIA</em></span>
    <br><br><br>
    <span class="sign-auth">Authorized Signatory</span>
  </td>
</tr></table>

<div style="text-align:center;font-size:8pt;color:#555;padding:4px 0 2px 0;font-style:italic;border-top:1px dashed #ccc;margin-top:2px;">
  This is a computer-generated invoice. No signature is required.
</div>

</div></body></html>"""

    try:
        pdf_bytes = WP(string=html, base_url=None).write_pdf()
    except Exception as e:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": f"PDF generation failed: {str(e)}"}, status_code=500)

    inv_no    = inv.get("InvoiceNo", str(inv_id))
    cust_raw  = cust.get("Name") or inv.get("CustomerName","Customer")
    safe_cust = "".join(c if c.isalnum() or c in "- " else "" for c in cust_raw).strip().replace(" ","_")[:30]
    filename  = f"Invoice_{safe_cust}_{inv_no}.pdf"

    from fastapi.responses import Response
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ── BILLING QUOTATIONS (v076: quotations) ────────────────────
@app.get("/api/billing/quotations/next")
async def get_next_quotation_no():
    last = await db["quotations"].find_one(sort=[("Id", -1)])
    if last and last.get("QuotationNo"):
        q = last["QuotationNo"]
        prefix = ''.join(c for c in q if c.isalpha())
        num = ''.join(c for c in q if c.isdigit())
        try: return {"QuotationNo": f"{prefix}{int(num)+1}"}
        except: pass
    return {"QuotationNo": "Q1"}

@app.get("/api/billing/quotations/peek")
async def peek_quotation_no():
    return await get_next_quotation_no()

@app.get("/api/billing/quotations/byno")
async def get_quotation_byno(no: str = ""):
    q = await db["quotations"].find_one({"QuotationNo": no}, {"_id": 0})
    if not q: return {}
    items = await db["quotation_items"].find(
        {"QuotationId": q.get("Id")}, {"_id": 0}).to_list(50)
    q["items"] = items
    return q

@app.get("/api/billing/quotations")
async def get_billing_quotations(from_date: str = "", to_date: str = "", customer: str = ""):
    query = {}
    if from_date: query["QuotationDate"] = {"$gte": from_date}
    if to_date:
        query.setdefault("QuotationDate", {})
        query["QuotationDate"]["$lte"] = to_date
    if customer: query["CustomerName"] = {"$regex": customer, "$options": "i"}
    quots = await db["quotations"].find(query, {"_id": 0}).sort("Id", -1).to_list(500)
    return quots

@app.get("/api/billing/quotations/{quot_id}")
async def get_billing_quotation(quot_id: int):
    q = await db["quotations"].find_one({"Id": quot_id}, {"_id": 0})
    if not q: return {}
    items = await db["quotation_items"].find({"QuotationId": quot_id}, {"_id": 0}).to_list(50)
    q["items"] = items
    return q

@app.post("/api/billing/quotations")
async def create_billing_quotation(request: Request):
    data = await request.json()
    data.pop("_id", None)
    items = data.pop("items", [])
    last = await db["quotations"].find_one(sort=[("Id", -1)])
    quot_id = (last["Id"] + 1) if last and "Id" in last else 1
    data["Id"] = quot_id
    await db["quotations"].insert_one(data)
    for i, item in enumerate(items):
        item.pop("_id", None)
        item["QuotationId"] = quot_id
        item["RowNo"] = i + 1
        await db["quotation_items"].insert_one(item)
    return {"success": True, "Id": quot_id, "QuotationNo": data.get("QuotationNo", "")}

@app.delete("/api/billing/quotations/{quot_id}")
async def delete_billing_quotation(quot_id: int):
    await db["quotations"].delete_one({"Id": quot_id})
    await db["quotation_items"].delete_many({"QuotationId": quot_id})
    return {"success": True}

# ── SIZES (for sales product sizes) ──────────────────────────
@app.get("/api/sizes")
async def get_sizes(product: str = ""):
    query = {"Product": product} if product else {}
    sizes = await db["product_sizes"].find(query, {"_id": 0}).to_list(500)
    return sizes

@app.post("/api/sizes/rename")
async def rename_size(request: Request):
    data = await request.json()
    old = data.get("old", "")
    new = data.get("new", "")
    await db["product_sizes"].update_many({"Size": old}, {"$set": {"Size": new}})
    return {"success": True}

# ── ATTENDANCE (v076: attendance + att_staff) ─────────────────
@app.get("/api/attendance/staff")
async def get_attendance_staff():
    staff = await db["att_staff"].find({}, {"_id": 0}).to_list(200)
    return staff

@app.post("/api/attendance/staff")
async def create_attendance_staff(request: Request):
    data = await request.json()
    data.pop("_id", None)
    last = await db["att_staff"].find_one(sort=[("Id", -1)])
    data["Id"] = (last["Id"] + 1) if last and "Id" in last else 1
    await db["att_staff"].insert_one(data)
    return {"success": True, "Id": data["Id"]}

@app.get("/api/attendance/employees")
async def get_employees():
    return await get_attendance_staff()

@app.get("/api/attendance/all")
async def get_all_attendance(month: str = "", year: str = ""):
    query = {}
    if month: query["Month"] = month
    if year: query["Year"] = year
    records = await db["attendance"].find(query, {"_id": 0}).to_list(1000)
    return records

@app.post("/api/attendance/upsert")
async def upsert_attendance(request: Request):
    data = await request.json()
    data.pop("_id", None)
    key = {"StaffId": data.get("StaffId"), "Date": data.get("Date")}
    await db["attendance"].update_one(key, {"$set": data}, upsert=True)
    return {"success": True}

@app.post("/api/attendance/bulk")
async def bulk_attendance(request: Request):
    data = await request.json()
    records = data if isinstance(data, list) else data.get("records", [])
    for rec in records:
        rec.pop("_id", None)
        key = {"StaffId": rec.get("StaffId"), "Date": rec.get("Date")}
        await db["attendance"].update_one(key, {"$set": rec}, upsert=True)
    return {"success": True, "count": len(records)}

@app.delete("/api/attendance")
async def delete_attendance(request: Request):
    data = await request.json()
    staff_id = data.get("StaffId")
    date = data.get("Date")
    await db["attendance"].delete_one({"StaffId": staff_id, "Date": date})
    return {"success": True}

# ── SALES INVOICENO UPDATE ─────────────────────────────────────
@app.post("/api/sales/{sno}/invoiceno")
async def set_sales_invoiceno(sno: int, request: Request):
    data = await request.json()
    # BUG FIX: match either Id or SNo — see update_sale for why (Id is the
    # only field actually set on insert, SNo is never populated)
    await db["sales_records"].update_one(
        {"$or": [{"Id": sno}, {"SNo": sno}]}, {"$set": {"InvoiceNo": data.get("InvoiceNo", "")}})
    return {"success": True}

# ── REPORTS ───────────────────────────────────────────────────
@app.get("/api/reports/sales")
async def sales_report(from_date: str = "", to_date: str = "",
                        customer: str = "", billing_type: str = ""):
    query = {}
    if from_date: query["OrderDate"] = {"$gte": from_date}
    if to_date:
        query.setdefault("OrderDate", {})
        query["OrderDate"]["$lte"] = to_date
    if customer: query["Customer"] = {"$regex": customer, "$options": "i"}
    if billing_type and billing_type != "All": query["BillingType"] = billing_type
    records = await db["sales_records"].find(query, {"_id": 0}).sort("OrderDate", -1).to_list(2000)
    return records

@app.get("/api/billing/reports/sales")
async def billing_sales_report(from_date: str = "", to_date: str = "",
                                 customer: str = "", inv_type: str = ""):
    query = {}
    if from_date: query["InvoiceDate"] = {"$gte": from_date}
    if to_date:
        query.setdefault("InvoiceDate", {})
        query["InvoiceDate"]["$lte"] = to_date
    if customer: query["CustomerName"] = {"$regex": customer, "$options": "i"}
    if inv_type and inv_type != "All": query["InvoiceType"] = inv_type
    invs = await db["sales_invoices"].find(query, {"_id": 0}).sort("InvoiceDate", -1).to_list(2000)
    return invs

# ── NOTES PUT (was missing) ────────────────────────────────────
@app.put("/api/notes/{note_id}")
async def update_note(note_id: int, request: Request):
    data = await request.json()
    data.pop("_id", None)
    await db["notes"].update_one({"Id": note_id}, {"$set": data})
    return {"success": True}

# ── FOLLOWUPS address/reopen ──────────────────────────────────
@app.put("/api/followups/{fid}/address")
async def address_followup(fid: int):
    await db["followups"].update_one({"Id": fid}, {"$set": {"Status": "Addressed"}})
    return {"success": True}

@app.put("/api/followups/{fid}/reopen")
async def reopen_followup(fid: int):
    await db["followups"].update_one({"Id": fid}, {"$set": {"Status": "Pending"}})
    return {"success": True}

# ── DELETE NOTES/FOLLOWUPS (v076 paths) ───────────────────────
@app.delete("/api/notes/{note_id}")
async def delete_note(note_id: int):
    await db["notes"].delete_one({"Id": note_id})
    return {"success": True}

@app.delete("/api/followups/{fid}")
async def delete_followup_v2(fid: int):
    await db["followups"].delete_one({"Id": fid})
    return {"success": True}

# ── PING / BILLING STATUS ─────────────────────────────────────
@app.get("/api/ping")
async def ping():
    return {"ok": True, "ts": str(datetime.utcnow())}

@app.get("/api/billing/status")
async def billing_status():
    inv_count = await db["sales_invoices"].count_documents({})
    cust_count = await db["rio_billing_customers"].count_documents({})
    prod_count = await db["products"].count_documents({})
    return {"invoices": inv_count, "customers": cust_count, "products": prod_count}

