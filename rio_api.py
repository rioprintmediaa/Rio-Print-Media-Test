import re
"""
rio_api.py — RIO PRINT MEDIA ERP v2.0
FastAPI backend replacing the PowerShell script.
Data stored in MongoDB Atlas.

Run locally:
    uvicorn rio_api:app --host 0.0.0.0 --port 8001 --reload

Deploy on Render.com:
    Start command: uvicorn rio_api:app --host 0.0.0.0 --port 8001
"""

import os, re, bcrypt, sys, secrets
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, Any

from fastapi import FastAPI, Request, Query
from pydantic import BaseModel
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from bson import ObjectId
from dotenv import load_dotenv

# Use logging so uvicorn captures and displays output properly
import logging
import logging.handlers
import pathlib

# ── Logging setup ────────────────────────────────────────────────
# Windows (local run): writes to C:\Rio\Logs\rio_app.log
# Render / Linux:      stdout only (visible in Render → Logs tab)
import platform as _platform

_fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")

# Set up "rio_api" logger cleanly — don't use basicConfig (conflicts with uvicorn)
logger = logging.getLogger("rio_api")
logger.setLevel(logging.DEBUG)
logger.propagate = False   # prevent double-logging via root logger

# Always add stdout handler
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
if not logger.handlers:    # avoid adding duplicate handlers on reload
    logger.addHandler(_sh)

# File handler — try C:\Rio\Logs on Windows, fallback to script directory
_LOG_FILE = None
_log_candidates = []
if _platform.system() == "Windows":
    _log_candidates = [
        pathlib.Path(r"C:\Rio\Logs"),
        pathlib.Path(os.path.dirname(os.path.abspath(__file__))) / "logs",
        pathlib.Path.cwd() / "logs",
    ]
else:
    # On Linux/Render: no file needed (use Render Logs tab)
    _log_candidates = []

for _log_candidate_dir in _log_candidates:
    try:
        _log_candidate_dir.mkdir(parents=True, exist_ok=True)
        _test = _log_candidate_dir / ".write_test"
        _test.touch(); _test.unlink()
        _LOG_FILE = _log_candidate_dir / "rio_app.log"
        _fh = logging.handlers.RotatingFileHandler(
            _LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8"
        )
        _fh.setFormatter(_fmt)
        logger.addHandler(_fh)
        logger.info("=== RIO — Log file: %s ===", _LOG_FILE)
        break
    except Exception as _log_ex:
        logger.debug("Log dir %s not writable: %s", _log_candidate_dir, _log_ex)

if not _LOG_FILE:
    if _platform.system() == "Windows":
        logger.warning("Could not create log file — check C:\\Rio\\Logs permissions")
    else:
        logger.info("=== RIO PRINT MEDIA ERP v2.0 — Render — check Logs tab ===")

load_dotenv(override=False)  # Never override Render environment variables

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
MONGO_URI  = os.environ.get("MONGO_URI", "")
MONGO_DB   = os.environ.get("MONGO_DB",  "RioPrintMedia_Test")
HTML_FILE  = os.environ.get("HTML_FILE", "Rio_Sales_Tracker_ONLINE.html")

# ── Startup diagnostics ──
logger.info("=" * 60)
logger.info("RIO PRINT MEDIA ERP v2.0 — STARTING UP")
logger.info(f"MONGO_DB  = {MONGO_DB}")
if MONGO_URI:
    _safe = re.sub(r':(.*?)@', ':***@', MONGO_URI)
    logger.info(f"MONGO_URI = SET → {_safe[:70]}")
else:
    logger.error("MONGO_URI = NOT SET — add it in Render Environment Variables!")
logger.info("=" * 60)

# ─────────────────────────────────────────────
#  DB
# ─────────────────────────────────────────────
_client: MongoClient = None
_db = None

def ensure_db() -> bool:
    """Connect to MongoDB if not already connected. Returns True if connected."""
    global _client, _db
    if _db is not None:
        try:
            _client.admin.command("ping")
            return True
        except Exception:
            _client = None
            _db = None
    if not MONGO_URI:
        logger.error("MONGO_URI not set — cannot connect to MongoDB")
        return False
    try:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
            socketTimeoutMS=15000,
        )
        _client.admin.command("ping")
        _db = _client[MONGO_DB]
        logger.info("MongoClient created, pinging Atlas...")
        # Create indexes
        try:
            _db["sales_records"].create_index([("SNo", DESCENDING)])
            _db["daily_expenses"].create_index([("ExpDate", DESCENDING)])
            _db["sales_invoices"].create_index([("InvoiceDate", DESCENDING)])
            _db["quotations"].create_index([("QuotationDate", DESCENDING)])
            _db["attendance"].create_index([("name", ASCENDING), ("date", ASCENDING)], unique=True)
            logger.info("Indexes created")
        except Exception:
            pass
        ensure_default_users()
        return True
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        _client = None
        _db = None
        return False

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False

def ensure_default_users():
    """Always ensure admin user exists."""
    try:
        existing = col("rio_users").find_one({"username": "admin"})
        if not existing:
            col("rio_users").insert_one({
                "username": "admin",
                "password": hash_password("rio@admin"),
                "role": "admin",
                "name": "Administrator"
            })
            logger.info("Admin user created: username=admin password=rio@admin")
        else:
            if existing.get("role") != "admin":
                col("rio_users").update_one({"username": "admin"}, {"$set": {"role": "admin"}})
    except Exception as e:
        logger.error(f"ensure_default_users error: {e}")


def get_db():
    return _db

def col(name: str) -> Collection:
    return _db[name]

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def clean(doc: dict) -> dict:
    """Remove MongoDB _id and convert ObjectId."""
    if doc is None:
        return None
    doc.pop("_id", None)
    return doc

def clean_list(docs) -> list:
    return [clean(d) for d in docs]

def to_float(v, default=None):
    if v is None or v == "":
        return default
    try:
        return float(v)
    except:
        return default

def to_int(v, default=None):
    if v is None or v == "":
        return default
    try:
        return int(v)
    except:
        return default

def fy_from_date(d: str) -> str:
    """Return FY string like '2024-25' from a date string."""
    try:
        dt = datetime.strptime(d[:10], "%Y-%m-%d")
        m, y = dt.month, dt.year
        if m >= 4:
            return f"{y}-{str(y+1)[-2:]}"
        else:
            return f"{y-1}-{str(y)[-2:]}"
    except:
        return ""

def current_fy() -> str:
    return fy_from_date(datetime.now().strftime("%Y-%m-%d"))

def fy_range(fy: str):
    """Return (from_date, to_date) strings for a FY like '2024-25'."""
    try:
        y = int(fy.split("-")[0])
        return f"{y}-04-01", f"{y+1}-03-31"
    except:
        return None, None

def next_invoice_no(inv_type: str, fy: str) -> str:
    fy_from, fy_to = fy_range(fy)
    if inv_type == "GST":
        pipeline = [
            {"$match": {
                "InvoiceNo": {"$regex": r"^R\d"},
                "$or": [{"FY": fy}, {"$and": [{"FY": None}, {"InvoiceDate": {"$gte": fy_from, "$lte": fy_to}}]}]
            }},
            {"$project": {"num": {"$toInt": {"$substr": ["$InvoiceNo", 1, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("sales_invoices").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"R{n:02d}"
    else:
        pipeline = [
            {"$match": {
                "InvoiceNo": {"$regex": r"^RN"},
                "$or": [{"FY": fy}, {"$and": [{"FY": None}, {"InvoiceDate": {"$gte": fy_from, "$lte": fy_to}}]}]
            }},
            {"$project": {"num": {"$toInt": {"$substr": ["$InvoiceNo", 2, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("sales_invoices").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"RN{n:02d}"

def next_quotation_no(q_type: str, fy: str) -> str:
    fy_from, fy_to = fy_range(fy)
    if q_type == "GST":
        pipeline = [
            {"$match": {"QuotationNo": {"$regex": r"^Q\d"}, "QuotationDate": {"$gte": fy_from, "$lte": fy_to}}},
            {"$project": {"num": {"$toInt": {"$substr": ["$QuotationNo", 1, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("quotations").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"Q{n:02d}"
    else:
        pipeline = [
            {"$match": {"QuotationNo": {"$regex": r"^QN"}, "QuotationDate": {"$gte": fy_from, "$lte": fy_to}}},
            {"$project": {"num": {"$toInt": {"$substr": ["$QuotationNo", 2, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("quotations").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"QN{n:02d}"

def next_product_code() -> str:
    pipeline = [
        {"$match": {"Code": {"$regex": r"^P"}}},
        {"$project": {"num": {"$toInt": {"$substr": ["$Code", 1, 10]}}}},
        {"$group": {"_id": None, "max": {"$max": "$num"}}}
    ]
    res = list(col("products").aggregate(pipeline))
    n = (res[0]["max"] if res else 0) + 1
    return f"P{n:03d}"

def next_id(collection_name: str, field: str = "Id") -> int:
    """Atomic ID generation using a counters collection."""
    result = _db["_counters"].find_one_and_update(
        {"_id": collection_name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True
    )
    return result["seq"]

def require_db():
    """Call at the start of every endpoint to ensure DB is ready."""
    if not ensure_db():
        raise Exception("Database not connected")

def init_indexes():
    """Create indexes for fast queries."""
    try:
        _db["sales_records"].create_index([("SNo", DESCENDING)])
        _db["daily_expenses"].create_index([("ExpDate", DESCENDING)])
        _db["sales_invoices"].create_index([("InvoiceDate", DESCENDING)])
        _db["quotations"].create_index([("QuotationDate", DESCENDING)])
        logger.info("Indexes created")
    except Exception as e:
        logger.warning(f"Index creation (non-fatal): {e}")

def init_counters():
    """Seed counters from current max IDs in each collection."""
    collections = [
        ("sales_records", "SNo"), ("daily_expenses", "Id"),
        ("notes", "Id"), ("followups", "Id"), ("rio_clients", "Id"),
        ("expense_categories", "Id"), ("jobs", "Id"), ("account_balances", "Id"),
        ("account_ledger", "Id"), ("products", "Id"), ("sales_invoices", "Id"),
        ("quotations", "Id"),
    ]
    for coll_name, field in collections:
        existing = _db["_counters"].find_one({"_id": coll_name})
        if not existing:
            pipeline = [{"$group": {"_id": None, "max": {"$max": f"${field}"}}}]
            res = list(_db[coll_name].aggregate(pipeline))
            max_val = to_int(res[0]["max"]) if res and res[0].get("max") is not None else 0
            _db["_counters"].update_one(
                {"_id": coll_name},
                {"$setOnInsert": {"seq": max_val}},
                upsert=True
            )

def set_sales_ledger_credits(sno: int, customer: str, job_name: str, payments: list):
    """Delete old ledger entries for a sales record and recreate them.
    After recreating, recalculate running balances for all affected accounts."""
    col("account_ledger").delete_many({"SalesRef": sno})
    affected = set()  # track which (account, fy) pairs need rebalancing
    for pay in payments:
        amt  = to_float(pay.get("Amt"))
        dt   = (pay.get("Date") or "").strip()
        mode = (pay.get("Mode") or "").strip()
        if not amt or amt <= 0 or not dt:
            continue
        acct_map = {
            "KVB MOM":     "KVB MOM",
            "KVB Mani":    "KVB Mani",
            "Indian Bank": "Indian Bank",
            "Cash":        "Cash Balance",
        }
        acct = acct_map.get(mode)
        if not acct:
            continue
        fy = fy_from_date(dt)
        if not fy:
            continue
        jn_str = f" — {job_name}" if job_name else ""
        desc = f"Sales: {customer}{jn_str}"
        col("account_ledger").insert_one({
            "Id": next_id("account_ledger"), "AccountName": acct, "EntryDate": dt,
            "Description": desc, "CreditAmt": amt, "DebitAmt": 0,
            "Balance": 0, "EntryType": "Credit", "FY": fy,
            "ExpenseRef": None, "SalesRef": sno
        })
        affected.add((acct, fy))
    # Recalculate running balances for all affected accounts
    for acct, fy in affected:
        recalc_ledger_balances(acct, fy)

def recalc_ledger_balances(account_name: str, fy: str):
    """
    Recalculate running balances for all entries of an account in a given FY,
    sorted by EntryDate then Id. Called after any edit or delete of a ledger entry
    to ensure the Balance column stays accurate throughout.
    """
    entries = list(col("account_ledger").find(
        {"AccountName": account_name, "FY": fy},
        sort=[("EntryDate", ASCENDING), ("Id", ASCENDING)]
    ))
    if not entries:
        return
    running = 0.0
    for entry in entries:
        running += to_float(entry.get("CreditAmt", 0))
        running -= to_float(entry.get("DebitAmt", 0))
        col("account_ledger").update_one(
            {"_id": entry["_id"]},
            {"$set": {"Balance": round(running, 2)}}
        )

# ─────────────────────────────────────────────
#  APP STARTUP
# ─────────────────────────────────────────────
_db_connected = False  # track real connection state

def _connect_mongo():
    """Attempt MongoDB connection. Returns True on success, False on failure."""
    global _client, _db, _db_connected
    if not MONGO_URI:
        logger.error("=" * 60)
        logger.error("MONGO_URI IS NOT SET!")
        logger.error("Go to Render → your service → Environment → Add:")
        logger.error("  MONGO_URI = mongodb+srv://user:pass@cluster...")
        logger.error("  MONGO_DB  = RioPrintMedia")
        logger.error("Then click Save and Manual Deploy")
        logger.error("=" * 60)
        _db_connected = False
        return False
    # Mask password for safe logging
    safe_uri = re.sub(r':(.*?)@', ':***@', MONGO_URI) if MONGO_URI else MONGO_URI
    logger.info(f"Connecting to MongoDB Atlas...")
    logger.info(f"URI: {safe_uri}")
    logger.info(f"DB:  {MONGO_DB}")
    try:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=25000,
            connectTimeoutMS=25000,
            socketTimeoutMS=30000,
            tls=True,
            retryWrites=True,
        )
        logger.info("MongoClient created, pinging Atlas...")
        _client.admin.command("ping")
        _db = _client[MONGO_DB]
        _db_connected = True
        logger.info(f"✓ MongoDB Atlas connected: {MONGO_DB}")
        return True
    except Exception as e:
        _db_connected = False
        err_str = str(e)
        logger.error(f"✗ MongoDB connection FAILED: {err_str}")
        if "Authentication failed" in err_str or "auth" in err_str.lower():
            logger.error("→ CHECK: Username and password in MONGO_URI")
            logger.error("→ Special chars in password must be URL-encoded (@ = %40)")
        elif "network" in err_str.lower() or "timeout" in err_str.lower() or "timed out" in err_str.lower():
            logger.error("→ CHECK: MongoDB Atlas Network Access")
            logger.error("→ Go to Atlas → Network Access → Add IP: 0.0.0.0/0 (Allow All)")
            logger.error("→ Render uses dynamic IPs, so 0.0.0.0/0 is required")
        elif "SSL" in err_str or "TLS" in err_str:
            logger.error("→ SSL/TLS error — check Atlas cluster TLS settings")
        logger.error(f"→ URI used (masked): {safe_uri[:60]}...")
        return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # DO NOT raise — keep server alive even if DB is temporarily down.
    # Render cold starts can be slow; server retries on first real request.
    connected = _connect_mongo()
    if connected:
        try:
            init_indexes()
            logger.info("Indexes ready")
        except Exception as e:
            logger.warning(f"init_indexes error (non-fatal): {e}")
        try:
            init_counters()
            logger.info("Counters initialised")
        except Exception as e:
            logger.warning(f"init_counters error (non-fatal): {e}")
        try:
            ensure_default_users()
            logger.info("Users ready")
        except Exception as e:
            logger.warning(f"ensure_default_users error (non-fatal): {e}")
    else:
        logger.warning("Server started WITHOUT DB — will retry on first request")
    yield
    if _client:
        _client.close()
        logger.info("MongoDB connection closed")

app = FastAPI(title="RIO PRINT MEDIA ERP v2.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def ok(data=None, **kwargs):
    if data is not None:
        return JSONResponse(content=data)
    return JSONResponse(content={"ok": True, **kwargs})

def err(msg, status=400):
    return JSONResponse(content={"error": msg}, status_code=status)

# ─────────────────────────────────────────────
#  LIVE HTML PATCHER — applied every request
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
#  SERVE HTML DASHBOARD
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    logger.info(f"GET / — serving {HTML_FILE}")
    if not os.path.exists(HTML_FILE):
        logger.error(f"HTML file not found: {HTML_FILE} — cwd={os.getcwd()}")
        return HTMLResponse(f"<h2>File not found: {HTML_FILE}</h2><p>CWD: {os.getcwd()}</p><p>Files: {os.listdir('.')[:20]}</p>", 404)
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()
    logger.info(f"Serving {len(html)} bytes")
    return HTMLResponse(html)

# ─────────────────────────────────────────────
#  MOBILE APP
# ─────────────────────────────────────────────


@app.post("/api/log")
async def client_log(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False}, status_code=400)
    level   = str(body.get("level",  "INFO")).upper()
    user    = str(body.get("user",   "unknown"))
    action  = str(body.get("action", ""))
    detail  = str(body.get("detail", ""))
    msg = f"[CLIENT] user={user} | {action} | {detail}"
    if level == "ERROR":
        logger.error(msg)
    elif level == "WARN":
        logger.warning(msg)
    else:
        logger.info(msg)
    return {"ok": True}


@app.get("/api/ping")
async def ping():
    connected = ensure_db()
    if not connected:
        return JSONResponse(
            content={"ok": False, "error": "MongoDB not connected. Check MONGO_URI on Render.", "db": MONGO_DB},
            status_code=503
        )
    try:
        _client.admin.command("ping")
        return {"ok": True, "db": MONGO_DB, "server": "MongoDB Atlas", "connected": True}
    except Exception as e:
        _db_connected = False
        return JSONResponse(
            content={"ok": False, "error": str(e), "db": MONGO_DB},
            status_code=503
        )

@app.get("/api/debug")
async def debug_info():
    """Public debug endpoint — shows connection state without exposing credentials"""
    safe_uri = re.sub(r':(.*?)@', ':***@', MONGO_URI) if MONGO_URI else "NOT SET"
    return JSONResponse(content={
        "mongo_uri_set": bool(MONGO_URI),
        "mongo_uri_masked": safe_uri[:80] if MONGO_URI else "NOT SET",
        "mongo_db": MONGO_DB,
        "db_connected": _db_connected,
        "hint": "If db_connected=false, go to MongoDB Atlas → Network Access → Add 0.0.0.0/0"
    })

# ─────────────────────────────────────────────
#  SALES RECORDS
# ─────────────────────────────────────────────
@app.get("/api/sales")
async def get_sales(
    limit: int = Query(2000, ge=1, le=5000),
    skip: int = Query(0, ge=0),
    fy: Optional[str] = Query(None),
    scope: Optional[str] = Query(None),
    user: Optional[str] = Query(None)
):
    if not ensure_db():
        return JSONResponse(content=[], status_code=503)
    try:
        query = {}
        if fy:
            fy_from, fy_to = fy_range(fy)
            if fy_from and fy_to:
                query = {"$or": [
                    {"FY": fy},
                    {"$and": [{"FY": {"$in": [None, ""]}}, {"OrderDate": {"$gte": fy_from, "$lte": fy_to}}]}
                ]}
        if scope == "own" and user:
            query["createdBy"] = user
        rows = list(col("sales_records").find(query, {"_id": 0})
                    .sort("SNo", DESCENDING).skip(skip).limit(limit))
        return JSONResponse(content=rows)
    except Exception as e:
        logger.error(f"get_sales error: {e}")
        return JSONResponse(content=[], status_code=500)

@app.post("/api/sales")
async def post_sales(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    sno = next_id("sales_records", "SNo")
    doc = {
        "SNo": sno,
        "createdBy":          b.get("createdBy", ""),
        "Customer":           b.get("Customer", ""),
        "Category":           b.get("Category", ""),
        "ProductSize":        b.get("ProductSize", ""),
        "Size1":              b.get("Size1", ""),
        "Qty1":               b.get("Qty1", ""),
        "Size2":              b.get("Size2", ""),
        "Qty2":               b.get("Qty2", ""),
        "Size3":              b.get("Size3", ""),
        "Qty3":               b.get("Qty3", ""),
        "BillingType":        b.get("BillingType", ""),
        "JobName":            b.get("JobName", ""),
        "OrderDate":          b.get("OrderDate"),
        "TotalAmount":        to_float(b.get("TotalAmount")),
        "AdvanceAmt":         to_float(b.get("AdvanceAmt")),
        "AdvanceDate":        b.get("AdvanceDate"),
        "AdvanceMode":        b.get("AdvanceMode", ""),
        "AdvanceDetails":     b.get("AdvanceDetails", ""),
        "BalanceSettledAmt":  to_float(b.get("BalanceSettledAmt")),
        "BalanceDate":        b.get("BalanceDate"),
        "BalanceMode":        b.get("BalanceMode", ""),
        "Balance1Details":    b.get("Balance1Details", ""),
        "Balance2Amt":        to_float(b.get("Balance2Amt")),
        "Balance2Date":       b.get("Balance2Date"),
        "Balance2Mode":       b.get("Balance2Mode", ""),
        "Balance2Details":    b.get("Balance2Details", ""),
        "Balance3Amt":        to_float(b.get("Balance3Amt")),
        "Balance3Date":       b.get("Balance3Date"),
        "Balance3Mode":       b.get("Balance3Mode", ""),
        "Balance3Details":    b.get("Balance3Details", ""),
        "RemainingBalance":   to_float(b.get("RemainingBalance")),
        "ProductId":          to_int(b.get("ProductId")),
        "Rate1":              to_float(b.get("Rate1")),
        "Rate2":              to_float(b.get("Rate2")),
        "InvoiceNo":          b.get("InvoiceNo", ""),
        "PFDesc":             b.get("PFDesc", ""),
        "PFAmt":              to_float(b.get("PFAmt"), 0.0),
        "PFGst":              to_float(b.get("PFGst"), 0.0),
        "PFTotal":            to_float(b.get("PFTotal"), 0.0),
        "FY":                 fy_from_date(b.get("OrderDate") or datetime.now().strftime("%Y-%m-%d")),
        "UpdatedAt":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    col("sales_records").insert_one(doc)
    # Auto-add client
    if doc["Customer"]:
        col("rio_clients").update_one(
            {"ClientName": doc["Customer"]},
            {"$setOnInsert": {"Id": next_id("rio_clients"), "ClientName": doc["Customer"]}},
            upsert=True
        )
    # Ledger credits
    payments = [
        {"Amt": doc["AdvanceAmt"],        "Date": doc["AdvanceDate"],  "Mode": doc["AdvanceMode"]},
        {"Amt": doc["BalanceSettledAmt"], "Date": doc["BalanceDate"],  "Mode": doc["BalanceMode"]},
        {"Amt": doc["Balance2Amt"],       "Date": doc["Balance2Date"], "Mode": doc["Balance2Mode"]},
        {"Amt": doc["Balance3Amt"],       "Date": doc["Balance3Date"], "Mode": doc["Balance3Mode"]},
    ]
    set_sales_ledger_credits(sno, doc["Customer"], doc["JobName"], payments)
    return ok({"ok": True, "SNo": sno})

@app.put("/api/sales/{sno}")
async def put_sales(sno: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    update = {
        "Customer":           b.get("Customer", ""),
        "Category":           b.get("Category", ""),
        "ProductSize":        b.get("ProductSize", ""),
        "Size1":              b.get("Size1", ""),
        "Qty1":               b.get("Qty1", ""),
        "Size2":              b.get("Size2", ""),
        "Qty2":               b.get("Qty2", ""),
        "Size3":              b.get("Size3", ""),
        "Qty3":               b.get("Qty3", ""),
        "BillingType":        b.get("BillingType", ""),
        "JobName":            b.get("JobName", ""),
        "OrderDate":          b.get("OrderDate"),
        "TotalAmount":        to_float(b.get("TotalAmount")),
        "AdvanceAmt":         to_float(b.get("AdvanceAmt")),
        "AdvanceDate":        b.get("AdvanceDate"),
        "AdvanceMode":        b.get("AdvanceMode", ""),
        "AdvanceDetails":     b.get("AdvanceDetails", ""),
        "BalanceSettledAmt":  to_float(b.get("BalanceSettledAmt")),
        "BalanceDate":        b.get("BalanceDate"),
        "BalanceMode":        b.get("BalanceMode", ""),
        "Balance1Details":    b.get("Balance1Details", ""),
        "Balance2Amt":        to_float(b.get("Balance2Amt")),
        "Balance2Date":       b.get("Balance2Date"),
        "Balance2Mode":       b.get("Balance2Mode", ""),
        "Balance2Details":    b.get("Balance2Details", ""),
        "Balance3Amt":        to_float(b.get("Balance3Amt")),
        "Balance3Date":       b.get("Balance3Date"),
        "Balance3Mode":       b.get("Balance3Mode", ""),
        "Balance3Details":    b.get("Balance3Details", ""),
        "RemainingBalance":   to_float(b.get("RemainingBalance")),
        "ProductId":          to_int(b.get("ProductId")),
        "Rate1":              to_float(b.get("Rate1")),
        "Rate2":              to_float(b.get("Rate2")),
        "PFDesc":             b.get("PFDesc", ""),
        "PFAmt":              to_float(b.get("PFAmt"), 0.0),
        "PFGst":              to_float(b.get("PFGst"), 0.0),
        "PFTotal":            to_float(b.get("PFTotal"), 0.0),
        "FY":                 fy_from_date(b.get("OrderDate") or datetime.now().strftime("%Y-%m-%d")),
        "UpdatedAt":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    col("sales_records").update_one({"SNo": sno}, {"$set": update})
    payments = [
        {"Amt": update["AdvanceAmt"],        "Date": update["AdvanceDate"],  "Mode": update["AdvanceMode"]},
        {"Amt": update["BalanceSettledAmt"], "Date": update["BalanceDate"],  "Mode": update["BalanceMode"]},
        {"Amt": update["Balance2Amt"],       "Date": update["Balance2Date"], "Mode": update["Balance2Mode"]},
        {"Amt": update["Balance3Amt"],       "Date": update["Balance3Date"], "Mode": update["Balance3Mode"]},
    ]
    set_sales_ledger_credits(sno, update["Customer"], update["JobName"], payments)
    return ok()

@app.delete("/api/sales/{sno}")
async def delete_sales(sno: int):
    if not ensure_db(): return err("Database not connected", 503)
    # Find affected accounts before deleting so we can recalc their balances
    affected_entries = list(col("account_ledger").find(
        {"SalesRef": sno}, {"AccountName": 1, "FY": 1}
    ))
    affected = set((e["AccountName"], e["FY"]) for e in affected_entries if e.get("AccountName") and e.get("FY"))
    col("account_ledger").delete_many({"SalesRef": sno})
    col("sales_records").delete_one({"SNo": sno})
    # Recalculate running balances for all affected accounts
    for acct, fy in affected:
        recalc_ledger_balances(acct, fy)
    return ok()

@app.post("/api/sales/{sno}/invoiceno")
async def patch_sales_invoiceno(sno: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    col("sales_records").update_one({"SNo": sno}, {"$set": {"InvoiceNo": b.get("InvoiceNo", "")}})
    return ok()

# ─────────────────────────────────────────────
#  EXPENSES
# ─────────────────────────────────────────────
@app.get("/api/expenses")
async def get_expenses(scope: Optional[str] = Query(None), user: Optional[str] = Query(None)):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    query = {}
    if scope == "own" and user:
        query["createdBy"] = user
    rows = list(col("daily_expenses").find(query, {"_id": 0}).sort([("ExpDate", DESCENDING), ("Id", DESCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/expenses")
async def post_expenses(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    new_id = next_id("daily_expenses")
    amt = to_float(b.get("Amount"), 0.0)
    doc = {
        "Id":          new_id,
        "createdBy":   (b.get("createdBy") or "").strip(),
        "ExpDate":     b.get("ExpDate"),
        "Category":    b.get("Category", ""),
        "SubCategory": b.get("SubCategory", ""),
        "PaymentMode": b.get("PaymentMode", ""),
        "Description": b.get("Description", ""),
        "Amount":      amt,
    }
    col("daily_expenses").insert_one(doc)
    # Auto-create ledger debit
    pm = (b.get("PaymentMode") or "").strip()
    acct_map = {"KVB MOM":"KVB MOM","KVB Mani":"KVB Mani","Indian Bank":"Indian Bank","Cash":"Cash Balance"}
    acct = acct_map.get(pm)
    if acct and new_id:
        exp_date = (b.get("ExpDate") or "").strip()
        fy = fy_from_date(exp_date)
        if fy:
            sub_cat = (b.get("SubCategory") or "").strip()
            desc_str = (b.get("Description") or "").strip()
            desc = f"Expense: {sub_cat} — {desc_str}" if desc_str else f"Expense: {sub_cat}"
            last = col("account_ledger").find_one(
                {"AccountName": acct, "FY": fy},
                sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
            )
            prev_bal = to_float(last["Balance"]) if last else 0.0
            new_bal  = prev_bal - amt
            led_id   = next_id("account_ledger")
            col("account_ledger").insert_one({
                "Id": led_id, "AccountName": acct, "EntryDate": exp_date,
                "Description": desc, "CreditAmt": 0, "DebitAmt": amt,
                "Balance": new_bal, "EntryType": "Expense", "FY": fy,
                "ExpenseRef": new_id, "SalesRef": None
            })
    return ok({"ok": True, "id": new_id})

@app.delete("/api/expenses/{exp_id}")
async def delete_expense(exp_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("account_ledger").delete_many({"ExpenseRef": exp_id})
    col("daily_expenses").delete_one({"Id": exp_id})
    return ok()

# ─────────────────────────────────────────────
#  NOTES
# ─────────────────────────────────────────────
@app.get("/api/notes")
async def get_notes(fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None)):
    query = {}
    if fr: query["NoteDate"] = {"$gte": fr}
    if to: query.setdefault("NoteDate", {})["$lte"] = to
    rows = list(col("notes").find(query, {"_id": 0}).sort([("NoteDate", DESCENDING), ("Id", DESCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/notes")
async def post_notes(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    new_id = next_id("notes")
    col("notes").insert_one({"Id": new_id, "NoteDate": b.get("NoteDate"), "NoteText": b.get("NoteText", "")})
    return ok()

@app.put("/api/notes/{note_id}")
async def put_notes(note_id: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    col("notes").update_one({"Id": note_id}, {"$set": {"NoteDate": b.get("NoteDate"), "NoteText": b.get("NoteText", "")}})
    return ok()

@app.delete("/api/notes/{note_id}")
async def delete_notes(note_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("notes").delete_one({"Id": note_id})
    return ok()

# ─────────────────────────────────────────────
#  FOLLOWUPS
# ─────────────────────────────────────────────
@app.get("/api/followups")
async def get_followups():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("followups").find({}, {"_id": 0}).sort([("IsAddressed", ASCENDING), ("FollowupDate", ASCENDING), ("Id", ASCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/followups")
async def post_followups(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    new_id = next_id("followups")
    col("followups").insert_one({
        "Id": new_id,
        "FollowupDate": b.get("FollowupDate"),
        "Priority":     b.get("Priority", ""),
        "FollowupText": b.get("FollowupText", ""),
        "IsAddressed":  0,
    })
    return ok()

@app.put("/api/followups/{fid}/address")
async def address_followup(fid: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("followups").update_one({"Id": fid}, {"$set": {"IsAddressed": 1}})
    return ok()

@app.put("/api/followups/{fid}/reopen")
async def reopen_followup(fid: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    new_date = b.get("FollowupDate") or datetime.now().strftime("%Y-%m-%d")
    col("followups").update_one({"Id": fid}, {"$set": {"IsAddressed": 0, "FollowupDate": new_date}})
    return ok()

@app.put("/api/followups/{fid}")
async def put_followup(fid: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    col("followups").update_one({"Id": fid}, {"$set": {
        "FollowupDate": b.get("FollowupDate"),
        "Priority":     b.get("Priority", ""),
        "FollowupText": b.get("FollowupText", ""),
    }})
    return ok()

@app.delete("/api/followups/{fid}")
async def delete_followup(fid: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("followups").delete_one({"Id": fid})
    return ok()

# ─────────────────────────────────────────────
#  CLIENTS
# ─────────────────────────────────────────────
@app.get("/api/clients")
async def get_clients():
    # Clients list is a shared lookup — always return all (used for sales dropdown autocomplete)
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("rio_clients").find({}, {"_id": 0, "ClientName": 1}).sort("ClientName", ASCENDING))
    return JSONResponse(content=[r["ClientName"] for r in rows if r.get("ClientName")])

@app.get("/api/rio_clients")
async def get_rio_clients(q: Optional[str] = Query(None)):
    """Alias for /api/billing/customers — used by customer autocomplete."""
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    query = {}
    if q: query["Name"] = {"$regex": q, "$options": "i"}
    rows = list(col("rio_clients").find(query, {"_id": 0}).sort("ClientName", ASCENDING))
    # Rename ClientName → Name for billing compatibility
    result = [{"Id": r.get("Id"), "Name": r.get("ClientName", r.get("Name", "")),
               "Mobile": r.get("Mobile",""), "GSTNo": r.get("GSTNo",""),
               "State": r.get("State",""), "StateCode": r.get("StateCode",""),
               "Type": r.get("Type","")} for r in rows]
    return JSONResponse(content=result)


@app.post("/api/clients")
async def post_clients(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    name = (b.get("ClientName") or "").strip()
    created_by = (b.get("createdBy") or "").strip()
    if name:
        col("rio_clients").update_one(
            {"ClientName": name},
            {"$setOnInsert": {"Id": next_id("rio_clients"), "ClientName": name, "createdBy": created_by}},
            upsert=True
        )
    return ok()

# ─────────────────────────────────────────────
#  CATEGORIES
# ─────────────────────────────────────────────
@app.delete("/api/clients/{client_name:path}")
async def delete_client(client_name: str):
    """Delete a client from the sales tracker clientsList."""
    if not ensure_db(): return JSONResponse(content={"error":"DB offline"}, status_code=503)
    name = client_name.strip()
    col("rio_clients").delete_one({"ClientName": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
    return ok({"success": True, "deleted": name})


@app.get("/api/categories")
async def get_categories():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("expense_categories").distinct("CategoryName"))
    return JSONResponse(content=sorted(rows))

@app.get("/api/expense_categories")
async def get_expense_categories_alias():
    """Alias for /api/categories — used by mobile/expense dropdown."""
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("expense_categories").find({}, {"_id": 0}).sort(
        [("CategoryName", ASCENDING), ("SubCategoryName", ASCENDING)]))
    return JSONResponse(content=rows)


@app.get("/api/categories/all")
async def get_categories_all():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("expense_categories").find({}, {"_id": 0}).sort([("CategoryName", ASCENDING), ("SubCategoryName", ASCENDING)]))
    from collections import defaultdict
    mp = defaultdict(list)
    for r in rows:
        mp[r["CategoryName"]].append(r["SubCategoryName"])
    return JSONResponse(content=[{"category": k, "subcats": v} for k, v in mp.items()])

@app.get("/api/categories/subcats")
async def get_subcats(cat: str = Query("")):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("expense_categories").find({"CategoryName": cat}, {"_id": 0, "SubCategoryName": 1}).sort("SubCategoryName", ASCENDING))
    subs = [r["SubCategoryName"] for r in rows]
    return JSONResponse(content=subs if subs else ["Other"])

@app.post("/api/categories")
async def post_category(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    cn = (b.get("CategoryName") or "").strip()
    sn = (b.get("SubCategoryName") or "").strip()
    if cn and sn:
        exists = col("expense_categories").find_one({"CategoryName": cn, "SubCategoryName": sn})
        if not exists:
            new_id = next_id("expense_categories")
            col("expense_categories").insert_one({"Id": new_id, "CategoryName": cn, "SubCategoryName": sn})
    return ok()

@app.post("/api/categories/sync")
async def sync_categories(request: Request):
    rows = await request.json()
    inserted = 0
    for row in (rows if isinstance(rows, list) else []):
        cn = (row.get("CategoryName") or "").strip()
        sn = (row.get("SubCategoryName") or "").strip()
        if cn and sn:
            exists = col("expense_categories").find_one({"CategoryName": cn, "SubCategoryName": sn})
            if not exists:
                new_id = next_id("expense_categories")
                col("expense_categories").insert_one({"Id": new_id, "CategoryName": cn, "SubCategoryName": sn})
                inserted += 1
    return ok({"ok": True, "inserted": inserted})

@app.delete("/api/categories/{cat_id}")
async def delete_category(cat_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("expense_categories").delete_one({"Id": cat_id})
    return ok()

# ─────────────────────────────────────────────
#  JOBS
# ─────────────────────────────────────────────
@app.get("/api/jobs")
async def get_jobs(fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None)):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    query = {}
    if fr: query["ConfirmedDate"] = {"$gte": fr}
    if to: query.setdefault("ConfirmedDate", {})["$lte"] = to
    rows = list(col("jobs").find(query, {"_id": 0}).sort("Id", DESCENDING))
    return JSONResponse(content=rows)

@app.post("/api/jobs")
async def post_jobs(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    new_id = next_id("jobs")
    # Auto-generate JobNo if not provided: J001, J002, ...
    job_no = (b.get("JobNo") or "").strip()
    if not job_no:
        pipeline = [{"$group": {"_id": None, "max": {"$max": "$Id"}}}]
        res = list(col("jobs").aggregate(pipeline))
        max_id = to_int(res[0]["max"]) if res else 0
        job_no = f"J{(max_id + 1):03d}"
    col("jobs").insert_one({
        "Id":           new_id,
        "JobNo":        job_no,
        "Customer":     b.get("Customer", ""),
        "JobName":      b.get("JobName", ""),
        "ConfirmedDate":b.get("ConfirmedDate"),
        "ProductSize":  b.get("ProductSize", ""),
        "Qty":          to_int(b.get("Qty")),
        "Status":       b.get("Status", ""),
        "DispatchDate": b.get("DispatchDate"),
    })
    return ok({"ok": True, "id": new_id, "jobNo": job_no})

@app.put("/api/jobs/{job_id}")
async def put_jobs(job_id: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    update = {
        "Customer":     b.get("Customer", ""),
        "JobName":      b.get("JobName", ""),
        "ConfirmedDate":b.get("ConfirmedDate"),
        "ProductSize":  b.get("ProductSize", ""),
        "Qty":          to_int(b.get("Qty")),
        "Status":       b.get("Status", ""),
        "DispatchDate": b.get("DispatchDate"),
    }
    # Only update JobNo if provided (don't overwrite existing)
    if b.get("JobNo"):
        update["JobNo"] = b.get("JobNo")
    col("jobs").update_one({"Id": job_id}, {"$set": update})
    return ok()

@app.delete("/api/jobs/{job_id}")
async def delete_jobs(job_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("jobs").delete_one({"Id": job_id})
    return ok()

# ─────────────────────────────────────────────
#  ACCOUNT BALANCES
# ─────────────────────────────────────────────
@app.get("/api/accountbalances")
async def get_acct_balances():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    rows = list(col("account_balances").find({}, {"_id": 0}).sort([("EntryDate", DESCENDING), ("Id", DESCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/accountbalances")
async def post_acct_balance(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    new_id = next_id("account_balances")
    col("account_balances").insert_one({
        "Id":          new_id,
        "AccountName": b.get("AccountName", ""),
        "EntryDate":   b.get("EntryDate"),
        "Balance":     to_float(b.get("Balance"), 0.0),
        "Notes":       b.get("Notes", ""),
    })
    return ok({"ok": True, "id": new_id})

@app.delete("/api/accountbalances/{ab_id}")
async def delete_acct_balance(ab_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("account_balances").delete_one({"Id": ab_id})
    return ok()

# ─────────────────────────────────────────────
#  LEDGER
# ─────────────────────────────────────────────
@app.get("/api/ledger/debug")
async def ledger_debug():
    total = col("account_ledger").count_documents({})
    rows = list(col("account_ledger").find({}, {"_id": 0}).sort("Id", DESCENDING).limit(20))
    return JSONResponse(content={"total": total, "rows": rows})

@app.get("/api/ledger/prev-closing")
async def ledger_prev_closing(fy: str = Query("")):
    if not fy:
        return JSONResponse(content=[])
    fy_year = int(fy.split("-")[0])
    prev_fy = f"{fy_year-1}-{str(fy_year)[-2:]}"
    result = []
    for acct in ["KVB MOM", "KVB Mani", "Indian Bank", "Cash Balance"]:
        last = col("account_ledger").find_one(
            {"AccountName": acct, "FY": prev_fy},
            sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
        )
        bal = to_float(last["Balance"]) if last else 0.0
        result.append({"AccountName": acct, "ClosingBalance": bal})
    return JSONResponse(content=result)

@app.get("/api/ledger/opening")
async def get_ledger_opening(fy: str = Query("")):
    if not fy:
        return JSONResponse(content=[])
    rows = list(col("account_opening_balances").find({"FY": fy}, {"_id": 0}))
    return JSONResponse(content=rows)

@app.post("/api/ledger/opening")
async def post_ledger_opening(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    fy = (b.get("FY") or "").strip()
    if not fy:
        return err("FY required")
    for acct in ["KVB MOM", "KVB Mani", "Indian Bank", "Cash Balance"]:
        val = to_float(b.get(acct), 0.0)
        exists = col("account_opening_balances").find_one({"AccountName": acct, "FY": fy})
        if exists:
            col("account_opening_balances").update_one(
                {"AccountName": acct, "FY": fy},
                {"$set": {"OpeningBal": val}}
            )
        else:
            col("account_opening_balances").insert_one({"AccountName": acct, "FY": fy, "OpeningBal": val})
    return ok()

@app.delete("/api/ledger/clear-opening")
async def clear_ledger_opening(fy: str = Query("")):
    if not ensure_db(): return err("Database not connected", 503)
    if not fy:
        return ok({"ok": False})
    col("account_ledger").delete_many({"EntryType": "Opening", "FY": fy})
    col("account_opening_balances").delete_many({"FY": fy})
    return ok()

@app.get("/api/ledger")
async def get_ledger(
    account: Optional[str] = Query(None),
    fy: Optional[str] = Query(None),
    month: Optional[str] = Query(None)
):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    if not fy:
        return JSONResponse(content=[])
    query = {"FY": fy}
    if account: query["AccountName"] = account
    if month:   query["EntryDate"] = {"$regex": f"^{month}"}
    rows = list(col("account_ledger").find(query, {"_id": 0}).sort([("EntryDate", ASCENDING), ("Id", ASCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/ledger")
async def post_ledger(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    acct  = (b.get("AccountName") or "").strip()
    dt    = (b.get("EntryDate") or "").strip() or datetime.now().strftime("%Y-%m-%d")
    desc  = (b.get("Description") or "").strip()
    cr    = to_float(b.get("CreditAmt"), 0.0)
    dr    = to_float(b.get("DebitAmt"), 0.0)
    etype = (b.get("EntryType") or "Manual").strip()
    fy    = (b.get("FY") or "").strip()
    if not acct: return err("AccountName required")
    if not fy:   return err("FY required")
    if etype == "Opening":
        new_bal = cr - dr
    else:
        last = col("account_ledger").find_one(
            {"AccountName": acct, "FY": fy},
            sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
        )
        if last:
            prev = to_float(last["Balance"], 0.0)
        else:
            ob = col("account_opening_balances").find_one({"AccountName": acct, "FY": fy})
            prev = to_float(ob["OpeningBal"]) if ob else 0.0
        new_bal = prev + cr - dr
    new_id = next_id("account_ledger")
    col("account_ledger").insert_one({
        "Id": new_id, "AccountName": acct, "EntryDate": dt,
        "Description": desc, "CreditAmt": cr, "DebitAmt": dr,
        "Balance": new_bal, "EntryType": etype, "FY": fy,
        "ExpenseRef": None, "SalesRef": None
    })
    return ok({"ok": True, "balance": new_bal})

@app.delete("/api/ledger/reset")
async def ledger_reset():
    if not ensure_db(): return err("Database not connected", 503)
    col("account_ledger").delete_many({})
    col("account_opening_balances").delete_many({})
    return ok()

@app.delete("/api/ledger/{led_id}")
async def delete_ledger_entry(led_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    col("account_ledger").delete_one({"Id": led_id})
    return ok()

@app.post("/api/ledger/migrate")
async def ledger_migrate(request: Request):
    b = await request.json()
    fy = (b.get("FY") or "").strip()
    if not fy: return err("FY required")
    fy_from, fy_to = fy_range(fy)
    exp_count = sales_count = skip_count = 0

    # Import expenses
    exp_rows = list(col("daily_expenses").find(
        {"ExpDate": {"$gte": fy_from, "$lte": fy_to}},
        {"_id": 0}
    ).sort([("ExpDate", ASCENDING), ("Id", ASCENDING)]))

    for row in exp_rows:
        exp_id = to_int(row.get("Id"))
        already = col("account_ledger").count_documents({"ExpenseRef": exp_id})
        if already > 0: skip_count += 1; continue
        pm = (row.get("PaymentMode") or "").strip()
        acct_map = {"KVB MOM":"KVB MOM","KVB Mani":"KVB Mani","Indian Bank":"Indian Bank","Cash":"Cash Balance"}
        acct = acct_map.get(pm)
        if not acct: skip_count += 1; continue
        exp_date = row.get("ExpDate", "")
        amt = to_float(row.get("Amount"), 0.0)
        sub_cat = (row.get("SubCategory") or "").strip()
        desc_str = (row.get("Description") or "").strip()
        desc = f"Expense: {sub_cat} — {desc_str}" if desc_str else f"Expense: {sub_cat}"
        last = col("account_ledger").find_one({"AccountName": acct, "FY": fy}, sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)])
        prev = to_float(last["Balance"]) if last else 0.0
        new_bal = prev - amt
        led_id = next_id("account_ledger")
        col("account_ledger").insert_one({
            "Id": led_id, "AccountName": acct, "EntryDate": exp_date,
            "Description": desc, "CreditAmt": 0, "DebitAmt": amt,
            "Balance": new_bal, "EntryType": "Expense", "FY": fy,
            "ExpenseRef": exp_id, "SalesRef": None
        })
        exp_count += 1

    # Import sales payments
    sales_rows = list(col("sales_records").find(
        {"OrderDate": {"$gte": fy_from, "$lte": fy_to}},
        {"_id": 0}
    ).sort([("OrderDate", ASCENDING), ("SNo", ASCENDING)]))

    acct_map = {"KVB MOM":"KVB MOM","KVB Mani":"KVB Mani","Indian Bank":"Indian Bank","Cash":"Cash Balance"}
    for row in sales_rows:
        sno = to_int(row.get("SNo"))
        already = col("account_ledger").count_documents({"SalesRef": sno})
        if already > 0: skip_count += 1; continue
        cust = (row.get("Customer") or "").strip()
        job  = (row.get("JobName") or "").strip()
        jn_str = f" — {job}" if job else ""
        desc = f"Sales: {cust}{jn_str}"
        payments = [
            {"Amt": row.get("AdvanceAmt"),        "Date": row.get("AdvanceDate"),  "Mode": row.get("AdvanceMode","")},
            {"Amt": row.get("BalanceSettledAmt"), "Date": row.get("BalanceDate"),  "Mode": row.get("BalanceMode","")},
            {"Amt": row.get("Balance2Amt"),       "Date": row.get("Balance2Date"), "Mode": row.get("Balance2Mode","")},
            {"Amt": row.get("Balance3Amt"),       "Date": row.get("Balance3Date"), "Mode": row.get("Balance3Mode","")},
        ]
        added = False
        for pay in payments:
            amt  = to_float(pay["Amt"])
            pdate = (pay["Date"] or "").strip()
            mode = (pay["Mode"] or "").strip()
            if not amt or amt <= 0 or not pdate: continue
            acct = acct_map.get(mode)
            if not acct: continue
            last = col("account_ledger").find_one({"AccountName": acct, "FY": fy}, sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)])
            prev = to_float(last["Balance"]) if last else 0.0
            new_bal = prev + amt
            led_id = next_id("account_ledger")
            col("account_ledger").insert_one({
                "Id": led_id, "AccountName": acct, "EntryDate": pdate,
                "Description": desc, "CreditAmt": amt, "DebitAmt": 0,
                "Balance": new_bal, "EntryType": "Credit", "FY": fy,
                "ExpenseRef": None, "SalesRef": sno
            })
            added = True
            sales_count += 1
        if not added: skip_count += 1

    return ok({"ok": True, "expenseEntries": exp_count, "salesEntries": sales_count, "skipped": skip_count})

# ─────────────────────────────────────────────
#  BILLING — CUSTOMERS
# ─────────────────────────────────────────────
@app.get("/api/billing/status")
async def billing_status():
    cc = col("rio_clients").count_documents({})
    pc = col("products").count_documents({})
    ic = col("sales_invoices").count_documents({})
    return JSONResponse(content={"ready": True, "server": "MongoDB Atlas", "database": MONGO_DB, "version": "3.0", "customers": cc, "products": pc, "invoices": ic})

@app.get("/api/billing/customers")
async def billing_get_customers(q: Optional[str] = Query(None), scope: Optional[str] = Query(None), user: Optional[str] = Query(None)):
    if q:
        query = {"$or": [
            {"ClientName": {"$regex": q, "$options": "i"}},
            {"Mobile": {"$regex": q, "$options": "i"}},
            {"GSTNo": {"$regex": q, "$options": "i"}},
        ]}
    else:
        query = {}
    # Customers are shared data — do NOT filter by createdBy/scope.
    # Scope (own/all) applies only to transactional data (sales, invoices, quotations).
    rows = list(col("rio_clients").find(query, {"_id": 0}).sort("ClientName", ASCENDING))
    # Rename ClientName → Name for billing compatibility
    result = []
    for r in rows:
        result.append({
            "Id": r.get("Id"), "Name": r.get("ClientName",""),
            "BillToAddress": r.get("BillToAddress",""), "ShipToAddress": r.get("ShipToAddress",""),
            "State": r.get("State",""), "StateCode": r.get("StateCode",""),
            "Mobile": r.get("Mobile",""), "GSTNo": r.get("GSTNo",""),
            "Email": r.get("Email",""), "CustomerType": r.get("CustomerType",""),
        })
    return JSONResponse(content=result)

@app.post("/api/billing/customers")
async def billing_post_customer(request: Request):
    b = await request.json()
    name = (b.get("Name") or "").strip()
    if not name: return err("Name required")
    existing = col("rio_clients").find_one({"ClientName": name})
    update_doc = {
        "BillToAddress": b.get("BillToAddress",""), "ShipToAddress": b.get("ShipToAddress",""),
        "State": b.get("State",""), "StateCode": b.get("StateCode",""),
        "Mobile": b.get("Mobile",""), "GSTNo": b.get("GSTNo",""),
        "Email": b.get("Email",""), "CustomerType": b.get("CustomerType",""),
    }
    if existing:
        col("rio_clients").update_one({"ClientName": name}, {"$set": update_doc})
        return ok({"success": True, "id": existing["Id"]})
    else:
        new_id = next_id("rio_clients")
        created_by = (b.get("createdBy") or "").strip()
        col("rio_clients").insert_one({"Id": new_id, "ClientName": name, "createdBy": created_by, **update_doc})
        return ok({"success": True, "id": new_id})

@app.get("/api/billing/customers/byname")
async def billing_customer_byname(name: str = Query("")):
    if not name: return err("name required")
    r = col("rio_clients").find_one({"ClientName": name}, {"_id": 0})
    if not r:
        r = col("rio_clients").find_one({"ClientName": {"$regex": name, "$options": "i"}}, {"_id": 0})
    if not r: return err("Not found", 404)
    return JSONResponse(content={"Id": r.get("Id"), "Name": r.get("ClientName",""), **{k: r.get(k,"") for k in ["BillToAddress","ShipToAddress","State","StateCode","Mobile","GSTNo","Email","CustomerType"]}})

@app.get("/api/billing/customers/{cust_id}")
async def billing_get_customer(cust_id: int):
    r = col("rio_clients").find_one({"Id": cust_id}, {"_id": 0})
    if not r: return err("Not found", 404)
    return JSONResponse(content={"Id": r.get("Id"), "Name": r.get("ClientName",""), **{k: r.get(k,"") for k in ["BillToAddress","ShipToAddress","State","StateCode","Mobile","GSTNo","Email","CustomerType"]}})

@app.put("/api/billing/customers/{cust_id}")
async def billing_put_customer(cust_id: int, request: Request):
    b = await request.json()
    ship = (b.get("ShipToAddress") or "").strip() or (b.get("BillToAddress") or "").strip()
    col("rio_clients").update_one({"Id": cust_id}, {"$set": {
        "ClientName": b.get("Name",""), "BillToAddress": b.get("BillToAddress",""),
        "ShipToAddress": ship, "State": b.get("State",""), "StateCode": b.get("StateCode",""),
        "Mobile": b.get("Mobile",""), "GSTNo": b.get("GSTNo",""),
        "Email": b.get("Email",""), "CustomerType": b.get("CustomerType",""),
    }})
    return ok({"success": True})

@app.delete("/api/billing/customers/{cust_id}")
async def billing_delete_customer(cust_id: int, user: Optional[str] = Query(None), scope: Optional[str] = Query(None)):
    if not ensure_db(): return err("Database not connected", 503)
    record = col("rio_clients").find_one({"Id": cust_id}, {"_id": 0, "createdBy": 1})
    if not record:
        return err("Customer not found", 404)
    # Scope check: scoped users can only delete their own records
    if scope == "own" and user:
        record_owner = record.get("createdBy", "")
        if record_owner and record_owner != user:
            return err("Access denied — you can only delete customers you created.", 403)
    col("rio_clients").delete_one({"Id": cust_id})
    return ok({"success": True})

# ─────────────────────────────────────────────
#  BILLING — PRODUCTS
# ─────────────────────────────────────────────
@app.get("/api/billing/products/nextcode")
async def billing_nextcode():
    return JSONResponse(content={"code": next_product_code()})

@app.get("/api/billing/products")
async def billing_get_products(q: Optional[str] = Query(None)):
    if q:
        query = {"$or": [{"Name": {"$regex": q, "$options": "i"}}, {"Code": {"$regex": q, "$options": "i"}}]}
    else:
        query = {}
    rows = list(col("products").find(query, {"_id": 0}).sort("Code", ASCENDING))
    return JSONResponse(content=rows)

@app.post("/api/billing/products")
async def billing_post_product(request: Request):
    b = await request.json()
    name = (b.get("Name") or "").strip()
    if not name: return err("Name required")
    code = (b.get("Code") or "").strip() or next_product_code()
    new_id = next_id("products")
    col("products").insert_one({
        "Id": new_id, "Code": code, "Name": name,
        "createdBy": (b.get("createdBy") or "").strip(),
        "PrintName": b.get("PrintName",""), "HSN": b.get("HSN",""),
        "Category": b.get("Category",""), "Unit": b.get("Unit","Nos"),
        "GSTRate": to_float(b.get("GSTRate"), 18.0),
    })
    return ok({"success": True, "id": new_id, "code": code})

@app.get("/api/billing/products/{prod_id}")
async def billing_get_product(prod_id: int):
    r = col("products").find_one({"Id": prod_id}, {"_id": 0})
    if not r: return err("Not found", 404)
    return JSONResponse(content=r)

@app.put("/api/billing/products/{prod_id}")
async def billing_put_product(prod_id: int, request: Request):
    b = await request.json()
    col("products").update_one({"Id": prod_id}, {"$set": {
        "Code": b.get("Code",""), "Name": b.get("Name",""),
        "PrintName": b.get("PrintName",""), "HSN": b.get("HSN",""),
        "Category": b.get("Category",""), "Unit": b.get("Unit","Nos"),
        "GSTRate": to_float(b.get("GSTRate"), 18.0),
    }})
    return ok({"success": True})

@app.delete("/api/billing/products/{prod_id}")
async def billing_delete_product(prod_id: int, user: Optional[str] = Query(None), scope: Optional[str] = Query(None)):
    if not ensure_db(): return err("Database not connected", 503)
    record = col("products").find_one({"Id": prod_id}, {"_id": 0, "createdBy": 1})
    if not record:
        return err("Product not found", 404)
    # Scope check: scoped users can only delete their own records
    if scope == "own" and user:
        record_owner = record.get("createdBy", "")
        if record_owner and record_owner != user:
            return err("Access denied — you can only delete products you created.", 403)
    col("products").delete_one({"Id": prod_id})
    return ok({"success": True})

# ─────────────────────────────────────────────
#  BILLING — INVOICE SEQUENCES
# ─────────────────────────────────────────────
@app.get("/api/billing/invoices/peek")
async def billing_invoice_peek(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"invoiceNo": next_invoice_no(type, fy)})

@app.get("/api/billing/invoices/next")
async def billing_invoice_next(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"invoiceNo": next_invoice_no(type, fy)})

@app.post("/api/billing/invoices/resetsequence")
async def billing_reset_sequence(type: str = Query("GST")):
    return ok({"success": True, "type": type})

# ─────────────────────────────────────────────
#  BILLING — INVOICES
# ─────────────────────────────────────────────
@app.get("/api/billing/invoices/byno")
async def billing_invoice_byno(invno: str = Query(""), fy: str = Query("")):
    if not invno: return err("invno required")
    query = {"InvoiceNo": invno}
    if fy:
        fy_from, fy_to = fy_range(fy)
        query["InvoiceDate"] = {"$gte": fy_from, "$lte": fy_to}
    inv = col("sales_invoices").find_one(query, {"_id": 0}, sort=[("Id", DESCENDING)])
    if not inv: return err("Not found", 404)
    inv_id = inv.get("Id")
    items = list(col("sales_items").find({"InvoiceId": inv_id}, {"_id": 0}).sort("SNo", ASCENDING))
    # Fetch customer details
    cust = {}
    if inv.get("CustomerId"):
        c = col("rio_clients").find_one({"Id": inv["CustomerId"]}, {"_id": 0})
        if c:
            cust = {"CustomerAddress": c.get("BillToAddress",""), "CustomerState": c.get("State",""),
                    "CustomerStateCode": c.get("StateCode",""), "CustomerMobile": c.get("Mobile",""),
                    "CustomerGST": c.get("GSTNo",""), "CustomerEmail": c.get("Email","")}
    # Format date
    try:
        inv["InvoiceDate"] = datetime.strptime(inv["InvoiceDate"][:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except: pass
    return JSONResponse(content={**inv, **cust, "Items": items})

@app.get("/api/billing/invoices")
async def billing_get_invoices(
    page: int = Query(1), pageSize: int = Query(50),
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None), q: Optional[str] = Query(None),
    scope: Optional[str] = Query(None), user: Optional[str] = Query(None)
):
    pageSize = max(1, min(pageSize, 500))
    query = {}
    if fr or to:
        query["InvoiceDate"] = {}
        if fr: query["InvoiceDate"]["$gte"] = fr
        if to: query["InvoiceDate"]["$lte"] = to
    if type == "GST":    query["BillingType"] = {"$in": ["GST", "IGST"]}
    if type == "NONGST": query["BillingType"] = "NON-GST"
    if q: query["$or"] = [{"CustomerName": {"$regex": q, "$options": "i"}}, {"InvoiceNo": {"$regex": q, "$options": "i"}}]
    if scope == "own" and user: query["createdBy"] = user
    total = col("sales_invoices").count_documents(query)
    skip  = (page - 1) * pageSize
    rows  = list(col("sales_invoices").find(query, {"_id": 0, "Id":1,"InvoiceNo":1,"InvoiceDate":1,"CustomerName":1,"BillingType":1,"SubTotal":1,"CGST":1,"SGST":1,"IGST":1,"TotalAmount":1,"Counter":1,"PaymentTerms":1})
                .sort([("InvoiceDate", DESCENDING), ("Id", DESCENDING)]).skip(skip).limit(pageSize))
    return JSONResponse(content={"data": rows, "total": total, "page": page, "pageSize": pageSize})

@app.get("/api/billing/invoices/{inv_id}")
async def billing_get_invoice(inv_id: int):
    r = col("sales_invoices").find_one({"Id": inv_id}, {"_id": 0})
    if not r: return err("Invoice not found", 404)
    return JSONResponse(content=r)

@app.post("/api/billing/invoices")
async def billing_post_invoice(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    inv_no = (b.get("InvoiceNo") or "").strip()
    if not inv_no: return err("InvoiceNo required")
    raw_date = b.get("InvoiceDate", "")
    try:
        inv_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        inv_date = datetime.now().strftime("%Y-%m-%d")
    fy = fy_from_date(inv_date)
    new_id = next_id("sales_invoices")
    doc = {
        "Id": new_id, "Branch": "HO", "InvoiceNo": inv_no, "InvoiceDate": inv_date,
        "createdBy": (b.get("createdBy") or "").strip(),
        "CustomerId": to_int(b.get("CustomerId"), 0),
        "CustomerName": b.get("CustomerName",""), "BillingType": b.get("BillingType",""),
        "PlaceOfSupply": b.get("PlaceOfSupply",""), "PlaceOfSupplyCode": b.get("PlaceOfSupplyCode",""),
        "SubTotal": to_float(b.get("SubTotal"),0), "CGST": to_float(b.get("CGST"),0),
        "SGST": to_float(b.get("SGST"),0), "IGST": to_float(b.get("IGST"),0),
        "TotalAmount": to_float(b.get("TotalAmount"),0),
        "Counter": b.get("Counter",""), "PaymentTerms": b.get("PaymentTerms",""), "FY": fy
    }
    col("sales_invoices").insert_one(doc)
    sno = 1
    for item in (b.get("Items") or []):
        if not item: continue
        qty = to_float(item.get("Qty"),0); rate = to_float(item.get("Rate"),0)
        tv  = to_float(item.get("TaxableValue"), qty*rate if qty and rate else 0)
        it  = to_float(item.get("Total"), tv)
        if not item.get("ProductName") and not tv: continue
        col("sales_items").insert_one({
            "InvoiceId": new_id, "SNo": sno,
            "ProductName": item.get("ProductName",""), "HSN": item.get("HSN",""),
            "Qty": qty, "Rate": rate, "TaxableValue": tv,
            "GSTRate": to_float(item.get("GSTRate"),0), "Total": it,
            "SizeNotes": item.get("SizeNotes","")
        })
        sno += 1
    return ok({"success": True, "id": new_id, "invoiceNo": inv_no})

@app.put("/api/billing/invoices/{inv_id}")
async def billing_put_invoice(inv_id: int, request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    raw_date = b.get("InvoiceDate","")
    try: inv_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: inv_date = datetime.now().strftime("%Y-%m-%d")
    col("sales_invoices").update_one({"Id": inv_id}, {"$set": {
        "InvoiceDate": inv_date, "CustomerId": to_int(b.get("CustomerId"),0),
        "CustomerName": b.get("CustomerName",""), "BillingType": b.get("BillingType",""),
        "PlaceOfSupply": b.get("PlaceOfSupply",""), "PlaceOfSupplyCode": b.get("PlaceOfSupplyCode",""),
        "SubTotal": to_float(b.get("SubTotal"),0), "CGST": to_float(b.get("CGST"),0),
        "SGST": to_float(b.get("SGST"),0), "IGST": to_float(b.get("IGST"),0),
        "TotalAmount": to_float(b.get("TotalAmount"),0),
        "Counter": b.get("Counter",""), "PaymentTerms": b.get("PaymentTerms",""),
    }})
    col("sales_items").delete_many({"InvoiceId": inv_id})
    sno = 1
    for item in (b.get("Items") or []):
        if not item: continue
        qty = to_float(item.get("Qty"),0); rate = to_float(item.get("Rate"),0)
        tv  = to_float(item.get("TaxableValue"), qty*rate if qty and rate else 0)
        it  = to_float(item.get("Total"), tv)
        if not item.get("ProductName") and not tv: continue
        col("sales_items").insert_one({
            "InvoiceId": inv_id, "SNo": sno,
            "ProductName": item.get("ProductName",""), "HSN": item.get("HSN",""),
            "Qty": qty, "Rate": rate, "TaxableValue": tv,
            "GSTRate": to_float(item.get("GSTRate"),0), "Total": it,
            "SizeNotes": item.get("SizeNotes","")
        })
        sno += 1
    inv_no = col("sales_invoices").find_one({"Id": inv_id}, {"InvoiceNo": 1})
    return ok({"success": True, "id": inv_id, "invoiceNo": inv_no.get("InvoiceNo","") if inv_no else ""})

@app.delete("/api/billing/invoices/{inv_id}")
async def billing_delete_invoice(inv_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    inv = col("sales_invoices").find_one({"Id": inv_id}, {"InvoiceNo": 1})
    if not inv: return err("Invoice not found", 404)
    inv_no = inv.get("InvoiceNo","")
    # Only allow deleting the most recent invoice in its series
    if inv_no.startswith("RN"):
        latest = col("sales_invoices").find_one({"InvoiceNo": {"$regex": r"^RN"}}, sort=[("Id", DESCENDING)])
    else:
        latest = col("sales_invoices").find_one({"InvoiceNo": {"$regex": r"^R\d"}}, sort=[("Id", DESCENDING)])
    if not latest or latest.get("Id") != inv_id:
        return err("Only the most recent invoice in this series can be deleted.", 403)
    col("sales_items").delete_many({"InvoiceId": inv_id})
    col("sales_invoices").delete_one({"Id": inv_id})
    col("sales_records").update_many({"InvoiceNo": inv_no}, {"$set": {"InvoiceNo": ""}})
    return ok({"success": True, "invoiceNo": inv_no})

# ─────────────────────────────────────────────
#  BILLING — QUOTATIONS
# ─────────────────────────────────────────────
@app.get("/api/billing/quotations/peek")
async def billing_quotation_peek(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"quotationNo": next_quotation_no(type, fy)})

@app.get("/api/billing/quotations/next")
async def billing_quotation_next(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"quotationNo": next_quotation_no(type, fy)})

@app.get("/api/billing/quotations/byno")
async def billing_quotation_byno(qno: str = Query("")):
    if not qno: return err("qno required")
    q = col("quotations").find_one({"QuotationNo": qno}, {"_id": 0})
    if not q: return err("Not found", 404)
    q_id = q.get("Id")
    items = list(col("quotation_items").find({"QuotationId": q_id}, {"_id": 0}).sort("SNo", ASCENDING))
    try:
        q["QuotationDate"] = datetime.strptime(q["QuotationDate"][:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except: pass
    try:
        q["ValidTill"] = datetime.strptime(q["ValidTill"][:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except: pass
    return JSONResponse(content={**q, "Items": items})

@app.get("/api/billing/quotations")
async def billing_get_quotations(
    page: int = Query(1), pageSize: int = Query(50),
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    scope: Optional[str] = Query(None), user: Optional[str] = Query(None)
):
    pageSize = max(1, min(pageSize, 500))
    query = {}
    if fr or to:
        query["QuotationDate"] = {}
        if fr: query["QuotationDate"]["$gte"] = fr
        if to: query["QuotationDate"]["$lte"] = to
    if type == "GST":    query["BillingType"] = {"$in": ["GST","IGST"]}
    if type == "NONGST": query["BillingType"] = "NON-GST"
    if scope == "own" and user: query["createdBy"] = user
    total = col("quotations").count_documents(query)
    skip  = (page - 1) * pageSize
    rows  = list(col("quotations").find(query, {"_id": 0})
                .sort([("QuotationDate", DESCENDING), ("Id", DESCENDING)]).skip(skip).limit(pageSize))
    return JSONResponse(content={"data": rows, "total": total, "page": page, "pageSize": pageSize})

@app.get("/api/billing/quotations/{quot_id}")
async def billing_get_quotation(quot_id: int):
    q = col("quotations").find_one({"Id": quot_id}, {"_id": 0})
    if not q: return err("Quotation not found", 404)
    items = list(col("quotation_items").find({"QuotationId": quot_id}, {"_id": 0}).sort("SNo", ASCENDING))
    return JSONResponse(content={**q, "Items": items})

@app.post("/api/billing/quotations")
async def billing_post_quotation(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    q_no = (b.get("QuotationNo") or "").strip()
    if not q_no: return err("QuotationNo required")
    try: q_date = datetime.strptime(b.get("QuotationDate","")[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: q_date = datetime.now().strftime("%Y-%m-%d")
    try: vt = datetime.strptime(b.get("ValidTill","")[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: vt = q_date
    new_id = next_id("quotations")
    doc = {
        "Id": new_id, "QuotationNo": q_no, "QuotationDate": q_date,
        "createdBy": (b.get("createdBy") or "").strip(),
        "CustomerId": to_int(b.get("CustomerId"),0),
        "CustomerName": b.get("CustomerName",""), "BillingType": b.get("BillingType",""),
        "PlaceOfSupply": b.get("PlaceOfSupply",""), "PlaceOfSupplyCode": b.get("PlaceOfSupplyCode",""),
        "SubTotal": to_float(b.get("SubTotal"),0), "CGST": to_float(b.get("CGST"),0),
        "SGST": to_float(b.get("SGST"),0), "IGST": to_float(b.get("IGST"),0),
        "TotalAmount": to_float(b.get("TotalAmount"),0),
        "PaymentTerms": b.get("PaymentTerms",""), "ValidTill": vt
    }
    col("quotations").insert_one(doc)
    sno = 1
    for item in (b.get("Items") or []):
        if not item: continue
        qty = to_float(item.get("Qty"),0); rate = to_float(item.get("Rate"),0)
        tv  = to_float(item.get("TaxableValue"), qty*rate if qty and rate else 0)
        it  = to_float(item.get("Total"), tv)
        if not item.get("ProductName") and not tv: continue
        col("quotation_items").insert_one({
            "QuotationId": new_id, "SNo": sno,
            "ProductName": item.get("ProductName",""), "HSN": item.get("HSN",""),
            "Qty": qty, "Rate": rate, "TaxableValue": tv,
            "GSTRate": to_float(item.get("GSTRate"),0), "Total": it,
            "SizeNotes": item.get("SizeNotes","")
        })
        sno += 1
    return ok({"success": True, "id": new_id, "quotationNo": q_no})

@app.delete("/api/billing/quotations/{quot_id}")
async def billing_delete_quotation(quot_id: int):
    if not ensure_db(): return err("Database not connected", 503)
    # Find this quotation first to determine its series (GST vs Non-GST)
    this_quot = col("quotations").find_one({"Id": quot_id}, {"QuotationNo": 1})
    if not this_quot:
        return err("Quotation not found", 404)
    qno = this_quot.get("QuotationNo", "")
    # Find the latest quotation in the SAME series
    if qno.startswith("QN"):
        latest = col("quotations").find_one({"QuotationNo": {"$regex": r"^QN"}}, sort=[("Id", DESCENDING)])
    else:
        latest = col("quotations").find_one({"QuotationNo": {"$regex": r"^Q\d"}}, sort=[("Id", DESCENDING)])
    if not latest or latest.get("Id") != quot_id:
        return err("Only the most recent quotation in this series can be deleted.", 403)
    col("quotation_items").delete_many({"QuotationId": quot_id})
    col("quotations").delete_one({"Id": quot_id})
    return ok({"success": True})

# ─────────────────────────────────────────────
#  BILLING — REPORTS
# ─────────────────────────────────────────────
@app.get("/api/billing/reports/sales")
async def billing_reports_sales(
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None)
):
    query = {}
    if fr or to:
        query["InvoiceDate"] = {}
        if fr: query["InvoiceDate"]["$gte"] = fr
        if to: query["InvoiceDate"]["$lte"] = to
    if type == "GST":    query["BillingType"] = {"$in": ["GST","IGST"]}
    if type == "NONGST": query["BillingType"] = "NON-GST"
    rows = list(col("sales_invoices").find(query, {"_id":0,"InvoiceNo":1,"InvoiceDate":1,"CustomerName":1,"BillingType":1,"SubTotal":1,"CGST":1,"SGST":1,"IGST":1,"TotalAmount":1})
                .sort([("InvoiceDate", DESCENDING), ("Id", DESCENDING)]))
    totals = {"SubTotal":0,"CGST":0,"SGST":0,"IGST":0,"TotalAmount":0}
    for r in rows:
        for k in totals:
            totals[k] += to_float(r.get(k),0)
    return JSONResponse(content={"data": rows, "count": len(rows), "totals": totals})

# ─────────────────────────────────────────────
#  BILLING — BACKUP (stub — data is in MongoDB)
# ─────────────────────────────────────────────
@app.post("/api/billing/backup")
async def billing_backup():
    return ok({"success": True, "message": "Data is stored in MongoDB Atlas — no local backup needed. Use MongoDB Atlas backup features.", "recentBackups": []})

@app.get("/api/billing/backups")
async def billing_backups():
    return JSONResponse(content=[])

@app.post("/api/billing/reset-sequences")
async def billing_reset_sequences():
    return ok({"message": "Sequences are auto-calculated from existing records in MongoDB.", "invoiceCount": 0, "quotationCount": 0})

# ─────────────────────────────────────────────
#  REPORTS (non-billing)
# ─────────────────────────────────────────────
@app.get("/api/reports/sales")
async def reports_sales(
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None)
):
    return await billing_reports_sales(fr=fr, to=to, type=type)

# ─────────────────────────────────────────────
#  CLIENT-SIDE LOGGING  →  uses main logger
# ─────────────────────────────────────────────
# Reuse the top-level logger (already configured above with file + stdout)
_log = logging.getLogger("rio_api")
# LOG_FILE exposed for /api/log/tail endpoint
LOG_FILE = _LOG_FILE  # None on Render, Path on Windows

class LogEntry(BaseModel):
    level:   str = "INFO"
    user:    str = "unknown"
    action:  str = ""
    detail:  str = ""
    page:    str = ""
    ts:      str = ""

@app.get("/api/log/where")
async def log_where():
    """Shows where the log file is (or tells you it's on Render stdout)."""
    import platform as _p
    return {
        "platform": _p.system(),
        "log_file": str(LOG_FILE) if LOG_FILE else None,
        "log_exists": LOG_FILE.exists() if LOG_FILE else False,
        "note": (
            f"Log file at: {LOG_FILE}" if LOG_FILE and LOG_FILE.exists()
            else "Running on Render/Linux — logs go to Render dashboard Logs tab, not a file."
        )
    }

@app.get("/api/log/tail")
async def log_tail(n: int = 100):
    """Return last n lines of the log file (Windows only; Render uses stdout)."""
    if not LOG_FILE or not LOG_FILE.exists():
        return {"lines": [], "note": "Log file not available on this platform. On Render, check the Logs tab in the dashboard."}
    try:
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        return {"lines": lines[-n:], "total": len(lines), "file": str(LOG_FILE)}
    except Exception as e:
        return {"lines": [], "error": str(e)}


# ─────────────────────────────────────────────
#  ATTENDANCE — matches PS1 v2.1 structure
#  Collection: att_records, att_staff
# ─────────────────────────────────────────────
@app.get("/api/attendance/ping")
async def att_ping():
    if not ensure_db(): return JSONResponse(content={"ok": False}, status_code=503)
    return {"ok": True}

@app.get("/api/attendance/staff")
async def get_att_staff():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        rows = list(col("att_staff").find({}, {"_id": 0}).sort("name", ASCENDING))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.post("/api/attendance/staff")
async def post_att_staff(request: Request):
    staff = await request.json()
    if not isinstance(staff, list): return err("Expected array")
    if not ensure_db(): return err("DB offline")
    try:
        col("att_staff").delete_many({})
        if staff:
            col("att_staff").insert_many(
                [{k:v for k,v in s.items()} for s in staff], ordered=False
            )
    except Exception as e:
        return err(str(e))
    return ok({"success": True})

# get_attendance: replaced by full version from attendance_api below
@app.post("/api/attendance/upsert")
async def att_upsert(request: Request):
    rec = await request.json()
    if not ensure_db(): return err("DB offline")
    try:
        rec_id = rec.get("id")
        name   = rec.get("name","").strip()
        date   = rec.get("date","").strip()
        if not name or not date: return err("name and date required")
        # Remove MongoDB _id if present
        rec.pop("_id", None)
        col("att_records").replace_one({"name": name, "date": date}, rec, upsert=True)
        return ok({"success": True})
    except Exception as e:
        return err(str(e))

@app.get("/api/attendance/all")
async def att_get_all():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        rows = list(col("att_records").find({}, {"_id": 0}).sort([("date", ASCENDING), ("name", ASCENDING)]))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.get("/api/attendance/search")
async def att_search(
    fr:   Optional[str] = Query(None, alias="from"),
    to:   Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    jobType: Optional[str] = Query(None)
):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        query = {}
        if fr or to:
            query["date"] = {}
            if fr: query["date"]["$gte"] = fr
            if to: query["date"]["$lte"] = to
        if name:    query["name"]    = name
        if jobType: query["jobType"] = jobType
        rows = list(col("att_records").find(query, {"_id": 0}).sort([("date", ASCENDING), ("name", ASCENDING)]))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.delete("/api/attendance/delete")
async def att_delete(
    fr:   Optional[str] = Query(None, alias="from"),
    to:   Optional[str] = Query(None),
    name: Optional[str] = Query(None)
):
    if not ensure_db(): return err("DB offline")
    try:
        query = {}
        if fr or to:
            query["date"] = {}
            if fr: query["date"]["$gte"] = fr
            if to: query["date"]["$lte"] = to
        if name: query["name"] = name
        result = col("att_records").delete_many(query)
        return ok({"deleted": result.deleted_count})
    except Exception as e:
        return err(str(e))

# upsert_attendance: replaced by attendance_api version below
@app.post("/api/auth/login")
async def login(request: Request):
    if not ensure_db():
        return JSONResponse(content={"ok": False, "error": "Database not connected. Please try again in a moment."}, status_code=503)
    b = await request.json()
    username = (b.get("username") or "").strip().lower()
    password = (b.get("password") or "").strip()
    if not username or not password:
        return err("Username and password required", 400)
    user = col("rio_users").find_one({"username": username}, {"_id": 0})
    if not user:
        return err("Invalid username or password", 401)
    if not verify_password(password, user["password"]):
        return err("Invalid username or password", 401)
    # Accept any role — built-in or custom roles created in User Management
    role = user.get("role", "guest") or "guest"
    scope = user.get("scope", "all")
    # Generate a session token and store it so protected endpoints can verify
    token = secrets.token_hex(32)
    col("rio_users").update_one(
        {"username": username},
        {"$set": {"session_token": token}}
    )
    return JSONResponse(content={
        "ok": True,
        "username": user["username"],
        "name": user.get("name", username),
        "role": role,
        "scope": scope,
        "token": token
    })

@app.get("/api/auth/users")
async def get_users():
    if not ensure_db():
        return JSONResponse(content={"error": "Database not connected"}, status_code=503)
    users = list(col("rio_users").find({}, {"_id": 0, "password": 0, "session_token": 0}))
    return JSONResponse(content=users)

@app.post("/api/auth/users")
async def create_user(request: Request):
    b = await request.json()
    username = (b.get("username") or "").strip().lower()
    password = (b.get("password") or "").strip()
    role     = (b.get("role") or "expense").strip()
    name     = (b.get("name") or username).strip()
    if not username or not password:
        return err("Username and password required")
    # Accept any role — including custom roles created in User Management
    if not role:
        role = "guest"
    if col("rio_users").find_one({"username": username}):
        return err("Username already exists")
    scope = (b.get("scope") or "all").strip()
    if scope not in {"all", "own"}: scope = "all"
    col("rio_users").insert_one({
        "username": username,
        "password": hash_password(password),
        "role": role,
        "name": name,
        "scope": scope
    })
    return ok({"ok": True})

@app.put("/api/auth/users/{username}")
async def update_user(username: str, request: Request):
    b = await request.json()
    update = {}
    if b.get("password"): update["password"] = hash_password(b["password"])
    if b.get("role"):     update["role"]     = b["role"]
    if b.get("name"):     update["name"]     = b["name"]
    if b.get("scope") in {"all","own"}: update["scope"] = b["scope"]
    col("rio_users").update_one({"username": username}, {"$set": update})
    return ok()

@app.delete("/api/auth/users/{username}")
async def delete_user(username: str):
    if username == "admin":
        return err("Cannot delete admin user")
    col("rio_users").delete_one({"username": username})
    return ok()


# ══════════════════════════════════════════════════════════════
# CUSTOM ROLES — stored in MongoDB so all machines share them
# ══════════════════════════════════════════════════════════════

@app.get("/api/roles")
async def get_roles():
    if not ensure_db():
        return JSONResponse(content=[], status_code=503)
    roles = list(col("rio_custom_roles").find({}, {"_id": 0}))
    return JSONResponse(content=roles)

@app.post("/api/roles")
async def save_role(request: Request):
    if not ensure_db(): return err("Database not connected", 503)
    b = await request.json()
    role_id = (b.get("id") or "").strip()
    if not role_id: return err("role id required")
    col("rio_custom_roles").update_one(
        {"id": role_id},
        {"$set": b},
        upsert=True
    )
    return ok({"ok": True})

@app.delete("/api/roles/{role_id}")
async def delete_role(role_id: str):
    if not ensure_db(): return err("Database not connected", 503)
    col("rio_custom_roles").delete_one({"id": role_id})
    return ok()

# ══════════════════════════════════════════════════════════════
# ATTENDANCE TRACKER — routes on /api/attendance/*
# ══════════════════════════════════════════════════════════════

SHIFT_START = 9  * 60   # 09:00 in minutes
SHIFT_END   = 20 * 60   # 20:00 in minutes
MAX_OUT     = 26 * 60   # safety cap

# ── Calculation helpers ───────────────────────────────────────────────────────
DAY_NAMES = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

def to_mins(t: str) -> int:
    if not t or ":" not in t:
        return 0
    parts = t.split(":")
    return int(parts[0]) * 60 + int(parts[1])

def get_day_name(ds: str) -> str:
    """Return weekday name for a YYYY-MM-DD date string."""
    try:
        parts = ds.split("-")
        if len(parts[0]) == 4:
            dt = date(int(parts[0]), int(parts[1]), int(parts[2]))
        else:
            dt = date(int(parts[2]), int(parts[1]), int(parts[0]))
        return DAY_NAMES[(dt.weekday() + 1) % 7]
    except Exception:
        return "?"

def norm_date(d: str) -> str:
    """Normalize to YYYY-MM-DD."""
    d = d.strip().replace("/", "-").replace("\\", "-")
    parts = d.split("-")
    if len(parts) != 3:
        return d
    if len(parts[0]) == 4:
        return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"

def calc(in_t: str, out_t: str, ds: str, rec_type: str, job_type: str = "fulltime", perm_mins: int = 0) -> dict:
    """Replicate PowerShell Calc() function."""
    if rec_type == "holiday":
        return {"totalWorked": 0, "extraHrs": 0, "lateHrs": 0, "status": "holiday"}
    if rec_type == "sunday-off":
        return {"totalWorked": 0, "extraHrs": 0, "lateHrs": 0, "status": "sunday"}
    if job_type == "parttime" and (not in_t or not out_t):
        return {"totalWorked": 0, "extraHrs": 0, "lateHrs": 0, "status": "absent"}
    if rec_type == "absent" or (not in_t and not out_t):
        return {"totalWorked": 0, "extraHrs": 0, "lateHrs": 0, "status": "absent"}
    if not in_t or not out_t:
        return {"totalWorked": 0, "extraHrs": 0, "lateHrs": 0, "status": "absent"}

    in_m  = to_mins(in_t)
    out_m = to_mins(out_t)
    if out_m <= in_m:
        out_m += 1440
    cap = min(out_m, MAX_OUT)
    tw  = cap - in_m

    if job_type == "parttime":
        st = "sunday" if get_day_name(ds) == "Sunday" else "ok"
        return {"totalWorked": max(0, tw - perm_mins), "extraHrs": 0, "lateHrs": 0, "status": st, "permMins": perm_mins}

    if get_day_name(ds) == "Sunday":
        tw_net = max(0, tw - perm_mins)
        return {"totalWorked": tw_net, "extraHrs": tw_net, "lateHrs": 0, "status": "sunday", "permMins": perm_mins}

    eh = max(0, SHIFT_START - in_m) + max(0, cap - SHIFT_END)
    la = max(0, in_m - SHIFT_START)
    lv = max(0, SHIFT_END - out_m) if out_m < SHIFT_END else 0
    lh = la + lv
    if in_m < SHIFT_START:
        st = "early"
    elif in_m > SHIFT_START:
        st = "late"
    else:
        st = "ok"
    tw_net = max(0, tw - perm_mins)
    return {"totalWorked": tw_net, "extraHrs": eh, "lateHrs": lh, "status": st, "permMins": perm_mins}

def rec_to_doc(r: dict) -> dict:
    """Strip MongoDB _id before returning to client."""
    r.pop("_id", None)
    return r

# ── Ping ──────────────────────────────────────────────────────────────────────
# /api/attendance/ping is already defined above — skipping duplicate

# ── Employees list ────────────────────────────────────────────────────────────
@app.get("/api/attendance/employees")
async def get_employees():
    if not ensure_db():
        return JSONResponse(content=[], status_code=503)
    names = col("attendance").distinct("name")
    return JSONResponse(content=sorted(names))

# ── Get records ───────────────────────────────────────────────────────────────
@app.get("/api/attendance")
async def get_attendance(
    name:     Optional[str] = Query(None),
    job_type: Optional[str] = Query(None),
    date_from:Optional[str] = Query(None),
    date_to:  Optional[str] = Query(None),
):
    if not ensure_db():
        return JSONResponse(content=[], status_code=503)

    query = {}
    if name:      query["name"] = name
    if job_type:  query["jobType"] = job_type

    rows = list(col("attendance").find(query, {"_id": 0}).sort([("date", ASCENDING), ("name", ASCENDING)]))

    # Date range filter in Python (dates stored as YYYY-MM-DD strings)
    if date_from or date_to:
        filtered = []
        for r in rows:
            rd = r.get("date", "")
            try:
                p = rd.split("-")
                if len(p[0]) == 4:
                    rdate = date(int(p[0]), int(p[1]), int(p[2]))
                else:
                    rdate = date(int(p[2]), int(p[1]), int(p[0]))
                if date_from:
                    df = date_from.split("-")
                    if rdate < date(int(df[0]), int(df[1]), int(df[2])):
                        continue
                if date_to:
                    dt = date_to.split("-")
                    if rdate > date(int(dt[0]), int(dt[1]), int(dt[2])):
                        continue
                filtered.append(r)
            except Exception:
                filtered.append(r)
        rows = filtered

    return JSONResponse(content=rows)

# ── Upsert (add or update) ────────────────────────────────────────────────────
@app.post("/api/attendance")
async def upsert_attendance(request: Request):
    if not ensure_db():
        return err("Database not connected", 503)
    b = await request.json()

    name     = (b.get("name") or "").strip()
    date_str = norm_date((b.get("date") or "").strip())
    rec_type = (b.get("type") or "work").strip().lower()
    job_type = (b.get("jobType") or "fulltime").strip().lower()
    in_t      = (b.get("inTime") or "").strip()
    out_t     = (b.get("outTime") or "").strip()
    perm_mins = int(b.get("permMins") or 0)

    if not name:      return err("name is required")
    if not date_str:  return err("date is required")

    # Recalculate
    c = calc(in_t, out_t, date_str, rec_type, job_type, perm_mins)

    doc = {
        "name":        name,
        "date":        date_str,
        "type":        rec_type,
        "jobType":     job_type,
        "inTime":      in_t  if rec_type == "work" else "",
        "outTime":     out_t if rec_type == "work" else "",
        "permMins":    perm_mins,
        "totalWorked": c["totalWorked"],
        "extraHrs":    c["extraHrs"],
        "lateHrs":     c["lateHrs"],
        "status":      c["status"],
    }

    col("attendance").update_one(
        {"name": name, "date": date_str},
        {"$set": doc},
        upsert=True
    )
    return ok({"record": doc})

# ── Bulk upsert (CSV import) ──────────────────────────────────────────────────
@app.post("/api/attendance/bulk")
async def bulk_upsert(request: Request):
    if not ensure_db():
        return err("Database not connected", 503)
    b    = await request.json()
    rows = b if isinstance(b, list) else b.get("records", [])
    imported = 0
    skipped  = 0
    for row in rows:
        name     = (row.get("name") or "").strip()
        date_str = norm_date((row.get("date") or "").strip())
        if not name or not date_str:
            skipped += 1
            continue
        rec_type = (row.get("type") or "work").strip().lower()
        job_type = (row.get("jobType") or "fulltime").strip().lower()
        in_t     = (row.get("inTime") or "").strip()
        out_t    = (row.get("outTime") or "").strip()
        perm_mins_row = int(row.get("permMins") or 0)
        c = calc(in_t, out_t, date_str, rec_type, job_type, perm_mins_row)
        doc = {
            "name":        name,
            "date":        date_str,
            "type":        rec_type,
            "jobType":     job_type,
            "inTime":      in_t  if rec_type == "work" else "",
            "outTime":     out_t if rec_type == "work" else "",
            "permMins":    perm_mins_row,
            "totalWorked": c["totalWorked"],
            "extraHrs":    c["extraHrs"],
            "lateHrs":     c["lateHrs"],
            "status":      c["status"],
        }
        col("attendance").update_one({"name": name, "date": date_str}, {"$set": doc}, upsert=True)
        imported += 1
    return ok({"imported": imported, "skipped": skipped})

# ── Recalculate all ───────────────────────────────────────────────────────────
@app.post("/api/attendance/recalculate")
async def recalculate_all():
    if not ensure_db():
        return err("Database not connected", 503)
    rows  = list(col("attendance").find({}, {"_id": 0}))
    count = 0
    for r in rows:
        c = calc(r.get("inTime",""), r.get("outTime",""), r.get("date",""), r.get("type","work"), r.get("jobType","fulltime"), int(r.get("permMins") or 0))
        col("attendance").update_one(
            {"name": r["name"], "date": r["date"]},
            {"$set": {"totalWorked": c["totalWorked"], "extraHrs": c["extraHrs"], "lateHrs": c["lateHrs"], "status": c["status"], "permMins": int(r.get("permMins") or 0)}}
        )
        count += 1
    return ok({"recalculated": count})

# ── Delete records ────────────────────────────────────────────────────────────
@app.delete("/api/attendance")
async def delete_attendance(
    name:      Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    if not ensure_db():
        return err("Database not connected", 503)

    # If no filters at all — delete EVERYTHING in the collection
    if not name and not date_from and not date_to:
        result = col("attendance").delete_many({})
        return ok({"deleted": result.deleted_count})

    # Filtered delete — build query
    query = {}
    if name: query["name"] = name
    rows = list(col("attendance").find(query, {"_id": 1, "date": 1}))

    ids_to_delete = []
    for r in rows:
        rd = r.get("date", "")
        try:
            p = rd.split("-")
            if len(p[0]) == 4:
                rdate = date(int(p[0]), int(p[1]), int(p[2]))
            else:
                rdate = date(int(p[2]), int(p[1]), int(p[0]))
            if date_from:
                df = date_from.split("-")
                if rdate < date(int(df[0]), int(df[1]), int(df[2])):
                    continue
            if date_to:
                dt = date_to.split("-")
                if rdate > date(int(dt[0]), int(dt[1]), int(dt[2])):
                    continue
            ids_to_delete.append(r["_id"])
        except Exception:
            ids_to_delete.append(r["_id"])  # include undated records

    if ids_to_delete:
        result = col("attendance").delete_many({"_id": {"$in": ids_to_delete}})
        deleted = result.deleted_count
    else:
        deleted = 0

    return ok({"deleted": deleted})

# ── Delete ALL records (both collections) ────────────────────────────────────
@app.delete("/api/attendance/all")
async def delete_all_attendance():
    if not ensure_db():
        return err("Database not connected", 503)
    try:
        r1 = col("attendance").delete_many({})
        r2 = col("att_records").delete_many({})
        return ok({"deleted": r1.deleted_count + r2.deleted_count,
                   "attendance": r1.deleted_count,
                   "att_records": r2.deleted_count})
    except Exception as e:
        return err(str(e))

# ── Delete single record by name+date ─────────────────────────────────────────
@app.delete("/api/attendance/record")
async def delete_record(name: str = Query(...), date: str = Query(...)):
    if not ensure_db():
        return err("Database not connected", 503)
    result = col("attendance").delete_one({"name": name, "date": norm_date(date)})
    return ok({"deleted": result.deleted_count})

# ══════════════════════════════════════════════════════════════
# END ATTENDANCE
# ══════════════════════════════════════════════════════════════
