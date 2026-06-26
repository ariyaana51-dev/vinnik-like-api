from flask import Flask, request, jsonify
import os
import asyncio
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import binascii
import aiohttp
import requests
import json
import like_pb2
import like_count_pb2
import uid_generator_pb2
from google.protobuf.message import DecodeError
from datetime import datetime, timedelta
from pymongo import MongoClient
import secrets
import string
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import urllib3
import time
import hashlib

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# MongoDB setup - FIXED FOR TERMUX
try:
    # Use direct connection without SRV
    client = MongoClient(
        "mongodb+srv://vijaydhiman200m_db_user:vijaydhiman200m_db_user@cluster0.59s7lx2.mongodb.net/?appName=Cluster0",
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000
    )
    # Test connection
    client.server_info()
    app.logger.info("✅ MongoDB connected successfully")
except Exception as e:
    app.logger.error(f"❌ MongoDB connection failed: {e}")
    # Fallback to local database or create in-memory storage
    client = None

# Initialize database collections
if client:
    db = client["vinnik"]
    keys_collection = db.api_keys
    batch_tracking_collection = db.batch_tracking
    admin_collection = db.admin_users
else:
    # Create in-memory collections as fallback
    keys_collection = {"_storage": {}}
    batch_tracking_collection = {"_storage": {}}
    admin_collection = {"_storage": {}}
    app.logger.warning("⚠️ Using in-memory storage (MongoDB not available)")

# Admin credentials (You can change these)
ADMIN_USERNAME = "NoobVellen"
ADMIN_PASSWORD_HASH = hashlib.sha256("Your_pss".encode()).hexdigest()  # Change this password

# Scheduler for daily reset at midnight UTC
scheduler = BackgroundScheduler(daemon=True)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

# Constants
BATCH_SIZE = 900
last_modified_time = {}

# Initialize admin user if not exists
def init_admin_user():
    try:
        if client:  # Only if MongoDB is available
            admin_user = admin_collection.find_one({"username": ADMIN_USERNAME})
            if not admin_user:
                admin_collection.insert_one({
                    "username": ADMIN_USERNAME,
                    "password_hash": ADMIN_PASSWORD_HASH,
                    "created_at": datetime.utcnow(),
                    "is_active": True
                })
                app.logger.info("✅ Admin user initialized")
    except Exception as e:
        app.logger.error(f"init_admin_user error: {e}")

# Call this function when app starts
init_admin_user()

def authenticate_admin(username, password):
    try:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        if client:
            # MongoDB authentication
            admin_user = admin_collection.find_one({
                "username": username,
                "password_hash": password_hash,
                "is_active": True
            })
            return admin_user is not None
        else:
            # In-memory authentication
            return (username == ADMIN_USERNAME and 
                   hashlib.sha256(password.encode()).hexdigest() == ADMIN_PASSWORD_HASH)
    except Exception as e:
        app.logger.error(f"authenticate_admin error: {e}")
        return False

def admin_required(f):
    def decorated_function(*args, **kwargs):
        try:
            auth = request.authorization
            if not auth or not authenticate_admin(auth.username, auth.password):
                return jsonify({"error": "Admin authentication required"}), 401
            return f(*args, **kwargs)
        except Exception as e:
            app.logger.error(f"admin_required decorator error: {e}")
            return jsonify({"error": "Authentication failed"}), 401
    decorated_function.__name__ = f.__name__
    return decorated_function

# ✅ FIXED SECTION: reset_remaining_requests function
def reset_remaining_requests():
    """Reset all active keys' remaining_requests to total_requests at 20:00 UTC daily"""
    try:
        if not client:
            return
            
        now = datetime.utcnow()
        app.logger.info(f"🔄 [SCHEDULED RESET] Starting daily quota reset at {now.isoformat()} UTC")
        
        # Find all active, non-expired keys that need resetting
        reset_cutoff = now.replace(hour=20, minute=0, second=0, microsecond=0)
        
        # 🔥 IMPROVED LOGIC: Reset only keys that haven't been reset since the cutoff time
        active_keys = keys_collection.find({
            "is_active": True,
            "expires_at": {"$gt": now},
            "$or": [
                {"last_reset": {"$lt": reset_cutoff}},
                {"last_reset": None}  # Handle case where last_reset might not exist
            ]
        })
        
        reset_count = 0
        for key in active_keys:
            result = keys_collection.update_one(
                {
                    "_id": key["_id"],
                    # Race condition prevention: only update if last_reset hasn't changed
                    "last_reset": key.get("last_reset")
                },
                {
                    "$set": {
                        "remaining_requests": key["total_requests"],
                        "last_reset": now
                    }
                }
            )
            if result.modified_count > 0:
                reset_count += 1
                
        app.logger.info(f"✅ [SCHEDULED RESET] Reset {reset_count} keys at {now.isoformat()} UTC")
    except Exception as e:
        app.logger.error(f"❌ [SCHEDULED RESET] reset_remaining_requests error: {e}")

def reset_batch_tracking_daily():
    """Daily reset for batch tracking data (preserves global_batch_index and next_batch_start)"""
    try:
        if not client:
            return
            
        now = datetime.utcnow()
        app.logger.info(f"🔄 Starting daily batch tracking reset at {now.isoformat()} UTC")
        
        servers = ["IND", "BR", "US", "SAC", "NA", "BD"]
        
        for server in servers:
            tracking = batch_tracking_collection.find_one({"server": server})
            if tracking:
                # Preserve global_batch_index and next_batch_start
                preserved_index = tracking.get("current_batch_index", 0)
                total_tokens = tracking.get("total_tokens", 0)
                next_batch_start = tracking.get("next_batch_start", (preserved_index + BATCH_SIZE) % total_tokens if total_tokens > 0 else 0)
                
                # Reset other fields
                reset_fields = {
                    "total_batches_processed": 0,
                    "total_requests": 0,
                    "successful_requests": 0,
                    "success_rate": "0%",
                    "last_reset": now,
                    "last_updated": now
                }
                
                # Keep the preserved values
                reset_fields["current_batch_index"] = preserved_index
                reset_fields["next_batch_start"] = next_batch_start
                reset_fields["batch_size"] = BATCH_SIZE
                reset_fields["total_tokens"] = total_tokens
                
                batch_tracking_collection.update_one(
                    {"server": server},
                    {"$set": reset_fields}
                )
                
                app.logger.info(f"✅ [{server}] Batch tracking reset (index preserved: {preserved_index}, next: {next_batch_start})")
            else:
                # Initialize if not exists
                tokens = load_tokens(server)
                total_tokens = len(tokens) if tokens else 0
                
                batch_tracking_collection.insert_one({
                    "server": server,
                    "current_batch_index": 0,
                    "next_batch_start": BATCH_SIZE % total_tokens if total_tokens > 0 else 0,
                    "total_tokens": total_tokens,
                    "batch_size": BATCH_SIZE,
                    "total_batches_processed": 0,
                    "total_requests": 0,
                    "successful_requests": 0,
                    "success_rate": "0%",
                    "last_reset": now,
                    "last_updated": now
                })
                app.logger.info(f"✅ [{server}] Batch tracking initialized")
        
        app.logger.info(f"🎯 Daily batch tracking reset completed at {now.isoformat()} UTC")
        
    except Exception as e:
        app.logger.error(f"reset_batch_tracking_daily error: {e}")

# Schedule both reset functions at 20:00 UTC (1:30 AM IST)
scheduler.add_job(reset_remaining_requests, 'cron', hour=20, minute=0, second=0, timezone='UTC')
scheduler.add_job(reset_batch_tracking_daily, 'cron', hour=20, minute=0, second=10, timezone='UTC')  # 10 seconds after

# ✅ FIXED SECTION: Helper function for next reset time calculation
# 🔥 IMPROVED LOGIC: Calculate next reset at 20:00 UTC (1:30 AM IST)
def get_next_reset_time():
    """
    Calculate the next reset time at 20:00 UTC (1:30 AM IST).
    If current time is before 20:00 UTC, next reset is today at 20:00 UTC.
    If current time is after 20:00 UTC, next reset is tomorrow at 20:00 UTC.
    """
    now = datetime.utcnow()
    reset_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
    
    if now >= reset_time:
        # Already past today's reset time, next reset is tomorrow
        reset_time += timedelta(days=1)
    
    return reset_time

# ✅ FIXED SECTION: Fallback reset function for scheduler failure
def perform_fallback_reset(key_data):
    """
    🔥 IMPROVED LOGIC: Fallback reset if scheduler fails.
    Checks if key needs resetting based on 20:00 UTC cutoff.
    Prevents duplicate resets using atomic operations.
    """
    try:
        if not client:
            return False
            
        now = datetime.utcnow()
        last_reset = key_data.get('last_reset')
        
        if not last_reset:
            # Never been reset, set initial reset time
            keys_collection.update_one(
                {"key": key_data["key"]},
                {"$set": {"last_reset": now, "remaining_requests": key_data["total_requests"]}}
            )
            return True
            
        if isinstance(last_reset, str):
            last_reset = datetime.fromisoformat(last_reset)
        
        # Calculate today's reset time at 20:00 UTC
        today_reset = now.replace(hour=20, minute=0, second=0, microsecond=0)
        
        # Check if key was last reset before today's reset time and we're past it
        if last_reset < today_reset <= now:            # Need to reset this key
            # Use atomic update to prevent race conditions
            result = keys_collection.update_one(
                {
                    "key": key_data["key"],
                    "last_reset": last_reset  # Only update if last_reset hasn't changed
                },
                {
                    "$set": {
                        "remaining_requests": key_data["total_requests"],
                        "last_reset": now
                    }
                }
            )
            
            if result.modified_count > 0:
                app.logger.info(f"🔄 [FALLBACK] Reset quota for key: {key_data['key'][:8]}...")
                return True
                
        return False
    except Exception as e:
        app.logger.error(f"❌ [FALLBACK] perform_fallback_reset error: {e}")
        return False

def get_batch_index(server_name):
    """Get current batch index from MongoDB - GLOBAL for all keys"""
    try:
        if not client:
            return 0
            
        tracking = batch_tracking_collection.find_one({"server": server_name})
        if tracking:
            return tracking.get("current_batch_index", 0)
        else:
            # Initialize if not exists
            tokens = load_tokens(server_name)
            total_tokens = len(tokens) if tokens else 0
            
            batch_tracking_collection.insert_one({
                "server": server_name,
                "current_batch_index": 0,
                "next_batch_start": BATCH_SIZE % total_tokens if total_tokens > 0 else 0,
                "total_tokens": total_tokens,
                "batch_size": BATCH_SIZE,
                "total_batches_processed": 0,
                "total_requests": 0,
                "successful_requests": 0,
                "success_rate": "0%",
                "last_updated": datetime.utcnow(),
                "last_reset": datetime.utcnow()
            })
            return 0
    except Exception as e:
        app.logger.error(f"get_batch_index error: {e}")
        return 0

def update_batch_index(server_name, new_index, success_count=0):
    """Update batch index in MongoDB - ALWAYS update for continuity"""
    try:
        if not client:
            return
            
        tokens = load_tokens(server_name)
        total_tokens = len(tokens) if tokens else 0
        next_batch_start = (new_index + BATCH_SIZE) % total_tokens if total_tokens > 0 else 0
        
        batch_tracking_collection.update_one(
            {"server": server_name},
            {
                "$set": {
                    "current_batch_index": new_index,
                    "next_batch_start": next_batch_start,
                    "last_updated": datetime.utcnow()
                },
                "$inc": {
                    "total_batches_processed": 1,  # ALWAYS increment batch count
                    "total_requests": 1,
                    "successful_requests": success_count
                }
            },
            upsert=True
        )
        app.logger.info(f"✅ [{server_name}] GLOBAL Batch index updated to {new_index} (Success: {success_count})")
    except Exception as e:
        app.logger.error(f"update_batch_index error: {e}")

# ✅ FINAL FIXED authenticate_key (NO AUTO RESET AFTER 0)

def authenticate_key(api_key):
    try:
        if not client:
            return {
                "key": api_key,
                "total_requests": 1000,
                "remaining_requests": 1000,
                "expires_at": datetime.utcnow() + timedelta(days=30),
                "is_active": True,
                "last_reset": datetime.utcnow()
            }

        key_data = keys_collection.find_one({"key": api_key})
        if not key_data:
            return None

        now = datetime.utcnow()

        # ❌ Expiry check
        if key_data.get('expires_at') and now > key_data['expires_at']:
            keys_collection.update_one({"key": api_key}, {"$set": {"is_active": False}})
            return None

        # ❌ Inactive key
        if not key_data.get('is_active', False):
            return None

        # ✅ RESET ONLY AT 20:00 UTC (1:30 AM IST)
        last_reset = key_data.get('last_reset')

        if last_reset:
            if isinstance(last_reset, str):
                last_reset = datetime.fromisoformat(last_reset)

            today_reset = now.replace(hour=20, minute=0, second=0, microsecond=0)

            # ✔️ Reset only once daily after 20:00 UTC
            if last_reset < today_reset <= now:
                perform_fallback_reset(key_data)
                result = keys_collection.update_one(
                    {
                        "key": api_key,
                        "last_reset": last_reset
                    },
                    {
                        "$set": {
                            "remaining_requests": key_data["total_requests"],
                            "last_reset": now
                        }
                    }
                )

                if result.modified_count > 0:
                    key_data = keys_collection.find_one({"key": api_key})

        else:
            # First time set
            keys_collection.update_one(
                {"key": api_key},
                {"$set": {"last_reset": now}}
            )
            key_data["last_reset"] = now

        # ❌ IMPORTANT: NO fallback reset here
        # (warna 0 hone ke baad bhi reset ho jayega)

        return key_data

    except Exception as e:
        app.logger.error(f"authenticate_key error: {e}")
        return None

def update_key_usage(api_key, decrement=1):
    try:
        if not client:
            return
            
        result = keys_collection.update_one(
            {
                "key": api_key,
                "remaining_requests": {"$gt": 0}  # ✅ IMPORTANT CHECK
            },
            {
                "$inc": {"remaining_requests": -decrement},
                "$set": {"last_used": datetime.utcnow()}
            }
        )
        
        if result.modified_count == 0:
            app.logger.warning(f"⚠️ Quota already exhausted for key: {api_key[:8]}...")
        else:
            app.logger.info(f"📉 Decremented quota for key: {api_key[:8]}...")

    except Exception as e:
        app.logger.error(f"❌ update_key_usage error: {e}")

def load_tokens(server_name):
    try:
        if server_name == "IND":
            with open("token_ind.json", "r") as f:
                tokens = json.load(f)
        elif server_name in {"BR", "US", "SAC", "NA"}:
            with open("token_br.json", "r") as f:
                tokens = json.load(f)
        else:
            with open("token_bd.json", "r") as f:
                tokens = json.load(f)
        return tokens
    except Exception as e:
        app.logger.error(f"load_tokens error for {server_name}: {e}")
        return None

def encrypt_message(plaintext):
    try:
        key = b'Yg&tc%DEuh6%Zc^8'
        iv = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded_message = pad(plaintext, AES.block_size)
        encrypted_message = cipher.encrypt(padded_message)
        return binascii.hexlify(encrypted_message).decode('utf-8')
    except Exception as e:
        app.logger.error(f"encrypt_message error: {e}")
        return None

def create_protobuf_message(user_id, region):
    try:
        message = like_pb2.like()
        message.uid = int(user_id)
        message.region = region
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"create_protobuf_message error: {e}")
        return None

async def send_request(encrypted_uid, token, url):
    try:
        edata = bytes.fromhex(encrypted_uid)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Authorization': f"Bearer {token}",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB54"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=edata, headers=headers, timeout=30) as resp:
                response_text = await resp.text()
                if resp.status != 200:
                    app.logger.error(f"send_request failed status: {resp.status}, response: {response_text}")
                    return f"ERROR:{resp.status}"
                
                # IMPROVED SUCCESS DETECTION
                if '"status":1' in response_text or '"status": 1' in response_text:
                    return {"status": "success", "response": response_text}
                elif '"status":0' in response_text or '"status": 0' in response_text:
                    return {"status": "failed", "response": response_text}
                else:
                    # If no clear status, check for other success indicators
                    if 'error' not in response_text.lower() and 'fail' not in response_text.lower():
                        return {"status": "success", "response": response_text}
                    else:
                        return {"status": "failed", "response": response_text}
    except asyncio.TimeoutError:
        app.logger.error(f"send_request timeout for token")
        return {"status": "timeout", "response": "Request timeout"}
    except Exception as e:
        app.logger.error(f"send_request error: {e}")
        return {"status": "exception", "response": str(e)}

async def send_multiple_requests(uid, server_name, url):
    try:
        app.logger.info(f"🚀 Starting GLOBAL batch requests for UID: {uid}, Server: {server_name}")
        
        region = server_name.upper()
        protobuf_message = create_protobuf_message(uid, region)
        if protobuf_message is None:
            app.logger.error("❌ Failed to create protobuf message")
            return None

        encrypted_uid = encrypt_message(protobuf_message)
        if encrypted_uid is None:
            app.logger.error("❌ Encryption failed")
            return None

        app.logger.info(f"✅ Encryption successful, encrypted UID length: {len(encrypted_uid)}")

        # Load tokens
        tokens = load_tokens(region)
        if not tokens:
            app.logger.error(f"❌ No tokens available for region {region}")
            return None

        total_tokens = len(tokens)
        app.logger.info(f"📊 Total tokens available: {total_tokens}")

        # Get current GLOBAL batch index from MongoDB
        current_index = get_batch_index(region)
        app.logger.info(f"📋 GLOBAL Batch index from DB: {current_index}")

        # Calculate batch range
        start_index = current_index
        end_index = start_index + BATCH_SIZE
        
        # Handle wrap-around if we exceed total tokens
        if end_index > total_tokens:
            batch_tokens = tokens[start_index:] + tokens[0:(end_index - total_tokens)]
            next_index = end_index - total_tokens
        else:
            batch_tokens = tokens[start_index:end_index]
            next_index = end_index

        app.logger.info(f"🔄 Processing GLOBAL batch starting from index {start_index}, using {len(batch_tokens)} tokens")

        # Send current batch
        tasks = []
        for i, token_obj in enumerate(batch_tokens):
            token = token_obj.get("token")
            if token:
                tasks.append(send_request(encrypted_uid, token, url))

        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # IMPROVED SUCCESS COUNTING
        success_count = 0
        failed_count = 0
        timeout_count = 0
        exception_count = 0
        
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                exception_count += 1
                app.logger.warning(f"Token {i}: Exception - {str(result)}")
            elif isinstance(result, dict):
                if result.get("status") == "success":
                    success_count += 1
                    app.logger.info(f"Token {i}: ✅ SUCCESS")
                elif result.get("status") == "failed":
                    failed_count += 1
                    app.logger.warning(f"Token {i}: ❌ FAILED")
                elif result.get("status") == "timeout":
                    timeout_count += 1
                    app.logger.warning(f"Token {i}: ⏰ TIMEOUT")
                else:
                    failed_count += 1
                    app.logger.warning(f"Token {i}: ❓ UNKNOWN")
            else:
                failed_count += 1
                app.logger.warning(f"Token {i}: 🚫 INVALID RESPONSE")

        # ✅ ALWAYS UPDATE BATCH INDEX FOR CONTINUITY
        update_batch_index(region, next_index, success_count)
        
        app.logger.info(f"🎯 [{region}] Batch completed!")
        app.logger.info(f"📊 Results: {success_count}✅ {failed_count}❌ {timeout_count}⏰ {exception_count}🚫")
        app.logger.info(f"📈 Success Rate: {(success_count/len(batch_tokens))*20:.1f}%")
        app.logger.info(f"🔄 Next batch index: {next_index}")

        return success_count
        
    except Exception as e:
        app.logger.error(f"💥 send_multiple_requests error ({server_name}): {e}")
        return None

def create_protobuf(uid):
    try:
        message = uid_generator_pb2.uid_generator()
        message.saturn_ = int(uid)
        message.garena = 1
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"create_protobuf error: {e}")
        return None

def enc(uid):
    protobuf_data = create_protobuf(uid)
    if protobuf_data is None:
        return None
    return encrypt_message(protobuf_data)

def make_request(encrypt, server_name, token):
    try:
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
        else:
            url = "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"

        edata = bytes.fromhex(encrypt)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Authorization': f"Bearer {token}",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB54"
        }
        response = requests.post(url, data=edata, headers=headers, verify=False, timeout=30)
        binary = response.content
        decode = decode_protobuf(binary)
        return decode
    except Exception as e:
        app.logger.error(f"make_request error: {e}")
        return None

def decode_protobuf(binary):
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary)
        return items
    except DecodeError as e:
        app.logger.error(f"decode_protobuf error: {e}")
        return None
    except Exception as e:
        app.logger.error(f"decode_protobuf unexpected error: {e}")
        return None

# SECURED API Key management endpoints with admin authentication
@app.route('/satyalkm/api/key/create', methods=['POST'])
@admin_required
def create_key():
    try:
        if not client:
            return jsonify({"error": "Database not available. Using in-memory storage."}), 500
            
        data = request.get_json()
        custom_key = data.get('custom_key')
        total_requests = int(data.get('total_requests', 1000))
        expiry_days = int(data.get('expiry_days', 30))
        notes = data.get('notes', '')

        if custom_key and keys_collection.find_one({"key": custom_key}):
            return jsonify({"error": "Custom key already exists"}), 400

        api_key = custom_key or ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        expires_at = datetime.utcnow() + timedelta(days=expiry_days)

        key_doc = {
            "key": api_key,
            "created_at": datetime.utcnow(),
            "expires_at": expires_at,
            "total_requests": total_requests,
            "remaining_requests": total_requests,
            "notes": notes,
            "is_active": True,
            "last_reset": datetime.utcnow()
        }
        keys_collection.insert_one(key_doc)

        return jsonify({
            "message": "API key created successfully",
            "key": api_key,
            "expires_at": expires_at.isoformat() + "Z",
            "total_requests": total_requests,
            "notes": notes
        }), 201
    except Exception as e:
        app.logger.error(f"create_key error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/key/update', methods=['PUT'])
@admin_required
def update_key():
    try:
        api_key = request.args.get('key')
        if not api_key:
            return jsonify({"error": "API key required"}), 400

        data = request.get_json()
        total_requests = data.get("total_requests")
        expiry_days = data.get("expiry_days")
        notes = data.get("notes")
        is_active = data.get("is_active")

        update_data = {}

        # ✅ Update requests
        if total_requests:
            update_data["total_requests"] = int(total_requests)
            update_data["remaining_requests"] = int(total_requests)

        # ✅ Update expiry
        if expiry_days:
            update_data["expires_at"] = datetime.utcnow() + timedelta(days=int(expiry_days))

        # ✅ Update notes
        if notes is not None:
            update_data["notes"] = notes

        # ✅ Activate / Deactivate
        if is_active is not None:
            update_data["is_active"] = is_active

        # ❌ Nothing to update
        if not update_data:
            return jsonify({"error": "No valid fields provided"}), 400

        # 🔥 Final update
        keys_collection.update_one(
            {"key": api_key},
            {"$set": update_data}
        )

        return jsonify({
            "message": "Key updated successfully",
            "new_total_requests": total_requests
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/key/check', methods=['GET'])
def check_key():
    try:
        api_key = request.headers.get('X-API-KEY') or request.args.get('key')
        if not api_key:
            return jsonify({"message": "Invalid key", "status": 3}), 401

        key_data = authenticate_key(api_key)
        if not key_data:
            return jsonify({"message": "Invalid key", "status": 3}), 403

        key_data.pop('_id', None)
        for dtfield in ['created_at', 'expires_at', 'last_reset', 'last_used']:
            if dtfield in key_data and isinstance(key_data[dtfield], datetime):
                key_data[dtfield] = key_data[dtfield].isoformat() + "Z"

        return jsonify(key_data), 200
    except Exception as e:
        app.logger.error(f"check_key error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/key/remove', methods=['DELETE'])
@admin_required
def remove_key():
    try:
        api_key = request.args.get('key')
        if not api_key:
            return jsonify({"error": "API key required"}), 400

        key_data = keys_collection.find_one({"key": api_key})
        if not key_data:
            return jsonify({"error": "Key not found"}), 404

        keys_collection.update_one(
            {"key": api_key},
            {"$set": {"is_active": False}}
        )

        return jsonify({
            "message": "Key removed (deactivated) successfully"
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/keys/list', methods=['GET'])
@admin_required
def list_keys():
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 5))
        skip = (page - 1) * limit

        total = keys_collection.count_documents({})
        keys = list(keys_collection.find().skip(skip).limit(limit))

        for k in keys:
            k['_id'] = str(k['_id'])
            if isinstance(k.get('expires_at'), datetime):
                k['expires_at'] = k['expires_at'].isoformat() + "Z"

        return jsonify({
            "keys": keys,
            "pagination": {
                "page": page,
                "total_pages": (total // limit) + 1,
                "has_next": skip + limit < total,
                "has_prev": page > 1
            },
            "statistics": {
                "total_keys": total,
                "active_keys": keys_collection.count_documents({"is_active": True}),
                "inactive_keys": keys_collection.count_documents({"is_active": False})
            }
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/batch/status', methods=['GET'])
@admin_required
def batch_status():
    try:
        server = request.args.get('server')
        
        if server:
            data = batch_tracking_collection.find_one({"server": server})
            if not data:
                return jsonify({"error": "No data"}), 404
            
            data['_id'] = str(data['_id'])
            return jsonify({"global_batch_status": {server: data}}), 200
        
        else:
            all_data = batch_tracking_collection.find()
            result = {}
            for d in all_data:
                result[d['server']] = d
                result[d['server']]['_id'] = str(d['_id'])
            
            return jsonify({"global_batch_status": result}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/batch/reset', methods=['POST'])
@admin_required
def reset_batch():
    try:
        server = request.args.get('server')

        if server:
            batch_tracking_collection.update_one(
                {"server": server},
                {
                    "$set": {
                        "total_batches_processed": 0,
                        "total_requests": 0,
                        "successful_requests": 0,
                        "success_rate": "0%",
                        "last_reset": datetime.utcnow()
                    }
                }
            )
            return jsonify({"message": f"{server} reset done"}), 200
        
        else:
            batch_tracking_collection.update_many(
                {},
                {
                    "$set": {
                        "total_batches_processed": 0,
                        "total_requests": 0,
                        "successful_requests": 0,
                        "success_rate": "0%",
                        "last_reset": datetime.utcnow()
                    }
                }
            )
            return jsonify({"message": "All servers reset done"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/satyalkm/api/force-reset', methods=['GET'])
def force_reset():
    try:
        now = datetime.utcnow()
        key = request.args.get("key")  # 👈 ye line

        if key:
            # 🔑 SINGLE KEY RESET
            key_data = keys_collection.find_one({"key": key})

            if not key_data:
                return jsonify({"error": "Key not found"}), 404

            keys_collection.update_one(
                {"key": key},
                {
                    "$set": {
                        "remaining_requests": key_data["total_requests"],
                        "last_reset": now
                    }
                }
            )

            return jsonify({
                "message": f"{key} reset done"
            }), 200

        else:
            # 🌐 ALL KEYS RESET
            for k in keys_collection.find({"is_active": True}):
                keys_collection.update_one(
                    {"_id": k["_id"]},
                    {
                        "$set": {
                            "remaining_requests": k["total_requests"],
                            "last_reset": now
                        }
                    }
                )

            return jsonify({
                "message": "All keys reset done"
            }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ FIXED SECTION: Main like endpoint with corrected quota check
@app.route('/api/<server_name>/<uid>', methods=['GET'])
def api_like(server_name, uid):
    api_key = request.args.get('key')
    if not api_key:
        return jsonify({"message": "Invalid key", "status": 3}), 401

    key_data = authenticate_key(api_key)
    if not key_data:
        return jsonify({"message": "Invalid key", "status": 3}), 403

    # ✅ FIXED SECTION: Quota check with correct next_reset_time
    if key_data.get('remaining_requests', 0) <= 0:
        # 🔥 IMPROVED LOGIC: Use dynamic UTC-based next reset calculation
        next_reset_time = get_next_reset_time()
        
        # Format the reset time for IST display (1:30 AM IST = 20:00 UTC)
        return jsonify({
            "message": "🚫 Your daily request quota is exhausted. Please wait until next reset at 1:30 AM IST.",
            "status": 3,
            "next_reset": next_reset_time.isoformat() + "Z"
        }), 429

    server_name = server_name.upper()

    try:
        tokens = load_tokens(server_name)
        if not tokens:
            return jsonify({"message": "Token loading failed", "status": 3}), 500

        token = tokens[0]['token']
        encrypted_uid = enc(uid)
        if not encrypted_uid:
            return jsonify({"message": "Encryption failed", "status": 3}), 500

        # Get before likes count
        before = make_request(encrypted_uid, server_name, token)
        if not before:
            return jsonify({"message": "Failed to get player info before likes", "status": 3}), 500

        jsone = MessageToJson(before)
        data_before = json.loads(jsone)
        account_info = data_before.get('AccountInfo', {})
        before_like = int(account_info.get('Likes', 0))
        player_level = int(account_info.get('level', 0))
        player_name = str(account_info.get('PlayerNickname', ''))
        player_uid = int(account_info.get('UID', 0))

        # Determine URL for like request
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/LikeProfile"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/LikeProfile"
        else:
            url = "https://clientbp.ggpolarbear.com/LikeProfile"

        # Send batch requests
        success_count = asyncio.run(send_multiple_requests(uid, server_name, url))

        # Get after likes count
        after = make_request(encrypted_uid, server_name, token)
        if not after:
            return jsonify({"message": "Failed to get player info after likes", "status": 3}), 500

        jsone_after = MessageToJson(after)
        data_after = json.loads(jsone_after)
        account_info_after = data_after.get('AccountInfo', {})
        after_like = int(account_info_after.get('Likes', 0))

        like_given = after_like - before_like

        updated_key_data = authenticate_key(api_key)
        if not updated_key_data:
            return jsonify({"message": "Invalid key", "status": 3}), 403

        if like_given > 0:
            status = 1
            update_key_usage(api_key, 1)

            response = {
                "response": {
                    "KeyExpiresAt": updated_key_data['expires_at'].isoformat() + "Z",
                    "KeyRemainingRequests": f"{updated_key_data['remaining_requests']}/{updated_key_data['total_requests']}",
                    "LikesGivenByAPI": like_given,
                    "LikesafterCommand": after_like,
                    "LikesbeforeCommand": before_like,
                    "PlayerNickname": player_name,
                    "UID": player_uid,
                    "PlayerLevel": player_level,
                    "BatchSuccessCount": success_count
                },
                "status": status
            }
        else:
            status = 2
            expires_at_str = updated_key_data['expires_at'].isoformat() + "Z"
            message = f"UID {player_uid} already used for today. Please wait until 1:30 AM IST for the next request."

            response = {
                "expires_at": expires_at_str,
                "message": message,
                "status": status
            }

        return jsonify(response), 200

    except Exception as e:
        app.logger.error(f"api_like error: {e}")
        return jsonify({"message": "Internal server error", "status": 3}), 500

# ✅ FIXED SECTION: Batch likes endpoint with corrected quota check
@app.route('/api/<server_name>/batch_likes/<uid>', methods=['GET'])
def batch_likes(server_name, uid):
    """Send exactly 20 likes using 20 tokens in one request"""
    try:
        api_key = request.args.get('key')
        if not api_key:
            return jsonify({"message": "API key required", "status": 3}), 401

        key_data = authenticate_key(api_key)
        if not key_data:
            return jsonify({"message": "Invalid or expired API key", "status": 3}), 403

        # ✅ FIXED SECTION: Quota check with correct next_reset_time
        if key_data.get('remaining_requests', 0) <= 0:
            next_reset_time = get_next_reset_time()
            return jsonify({
                "message": "🚫 Your daily request quota is exhausted. Please wait until next reset at 1:30 AM IST.",
                "next_reset": next_reset_time.isoformat() + "Z",
                "status": 3
            }), 429

        server_name = server_name.upper()
        
        # Load tokens for the server
        tokens = load_tokens(server_name)
        if not tokens or len(tokens) < 20:
            return jsonify({
                "message": f"Insufficient tokens available for {server_name}",
                "available_tokens": len(tokens) if tokens else 0,
                "required_tokens": 20,
                "status": 3
            }), 500

        # Get current GLOBAL batch index
        current_index = get_batch_index(server_name)
        
        # Select 20 consecutive tokens starting from current index
        total_tokens = len(tokens)
        selected_tokens = []
        
        # Collect 20 tokens with wrap-around if needed
        for i in range(20):
            token_index = (current_index + i) % total_tokens
            if tokens[token_index].get("token"):
                selected_tokens.append(tokens[token_index]["token"])
        
        if len(selected_tokens) < 20:
            return jsonify({
                "message": "Could not find 20 valid tokens",
                "valid_tokens_found": len(selected_tokens),
                "status": 3
            }), 500

        # Create protobuf message and encrypt
        region = server_name.upper()
        protobuf_message = create_protobuf_message(uid, region)
        if protobuf_message is None:
            return jsonify({"message": "Failed to create protobuf message", "status": 3}), 500

        encrypted_uid = encrypt_message(protobuf_message)
        if encrypted_uid is None:
            return jsonify({"message": "Encryption failed", "status": 3}), 500

        # Determine URL for like request
        if server_name == "IND":
            url = "https://client.ind.freefiremobile.com/LikeProfile"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            url = "https://client.us.freefiremobile.com/LikeProfile"
        else:
            url = "https://clientbp.ggpolarbear.com/LikeProfile"

        # Get player info before sending likes
        token = selected_tokens[0]  # Use first token for checking
        encrypted_uid_check = enc(uid)
        if not encrypted_uid_check:
            return jsonify({"message": "Encryption failed for player info", "status": 3}), 500

        before = make_request(encrypted_uid_check, server_name, token)
        if not before:
            return jsonify({"message": "Failed to get player info", "status": 3}), 500

        jsone = MessageToJson(before)
        data_before = json.loads(jsone)
        account_info = data_before.get('AccountInfo', {})
        before_like = int(account_info.get('Likes', 0))
        player_level = int(account_info.get('level', 0))
        player_name = str(account_info.get('PlayerNickname', ''))
        player_uid = int(account_info.get('UID', 0))

        # Send 20 likes concurrently
        app.logger.info(f"🚀 Sending 20 likes to UID: {uid}, Server: {server_name}")
        
        async def send_20_likes():
            tasks = []
            for i, token in enumerate(selected_tokens):
                task = asyncio.create_task(send_request(encrypted_uid, token, url))
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = 0
            failed_count = 0
            detailed_results = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    detailed_results.append({"token_index": i, "status": "exception", "error": str(result)})
                    failed_count += 1
                elif isinstance(result, dict):
                    if result.get("status") == "success":
                        detailed_results.append({"token_index": i, "status": "success"})
                        success_count += 1
                    else:
                        detailed_results.append({"token_index": i, "status": result.get("status", "failed"), "response": result.get("response", "")})
                        failed_count += 1
                else:
                    detailed_results.append({"token_index": i, "status": "unknown", "response": str(result)})
                    failed_count += 1
            
            return success_count, failed_count, detailed_results

        # Run the async function
        success_count, failed_count, detailed_results = asyncio.run(send_20_likes())
        
        # Update GLOBAL batch index (move forward by 20)
        new_index = (current_index + 20) % total_tokens
        update_batch_index(server_name, new_index, success_count)
        
        # Get player info after sending likes
        after = make_request(encrypted_uid_check, server_name, token)
        if after:
            jsone_after = MessageToJson(after)
            data_after = json.loads(jsone_after)
            account_info_after = data_after.get('AccountInfo', {})
            after_like = int(account_info_after.get('Likes', 0))
        else:
            after_like = before_like  # If failed to get after info, use before count

        like_given = after_like - before_like
        
        # Update API key usage (count as 1 request regardless of 20 tokens used)
        update_key_usage(api_key, 1)
        
        # Get updated key info
        updated_key_data = authenticate_key(api_key)
        
        # Prepare response
        if like_given > 0:
            status = 1
            response = {
                "response": {
                    "successful_likes": success_count,
                    "failed_likes": failed_count,
                    "total_likes_sent": like_given,
                    "before_likes": before_like,
                    "after_likes": after_like,
                    "player_info": {
                        "uid": player_uid,
                        "name": player_name,
                        "level": player_level
                    },
                    "api_key_info": {
                        "remaining_requests": updated_key_data['remaining_requests'],
                        "total_requests": updated_key_data['total_requests'],
                        "expires_at": updated_key_data['expires_at'].isoformat() + "Z"
                    },
                    "server": server_name,
                    "tokens_used": 20,
                    "next_batch_start": new_index,
                    "detailed_results": detailed_results
                },
                "status": status,
                "message": f"Successfully sent {like_given} likes using 20 tokens"
            }
        else:
            status = 2
            response = {
                "status": status,
                "message": "No likes were added. Player might have reached daily limit.",
                "details": {
                    "before_likes": before_like,
                    "after_likes": after_like,
                    "successful_api_calls": success_count,
                    "failed_api_calls": failed_count,
                    "player_uid": player_uid,
                    "server": server_name
                }
            }

        app.logger.info(f"✅ 20-token batch completed: {success_count}✅ {failed_count}❌ Likes added: {like_given}")
        
        return jsonify(response), 200

    except Exception as e:
        app.logger.error(f"batch_likes error: {e}")
        return jsonify({
            "message": "Internal server error",
            "error": str(e),
            "status": 3
        }), 500

# Favicon route
@app.route('/favicon.ico')
def favicon():
    return '', 204

# Test route with GLOBAL batch info
@app.route('/test/<server_name>/<uid>')
def test_route(server_name, uid):
    try:
        tokens = load_tokens(server_name.upper())
        region = server_name.upper()
        
        # Calculate current GLOBAL batch info
        total_tokens = len(tokens) if tokens else 0
        current_index = get_batch_index(region)
        start_index = current_index
        end_index = min(current_index + BATCH_SIZE, total_tokens)
        
        # Get GLOBAL batch tracking info from DB
        tracking_info = None
        if client:
            tracking_info = batch_tracking_collection.find_one({"server": region})
        
        return jsonify({
            "tokens_count": total_tokens,
            "server": server_name,
            "uid": uid,
            "status": "test_ok",
            "global_batch_index": current_index,
            "global_batch_range": f"{start_index}-{end_index}",
            "total_batches_processed": tracking_info.get("total_batches_processed", 0) if tracking_info else 0,
            "total_requests": tracking_info.get("total_requests", 0) if tracking_info else 0,
            "successful_requests": tracking_info.get("successful_requests", 0) if tracking_info else 0,
            "last_updated": tracking_info.get("last_updated").isoformat() + "Z" if tracking_info and tracking_info.get("last_updated") else "Never",
            "last_reset": tracking_info.get("last_reset").isoformat() + "Z" if tracking_info and tracking_info.get("last_reset") else "Never"
        })
    except Exception as e:
        return jsonify({"error": str(e)})

# Admin login endpoint
@app.route('/satyalkm/admin/login', methods=['POST'])
def admin_login():
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        if authenticate_admin(username, password):
            return jsonify({
                "message": "Login successful",
                "status": 1
            }), 200
        else:
            return jsonify({
                "error": "Invalid credentials",
                "status": 0
            }), 401
    except Exception as e:
        app.logger.error(f"admin_login error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return jsonify({
        "message": "Like API is running",
        "new_endpoint": "/api/<server>/batch_likes/<uid>?key=API_KEY",
        "note": "Batch likes endpoint uses exactly 20 tokens"
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
