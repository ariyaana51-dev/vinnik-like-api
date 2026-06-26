from flask import Flask, request, jsonify
import os
import asyncio
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MesfsageToJson
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

# MongoDB setup with improved error handling
try:
    # Updated connection string with database name
    connection_string = "mongodb+srv://vijaydhiman200m_db_user:vijaydhiman200m_db_user@cluster0.59s7lx2.mongodb.net/?appName=Cluster0"
    
    # Add connection timeout and retry settings
    client = MongoClient(
        connection_string,
        serverSelectionTimeoutMS=10000,  # 10 second timeout
        connectTimeoutMS=15000,  # 15 second connection timeout
        socketTimeoutMS=45000,  # 45 second socket timeout
        maxPoolSize=50,
        minPoolSize=10,
        retryWrites=True,
        retryReads=True
    )
    
    # Test the connection
    client.admin.command('ping')
    app.logger.info("✅ MongoDB connected successfully")
    
    db = client["vinnik"]
    keys_collection = db.api_keys
    batch_tracking_collection = db.batch_tracking
    admin_collection = db.admin_users
    
except Exception as e:
    app.logger.error(f"❌ MongoDB connection failed: {e}")
    # Create dummy collections to prevent app crash during initialization
    class DummyCollection:
        def find_one(self, *args, **kwargs): return None
        def insert_one(self, *args, **kwargs): 
            class Result:
                inserted_id = None
            return Result()
        def update_one(self, *args, **kwargs): 
            class Result:
                modified_count = 0
                upserted_id = None
                matched_count = 0
            return Result()
        def count_documents(self, *args, **kwargs): return 0
        def find(self, *args, **kwargs): return []
        def sort(self, *args, **kwargs): return self
        def skip(self, *args, **kwargs): return self
        def limit(self, *args, **kwargs): return []
    
    client = None
    db = None
    keys_collection = DummyCollection()
    batch_tracking_collection = DummyCollection()
    admin_collection = DummyCollection()

# Admin credentials (You can change these)
ADMIN_USERNAME = "NoobVellen"
ADMIN_PASSWORD_HASH = hashlib.sha256("your_pass".encode()).hexdigest()  # Change this password

# Scheduler for daily reset at midnight UTC
scheduler = BackgroundScheduler(daemon=True)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

# Constants
BATCH_SIZE = 220
last_modified_time = {}

def is_mongodb_connected():
    """Check if MongoDB is connected"""
    return client is not None and db is not None

# Initialize admin user if not exists
def init_admin_user():
    try:
        if not is_mongodb_connected():
            app.logger.warning("⚠️ MongoDB not connected, skipping admin initialization")
            return
            
        admin_user = admin_collection.find_one({"username": ADMIN_USERNAME})
        if not admin_user:
            admin_collection.insert_one({
                "username": ADMIN_USERNAME,
                "password_hash": ADMIN_PASSWORD_HASH,
                "created_at": datetime.utcnow(),
                "is_active": True
            })
            app.logger.info("✅ Admin user initialized")
        else:
            app.logger.info("✅ Admin user already exists")
    except Exception as e:
        app.logger.error(f"init_admin_user error: {e}")

# Call this function when app starts
init_admin_user()

def authenticate_admin(username, password):
    try:
        if not is_mongodb_connected():
            app.logger.error("MongoDB not connected for admin authentication")
            return False
            
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        admin_user = admin_collection.find_one({
            "username": username,
            "password_hash": password_hash,
            "is_active": True
        })
        return admin_user is not None
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

def reset_remaining_requests():
    try:
        if not is_mongodb_connected():
            app.logger.warning("⚠️ MongoDB not connected, skipping request reset")
            return
            
        now = datetime.utcnow()
        active_keys = keys_collection.find({
            "is_active": True,
            "expires_at": {"$gt": now}
        })
        for key in active_keys:
            keys_collection.update_one(
                {"_id": key["_id"]},
                {
                    "$set": {
                        "remaining_requests": key["total_requests"],
                        "last_reset": now
                    }
                }
            )
        app.logger.info(f"Requests reset at {now.isoformat()} UTC")
    except Exception as e:
        app.logger.error(f"reset_remaining_requests error: {e}")

def reset_batch_tracking_daily():
    """Daily reset for batch tracking data (preserves global_batch_index and next_batch_start)"""
    try:
        if not is_mongodb_connected():
            app.logger.warning("⚠️ MongoDB not connected, skipping batch tracking reset")
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

# Schedule both reset functions
scheduler.add_job(reset_remaining_requests, 'cron', hour=0, minute=0, second=0, timezone='UTC')
scheduler.add_job(reset_batch_tracking_daily, 'cron', hour=0, minute=0, second=10, timezone='UTC')  # 10 seconds after

def get_batch_index(server_name):
    """Get current batch index from MongoDB - GLOBAL for all keys"""
    try:
        if not is_mongodb_connected():
            app.logger.warning(f"⚠️ MongoDB not connected for batch index ({server_name})")
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
        if not is_mongodb_connected():
            app.logger.warning(f"⚠️ MongoDB not connected for batch index update ({server_name})")
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

def authenticate_key(api_key):
    try:
        if not is_mongodb_connected():
            app.logger.warning("⚠️ MongoDB not connected for key authentication")
            return None
            
        key_data = keys_collection.find_one({"key": api_key})
        if not key_data:
            return None
        now = datetime.utcnow()
        if key_data.get('expires_at') and now > key_data['expires_at']:
            keys_collection.update_one({"key": api_key}, {"$set": {"is_active": False}})
            return None
        if not key_data.get('is_active', False):
            return None
        last_reset = key_data.get('last_reset')
        if last_reset:
            if isinstance(last_reset, str):
                last_reset = datetime.fromisoformat(last_reset)
            if last_reset.date() < now.date():
                keys_collection.update_one(
                    {"key": api_key},
                    {"$set": {
                        "remaining_requests": key_data['total_requests'],
                        "last_reset": now
                    }}
                )
                key_data['remaining_requests'] = key_data['total_requests']
        return key_data
    except Exception as e:
        app.logger.error(f"authenticate_key error: {e}")
        return None

def update_key_usage(api_key, decrement=1):
    try:
        if not is_mongodb_connected():
            app.logger.warning("⚠️ MongoDB not connected for key usage update")
            return
            
        keys_collection.update_one(
            {"key": api_key},
            {
                "$inc": {"remaining_requests": -decrement},
                "$set": {"last_used": datetime.utcnow()}
            }
        )
    except Exception as e:
        app.logger.error(f"update_key_usage error: {e}")

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
        app.logger.info(f"📈 Success Rate: {(success_count/len(batch_tokens))*220:.1f}%")
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

# Health check endpoint
@app.route('/satyalkm/health', methods=['GET'])
def health_check():
    try:
        mongo_status = "connected" if is_mongodb_connected() else "disconnected"
        return jsonify({
            "status": "ok",
            "mongodb": mongo_status,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "admin_initialized": True,
            "scheduler_running": scheduler.running
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# SECURED API Key management endpoints with admin authentication
@app.route('/satyalkm/api/key/create', methods=['POST'])
@admin_required
def create_key():
    try:
        data = request.get_json()
        custom_key = data.get('custom_key')
        total_requests = int(data.get('total_requests', 1000))
        expiry_days = int(data.get('expiry_days', 30))
        notes = data.get('notes', '')

        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500

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

@app.route('/api/key/check', methods=['GET'])
def check_key():
    try:
        api_key = request.headers.get('X-API-KEY') or request.args.get('key')
        if not api_key:
            return jsonify({"message": "Invalid key", "status": 3}), 401

        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500

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

@app.route('/satyalkm/api/key/remove', methods=['DELETE'])
@admin_required
def remove_key():
    try:
        api_key = request.headers.get('X-API-KEY') or request.args.get('key')
        if not api_key:
            return jsonify({"message": "Invalid key", "status": 3}), 401

        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500

        key_data = authenticate_key(api_key)
        if not key_data:
            return jsonify({"message": "Invalid key", "status": 3}), 403

        result = keys_collection.update_one({"key": api_key}, {"$set": {"is_active": False}})
        if result.modified_count == 1:
            return jsonify({"message": "API key deactivated successfully"}), 200
        else:
            return jsonify({"error": "Failed to deactivate API key"}), 400
    except Exception as e:
        app.logger.error(f"remove_key error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/satyalkm/api/key/update', methods=['PUT'])
@admin_required
def update_key():
    try:
        api_key = request.headers.get('X-API-KEY') or request.args.get('key')
        if not api_key:
            return jsonify({"message": "Invalid key", "status": 3}), 401

        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500

        key_data = authenticate_key(api_key)
        if not key_data:
            return jsonify({"message": "Invalid key", "status": 3}), 403

        data = request.get_json()
        update_fields = {}

        if 'total_requests' in data:
            total_requests = int(data['total_requests'])
            update_fields['total_requests'] = total_requests
            if total_requests > key_data.get('total_requests', 0):
                update_fields['remaining_requests'] = total_requests - (key_data.get('total_requests', 0) - key_data.get('remaining_requests', 0))

        if 'expiry_days' in data:
            expiry_days = int(data['expiry_days'])
            update_fields['expires_at'] = datetime.utcnow() + timedelta(days=expiry_days)

        if 'is_active' in data:
            update_fields['is_active'] = bool(data['is_active'])

        if 'notes' in data:
            update_fields['notes'] = str(data['notes'])

        if not update_fields:
            return jsonify({"error": "No valid fields to update"}), 400

        result = keys_collection.update_one({"key": api_key}, {"$set": update_fields})
        if result.modified_count == 1:
            return jsonify({"message": "API key updated successfully"}), 200
        else:
            return jsonify({"error": "No changes made to API key"}), 400
    except Exception as e:
        app.logger.error(f"update_key error: {e}")
        return jsonify({"error": str(e)}), 500

# NEW ENDPOINT: List all API keys
@app.route('/satyalkm/api/keys/list', methods=['GET'])
@admin_required
def list_all_keys():
    try:
        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500
            
        # Get query parameters for filtering
        show_inactive = request.args.get('show_inactive', 'false').lower() == 'true'
        limit = int(request.args.get('limit', 50))
        page = int(request.args.get('page', 1))
        
        # Build query
        query = {}
        if not show_inactive:
            query["is_active"] = True
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Get total count
        total_keys = keys_collection.count_documents(query)
        total_pages = (total_keys + limit - 1) // limit
        
        # Get keys with pagination
        keys_cursor = keys_collection.find(query).sort("created_at", -1).skip(skip).limit(limit)
        
        keys_list = []
        for key in keys_cursor:
            key_data = {
                "key": key.get("key"),
                "total_requests": key.get("total_requests"),
                "remaining_requests": key.get("remaining_requests"),
                "is_active": key.get("is_active", True),
                "created_at": key.get("created_at").isoformat() + "Z" if key.get("created_at") else None,
                "expires_at": key.get("expires_at").isoformat() + "Z" if key.get("expires_at") else None,
                "last_used": key.get("last_used").isoformat() + "Z" if key.get("last_used") else "Never",
                "last_reset": key.get("last_reset").isoformat() + "Z" if key.get("last_reset") else None,
                "notes": key.get("notes", "")
            }
            keys_list.append(key_data)
        
        # Calculate statistics
        active_keys_count = keys_collection.count_documents({"is_active": True})
        inactive_keys_count = keys_collection.count_documents({"is_active": False})
        total_requests_available = sum([key.get('remaining_requests', 0) for key in keys_list if key.get('is_active')])
        
        response_data = {
            "keys": keys_list,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_keys": total_keys,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "statistics": {
                "active_keys": active_keys_count,
                "inactive_keys": inactive_keys_count,
                "total_keys": total_keys,
                "total_requests_available": total_requests_available
            },
            "status": 1
        }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        app.logger.error(f"list_all_keys error: {e}")
        return jsonify({"error": str(e)}), 500

# Main like endpoint (without satyalkm prefix)
@app.route('/api/<server_name>/<uid>', methods=['GET'])
def api_like(server_name, uid):
    api_key = request.args.get('key')
    if not api_key:
        return jsonify({"message": "Invalid key", "status": 3}), 401

    if not is_mongodb_connected():
        return jsonify({"error": "Database not connected"}), 500

    key_data = authenticate_key(api_key)
    if not key_data:
        return jsonify({"message": "Invalid key", "status": 3}), 403

    if key_data.get('remaining_requests', 0) <= 0:
        next_reset_time = (datetime.utcnow() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return jsonify({
            "message": "Your daily request quota is exhausted. Please wait until the next reset at 12:00 AM UTC to continue using the API.",
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
            message = f"UID {player_uid} already used for today. Please wait until 1:30 AM Sri Lankan time for the next request."

            response = {
                "expires_at": expires_at_str,
                "message": message,
                "status": status
            }

        return jsonify(response), 200

    except Exception as e:
        app.logger.error(f"api_like error: {e}")
        return jsonify({"message": "Internal server error", "status": 3}), 500

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

# Reset GLOBAL batch index endpoint
@app.route('/satyalkm/api/batch/reset', methods=['POST'])
@admin_required
def reset_batch():
    try:
        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500
            
        server_name = request.args.get('server', '').upper()
        servers = [server_name] if server_name else ["IND", "BR", "US", "SAC", "NA", "BD"]

        reset_fields = {
            "total_batches_processed": 0,
            "total_requests": 0,
            "successful_requests": 0,
            "success_rate": "0%",
            "batch_size": 220,
            "last_updated": datetime.utcnow(),
            "last_reset": datetime.utcnow()
        }

        for server in servers:
            tracking = batch_tracking_collection.find_one({"server": server})
            if tracking:
                # Preserve global_batch_index and next_batch_start
                preserved_index = tracking.get("current_batch_index", 0)
                total_tokens = tracking.get("total_tokens", 0)
                next_batch_start = tracking.get("next_batch_start", (preserved_index + BATCH_SIZE) % total_tokens if total_tokens > 0 else 0)

                reset_fields["current_batch_index"] = preserved_index
                reset_fields["next_batch_start"] = next_batch_start
                reset_fields["total_tokens"] = total_tokens

                batch_tracking_collection.update_one(
                    {"server": server},
                    {"$set": reset_fields}
                )

        return jsonify({
            "message": "Batch data reset for all servers (index preserved)",
            "status": 1
        }), 200

    except Exception as e:
        app.logger.error(f"reset_batch error: {e}")
        return jsonify({"error": str(e)}), 500

# Get GLOBAL batch status endpoint
@app.route('/satyalkm/api/batch/status', methods=['GET'])
@admin_required
def batch_status():
    try:
        if not is_mongodb_connected():
            return jsonify({"error": "Database not connected"}), 500
            
        server_name = request.args.get('server', '').upper()
        status_info = {}
        
        if server_name:
            tokens = load_tokens(server_name)
            total_tokens = len(tokens) if tokens else 0
            current_index = get_batch_index(server_name)
            tracking_info = batch_tracking_collection.find_one({"server": server_name})
            
            status_info[server_name] = {
                "global_batch_index": current_index,
                "next_batch_start": tracking_info.get("next_batch_start", (current_index + BATCH_SIZE) % total_tokens if total_tokens > 0 else 0),
                "total_tokens": total_tokens,
                "batch_size": BATCH_SIZE,
                "total_batches_processed": tracking_info.get("total_batches_processed", 0) if tracking_info else 0,
                "total_requests": tracking_info.get("total_requests", 0) if tracking_info else 0,
                "successful_requests": tracking_info.get("successful_requests", 0) if tracking_info else 0,
                "success_rate": f"{(tracking_info.get('successful_requests', 0) / tracking_info.get('total_requests', 1)) * 100:.1f}%" if tracking_info and tracking_info.get('total_requests', 0) > 0 else "0%",
                "last_updated": tracking_info.get("last_updated").isoformat() + "Z" if tracking_info and tracking_info.get("last_updated") else "Never",
                "last_reset": tracking_info.get("last_reset").isoformat() + "Z" if tracking_info and tracking_info.get("last_reset") else "Never"
            }
        else:
            servers = ["IND", "BR", "US", "SAC", "NA", "BD"]
            for server in servers:
                tokens = load_tokens(server)
                total_tokens = len(tokens) if tokens else 0
                current_index = get_batch_index(server)
                tracking_info = batch_tracking_collection.find_one({"server": server})
                
                status_info[server] = {
                    "global_batch_index": current_index,
                    "next_batch_start": tracking_info.get("next_batch_start", (current_index + BATCH_SIZE) % total_tokens if total_tokens > 0 else 0),
                    "total_tokens": total_tokens,
                    "batch_size": BATCH_SIZE,
                    "total_batches_processed": tracking_info.get("total_batches_processed", 0) if tracking_info else 0,
                    "total_requests": tracking_info.get("total_requests", 0) if tracking_info else 0,
                    "successful_requests": tracking_info.get("successful_requests", 0) if tracking_info else 0,
                    "success_rate": f"{(tracking_info.get('successful_requests', 0) / tracking_info.get('total_requests', 1)) * 100:.1f}%" if tracking_info and tracking_info.get('total_requests', 0) > 0 else "0%",
                    "last_updated": tracking_info.get("last_updated").isoformat() + "Z" if tracking_info and tracking_info.get("last_updated") else "Never",
                    "last_reset": tracking_info.get("last_reset").isoformat() + "Z" if tracking_info and tracking_info.get("last_reset") else "Never"
                }

        return jsonify({"global_batch_status": status_info, "status": 1}), 200
    except Exception as e:
        app.logger.error(f"batch_status error: {e}")
        return jsonify({"error": str(e)}), 500

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
        "mongodb_connected": is_mongodb_connected(),
        "version": "1.0.0"
    })

if __name__ == '__main__':
    app.run(debug=True)
