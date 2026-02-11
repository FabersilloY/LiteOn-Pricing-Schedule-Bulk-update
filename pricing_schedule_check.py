#!/usr/bin/env python3
import sys
import subprocess
import json
import os
import signal
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

# ============================================================================
# Cache configuration (shared with site_lookup.py)
# ============================================================================
CACHE_DIR = Path.home() / ".cache" / "site_lookup"
CACHE_FILE = CACHE_DIR / "sites_cache.json"
CACHE_MAX_AGE_DAYS = 7

# Progress file for resuming interrupted runs
PROGRESS_FILE = CACHE_DIR / "pricing_check_progress.json"

# Odd ones out tracking file
ODD_ONES_FILE = CACHE_DIR / "pricing_odd_ones_out.json"

# JWT refresh interval (10 minutes)
JWT_REFRESH_INTERVAL = 600  # seconds

# Global state for JWT management
_jwt_refresh_timer: Optional[threading.Timer] = None
_jwt_lock = threading.Lock()


# ============================================================================
# PFID Parsing
# ============================================================================
def parse_pfid_input(args: List[str]) -> Dict[str, Optional[str]]:
    """
    Parse PFID input from command line arguments.
    Supports both space-separated and dash-separated formats:
    - ACN only: "0051" or "0051"
    - ACN-ACC: "0051 09" or "0051-09"
    - ACN-ACC-ACG: "0051 09 02" or "0051-09-02"
    - ACN-ACC-ACG-ACS: "0051 09 02 02" or "0051-09-02-02"
    
    Returns dict with keys: acn, acc, acg, acs, mode
    """
    # Join all args and normalize separators
    combined = " ".join(args)
    
    # Split by both spaces and dashes
    parts = []
    for part in combined.replace("-", " ").split():
        if part.strip():
            parts.append(part.strip())
    
    result = {
        "acn": None,
        "acc": None,
        "acg": None,
        "acs": None,
        "mode": None
    }
    
    if len(parts) >= 1:
        result["acn"] = parts[0]
        result["mode"] = "acn-only"
    
    if len(parts) >= 2:
        result["acc"] = parts[1]
        result["mode"] = "acn-acc"
    
    if len(parts) >= 3:
        result["acg"] = parts[2]
        result["mode"] = "acn-acc-acg"
    
    if len(parts) >= 4:
        result["acs"] = parts[3]
        result["mode"] = "acn-acc-acg-acs"
    
    return result


def build_pfid_prefix(acn: str, acc: str = None, acg: str = None, acs: str = None) -> str:
    """
    Build a PFID prefix from components.
    PFID format: ACN-ACC-ACG-ACS (e.g., 0051-09-02-02)
    """
    parts = [acn]
    if acc:
        parts.append(acc)
    if acg:
        parts.append(acg)
    if acs:
        parts.append(acs)
    return "-".join(parts)


def format_pfid_display(acn: str, acc: str = None, acg: str = None, acs: str = None) -> str:
    """
    Format PFID components for display.
    """
    if acs:
        return f"ACN: {acn}, ACC: {acc}, ACG: {acg}, ACS: {acs}"
    elif acg:
        return f"ACN: {acn}, ACC: {acc}, ACG: {acg}"
    elif acc:
        return f"ACN: {acn}, ACC: {acc}"
    else:
        return f"ACN: {acn}"


# ============================================================================
# JWT Management
# ============================================================================
def get_jwt() -> bool:
    """
    Execute set_jwt.sh to get/refresh the JWT token.
    Returns True if successful, False otherwise.
    """
    print("[INFO] Obtaining JWT token...")
    
    # We need to source the script in a subshell and capture the exported variable
    # Since we can't directly source in Python, we'll run the underlying command
    # and set the environment variable ourselves
    try:
        # Run powerflex_get_jwt_pass.sh directly and capture the JWT
        result = subprocess.run(
            ["powerflex_get_jwt_pass.sh", "prd"],
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout for potential password prompts
        )
        
        if result.returncode != 0:
            print(f"[ERROR] Failed to get JWT: {result.stderr}")
            return False
        
        jwt_token = result.stdout.strip()
        if not jwt_token or jwt_token == "null":
            print("[ERROR] JWT token is empty or null")
            return False
        
        # Set the environment variable
        os.environ["EDF_JWT"] = jwt_token
        print("[INFO] JWT token obtained successfully")
        return True
        
    except subprocess.TimeoutExpired:
        print("[ERROR] JWT retrieval timed out (possible password prompt)")
        print("[INFO] Please run 'source set_jwt.sh' manually first to cache credentials")
        return False
    except FileNotFoundError:
        print("[ERROR] powerflex_get_jwt_pass.sh not found in PATH")
        return False
    except Exception as e:
        print(f"[ERROR] Failed to get JWT: {e}")
        return False


def start_jwt_refresh_timer():
    """Start a background timer to refresh JWT every 10 minutes."""
    global _jwt_refresh_timer
    
    def refresh_jwt():
        global _jwt_refresh_timer
        with _jwt_lock:
            print("\n[INFO] Refreshing JWT token (10 minute interval)...")
            if get_jwt():
                print("[INFO] JWT refreshed successfully")
            else:
                print("[WARNING] JWT refresh failed - API calls may fail")
            
            # Schedule next refresh
            _jwt_refresh_timer = threading.Timer(JWT_REFRESH_INTERVAL, refresh_jwt)
            _jwt_refresh_timer.daemon = True
            _jwt_refresh_timer.start()
    
    # Cancel any existing timer
    stop_jwt_refresh_timer()
    
    # Start new timer
    _jwt_refresh_timer = threading.Timer(JWT_REFRESH_INTERVAL, refresh_jwt)
    _jwt_refresh_timer.daemon = True
    _jwt_refresh_timer.start()


def stop_jwt_refresh_timer():
    """Stop the JWT refresh timer."""
    global _jwt_refresh_timer
    if _jwt_refresh_timer is not None:
        _jwt_refresh_timer.cancel()
        _jwt_refresh_timer = None


# ============================================================================
# Cache Management (shared with site_lookup.py)
# ============================================================================
def is_cache_valid() -> bool:
    """Check if the cache file exists and is less than 7 days old."""
    if not CACHE_FILE.exists():
        return False
    
    cache_mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
    cache_age = datetime.now() - cache_mtime
    
    return cache_age < timedelta(days=CACHE_MAX_AGE_DAYS)


def load_cached_data() -> Optional[List[Dict[str, Any]]]:
    """Load site data from cache file."""
    if not CACHE_FILE.exists():
        return None
    
    try:
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARNING] Failed to load cache: {e}")
        return None


def save_to_cache(data: List[Dict[str, Any]]):
    """Save site data to cache file."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f)
        print(f"[INFO] Cache updated: {CACHE_FILE}")
    except IOError as e:
        print(f"[WARNING] Failed to save cache: {e}")


def fetch_site_data_from_api() -> List[Dict[str, Any]]:
    """Fetch site data from PowerFlex API."""
    print("[INFO] Fetching site data from PowerFlex API...")
    
    try:
        result = subprocess.run([
            'curl_device_manager.sh',
            '-H', 'Content-Type: application/json',
            '-X', 'GET',
            'https://powerflex.io/asset-mgmt/api/site?barebones=true'
        ], capture_output=True, text=True, check=True)
        
        # Check for JWT expiration
        if "Jwt is expired" in result.stdout or "expired" in result.stdout.lower():
            print("[ERROR] JWT token has expired. Attempting refresh...")
            if get_jwt():
                # Retry the request
                result = subprocess.run([
                    'curl_device_manager.sh',
                    '-H', 'Content-Type: application/json',
                    '-X', 'GET',
                    'https://powerflex.io/asset-mgmt/api/site?barebones=true'
                ], capture_output=True, text=True, check=True)
            else:
                sys.exit(1)
        
        # Find the JSON content (skip curl debug output)
        stdout_lines = result.stdout.strip().split('\n')
        json_content = ""
        json_started = False
        
        for line in stdout_lines:
            if line.strip().startswith('[') or line.strip().startswith('{'):
                json_started = True
            if json_started:
                json_content += line + "\n"
        
        if not json_content.strip():
            print("[ERROR] No valid JSON data received from API")
            sys.exit(1)
        
        data = json.loads(json_content)
        
        # Ensure it's a list
        if not isinstance(data, list):
            data = [data]
        
        # Save to cache
        save_to_cache(data)
        
        return data
        
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Error fetching data: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Error parsing JSON response: {e}")
        sys.exit(1)


def get_site_data(refresh: bool = False) -> List[Dict[str, Any]]:
    """Get site data from cache or API."""
    if not refresh and is_cache_valid():
        print("[INFO] Using cached site data")
        cached_data = load_cached_data()
        if cached_data:
            return cached_data
    elif CACHE_FILE.exists():
        cache_mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        cache_age = datetime.now() - cache_mtime
        print(f"[INFO] Cache is {cache_age.days} days old (max {CACHE_MAX_AGE_DAYS} days). Refreshing...")
    
    return fetch_site_data_from_api()


def get_accs_for_acn(site_data: List[Dict[str, Any]], acn_id: str) -> List[str]:
    """Get all ACC IDs for a given ACN ID from the site data."""
    accs = set()
    for site in site_data:
        if site.get('acn_id') == acn_id:
            acc_id = site.get('acc_id')
            if acc_id:
                accs.add(acc_id)
    return sorted(list(accs))


# ============================================================================
# Progress Management
# ============================================================================
def load_progress() -> Optional[Dict[str, Any]]:
    """Load progress from file."""
    if not PROGRESS_FILE.exists():
        return None
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def save_progress(progress: Dict[str, Any]):
    """Save progress to file."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)
    except IOError as e:
        print(f"[WARNING] Failed to save progress: {e}")


def delete_progress():
    """Delete progress file."""
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except IOError:
        pass


# ============================================================================
# Odd Ones Out Tracking (Multi-Site Support)
# ============================================================================
def get_site_key(acn_id: str, acc_id: str = None, acg_id: str = None, acs_id: str = None, mode: str = None) -> str:
    """
    Generate a unique key for a site based on PFID components.
    Key format matches the mode:
    - acn-only: ACN
    - acn-acc: ACN-ACC
    - acn-acc-acg: ACN-ACC-ACG
    - acn-acc-acg-acs: ACN-ACC-ACG-ACS
    """
    if mode == "acn-only" or (acc_id is None and acg_id is None and acs_id is None):
        return acn_id
    elif mode == "acn-acc" or (acg_id is None and acs_id is None):
        return f"{acn_id}-{acc_id}"
    elif mode == "acn-acc-acg" or acs_id is None:
        return f"{acn_id}-{acc_id}-{acg_id}"
    else:
        return f"{acn_id}-{acc_id}-{acg_id}-{acs_id}"


def load_all_odd_ones_out() -> Dict[str, Any]:
    """Load the entire odd ones out file."""
    if not ODD_ONES_FILE.exists():
        return {"sites": {}}
    
    try:
        with open(ODD_ONES_FILE, 'r') as f:
            data = json.load(f)
            # Migrate old format to new format if needed
            if "sites" not in data and "stations" in data:
                # Old format - migrate
                return {"sites": {}}
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARNING] Failed to load odd ones out: {e}")
        return {"sites": {}}


def save_all_odd_ones_out(data: Dict[str, Any]):
    """Save the entire odd ones out file."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(ODD_ONES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        print(f"[WARNING] Failed to save odd ones out: {e}")


def save_odd_ones_out(odd_ones: List[Dict[str, Any]], correct_schedule: List[Dict], 
                      acn_id: str = None, acc_id: str = None, acg_id: str = None, 
                      acs_id: str = None, mode: str = None):
    """
    Save odd ones out for a specific site to the JSON file.
    Supports multiple sites in the same file.
    """
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Determine site key from odd_ones if not provided
        if acn_id is None and odd_ones:
            acn_id = odd_ones[0].get("acn_id")
        if acc_id is None and odd_ones and mode != "acn-only":
            # Check if all stations have the same ACC
            accs = set(r.get("acc_id") for r in odd_ones)
            if len(accs) == 1:
                acc_id = list(accs)[0]
                if mode is None:
                    mode = "acn-acc"
            elif mode is None:
                mode = "acn-only"
        
        site_key = get_site_key(acn_id, acc_id, acg_id, acs_id, mode)
        
        # Load existing data
        all_data = load_all_odd_ones_out()
        
        # Create site entry
        site_data = {
            "mode": mode or ("acn-acc" if acc_id else "acn-only"),
            "acn_id": acn_id,
            "acc_id": acc_id,
            "acg_id": acg_id,
            "acs_id": acs_id,
            "saved_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "correct_schedule": correct_schedule,
            "stations": []
        }
        
        for r in odd_ones:
            station_data = {
                "pfid": r.get("pfid"),
                "acn_id": r.get("acn_id"),
                "acc_id": r.get("acc_id"),
                "acg_id": r.get("acg_id"),
                "acs_id": r.get("acs_id"),
                "current_schedule": r.get("schedule"),
                "mismatches": r.get("mismatches"),
                "update_status": r.get("update_status", "pending"),
                "rejection_reason": r.get("rejection_reason")
            }
            site_data["stations"].append(station_data)
        
        # Update the site in the data
        all_data["sites"][site_key] = site_data
        
        # Save
        save_all_odd_ones_out(all_data)
        
        print(f"[INFO] Odd ones out saved for site {site_key}")
    except IOError as e:
        print(f"[WARNING] Failed to save odd ones out: {e}")


def load_odd_ones_out(site_key: str = None) -> Optional[Dict[str, Any]]:
    """Load odd ones out for a specific site, or the entire file if no key provided."""
    all_data = load_all_odd_ones_out()
    
    if site_key is None:
        return all_data
    
    return all_data.get("sites", {}).get(site_key)


def update_odd_ones_status(pfid: str, status: str, rejection_reason: str = None, site_key: str = None):
    """
    Update the status of a specific station in the odd ones out file.
    """
    all_data = load_all_odd_ones_out()
    
    # Find the station across all sites if site_key not provided
    sites_to_check = [site_key] if site_key else list(all_data.get("sites", {}).keys())
    
    for sk in sites_to_check:
        site_data = all_data.get("sites", {}).get(sk)
        if not site_data:
            continue
        
        for station in site_data.get("stations", []):
            if station.get("pfid") == pfid:
                station["update_status"] = status
                station["rejection_reason"] = rejection_reason
                station["last_attempt"] = datetime.now().isoformat()
                site_data["last_updated"] = datetime.now().isoformat()
                save_all_odd_ones_out(all_data)
                return


def get_sites_with_pending_work() -> List[Dict[str, Any]]:
    """
    Get a list of sites that have pending/rejected/errored stations.
    Returns list of dicts with site info and station counts.
    """
    all_data = load_all_odd_ones_out()
    sites_with_work = []
    
    for site_key, site_data in all_data.get("sites", {}).items():
        stations = site_data.get("stations", [])
        
        pending = len([s for s in stations if s.get("update_status") == "pending"])
        rejected = len([s for s in stations if s.get("update_status") == "rejected"])
        errored = len([s for s in stations if s.get("update_status") == "error"])
        accepted = len([s for s in stations if s.get("update_status") == "accepted"])
        
        total_pending = pending + rejected + errored
        
        if total_pending > 0:
            sites_with_work.append({
                "site_key": site_key,
                "acn_id": site_data.get("acn_id"),
                "acc_id": site_data.get("acc_id"),
                "mode": site_data.get("mode"),
                "saved_at": site_data.get("saved_at"),
                "last_updated": site_data.get("last_updated"),
                "pending": pending,
                "rejected": rejected,
                "errored": errored,
                "accepted": accepted,
                "total_pending": total_pending,
                "total_stations": len(stations)
            })
    
    # Sort by last_updated (most recent first)
    sites_with_work.sort(key=lambda x: x.get("last_updated", ""), reverse=True)
    
    return sites_with_work


def remove_completed_site(site_key: str):
    """
    Remove a site from the odd ones out file if all stations are accepted.
    """
    all_data = load_all_odd_ones_out()
    
    site_data = all_data.get("sites", {}).get(site_key)
    if not site_data:
        return
    
    stations = site_data.get("stations", [])
    all_accepted = all(s.get("update_status") == "accepted" for s in stations)
    
    if all_accepted:
        del all_data["sites"][site_key]
        save_all_odd_ones_out(all_data)
        print(f"[INFO] Site {site_key} completed and removed from tracking.")


def check_resume_progress() -> Optional[Dict[str, Any]]:
    """Check if there's progress to resume and ask user."""
    progress = load_progress()
    if not progress:
        return None
    
    print(f"\n{'='*70}")
    print("  PREVIOUS RUN DETECTED")
    print(f"{'='*70}")
    print(f"  Mode: {progress.get('mode', 'unknown')}")
    print(f"  ACN: {progress.get('acn_id', 'N/A')}")
    if progress.get('mode') == 'acn-acc':
        print(f"  ACC: {progress.get('acc_id', 'N/A')}")
    else:
        completed = progress.get('completed_accs', [])
        total = progress.get('total_accs', 0)
        print(f"  Progress: {len(completed)}/{total} ACCs completed")
    print(f"  Started: {progress.get('started_at', 'N/A')}")
    print(f"{'='*70}")
    
    response = input("\n  Do you want to continue where you left off? (yes/no): ").strip().lower()
    
    if response in ('yes', 'y'):
        return progress
    else:
        delete_progress()
        return None


# ============================================================================
# API Functions
# ============================================================================
def fetch_station_data(acn_id, acc_id):
    """Fetch all station data for a given ACN/ACC."""
    url = f"https://powerflex.io/session-manager/stations/dashboard/acn/{acn_id}?acc={acc_id}"
    cmd = [
        "curl_device_manager.sh",
        "-X", "GET",
        url,
        "--header", "Content-Type: application/json"
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, check=True, text=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Error fetching station data: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"[ERROR] Error parsing station data: {e}")
        return {}


def get_configuration(pfid, key):
    """Get a configuration value from a station."""
    url = f"https://powerflex.io/edge-device-manager/ocppCommands/get_configuration/{pfid}"
    cmd = [
        "curl_device_manager.sh",
        "-X", "POST",
        url,
        "-H", "accept: application/json",
        "-H", "Content-Type: application/json",
        "-d", json.dumps({"key": [key]})
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, check=True, text=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        return {"error": str(e)}
    except json.JSONDecodeError:
        return {"error": "Invalid JSON response"}


def set_configuration(pfid, key, value):
    """Set a configuration value on a station."""
    url = f"https://powerflex.io/edge-device-manager/ocppCommands/change_configuration/{pfid}"
    cmd = [
        "curl_device_manager.sh",
        "-X", "POST",
        url,
        "-H", "accept: application/json",
        "-H", "Content-Type: application/json",
        "-d", json.dumps({"key": key, "value": value})
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, check=True, text=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        return {"error": str(e)}
    except json.JSONDecodeError:
        return {"error": "Invalid JSON response"}


def extract_pricing_schedule(response):
    """Extract pricing schedule from the API response."""
    try:
        config_keys = response.get("natsResponse", {}).get("configuration_key", [])
        for item in config_keys:
            if item.get("key") == "PricingSchedule":
                return json.loads(item.get("value", "[]"))
    except (json.JSONDecodeError, TypeError, KeyError):
        pass
    return None


def extract_pricing_enabled(response):
    """Extract PricingScheduleEnable from the API response."""
    try:
        config_keys = response.get("natsResponse", {}).get("configuration_key", [])
        for item in config_keys:
            if item.get("key") == "PricingScheduleEnable":
                val = item.get("value", "").lower()
                return val == "true"
    except (TypeError, KeyError):
        pass
    return None


def check_schedule_values(schedule, expected_f=0.5):
    """Check if all f values match the expected value."""
    if not schedule:
        return None, []
    mismatches = []
    for entry in schedule:
        t = entry.get("t")
        f = entry.get("f")
        if f != expected_f:
            mismatches.append({"t": t, "f": f})
    return len(mismatches) == 0, mismatches


def format_schedule(schedule):
    """Format schedule as a readable string."""
    if not schedule:
        return "N/A"
    parts = [f"t={e.get('t')}:f={e.get('f')}" for e in schedule]
    return ", ".join(parts)


# ============================================================================
# Main Processing Functions
# ============================================================================
def process_stations(acn_id: str, acc_id: str, acg_id: str = None, acs_id: str = None, 
                     all_results: List[Dict] = None) -> List[Dict]:
    """
    Process stations based on PFID hierarchy level.
    - ACN-ACC: all stations under that ACC
    - ACN-ACC-ACG: stations matching the ACG prefix
    - ACN-ACC-ACG-ACS: single station
    """
    # Build display string
    display = format_pfid_display(acn_id, acc_id, acg_id, acs_id)
    
    print(f"\n{'─'*70}")
    print(f"  Processing {display}")
    print(f"{'─'*70}")
    
    print("[INFO] Fetching station data...")
    stations = fetch_station_data(acn_id, acc_id)
    
    if not stations:
        print("[WARNING] No stations found for this ACC")
        return all_results or []
    
    # Build PFID prefix for filtering
    pfid_prefix = build_pfid_prefix(acn_id, acc_id, acg_id, acs_id) if acg_id else None
    
    # Filter for LiteON stations only (case insensitive)
    liteon_entries = []
    other_models = {}
    filtered_out = 0
    
    for entry in stations.values():
        if not entry.get("pfid"):
            continue
        
        pfid = entry.get("pfid")
        
        # Filter by PFID prefix if specified (ACG or ACS level)
        if pfid_prefix:
            if not pfid.startswith(pfid_prefix):
                filtered_out += 1
                continue
        
        evse_type = entry.get("evse_type", "Unknown")
        if "liteon" in evse_type.lower():
            liteon_entries.append(entry)
        else:
            other_models[evse_type] = other_models.get(evse_type, 0) + 1
    
    pfids = [entry.get("pfid") for entry in liteon_entries]
    
    print(f"  LiteON stations to check: {len(pfids)}")
    if filtered_out > 0:
        print(f"  Filtered out (different ACG/ACS): {filtered_out}")
    if other_models:
        print(f"  Skipping other models: {dict(sorted(other_models.items(), key=lambda x: -x[1]))}")
    
    if not pfids:
        if pfid_prefix:
            print(f"[WARNING] No LiteON stations found matching prefix: {pfid_prefix}")
        else:
            print("[WARNING] No LiteON stations found")
        return all_results or []
    
    if all_results is None:
        all_results = []
    
    for i, pfid in enumerate(pfids, 1):
        print(f"[{i}/{len(pfids)}] Checking {pfid}...", end=" ", flush=True)
        
        # Get PricingScheduleEnable first
        enable_response = get_configuration(pfid, "PricingScheduleEnable")
        enabled = extract_pricing_enabled(enable_response)
        
        # If not enabled (unknown or false), enable it first then recheck
        if enabled is not True:
            print("enabling...", end=" ", flush=True)
            set_configuration(pfid, "PricingScheduleEnable", "true")
            enabled = True
        
        # Get PricingSchedule
        schedule_response = get_configuration(pfid, "PricingSchedule")
        schedule = extract_pricing_schedule(schedule_response)
        
        # Check for mismatches
        all_correct, mismatches = check_schedule_values(schedule)
        
        # Extract ACG and ACS from PFID for tracking
        pfid_parts = pfid.split("-")
        station_acg = pfid_parts[2] if len(pfid_parts) > 2 else None
        station_acs = pfid_parts[3] if len(pfid_parts) > 3 else None
        
        all_results.append({
            "pfid": pfid,
            "acn_id": acn_id,
            "acc_id": acc_id,
            "acg_id": station_acg,
            "acs_id": station_acs,
            "schedule": schedule,
            "enabled": enabled,
            "all_correct": all_correct,
            "mismatches": mismatches
        })
        
        status = "✓" if all_correct else "✗" if all_correct is False else "?"
        print(status)
    
    return all_results


# Alias for backward compatibility
def process_acc(acn_id: str, acc_id: str, all_results: List[Dict] = None) -> List[Dict]:
    """Process a single ACN-ACC combination and return results."""
    return process_stations(acn_id, acc_id, all_results=all_results)


def print_results(results: List[Dict]):
    """Print the results summary."""
    print(f"\n{'='*70}")
    print("  RESULTS")
    print(f"{'='*70}\n")
    
    # Separate into correct and odd-ones-out
    correct = [r for r in results if r["all_correct"] is True]
    odd_ones = [r for r in results if r["all_correct"] is False]
    unknown = [r for r in results if r["all_correct"] is None]
    
    # Print odd ones out first (the important ones)
    if odd_ones:
        print(f"{'─'*70}")
        print("  ⚠️  ODD ONES OUT (f != 0.5)")
        print(f"{'─'*70}")
        for r in odd_ones:
            enabled_str = "✓ Enabled" if r["enabled"] else "✗ Disabled" if r["enabled"] is False else "? Unknown"
            print(f"\n  PFID: {r['pfid']} (ACN: {r.get('acn_id', 'N/A')}, ACC: {r.get('acc_id', 'N/A')})")
            print(f"  Enabled: {enabled_str}")
            print(f"  Schedule: {format_schedule(r['schedule'])}")
            print(f"  Issues:")
            for m in r["mismatches"]:
                print(f"    - t={m['t']} has f={m['f']} (expected 0.5)")
    
    # Print unknown
    if unknown:
        print(f"\n{'─'*70}")
        print("  ❓ UNKNOWN (could not fetch schedule)")
        print(f"{'─'*70}")
        for r in unknown:
            enabled_str = "✓ Enabled" if r["enabled"] else "✗ Disabled" if r["enabled"] is False else "? Unknown"
            print(f"\n  PFID: {r['pfid']} (ACN: {r.get('acn_id', 'N/A')}, ACC: {r.get('acc_id', 'N/A')})")
            print(f"  Enabled: {enabled_str}")
    
    # Print correct ones (summary)
    if correct:
        print(f"\n{'─'*70}")
        print(f"  ✅ CORRECT ({len(correct)} stations with all f=0.5)")
        print(f"{'─'*70}")
        for r in correct:
            enabled_str = "✓" if r["enabled"] else "✗" if r["enabled"] is False else "?"
            print(f"  {r['pfid']} | ACN: {r.get('acn_id', 'N/A')} | ACC: {r.get('acc_id', 'N/A')} | Enabled: {enabled_str}")
    
    # Summary
    print(f"\n{'='*70}")
    print("  SUMMARY")
    print(f"{'='*70}")
    print(f"  Total stations:    {len(results)}")
    print(f"  Correct (f=0.5):   {len(correct)}")
    print(f"  Odd ones out:      {len(odd_ones)}")
    print(f"  Unknown:           {len(unknown)}")
    print(f"{'='*70}\n")
    
    return odd_ones, correct


def get_correct_schedule(correct_stations):
    """Get a correct schedule from a working station, or use default."""
    default_schedule = [
        {"t": 0, "f": 0.5},
        {"t": 4, "f": 0.5},
        {"t": 8, "f": 0.5},
        {"t": 16, "f": 0.5},
        {"t": 20, "f": 0.5}
    ]
    
    if correct_stations and correct_stations[0].get("schedule"):
        return correct_stations[0]["schedule"]
    
    return default_schedule


def prompt_update_odd_ones(odd_ones, correct_stations, skip_prompt: bool = False, 
                          acn_id: str = None, acc_id: str = None, acg_id: str = None,
                          acs_id: str = None, mode: str = None):
    """Prompt user to update odd ones out with correct schedule."""
    correct_schedule = get_correct_schedule(correct_stations)
    correct_schedule_str = json.dumps(correct_schedule)
    
    # Determine site context from odd_ones if not provided
    if acn_id is None and odd_ones:
        acn_id = odd_ones[0].get("acn_id")
    
    # Save odd ones out to file for tracking (multi-site support)
    save_odd_ones_out(odd_ones, correct_schedule, acn_id=acn_id, acc_id=acc_id, 
                      acg_id=acg_id, acs_id=acs_id, mode=mode)
    
    # Get site key for status updates
    site_key = get_site_key(acn_id, acc_id, acg_id, acs_id, mode) if acn_id else None
    
    print(f"\n{'='*70}")
    print("  UPDATE ODD ONES OUT?")
    print(f"{'='*70}")
    print(f"\n  {len(odd_ones)} station(s) have incorrect PricingSchedule.")
    print(f"\n  Correct schedule to apply:")
    print(f"  {correct_schedule_str}")
    print(f"\n  Stations to update:")
    for r in odd_ones:
        print(f"    - {r['pfid']} (ACN: {r.get('acn_id', 'N/A')}, ACC: {r.get('acc_id', 'N/A')})")
    
    if skip_prompt:
        do_update = True
    else:
        response = input("\n  Do you want to update these stations? (yes/no): ").strip().lower()
        do_update = response in ("yes", "y")
    
    if do_update:
        print(f"\n[INFO] Updating {len(odd_ones)} station(s)...\n")
        
        accepted_count = 0
        rejected_count = 0
        
        for i, r in enumerate(odd_ones, 1):
            pfid = r["pfid"]
            print(f"[{i}/{len(odd_ones)}] Updating {pfid}...", end=" ", flush=True)
            
            result = set_configuration(pfid, "PricingSchedule", correct_schedule_str)
            
            nats_response = result.get("natsResponse", {})
            if isinstance(nats_response, dict):
                status = nats_response.get("status", "Unknown")
                if status == "Accepted":
                    print("✓ Accepted")
                    update_odd_ones_status(pfid, "accepted", site_key=site_key)
                    accepted_count += 1
                else:
                    # Extract rejection reason from the response
                    rejection_reason = None
                    
                    # Try various fields where rejection reason might be
                    if "error" in nats_response:
                        rejection_reason = nats_response.get("error")
                    elif "message" in nats_response:
                        rejection_reason = nats_response.get("message")
                    elif "reason" in nats_response:
                        rejection_reason = nats_response.get("reason")
                    else:
                        # Include full response if no specific reason found
                        rejection_reason = json.dumps(nats_response)
                    
                    print(f"⚠ {status}")
                    if rejection_reason:
                        print(f"      Reason: {rejection_reason}")
                    
                    update_odd_ones_status(pfid, "rejected", rejection_reason, site_key=site_key)
                    rejected_count += 1
            else:
                error_msg = str(nats_response) if nats_response else "Unknown error"
                print(f"? {error_msg}")
                update_odd_ones_status(pfid, "error", error_msg, site_key=site_key)
                rejected_count += 1
            
            # Check for error in the result itself
            if "error" in result:
                error_msg = result.get("error")
                print(f"      Error: {error_msg}")
                update_odd_ones_status(pfid, "error", error_msg, site_key=site_key)
        
        print(f"\n[INFO] Update complete.")
        print(f"  Accepted: {accepted_count}")
        print(f"  Rejected/Error: {rejected_count}")
        
        if rejected_count > 0:
            print(f"\n[INFO] Rejected stations saved to: {ODD_ONES_FILE}")
            print(f"[INFO] Use --retry flag to retry updating rejected stations.")
    else:
        print("\n[INFO] No changes made.")
        print(f"[INFO] Odd ones out saved to: {ODD_ONES_FILE}")


def main_pfid(pfid_info: Dict[str, Optional[str]], resume_progress: Dict = None):
    """
    Main function that handles all PFID hierarchy levels.
    
    Modes:
    - acn-only: Process all ACCs under an ACN
    - acn-acc: Process all stations under an ACC
    - acn-acc-acg: Process all stations matching an ACG prefix
    - acn-acc-acg-acs: Process a single station
    """
    acn = pfid_info["acn"]
    acc = pfid_info["acc"]
    acg = pfid_info["acg"]
    acs = pfid_info["acs"]
    mode = pfid_info["mode"]
    
    display = format_pfid_display(acn, acc, acg, acs)
    site_key = get_site_key(acn, acc, acg, acs, mode)
    
    print(f"\n{'='*70}")
    if mode == "acn-only":
        print(f"  Pricing Schedule Check for ALL ACCs under ACN: {acn}")
    elif mode == "acn-acc-acg-acs":
        print(f"  Pricing Schedule Check for Single Station: {site_key}")
    elif mode == "acn-acc-acg":
        print(f"  Pricing Schedule Check for ACG: {site_key}")
    else:
        print(f"  Pricing Schedule Check for {display}")
    print(f"{'='*70}\n")
    
    # ACN-only mode: iterate through all ACCs
    if mode == "acn-only":
        print("[INFO] Loading site data to find all ACCs...")
        site_data = get_site_data()
        
        accs = get_accs_for_acn(site_data, acn)
        
        if not accs:
            print(f"[ERROR] No ACCs found for ACN: {acn}")
            sys.exit(1)
        
        print(f"[INFO] Found {len(accs)} ACC(s) for ACN {acn}: {', '.join(accs)}")
        
        # Check for resume
        completed_accs = []
        all_results = []
        
        if resume_progress and resume_progress.get('acn_id') == acn:
            completed_accs = resume_progress.get('completed_accs', [])
            all_results = resume_progress.get('results', [])
            print(f"[INFO] Resuming from previous run. {len(completed_accs)} ACC(s) already completed.")
        
        # Initialize progress
        progress = {
            "mode": "acn-only",
            "acn_id": acn,
            "total_accs": len(accs),
            "completed_accs": completed_accs,
            "results": all_results,
            "started_at": resume_progress.get('started_at') if resume_progress else datetime.now().isoformat()
        }
        
        # Process each ACC
        for i, acc_id in enumerate(accs, 1):
            if acc_id in completed_accs:
                print(f"\n[{i}/{len(accs)}] Skipping ACC {acc_id} (already completed)")
                continue
            
            print(f"\n[{i}/{len(accs)}] Processing ACC: {acc_id}")
            all_results = process_stations(acn, acc_id, all_results=all_results)
            
            # Update progress
            completed_accs.append(acc_id)
            progress['completed_accs'] = completed_accs
            progress['results'] = all_results
            save_progress(progress)
        
        results = all_results
    else:
        # All other modes: process specific stations
        # Save progress for potential resume
        progress = {
            "mode": mode,
            "acn_id": acn,
            "acc_id": acc,
            "acg_id": acg,
            "acs_id": acs,
            "started_at": datetime.now().isoformat()
        }
        save_progress(progress)
        
        results = process_stations(acn, acc, acg, acs)
    
    # Print results
    odd_ones, correct = print_results(results)
    
    if odd_ones:
        prompt_update_odd_ones(odd_ones, correct, acn_id=acn, acc_id=acc, 
                               acg_id=acg, acs_id=acs, mode=mode)
    
    # Delete progress on completion
    delete_progress()


# Keep backward-compatible aliases
def main_single_acc(acn_id: str, acc_id: str):
    """Main function for single ACN-ACC mode."""
    pfid_info = {"acn": acn_id, "acc": acc_id, "acg": None, "acs": None, "mode": "acn-acc"}
    main_pfid(pfid_info)


def main_acn_only(acn_id: str, resume_progress: Dict = None):
    """Main function for ACN-only mode (all ACCs under the ACN)."""
    pfid_info = {"acn": acn_id, "acc": None, "acg": None, "acs": None, "mode": "acn-only"}
    main_pfid(pfid_info, resume_progress)


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\n\n[INFO] Interrupted by user. Progress has been saved.")
    print("[INFO] Run the script again to resume from where you left off.")
    stop_jwt_refresh_timer()
    sys.exit(0)


def main_retry(site_key: str = None):
    """
    Main function for --retry mode: retry updating previously failed stations.
    If site_key is None, show a menu of sites with pending work.
    """
    print(f"\n{'='*70}")
    print("  RETRY MODE - Update Previously Failed Stations")
    print(f"{'='*70}\n")
    
    # If no site specified, show menu of sites with pending work
    if site_key is None:
        sites_with_work = get_sites_with_pending_work()
        
        if not sites_with_work:
            print("[INFO] No sites with pending updates found.")
            print(f"[INFO] File location: {ODD_ONES_FILE}")
            sys.exit(0)
        
        print("  Sites with pending updates:\n")
        print(f"  {'#':<4} {'Site':<15} {'Pending':<10} {'Rejected':<10} {'Accepted':<10} {'Last Updated'}")
        print(f"  {'-'*4} {'-'*15} {'-'*10} {'-'*10} {'-'*10} {'-'*20}")
        
        for i, site in enumerate(sites_with_work, 1):
            site_display = site['site_key']
            if site['mode'] == 'acn-only':
                site_display += " (full ACN)"
            
            # Format last updated time
            try:
                last_updated = datetime.fromisoformat(site['last_updated'])
                age = datetime.now() - last_updated
                if age.days > 0:
                    age_str = f"{age.days}d ago"
                elif age.seconds >= 3600:
                    age_str = f"{age.seconds // 3600}h ago"
                else:
                    age_str = f"{age.seconds // 60}m ago"
            except:
                age_str = "Unknown"
            
            pending_str = str(site['pending'] + site['rejected'] + site['errored'])
            rejected_str = str(site['rejected'])
            accepted_str = str(site['accepted'])
            
            print(f"  {i:<4} {site_display:<15} {pending_str:<10} {rejected_str:<10} {accepted_str:<10} {age_str}")
        
        print(f"\n  {'-'*70}")
        print(f"  Enter site number (1-{len(sites_with_work)}), 'all' to retry all, or 'q' to quit")
        
        while True:
            choice = input("\n  Your choice: ").strip().lower()
            
            if choice == 'q':
                print("\n[INFO] Exiting.")
                sys.exit(0)
            elif choice == 'all':
                # Retry all sites
                for site in sites_with_work:
                    main_retry_site(site['site_key'])
                return
            else:
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(sites_with_work):
                        site_key = sites_with_work[idx]['site_key']
                        break
                    else:
                        print(f"  Invalid choice. Enter 1-{len(sites_with_work)}.")
                except ValueError:
                    print(f"  Invalid input. Enter a number, 'all', or 'q'.")
    
    # Retry the selected site
    main_retry_site(site_key)


def main_retry_site(site_key: str):
    """
    Retry updating stations for a specific site.
    """
    print(f"\n{'─'*70}")
    print(f"  Retrying site: {site_key}")
    print(f"{'─'*70}\n")
    
    # Load site data
    site_data = load_odd_ones_out(site_key)
    
    if not site_data:
        print(f"[ERROR] No data found for site: {site_key}")
        return
    
    # Show file info
    saved_at = site_data.get("saved_at", "Unknown")
    last_updated = site_data.get("last_updated", saved_at)
    
    try:
        saved_datetime = datetime.fromisoformat(saved_at)
        age = datetime.now() - saved_datetime
        age_str = f"{age.days}d {age.seconds // 3600}h {(age.seconds % 3600) // 60}m ago"
    except:
        age_str = "Unknown"
    
    print(f"  ACN: {site_data.get('acn_id', 'N/A')}")
    if site_data.get('acc_id'):
        print(f"  ACC: {site_data.get('acc_id')}")
    print(f"  Mode: {site_data.get('mode', 'unknown')}")
    print(f"  First saved: {saved_at} ({age_str})")
    print(f"  Last updated: {last_updated}")
    
    stations = site_data.get("stations", [])
    correct_schedule = site_data.get("correct_schedule", [])
    
    if not stations:
        print("\n[INFO] No stations found for this site.")
        return
    
    # Categorize stations
    pending = [s for s in stations if s.get("update_status") == "pending"]
    rejected = [s for s in stations if s.get("update_status") == "rejected"]
    errored = [s for s in stations if s.get("update_status") == "error"]
    accepted = [s for s in stations if s.get("update_status") == "accepted"]
    
    print(f"\n  Station Status Summary:")
    print(f"    Pending:  {len(pending)}")
    print(f"    Rejected: {len(rejected)}")
    print(f"    Errored:  {len(errored)}")
    print(f"    Accepted: {len(accepted)}")
    
    # Show rejected/errored stations with reasons
    to_retry = pending + rejected + errored
    
    if not to_retry:
        print(f"\n[INFO] All stations for this site have been successfully updated!")
        remove_completed_site(site_key)
        return
    
    print(f"\n{'─'*70}")
    print(f"  Stations to retry ({len(to_retry)}):")
    print(f"{'─'*70}")
    
    for s in to_retry:
        status = s.get("update_status", "pending")
        reason = s.get("rejection_reason", "")
        last_attempt = s.get("last_attempt", "Never")
        
        print(f"\n  PFID: {s.get('pfid')} (ACN: {s.get('acn_id', 'N/A')}, ACC: {s.get('acc_id', 'N/A')})")
        print(f"    Status: {status}")
        if reason:
            print(f"    Reason: {reason}")
        if last_attempt and last_attempt != "Never":
            print(f"    Last attempt: {last_attempt}")
    
    print(f"\n{'─'*70}")
    print(f"  Correct schedule to apply:")
    print(f"  {json.dumps(correct_schedule)}")
    print(f"{'─'*70}")
    
    response = input("\n  Do you want to retry updating these stations? (yes/no): ").strip().lower()
    
    if response not in ("yes", "y"):
        print("\n[INFO] No changes made.")
        return
    
    # Perform updates
    correct_schedule_str = json.dumps(correct_schedule)
    
    print(f"\n[INFO] Updating {len(to_retry)} station(s)...\n")
    
    accepted_count = 0
    rejected_count = 0
    
    for i, s in enumerate(to_retry, 1):
        pfid = s["pfid"]
        print(f"[{i}/{len(to_retry)}] Updating {pfid}...", end=" ", flush=True)
        
        result = set_configuration(pfid, "PricingSchedule", correct_schedule_str)
        
        nats_response = result.get("natsResponse", {})
        if isinstance(nats_response, dict):
            status = nats_response.get("status", "Unknown")
            if status == "Accepted":
                print("✓ Accepted")
                update_odd_ones_status(pfid, "accepted", site_key=site_key)
                accepted_count += 1
            else:
                rejection_reason = None
                
                if "error" in nats_response:
                    rejection_reason = nats_response.get("error")
                elif "message" in nats_response:
                    rejection_reason = nats_response.get("message")
                elif "reason" in nats_response:
                    rejection_reason = nats_response.get("reason")
                else:
                    rejection_reason = json.dumps(nats_response)
                
                print(f"⚠ {status}")
                if rejection_reason:
                    print(f"      Reason: {rejection_reason}")
                
                update_odd_ones_status(pfid, "rejected", rejection_reason, site_key=site_key)
                rejected_count += 1
        else:
            error_msg = str(nats_response) if nats_response else "Unknown error"
            print(f"? {error_msg}")
            update_odd_ones_status(pfid, "error", error_msg, site_key=site_key)
            rejected_count += 1
        
        if "error" in result:
            error_msg = result.get("error")
            print(f"      Error: {error_msg}")
            update_odd_ones_status(pfid, "error", error_msg, site_key=site_key)
    
    print(f"\n[INFO] Retry complete for site {site_key}.")
    print(f"  Accepted: {accepted_count}")
    print(f"  Rejected/Error: {rejected_count}")
    
    # Check if all done for this site
    if rejected_count == 0:
        remove_completed_site(site_key)
    else:
        print(f"\n[INFO] Still-failing stations updated in: {ODD_ONES_FILE}")
        print(f"[INFO] Run with --retry again to retry.")


def main():
    """Main entry point."""
    # Set up signal handler for graceful interruption
    signal.signal(signal.SIGINT, signal_handler)
    
    # Check for --retry flag
    if "--retry" in sys.argv:
        # Get JWT token first
        if not get_jwt():
            print("[ERROR] Failed to obtain JWT token. Please run 'source set_jwt.sh' manually first.")
            sys.exit(1)
        
        start_jwt_refresh_timer()
        try:
            main_retry()
        finally:
            stop_jwt_refresh_timer()
        return
    
    # Parse arguments - support flexible PFID input
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pfid_components>")
        print(f"       {sys.argv[0]} --retry")
        print(f"")
        print(f"  PFID Format: ACN-ACC-ACG-ACS (can use spaces or dashes)")
        print(f"")
        print(f"  Entire ACN:    {sys.argv[0]} 0051")
        print(f"  Single ACC:    {sys.argv[0]} 0051 09       or  {sys.argv[0]} 0051-09")
        print(f"  Single ACG:    {sys.argv[0]} 0051 09 02    or  {sys.argv[0]} 0051-09-02")
        print(f"  Single ACS:    {sys.argv[0]} 0051 09 02 01 or  {sys.argv[0]} 0051-09-02-01")
        print(f"  Retry mode:    {sys.argv[0]} --retry")
        sys.exit(1)
    
    # Parse PFID from remaining arguments
    pfid_args = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    pfid_info = parse_pfid_input(pfid_args)
    
    if not pfid_info["acn"]:
        print("[ERROR] At least ACN must be specified")
        sys.exit(1)
    
    # Display parsed PFID
    print(f"[INFO] Parsed PFID: {format_pfid_display(pfid_info['acn'], pfid_info['acc'], pfid_info['acg'], pfid_info['acs'])}")
    print(f"[INFO] Mode: {pfid_info['mode']}")
    
    # Check for resume (only for ACN-only mode)
    resume_progress = None
    if pfid_info["mode"] == "acn-only":
        resume_progress = check_resume_progress()
        
        # Validate resume matches current request
        if resume_progress:
            if resume_progress.get('acn_id') != pfid_info["acn"]:
                print(f"[WARNING] Resume data is for ACN {resume_progress.get('acn_id')}, but you requested ACN {pfid_info['acn']}")
                print("[INFO] Starting fresh...")
                resume_progress = None
                delete_progress()
            elif resume_progress.get('mode') != 'acn-only':
                print("[WARNING] Resume data is for a different mode")
                print("[INFO] Starting fresh...")
                resume_progress = None
                delete_progress()
    
    # Get JWT token
    if not get_jwt():
        print("[ERROR] Failed to obtain JWT token. Please run 'source set_jwt.sh' manually first.")
        sys.exit(1)
    
    # Start JWT refresh timer
    start_jwt_refresh_timer()
    
    try:
        main_pfid(pfid_info, resume_progress)
    finally:
        # Clean up
        stop_jwt_refresh_timer()


if __name__ == "__main__":
    main()
