# Pricing Schedule Check

A tool to verify and update `PricingSchedule` configurations on LiteON EV charging stations via the PowerFlex API.

## Features

- **Multi-level PFID support** - Check stations at ACN, ACC, ACG, or single station (ACS) level
- **Flexible input format** - Use spaces or dashes (`0051 09 02` or `0051-09-02`)
- **Automatic JWT management** - Auto-refreshes authentication token every 10 minutes
- **Progress tracking** - Resume interrupted runs after Ctrl+C
- **Multi-site tracking** - Work on multiple sites and retry failed updates later
- **Rejection tracking** - Stores rejection reasons for failed updates

## Requirements

### System Dependencies

- **Python 3.8+** (standard library only, no pip packages required)
- **jq** - JSON processor (used by JWT scripts)
- **curl** - HTTP client

### PowerFlex Scripts (must be in PATH)

| Script | Description |
|--------|-------------|
| `curl_device_manager.sh` | Wrapper for authenticated PowerFlex API calls |
| `powerflex_get_jwt_pass.sh` | Retrieves JWT token from Keycloak |
| `set_jwt.sh` | Sources JWT into environment (for manual authentication) |

### Shared Cache

This script shares the site data cache with `site_lookup.py`:

```
~/.cache/site_lookup/
├── sites_cache.json              # Site data (shared, 7-day TTL)
├── pricing_check_progress.json   # Progress for interrupted runs
└── pricing_odd_ones_out.json     # Stations needing updates + rejection tracking
```

## Installation

1. Ensure the script is executable:
   ```bash
   chmod +x pricing_schedule_check.py
   ```

2. Verify dependencies are available:
   ```bash
   which curl_device_manager.sh powerflex_get_jwt_pass.sh jq
   ```

3. **(First run)** Cache your Keycloak credentials:
   ```bash
   source set_jwt.sh
   ```
   This prompts for your username and password, which are then stored in your system keyring.

## Usage

### Basic Syntax

```bash
./pricing_schedule_check.py <pfid_components>
./pricing_schedule_check.py --retry
```

### PFID Hierarchy

The PFID format is: **ACN-ACC-ACG-ACS**

| Level | Example | What it checks |
|-------|---------|----------------|
| ACN | `0051` | All ACCs under the ACN |
| ACC | `0051-09` | All stations under the ACC |
| ACG | `0051-09-02` | Stations matching the ACG prefix |
| ACS | `0051-09-02-01` | Single station |

### Input Formats

Both space-separated and dash-separated formats work:

```bash
# These are equivalent:
./pricing_schedule_check.py 0051 09 02
./pricing_schedule_check.py 0051-09-02
```

### Examples

```bash
# Check all stations under ACN 0051
./pricing_schedule_check.py 0051

# Check all stations under ACC 09 of ACN 0051
./pricing_schedule_check.py 0051-09

# Check all stations in ACG 02
./pricing_schedule_check.py 0051 09 02

# Check a single station
./pricing_schedule_check.py 0051-09-02-01

# Retry failed updates from previous runs
./pricing_schedule_check.py --retry
```

## Workflow

### Standard Check

1. Script fetches station data from PowerFlex API
2. Filters for **LiteON stations only** (other models are skipped)
3. For each station:
   - Enables `PricingScheduleEnable` if not already enabled
   - Retrieves current `PricingSchedule`
   - Checks if all `f` values equal `0.5`
4. Displays results summary (correct, incorrect, unknown)
5. Prompts to update stations with incorrect schedules
6. Tracks rejections with reasons for later retry

### Sample Output

```
======================================================================
  Pricing Schedule Check for ACN: 0051, ACC: 09
======================================================================

[INFO] Fetching station data...
  LiteON stations to check: 12
  Skipping other models: {'ABB': 3, 'ChargePoint': 2}

[1/12] Checking 0051-09-01-01... ✓
[2/12] Checking 0051-09-01-02... enabling... ✓
[3/12] Checking 0051-09-02-01... ✗
...

======================================================================
  RESULTS
======================================================================

──────────────────────────────────────────────────────────────────────
  ⚠️  ODD ONES OUT (f != 0.5)
──────────────────────────────────────────────────────────────────────

  PFID: 0051-09-02-01 (ACN: 0051, ACC: 09)
  Enabled: ✓ Enabled
  Schedule: t=0:f=0.3, t=4:f=0.3, t=8:f=0.5, t=16:f=0.5, t=20:f=0.5
  Issues:
    - t=0 has f=0.3 (expected 0.5)
    - t=4 has f=0.3 (expected 0.5)

======================================================================
  SUMMARY
======================================================================
  Total stations:    12
  Correct (f=0.5):   11
  Odd ones out:      1
  Unknown:           0
======================================================================
```

### Retry Mode

When updates fail (rejected by station), run `--retry` to see all pending work:

```
======================================================================
  RETRY MODE - Update Previously Failed Stations
======================================================================

  Sites with pending updates:

  #    Site            Pending    Rejected   Accepted   Last Updated
  ---- --------------- ---------- ---------- ---------- --------------------
  1    0051 (full ACN) 5          2          10         2h ago
  2    0052-37         3          1          8          5m ago

  ----------------------------------------------------------------------
  Enter site number (1-2), 'all' to retry all, or 'q' to quit

  Your choice: 1
```

Selecting a site shows rejection reasons:

```
──────────────────────────────────────────────────────────────────────
  Stations to retry (2):
──────────────────────────────────────────────────────────────────────

  PFID: 0051-09-02-01 (ACN: 0051, ACC: 09)
    Status: rejected
    Reason: Station is offline
    Last attempt: 2026-02-11T19:30:00

  PFID: 0051-09-03-02 (ACN: 0051, ACC: 09)
    Status: rejected
    Reason: Configuration locked
    Last attempt: 2026-02-11T19:31:00
```

## Expected Schedule

The correct `PricingSchedule` has `f=0.5` for all time periods:

```json
[
  {"t": 0, "f": 0.5},
  {"t": 4, "f": 0.5},
  {"t": 8, "f": 0.5},
  {"t": 16, "f": 0.5},
  {"t": 20, "f": 0.5}
]
```

## Progress & Resume

### Interrupt Handling

Press **Ctrl+C** at any time. Progress is automatically saved.

```
^C

[INFO] Interrupted by user. Progress has been saved.
[INFO] Run the script again to resume from where you left off.
```

### Resuming

Run the same command again:

```bash
./pricing_schedule_check.py 0051
```

You'll be prompted:

```
======================================================================
  PREVIOUS RUN DETECTED
======================================================================
  Mode: acn-only
  ACN: 0051
  Progress: 3/7 ACCs completed
  Started: 2026-02-11T18:45:00
======================================================================

  Do you want to continue where you left off? (yes/no):
```

## Output Files

All files stored in `~/.cache/site_lookup/`:

| File | Purpose |
|------|---------|
| `sites_cache.json` | Cached site data (shared with site_lookup.py, 7-day TTL) |
| `pricing_check_progress.json` | Resume data for interrupted runs |
| `pricing_odd_ones_out.json` | Multi-site tracking of stations needing updates |

### Odd Ones Out File Structure

```json
{
  "sites": {
    "0051": {
      "mode": "acn-only",
      "acn_id": "0051",
      "saved_at": "2026-02-11T18:00:00",
      "last_updated": "2026-02-11T19:30:00",
      "correct_schedule": [{"t": 0, "f": 0.5}, ...],
      "stations": [
        {
          "pfid": "0051-09-02-01",
          "update_status": "rejected",
          "rejection_reason": "Station is offline",
          "last_attempt": "2026-02-11T19:30:00"
        }
      ]
    },
    "0052-37": {
      "mode": "acn-acc",
      ...
    }
  }
}
```

## Troubleshooting

### JWT Token Issues

```
[ERROR] Failed to obtain JWT token. Please run 'source set_jwt.sh' manually first.
```

**Solution:** Run manually to cache credentials:
```bash
source set_jwt.sh
```

### Timeout on First Run

The first run may prompt for credentials. If the script times out:

```bash
# Run JWT script manually first
source set_jwt.sh

# Then run the check
./pricing_schedule_check.py 0051-09
```

### No Stations Found

```
[WARNING] No LiteON stations found matching prefix: 0051-09-99
```

**Cause:** The ACG/ACS doesn't exist or has no LiteON stations.

### Station Update Rejected

When a station rejects the update:

```
[3/5] Updating 0051-09-02-01... ⚠ Rejected
      Reason: Station is offline
```

The rejection is tracked. Use `--retry` later when the station is available.

## Notes

- Only **LiteON** stations are checked (ABB, ChargePoint, etc. are skipped)
- JWT token auto-refreshes every **10 minutes**
- Site cache auto-refreshes after **7 days**
- Progress saved after each ACC completes (for ACN-only mode)
- Multiple sites can be tracked simultaneously for retry

## Quick Reference

```bash
# Full ACN scan
./pricing_schedule_check.py 0051

# Single ACC
./pricing_schedule_check.py 0051-09

# Single ACG
./pricing_schedule_check.py 0051-09-02

# Single station
./pricing_schedule_check.py 0051-09-02-01

# Retry failed updates
./pricing_schedule_check.py --retry
```
