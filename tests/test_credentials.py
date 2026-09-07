# tests/test_credentials.py (FIXED TYPO)
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / '.env')

print("=" * 80)
print("SENTINEL CREDENTIALS TEST - COPERNICUS CDSE FIX")
print("=" * 80)

results = {
    'database': False,
    'copernicus': False,
    'nasa_cmr': False,
    'api': False
}

# ========== Test 1: DATABASE ==========
print("\n[1] DATABASE CONNECTION")
print("-" * 80)
try:
    from sqlalchemy import create_engine, text
    db_url = os.getenv('DATABASE_URL')
    engine = create_engine(db_url, pool_pre_ping=True, echo=False)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT VERSION()"))
        version = result.fetchone()[0]
        print(f"✅ Database Connected: {version[:50]}...")
        results['database'] = True
except Exception as e:
    print(f"❌ Database Failed: {str(e)[:100]}")

# ========== Test 2: COPERNICUS - FIXED TYPO ==========
print("\n[2] COPERNICUS (Sentinel-1 Download)")
print("-" * 80)
print("ℹ️  Using: Copernicus Data Space Ecosystem (CDSE) - Updated Endpoint\n")

user = os.getenv('COPERNICUS_USER')
pwd = os.getenv('COPERNICUS_PASSWORD')

if not user or not pwd:
    print(f"❌ Missing COPERNICUS_USER or COPERNICUS_PASSWORD in .env")
else:
    print(f"📧 Testing with: {user}")
    
    try:
        # FIXED: Use CDSE realm instead of EODC
        r = requests.post(
            'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
            data={
                'grant_type': 'password',
                'username': user,
                'password': pwd,
                'client_id': 'cdse-public'
            },
            timeout=10
        )
        
        if r.status_code == 200:
            data = r.json()
            if 'access_token' in data:
                access_token = data['access_token']  # FIX: Proper variable name
                token_display = access_token[:20] + "..." + access_token[-10:]  # FIX: Use access_token
                print(f"✅ Copernicus CDSE: WORKING")
                print(f"   Token obtained: {token_display}")
                print(f"   Expires in: {data.get('expires_in', 'unknown')} seconds")
                print(f"   Scope: {data.get('scope', 'unknown')}")
                results['copernicus'] = True
            else:
                print(f"❌ No access token in response")
                print(f"   Response: {data}")
                
        elif r.status_code == 401:
            print(f"❌ Copernicus: 401 UNAUTHORIZED")
            print(f"   → Email or password is WRONG")
            print(f"   → Check your credentials at: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/account/")
            
        elif r.status_code == 400:
            error_desc = r.json().get('error_description', r.text)
            print(f"❌ Copernicus: 400 BAD REQUEST")
            print(f"   Error: {error_desc[:100]}")
            
        else:
            print(f"❌ Copernicus: {r.status_code}")
            print(f"   Response: {r.text[:200]}")
            
    except requests.exceptions.Timeout:
        print(f"⚠️  Copernicus: TIMEOUT (network slow)")
    except requests.exceptions.ConnectionError:
        print(f"❌ Copernicus: CONNECTION ERROR (offline?)")
    except Exception as e:
        print(f"❌ Copernicus Error: {str(e)[:100]}")

# ========== Test 3: NASA EARTHDATA ==========
print("\n[3] NASA EARTHDATA (MODIS/GPM Download)")
print("-" * 80)

token = os.getenv('NASA_EARTHDATA_TOKEN')
user = os.getenv('NASA_EARTHDATA_USER')

if not token or not user:
    print(f"❌ Missing NASA credentials")
else:
    print(f"📧 Testing with: {user}")
    
    try:
        r = requests.get(
            'https://cmr.earthdata.nasa.gov/search/granules.json',
            params={'short_name': 'MOD09GA', 'page_size': 1},
            headers={'Authorization': f'Bearer {token}'},
            timeout=10
        )
        
        if r.status_code == 200:
            print(f"✅ NASA CMR API: WORKING")
            data = r.json()
            print(f"   → Can query MODIS/GPM data")
            results['nasa_cmr'] = True
        elif r.status_code == 401:
            print(f"❌ NASA: 401 UNAUTHORIZED")
            print(f"   → Token expired or invalid")
        else:
            print(f"⚠️  NASA: {r.status_code}")
            
    except Exception as e:
        print(f"❌ NASA Error: {str(e)[:80]}")

# ========== Test 4: API ==========
print("\n[4] LOCAL API SERVER")
print("-" * 80)
try:
    r = requests.get('http://localhost:8000/api/health', timeout=5)
    if r.status_code == 200:
        print(f"✅ API Server: RUNNING")
        results['api'] = True
except:
    print(f"❌ API Server: NOT RUNNING")

# ========== SUMMARY ==========
print("\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)

required = ['database', 'nasa_cmr', 'api']

print("\n✅ REQUIRED:")
for service in required:
    status = "✅" if results[service] else "❌"
    print(f"   {status} {service.upper()}")

print("\n⚠️  OPTIONAL:")
print(f"   {'✅' if results['copernicus'] else '⚠️'} COPERNICUS (Sentinel-1)")

all_required = all(results[s] for s in required)

print("\n" + "=" * 80)
if all_required:
    print("🎉 READY TO START DOWNLOADING DATA")
    if results['copernicus']:
        print("   ✅ Sentinel-1 + MODIS + GPM")
    else:
        print("   ✅ MODIS + GPM (Sentinel-1 retry later)")
else:
    print("❌ FIX REQUIRED SERVICES FIRST")
print("=" * 80)