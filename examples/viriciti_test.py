#!/usr/bin/env python3
"""
ViriCiti API Tester - Run this locally to test API responses.
Usage: python viriciti_test.py YOUR_API_KEY
"""

import sys
import json
import asyncio
from datetime import datetime

try:
    import httpx
except ImportError:
    print("Installing httpx...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "websockets"])
    import httpx

try:
    import websockets
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets


API_BASE = "https://sdk.viriciti.com"


async def test_vehicles(api_key: str) -> list:
    """Test GET /api/v1/my/assets"""
    print("\n" + "="*60)
    print("TEST 1: Fetch Vehicle List")
    print("Endpoint: GET /api/v1/my/assets")
    print("="*60)

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{API_BASE}/api/v1/my/assets",
                headers={"x-api-key": api_key},
                timeout=15.0
            )
            print(f"Status: {resp.status_code}")
            print(f"Headers: {dict(resp.headers)}")

            data = resp.json()
            print(f"\nResponse ({len(data)} vehicles):")
            print(json.dumps(data, indent=2))

            if resp.status_code == 200:
                print(f"\n✓ SUCCESS: Found {len(data)} vehicles")
                for v in data[:5]:  # Show first 5
                    print(f"  - {v.get('vid')}: {v.get('name')}")
                if len(data) > 5:
                    print(f"  ... and {len(data) - 5} more")
            else:
                print(f"\n✗ ERROR: {resp.status_code}")

            return data

        except Exception as e:
            print(f"\n✗ EXCEPTION: {type(e).__name__}: {e}")
            return []


async def test_current_soc(api_key: str, vehicles: list) -> dict:
    """Test POST /api/v2/state"""
    print("\n" + "="*60)
    print("TEST 2: Fetch Current SOC")
    print("Endpoint: POST /api/v2/state")
    print("="*60)

    if not vehicles:
        print("Skipping - no vehicles found")
        return {}

    # Build request for all vehicles
    request_body = {v["vid"]: ["soc", "odo"] for v in vehicles}
    print(f"Request body: {json.dumps(request_body, indent=2)}")

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{API_BASE}/api/v2/state",
                headers={
                    "x-api-key": api_key,
                    "Content-Type": "application/json"
                },
                json=request_body,
                timeout=15.0
            )
            print(f"\nStatus: {resp.status_code}")

            data = resp.json()
            print(f"\nResponse:")
            print(json.dumps(data, indent=2))

            if resp.status_code == 200:
                print(f"\n✓ SUCCESS: Got data for {len(data)} vehicles")
                for vid, params in list(data.items())[:5]:
                    soc = params.get("soc", {}).get("value", "N/A")
                    print(f"  - {vid}: SOC = {soc}%")
            else:
                print(f"\n✗ ERROR: {resp.status_code}")

            return data

        except Exception as e:
            print(f"\n✗ EXCEPTION: {type(e).__name__}: {e}")
            return {}


async def test_websocket(api_key: str, vehicles: list, duration: int = 10):
    """Test WebSocket live stream"""
    print("\n" + "="*60)
    print("TEST 3: WebSocket Live Stream")
    print(f"Endpoint: wss://sdk.viriciti.com/api/v2/live")
    print(f"Duration: {duration} seconds")
    print("="*60)

    url = f"wss://sdk.viriciti.com/api/v2/live?apiKey={api_key}"

    try:
        async with websockets.connect(url) as ws:
            print("✓ Connected!")

            # Subscribe to SOC for all vehicles
            if vehicles:
                subscription = {"vehicles": {v["vid"]: ["soc"] for v in vehicles}}
                await ws.send(json.dumps(subscription))
                print(f"Sent subscription for {len(vehicles)} vehicles")

            # Listen for messages
            print(f"\nListening for {duration} seconds...")
            start = asyncio.get_event_loop().time()
            msg_count = 0

            while asyncio.get_event_loop().time() - start < duration:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    msg_count += 1
                    data = json.loads(msg)

                    msg_type = data.get("type", "unknown")
                    if msg_type == "vehicles" and data.get("payload", {}).get("label") == "soc":
                        p = data["payload"]
                        ts = datetime.fromtimestamp(p["time"]/1000).strftime("%H:%M:%S")
                        print(f"  [{ts}] SOC: {p['vid']} = {p['value']}%")
                    elif msg_type == "acl":
                        print(f"  [ACL] Received access control list")
                    elif msg_type == "error":
                        print(f"  [ERROR] {data.get('payload')}")
                    else:
                        print(f"  [{msg_type}] {json.dumps(data)[:100]}...")

                except asyncio.TimeoutError:
                    continue

            print(f"\n✓ Received {msg_count} messages in {duration}s")

    except Exception as e:
        print(f"\n✗ EXCEPTION: {type(e).__name__}: {e}")


async def main():
    if len(sys.argv) < 2:
        print("Usage: python viriciti_test.py YOUR_API_KEY")
        print("\nGet your API key from: https://portal.viriciti.com/sdk")
        sys.exit(1)

    api_key = sys.argv[1]
    print("ViriCiti API Tester")
    print("="*60)
    print(f"API Key: {api_key[:8]}...{api_key[-4:]}")

    # Test 1: Vehicle list
    vehicles = await test_vehicles(api_key)

    # Test 2: Current SOC
    await test_current_soc(api_key, vehicles)

    # Test 3: WebSocket (10 seconds)
    await test_websocket(api_key, vehicles, duration=10)

    print("\n" + "="*60)
    print("TESTING COMPLETE")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
