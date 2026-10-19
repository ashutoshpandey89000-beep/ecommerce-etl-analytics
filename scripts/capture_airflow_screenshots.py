"""
Airflow UI Automated Screenshot Capture Utility.

Uses Chrome DevTools Protocol (CDP) over WebSockets to capture high-resolution,
fully-rendered screenshots of running Apache Airflow web interfaces:
  - Task Grid View with individual task metrics and durations
  - DAG Dependency Graph View
  - Task Execution Gantt Timeline View
  - Airflow Home DAG Catalog Dashboard
"""

import asyncio
import base64
import json
import os
import subprocess
import time
from typing import Any, Dict, List
import requests
import websockets

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
if not os.path.exists(CHROME_PATH):
    CHROME_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

class CDPClient:
    def __init__(self, ws):
        self.ws = ws
        self.msg_id = 1

    async def call(self, method, params=None):
        cur_id = self.msg_id
        self.msg_id += 1
        req = {"id": cur_id, "method": method}
        if params:
            req["params"] = params
        await self.ws.send(json.dumps(req))
        while True:
            raw = await self.ws.recv()
            data = json.loads(raw)
            if data.get("id") == cur_id:
                if "error" in data:
                    raise RuntimeError(f"CDP error for {method}: {data['error']}")
                return data.get("result", {})

async def capture_all_pages(pages, width=1600, height=1000):
    # Start Chrome with remote debugging
    proc = subprocess.Popen([
        CHROME_PATH,
        "--headless=new",
        "--remote-debugging-port=9222",
        "--disable-gpu",
        "--no-sandbox",
        f"--window-size={width},{height}",
        "about:blank"
    ])

    try:
        # Wait for debugging port
        for _ in range(30):
            try:
                r = requests.get("http://127.0.0.1:9222/json/version", timeout=1)
                if r.status_code == 200:
                    break
            except Exception:
                time.sleep(0.2)
        else:
            raise RuntimeError("Chrome debugging port 9222 did not open.")

        targets = requests.get("http://127.0.0.1:9222/json", timeout=2).json()
        page_target = [t for t in targets if t.get("type") == "page"][0]
        ws_url = page_target["webSocketDebuggerUrl"]

        async with websockets.connect(ws_url, max_size=50 * 1024 * 1024) as ws:
            client = CDPClient(ws)
            await client.call("Page.enable")
            await client.call("DOM.enable")
            await client.call("Network.enable")
            await client.call("Emulation.setDeviceMetricsOverride", {
                "width": width,
                "height": height,
                "deviceScaleFactor": 1,
                "mobile": False
            })

            for page_info in pages:
                url = page_info["url"]
                out_path = page_info["out_path"]
                wait_sec = page_info.get("wait", 4)
                print(f"\n--- Navigating to: {url} ---")
                await client.call("Page.navigate", {"url": url})
                
                print(f"Waiting {wait_sec}s for data loading and UI layout...")
                await asyncio.sleep(wait_sec)

                # Capture full or viewport screenshot
                result = await client.call("Page.captureScreenshot", {"format": "png"})
                img_data = base64.b64decode(result["data"])
                
                os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
                with open(out_path, "wb") as f:
                    f.write(img_data)
                print(f"Captured: {out_path} ({len(img_data)} bytes)")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    shots_dir = os.path.join(base_dir, "screenshots")
    
    pages_to_capture = [
        {
            "url": "http://localhost:8080/dags/ecommerce_etl_pipeline/grid?task_id=generate_sample_data",
            "out_path": os.path.join(shots_dir, "airflow_grid_task_generate_sample_data.png"),
            "wait": 5
        },
        {
            "url": "http://localhost:8080/dags/ecommerce_etl_pipeline/grid?task_id=validate_transform_load",
            "out_path": os.path.join(shots_dir, "airflow_grid_task_validate_transform_load.png"),
            "wait": 5
        },
        {
            "url": "http://localhost:8080/dags/ecommerce_etl_pipeline/grid",
            "out_path": os.path.join(shots_dir, "airflow_dag_grid_view.png"),
            "wait": 5
        },
        {
            "url": "http://localhost:8080/dags/ecommerce_etl_pipeline/graph",
            "out_path": os.path.join(shots_dir, "airflow_dag_graph_view.png"),
            "wait": 5
        },
        {
            "url": "http://localhost:8080/dags/ecommerce_etl_pipeline/gantt",
            "out_path": os.path.join(shots_dir, "airflow_dag_gantt_view.png"),
            "wait": 5
        },
        {
            "url": "http://localhost:8080/home",
            "out_path": os.path.join(shots_dir, "airflow_home_dags_list.png"),
            "wait": 4
        }
    ]

    asyncio.run(capture_all_pages(pages_to_capture))
