"""
WMS Drive → BigQuery Sync Tool
Chạy trên máy bằng lệnh:
    python sync_wms_local.py                    # sync nhanh 1 lần
    python sync_wms_local.py --daemon           # tự động chạy lặp lại mỗi 5 phút
    python sync_wms_local.py --full --daemon    # chạy full sync định kỳ mỗi 5 phút
    python sync_wms_local.py --folder PICKING   # chỉ sync 1 folder cụ thể
"""

import os, re, io, sys, json, argparse, warnings, time
from datetime import datetime, date, timedelta

import pandas as pd
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
PROJECT_ID   = "jingdong-505914"
try:
    import streamlit as _st_check
    if hasattr(_st_check, "secrets") and "gcp_service_account" in _st_check.secrets:
        PROJECT_ID = _st_check.secrets["gcp_service_account"].get("project_id", PROJECT_ID)
except Exception:
    pass
DATASET_ID   = "wms_data"
LOG_TABLE    = f"{PROJECT_ID}.{DATASET_ID}.sync_file_logs"

SERVICE_ACCOUNT_JSON   = "service_account.json"
OAUTH_CREDENTIALS_JSON = "credentials.json"
OAUTH_TOKEN_JSON       = "token.json"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
]

FOLDERS_CONFIG = [
    {
        "folder_id":   "18yVGY6o3bFKPkfrDcVWkGG1fof686m9e",
        "folder_name": "CHECK IN",
        "stg_table":   "stg_checkin",
        "fact_table":  "fact_checkin",
        "primary_keys":["worker_id","checkin_time"],
        "time_col":    "checkin_time",
        "column_mapping": {
            "EmployeeCode": "worker_id",
            "EmployeeName": "worker_name",
            "AttendanceGroupName": "group_name",
            "PositionName": "job_title",
            "PunchCardTime": "checkin_time",
        },
    },
    {
        "folder_id":   "1qp8of7aCBGppEQZ11mN1qTbyGl-ndlcf",
        "folder_name": "HANDOVER",
        "stg_table":   "stg_handover",
        "fact_table":  "fact_handover",
        "primary_keys":["order_no"],
        "time_col":    "handover_time",
        "column_mapping": {
            "JD Outbound NO.": "order_no",
            "Carrier Name":    "carrier_name",
            "Owner Name":      "owner_name",
            "Handover Time":   "handover_time",
            "Staff NO.":       "worker_id",
            "Name":            "worker_name",
            "email":           "worker_email",
        },
    },
    {
        "folder_id":   "1ymmRHEpcYkhJxCNxBrNbGinrX47tiUGi",
        "folder_name": "INVENTORY",
        "stg_table":   "stg_inventory",
        "fact_table":  "fact_inventory",
        "primary_keys":["inventory_check_no","sku","location","owner_name"],
        "time_col":    "inventory_time",
        "column_mapping": {
            "Inventory Check NO.":       "inventory_check_no",
            "Owner Name":                "owner_name",
            "Goods NO.":                 "sku",
            "Goods Name":               "product_name",
            "Inventory QTY.":           "system_qty",
            "Inventory Check Difference":"difference_qty",
            "Cell":                     "location",
            "First Check Operator":     "worker_id",
            "First Check Time":         "inventory_time",
        },
    },
    {
        "folder_id":   "15DAw6MKgbqla2plAjkaEdCFQu51EI973",
        "folder_name": "PACKING",
        "stg_table":   "stg_packing",
        "fact_table":  "fact_packing",
        "primary_keys":["order_no","sku"],
        "time_col":    "pack_time",
        "column_mapping": {
            "Packing Time":          "pack_time",
            "JD Outbound NO.":       "order_no",
            "Store NO.":             "store_code",
            "Owner Name":            "owner_name",
            "JD SKU Code":           "sku",
            "Goods Name":            "product_name",
            "Actual Pack Quantity":  "packing_qty",
            "Packer Name":           "worker_name",
            "Actual Consumables Code":"packing_material_code",
            "Actual Consumables Name":"packing_material_name",
        },
    },
    {
        "folder_id":   "1vt4nkf1hvDOupSmyKvq2uXJo2EJdxMJT",
        "folder_name": "PICKING",
        "stg_table":   "stg_picking",
        "fact_table":  "fact_picking",
        "primary_keys":["order_no","sku"],
        "time_col":    "start_time",
        "column_mapping": {
            "Owner Name":           "owner_name",
            "JD Outbound NO.":      "order_no",
            "JD SKU Code":          "sku",
            "Goods Name":           "product_name",
            "Cell":                 "location",
            "Expected Picking QTY.":"expected_qty",
            "Actual Picking QTY.":  "actual_qty",
            "Task Receiving Time":  "start_time",
            "Picking Completion Time":"end_time",
            "Staff NO.":            "worker_id",
            "Name":                 "worker_name",
            "email":                "worker_email",
        },
    },
    {
        "folder_id":   "1TTNZe-PLxR4QIzoYi8c5858lCjyP4S_Z",
        "folder_name": "PUT AWAY",
        "stg_table":   "stg_putaway",
        "fact_table":  "fact_putaway",
        "primary_keys":["order_no","sku","location"],
        "time_col":    "putaway_time",
        "column_mapping": {
            "JD Inbound NO.":   "order_no",
            "Task Type":        "cargo_type",
            "Goods NO.":        "sku",
            "Goods Name":       "product_name",
            "Putaway QTY.":     "putaway_qty",
            "Cell Code":        "location",
            "Putaway Personnel":"worker_id",
            "Putaway Time":     "putaway_time",
            "Owner Name":       "owner_name",
        },
    },
    {
        "folder_id":   "1WScpNe7T70xt5CAO9r657wIWZcdfFogi",
        "folder_name": "RECEIVING",
        "stg_table":   "stg_receiving",
        "fact_table":  "fact_receiving",
        "primary_keys":["order_no","sku"],
        "time_col":    "receiving_time",
        "column_mapping": {
            "Task Type":           "cargo_type",
            "JD Inbound NO.":      "order_no",
            "Goods NO.":           "sku",
            "Goods Name":          "product_name",
            "Receiving QTY.":      "receiving_qty",
            "Received By":         "worker_id",
            "Start Receiving Time":"receiving_time",
            "Owner Name":          "owner_name",
        },
    },
    {
        "folder_id":   "1UKwJHy0M1UGmzE9uBAHxC5eNUNHsEtbi",
        "folder_name": "UNLOADING",
        "stg_table":   "stg_unloading",
        "fact_table":  "fact_unloading",
        "primary_keys":["worker_id","unload_time"],
        "time_col":    "unload_time",
        "column_mapping": {
            "Owner Name":         "owner_name",
            "Unloading Personnel":"worker_id",
            "Unloading Time":     "unload_time",
            "Unload_Staff_ID":    "worker_id",
            "Unload_Time":        "unload_time",
        },
    },
    {
        "folder_id":   "1ncaVBctXMUkiBggKIs43YrQtfFbIECk4",
        "folder_name": "UNSHELF",
        "stg_table":   "stg_unshelf",
        "fact_table":  "fact_unshelf",
        "primary_keys":["worker_id","unshelf_time"],
        "time_col":    "unshelf_time",
        "column_mapping": {
            "Operator":   "worker_id",
            "Start Time": "unshelf_time",
        },
    },
    {
        "folder_id":   "1MuB5nSI54TnKsYJcKmogAdy3gSbO4RWp",
        "folder_name": "BACKLOG",
        "stg_table":   "stg_backlog",
        "fact_table":  "fact_backlog",
        "primary_keys":["order_no"],
        "time_col":    "receive_date",
        "column_mapping": {
            "Receive Date":        "receive_date",
            "JD Inbound NO.":      "order_no",
            "To Be Received QTY.": "to_be_received_qty",
            "Task Type":           "cargo_type",
            "Owner Name":          "owner_name",
        },
    },
]

DATE_RANGE_PATTERN = re.compile(
    r'^(\d{4})-(\d{1,2})-(\d{1,2})_(\d{1,2})(?:-(\d{1,2}))?$'
)

def parse_date_range_filename(filename: str):
    name = os.path.splitext(filename)[0].strip()
    m = DATE_RANGE_PATTERN.match(name)
    if m:
        year, m1, d1, part2, part3 = m.groups()
        try:
            start = date(int(year), int(m1), int(d1))
            end   = date(int(year), int(part2), int(part3)) if part3 else date(int(year), int(m1), int(part2))
            return start, end
        except ValueError:
            pass

    m_single = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', name)
    if m_single:
        y, m_val, d = m_single.groups()
        try:
            dt = date(int(y), int(m_val), int(d))
            return dt, dt
        except ValueError:
            pass

    return None

def get_period_boundary(today=None):
    today = today or datetime.now().date()
    first = today.replace(day=1)
    return (first - timedelta(days=1)).replace(day=1)

def select_files_to_scan(all_files: list, is_full_sync: bool) -> list:
    batch_files, ondemand_files = [], []
    for f in all_files:
        dr = parse_date_range_filename(f["name"])
        if dr:
            f = dict(f); f["_start"], f["_end"] = dr
            batch_files.append(f)
        else:
            ondemand_files.append(f)
    selected = []
    if ondemand_files:
        latest = max(ondemand_files, key=lambda x: x["modifiedTime"])
        selected.append(latest)
    boundary = get_period_boundary()
    for f in batch_files:
        if is_full_sync or f["_end"] >= boundary:
            selected.append(f)
    return selected

def get_credentials():
    # 1. Ưu tiên đọc từ Streamlit secrets (định dạng TOML trên Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            sec = None
            if "gcp_service_account" in st.secrets:
                sec = dict(st.secrets["gcp_service_account"])
            elif "service_account" in st.secrets:
                sec = dict(st.secrets["service_account"])
            elif "gcp" in st.secrets:
                sec = dict(st.secrets["gcp"])
            elif "type" in st.secrets and st.secrets.get("type") == "service_account":
                sec = dict(st.secrets)
            if sec:
                return service_account.Credentials.from_service_account_info(sec, scopes=SCOPES)
    except Exception:
        pass

    # 2. Fallback đọc file local service_account.json
    if os.path.exists(SERVICE_ACCOUNT_JSON):
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_JSON, scopes=SCOPES)

    creds = None
    if os.path.exists(OAUTH_TOKEN_JSON):
        creds = Credentials.from_authorized_user_file(OAUTH_TOKEN_JSON, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("🌐 Mở browser để đăng nhập Google...")
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDENTIALS_JSON, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(OAUTH_TOKEN_JSON, "w") as f:
            f.write(creds.to_json())
    return creds

def get_owner_customer_type(owner_name) -> str:
    if owner_name is None or (isinstance(owner_name, float) and pd.isna(owner_name)):
        return "B2C"
    low = str(owner_name).strip().lower()
    if any(x in low for x in ["jinhe","chint","envision"]):
        return "B2B"
    return "B2C"

def sync_folder(cfg, drive_svc, bq_client, processed_map, is_full_sync, dry_run, only_folder):
    folder_name = cfg["folder_name"]
    if only_folder and only_folder.upper() != folder_name.upper():
        return 0, 0

    print(f"\n📂 {folder_name}", flush=True)

    all_files = []
    for attempt in range(2):
        try:
            results = drive_svc.files().list(
                q=f"'{cfg['folder_id']}' in parents and trashed=false",
                fields="files(id,name,modifiedTime)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=200,
            ).execute()
            all_files = [f for f in results.get("files", [])
                         if f["name"].lower().endswith((".xlsx",".xls",".csv"))]
            break
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
                continue
            print(f"  ❌ Lỗi liệt kê file: {e}")
            return 0, 0

    candidates = select_files_to_scan(all_files, is_full_sync)

    synced, skipped = 0, 0
    for file_meta in candidates:
        fid   = file_meta["id"]
        fname = file_meta["name"]
        mtime = file_meta["modifiedTime"]

        if processed_map.get(fid) == mtime:
            skipped += 1
            continue

        print(f"  📥 Đọc: {fname} ...", end=" ", flush=True)

        if dry_run:
            print("[DRY RUN]")
            synced += 1
            continue

        try:
            req = drive_svc.files().get_media(fileId=fid)
            fh  = io.BytesIO()
            dl  = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
            fh.seek(0)

            if fname.lower().endswith(".csv"):
                df = pd.read_csv(fh)
            else:
                try:
                    df = pd.read_excel(fh, sheet_name=0, engine="calamine")
                except Exception:
                    df = pd.read_excel(fh, sheet_name=0)

            df.columns = [str(c).strip() for c in df.columns]
            _original_columns_for_debug = list(df.columns)

            if cfg.get("column_mapping"):
                def _norm_col(s):
                    return re.sub(r'[^a-zA-Z0-9]', '', str(s)).lower()

                norm_mapping = {_norm_col(k): v for k, v in cfg["column_mapping"].items()}
                rename_dict = {}
                for col in df.columns:
                    key = _norm_col(col)
                    if key in norm_mapping:
                        rename_dict[col] = norm_mapping[key]
                df = df.rename(columns=rename_dict)

            if "owner_name" in df.columns:
                df["customer_type"] = df["owner_name"].apply(get_owner_customer_type)

            if cfg["folder_name"] == "INVENTORY":
                if "system_qty" in df.columns:
                    df["system_qty"] = pd.to_numeric(df["system_qty"], errors="coerce").fillna(0).astype(int)
                if "difference_qty" in df.columns:
                    df["difference_qty"] = pd.to_numeric(df["difference_qty"], errors="coerce").fillna(0).astype(int)
                if "system_qty" in df.columns and "difference_qty" in df.columns:
                    df["actual_qty"] = df["system_qty"] - df["difference_qty"]

            import re as _re
            df.columns = [_re.sub(r'[^a-zA-Z0-9_]', '_', c) for c in df.columns]

            target = bq_client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{cfg['fact_table']}")
            target_cols = [f.name for f in target.schema]
            cols_keep = [c for c in df.columns if c in target_cols]
            if not cols_keep:
                print(f"⚠️ Không khớp cột nào — bỏ qua")
                continue

            if cfg["time_col"] not in cols_keep:
                print(f"❌ Thiếu cột thời gian '{cfg['time_col']}' sau khi map.")
                continue

            df = df[cols_keep]

            df_stg = df.copy()
            for c in df_stg.columns:
                df_stg[c] = df_stg[c].astype(str).replace(
                    {"nan": None, "None": None, "<NA>": None, "NaT": None})

            stg_ref = f"{PROJECT_ID}.{DATASET_ID}.{cfg['stg_table']}"
            job = bq_client.load_table_from_dataframe(
                df_stg, stg_ref,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
            job.result()

            schema_map = {f.name: f.field_type for f in target.schema}
            type_conv  = {"FLOAT": "FLOAT64", "INTEGER": "INT64", "BOOLEAN": "BOOL"}
            pk_list    = cfg["primary_keys"]
            time_col   = cfg["time_col"]

            insert_cols = ", ".join(f"`{c}`" for c in cols_keep)
            insert_vals = ", ".join(
                f"CAST(S.`{c}` AS {type_conv.get(schema_map.get(c, 'STRING').upper(), schema_map.get(c, 'STRING'))})"
                for c in cols_keep)

            update_set = ", ".join(
                f"T.`{c}` = CAST(S.`{c}` AS {type_conv.get(schema_map.get(c, 'STRING').upper(), schema_map.get(c, 'STRING'))})"
                for c in cols_keep if c not in pk_list)

            pk_cond = " AND ".join(
                f"CAST(T.`{k}` AS DATETIME) = CAST(S.`{k}` AS DATETIME)"
                if ("time" in k or "date" in k) else f"T.`{k}` = S.`{k}`"
                for k in pk_list)

            pk_partition = ", ".join(f"`{k}`" for k in pk_list)
            merge_sql = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.{cfg['fact_table']}` T
            USING (
              SELECT * FROM `{stg_ref}`
              WHERE `{time_col}` IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {pk_partition}
                ORDER BY CAST(`{time_col}` AS TIMESTAMP) DESC
              ) = 1
            ) S ON {pk_cond}
            {"WHEN MATCHED THEN UPDATE SET " + update_set if update_set else ""}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            bq_client.query(merge_sql).result()

            log_sql = f"""
            MERGE `{LOG_TABLE}` L
            USING (SELECT '{fid}' AS file_id,'{fname}' AS file_name,
                          '{folder_name}' AS folder_name,'{mtime}' AS modified_time,
                          {len(df)} AS row_count, CURRENT_TIMESTAMP() AS synced_at) N
            ON L.file_id = N.file_id
            WHEN MATCHED     THEN UPDATE SET L.modified_time=N.modified_time, L.row_count=N.row_count, L.synced_at=N.synced_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            bq_client.query(log_sql).result()
            processed_map[fid] = mtime
            synced += 1
            print(f"✅ {len(df):,} dòng")

        except Exception as e:
            print(f"❌ Lỗi: {e}")

    return synced, skipped

def run_sync_cycle(drive_svc, bq_client, is_full, dry_run, folder):
    try:
        rows = bq_client.query(
            f"SELECT file_id, modified_time FROM `{LOG_TABLE}`").result()
        processed_map = {r.file_id: r.modified_time for r in rows}
    except Exception:
        processed_map = {}

    total_synced = total_skipped = 0
    start_time = datetime.now()

    for cfg in FOLDERS_CONFIG:
        s, sk = sync_folder(
            cfg, drive_svc, bq_client, processed_map,
            is_full_sync=is_full,
            dry_run=dry_run,
            only_folder=folder,
        )
        total_synced  += s
        total_skipped += sk

    elapsed = (datetime.now() - start_time).seconds
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp_str}] Đã quét xong ({elapsed}s) — Đã nạp mới: {total_synced} file | Không đổi: {total_skipped} file", flush=True)
    return {
        "synced": total_synced,
        "skipped": total_skipped,
        "elapsed": elapsed,
        "timestamp": timestamp_str
    }

def main():
    parser = argparse.ArgumentParser(description="WMS Drive → BigQuery Sync")
    parser.add_argument("--full",     action="store_true", help="Full sync toàn bộ lịch sử")
    parser.add_argument("--folder",   type=str, default=None, help="Chỉ sync 1 folder")
    parser.add_argument("--dry-run",  action="store_true", help="Dry run test file")
    parser.add_argument("--daemon",   action="store_true", help="Chạy nền liên tục định kỳ")
    parser.add_argument("--interval", type=int, default=300, help="Thời gian delay giữa các lần quét (giây), mặc định 300s = 5p")
    args = parser.parse_args()

    creds     = get_credentials()
    drive_svc = build("drive", "v3", credentials=creds)
    bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)

    if not args.daemon:
        run_sync_cycle(drive_svc, bq_client, args.full, args.dry_run, args.folder)
        return

    print(f"🚀 Chế độ Tự Động (Daemon Mode) đã kích hoạt: Quét Drive mỗi {args.interval} giây ({args.interval // 60} phút).")
    print("Nhấn Ctrl + C để dừng script bất kỳ lúc nào.\n")

    while True:
        try:
            run_sync_cycle(drive_svc, bq_client, args.full, args.dry_run, args.folder)
        except Exception as err:
            print(f"⚠️ Lỗi trong chu kỳ quét: {err}")
        
        print(f"⏳ Tạm nghỉ {args.interval} giây trước lần quét kế tiếp...")
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n🛑 Đã nhận tín hiệu dừng từ người dùng. Tắt sync script.")
            break

if __name__ == "__main__":
    main()
