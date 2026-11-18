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
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

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
        "folder_id":   "1ncaVBctXMUkiBggKIs43YrQtfFbIECk4",
        "folder_name": "ORDER",
        "stg_table":   "stg_order",
        "fact_table":  "fact_order",
        "primary_keys":["order_date", "owner_code", "outbound_type", "order_status"],
        "time_col":    "order_date",
        "column_mapping": {
            "Date": "order_date",
            "Owner": "owner_code",
            "Owner Name": "owner_name",
            "Outbound Type": "outbound_type",
            "Order Status": "order_status",
            "Total Packing QTY.": "total_orders",
            "7:00": "h07", "8:00": "h08", "9:00": "h09", "10:00": "h10",
            "11:00": "h11", "12:00": "h12", "13:00": "h13", "14:00": "h14",
            "15:00": "h15", "16:00": "h16", "17:00": "h17", "18:00": "h18",
            "19:00": "h19", "20:00": "h20", "21:00": "h21", "22:00": "h22",
            "23:00": "h23", "00:00": "h00", "01:00": "h01", "02:00": "h02",
            "03:00": "h03", "04:00": "h04", "05:00": "h05", "06:00": "h06",
        },
    },
    {
        "folder_id":   "1MuB5nSI54TnKsYJcKmogAdy3gSbO4RWp",
        "folder_name": "BACKLOG",
        "stg_table":   "stg_backlog",
        "fact_table":  "fact_backlog",
        "primary_keys":["order_no"],
        "time_col":    "received_time",
        "column_mapping": {
            "JD Outbound NO.":     "order_no",
            "Owner Name":          "owner_name",
            "Piece QTY.":          "piece_qty",
            "Received Time":       "received_time",
            "Receive Date":        "received_time",
            "JD Inbound NO.":      "order_no",
            "To Be Received QTY.": "piece_qty",
            "Production End Time": "production_end_time",
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
    # Tất cả file on-demand đều là ứng viên quét; processed_map sẽ bỏ qua file không đổi
    selected = list(ondemand_files)
    boundary = get_period_boundary()
    for f in batch_files:
        if is_full_sync or f["_end"] >= boundary:
            selected.append(f)
    return selected

def get_credentials():
    # 1. Đọc từ biến môi trường (chuỗi JSON) - Thường dùng khi deploy lên Cloud/Docker
    env_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON") or os.environ.get("GCP_SA_KEY")
    if env_json:
        try:
            sec_dict = json.loads(env_json)
            return service_account.Credentials.from_service_account_info(sec_dict, scopes=SCOPES)
        except Exception as e_env:
            print(f"⚠️ Không thể giải mã GCP_SERVICE_ACCOUNT_JSON: {e_env}")

    # 2. Đọc từ Streamlit secrets (khi chạy trên Streamlit Community Cloud)
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

    # 3. Đọc từ file secrets.toml cục bộ nếu có
    sec_file = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
    if os.path.exists(sec_file):
        try:
            import tomllib
            with open(sec_file, "rb") as f_sec:
                t_sec = tomllib.load(f_sec)
                sec_dict = t_sec.get("gcp_service_account") or t_sec.get("service_account")
                if sec_dict:
                    return service_account.Credentials.from_service_account_info(sec_dict, scopes=SCOPES)
        except Exception:
            pass

    # 4. Fallback đọc file local service_account.json
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

def transform_df_for_stage(df_in, stage_cfg, bq_client):
    """Ánh xạ cột và chuẩn hóa kiểu dữ liệu cho một stage cụ thể."""
    df = df_in.copy()
    def _norm_col(s):
        return re.sub(r'[^a-zA-Z0-9]', '', str(s)).lower()

    if stage_cfg.get("column_mapping"):
        norm_mapping = {_norm_col(k): v for k, v in stage_cfg["column_mapping"].items()}
        rename_dict = {}
        for col in df.columns:
            key = _norm_col(col)
            if key in norm_mapping:
                rename_dict[col] = norm_mapping[key]
        df = df.rename(columns=rename_dict)

    if "owner_name" in df.columns:
        df["customer_type"] = df["owner_name"].apply(get_owner_customer_type)

    if stage_cfg["folder_name"] == "INVENTORY":
        if "system_qty" in df.columns:
            df["system_qty"] = pd.to_numeric(df["system_qty"], errors="coerce").fillna(0).astype(int)
        if "difference_qty" in df.columns:
            df["difference_qty"] = pd.to_numeric(df["difference_qty"], errors="coerce").fillna(0).astype(int)
        if "system_qty" in df.columns and "difference_qty" in df.columns:
            df["actual_qty"] = df["system_qty"] - df["difference_qty"]

    if stage_cfg["folder_name"] == "BACKLOG":
        if "piece_qty" in df.columns:
            df["piece_qty"] = pd.to_numeric(df["piece_qty"], errors="coerce").fillna(0).astype(float)
        if "received_time" not in df.columns or df["received_time"].isnull().all():
            df["received_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            df["received_time"] = df["received_time"].fillna(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if stage_cfg["folder_name"] == "PICKING":
        if ("start_time" not in df.columns or df["start_time"].isnull().all()) and "end_time" in df.columns:
            df["start_time"] = df["end_time"]

    if stage_cfg["folder_name"] == "ORDER":
        if "order_date" in df.columns:
            df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        numeric_cols = ["total_orders", "h07", "h08", "h09", "h10", "h11", "h12", "h13", "h14",
                        "h15", "h16", "h17", "h18", "h19", "h20", "h21", "h22", "h23",
                        "h00", "h01", "h02", "h03", "h04", "h05", "h06"]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', c) for c in df.columns]

    target = bq_client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{stage_cfg['fact_table']}")
    target_cols = [f.name for f in target.schema]
    cols_keep = [c for c in df.columns if c in target_cols]

    return df, cols_keep, target

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
                orderBy="modifiedTime desc",
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

    if folder_name == "BACKLOG":
        # BACKLOG là dữ liệu Snapshot tồn đọng thời gian thực.
        # Luôn chỉ chọn DUY NHẤT 1 file có modifiedTime mới nhất trên Drive.
        candidates = [max(all_files, key=lambda f: f.get("modifiedTime", ""))] if all_files else []
    else:
        candidates = select_files_to_scan(all_files, is_full_sync)

    synced, skipped = 0, 0
    for file_meta in candidates:
        fid   = file_meta["id"]
        fname = file_meta["name"]
        mtime = file_meta["modifiedTime"]

        if not is_full_sync and processed_map.get(fid) == mtime:
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
            df_raw_loaded = df.copy()

            active_cfg = cfg
            df, cols_keep, target = transform_df_for_stage(df_raw_loaded, active_cfg, bq_client)

            # Tự động nhận diện nếu file bị tải nhầm thư mục (ví dụ file Đóng gói up vào Lấy hàng)
            if not cols_keep or active_cfg["time_col"] not in cols_keep:
                detected_cfg = None
                for alt_cfg in FOLDERS_CONFIG:
                    if alt_cfg["folder_name"] == cfg["folder_name"]:
                        continue
                    test_df, test_cols, test_target = transform_df_for_stage(df_raw_loaded, alt_cfg, bq_client)
                    if test_cols and alt_cfg["time_col"] in test_cols:
                        detected_cfg = alt_cfg
                        df, cols_keep, target = test_df, test_cols, test_target
                        break

                if detected_cfg:
                    print(f"  ↪️ File thuộc khâu '{detected_cfg['folder_name']}' (bị đặt nhầm vào '{cfg['folder_name']}') -> Tự động chuyển nạp vào {detected_cfg['fact_table']} ... ", end="", flush=True)
                    active_cfg = detected_cfg
                else:
                    if not cols_keep:
                        print(f"⚠️ Không khớp cột nào — bỏ qua")
                    else:
                        print(f"❌ Thiếu cột thời gian '{active_cfg['time_col']}' sau khi map.")
                    continue

            df = df[cols_keep]

            df_stg = df.copy()
            for c in df_stg.columns:
                df_stg[c] = df_stg[c].astype(str).replace(
                    {"nan": None, "None": None, "<NA>": None, "NaT": None})

            stg_ref = f"{PROJECT_ID}.{DATASET_ID}.{active_cfg['stg_table']}"
            job = bq_client.load_table_from_dataframe(
                df_stg, stg_ref,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE"
                ))
            job.result()

            schema_map = {f.name: f.field_type for f in target.schema}
            type_conv  = {"FLOAT": "FLOAT64", "INTEGER": "INT64", "BOOLEAN": "BOOL"}
            pk_list    = active_cfg["primary_keys"]
            time_col   = active_cfg["time_col"]

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
            MERGE `{PROJECT_ID}.{DATASET_ID}.{active_cfg['fact_table']}` T
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
            if active_cfg["folder_name"] == "BACKLOG":
                # Đối với BACKLOG: Truncate bảng fact_backlog và nạp danh sách đơn mới nhất
                # để snapshot luôn sạch và loại bỏ các đơn cũ đã được hoàn thành.
                bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{active_cfg['fact_table']}`").result()
                insert_sql = f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{active_cfg['fact_table']}` ({insert_cols})
                SELECT {insert_vals}
                FROM `{stg_ref}` S
                WHERE `{time_col}` IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                  PARTITION BY {pk_partition}
                  ORDER BY CAST(`{time_col}` AS TIMESTAMP) DESC
                ) = 1
                """
                bq_client.query(insert_sql).result()

                # Tự động quét và bắn tin nhắn cảnh báo đơn cận hạn/trễ hạn vào Nhóm Chat Zalo
                try:
                    import zalo_notifier
                    z_res = zalo_notifier.scan_and_alert_backlog_sla(df, force_send=True, notify_if_safe=True)
                    if z_res.get("status") == "sent":
                        print(f"  📢 Zalo Bot: {z_res.get('message')}")
                    elif z_res.get("status") == "error":
                        print(f"  ⚠️ Zalo Bot: {z_res.get('message')}")
                except Exception:
                    pass
            else:
                bq_client.query(merge_sql).result()

                # Nếu vừa nạp file PACKING mới: Tự động cập nhật tiến độ đóng gói vào Nhóm Zalo
                if active_cfg["folder_name"] == "PACKING":
                    try:
                        import zalo_notifier
                        z_res = zalo_notifier.scan_and_alert_packing_progress(force_send=True)
                        if z_res.get("status") == "sent":
                            print(f"  📢 Zalo Bot: {z_res.get('message')}")
                        elif z_res.get("status") == "error":
                            print(f"  ⚠️ Zalo Bot: {z_res.get('message')}")
                    except Exception:
                        pass

            log_sql = f"""
            MERGE `{LOG_TABLE}` L
            USING (SELECT '{fid}' AS file_id,'{fname}' AS file_name,
                          '{active_cfg["folder_name"]}' AS folder_name,'{mtime}' AS modified_time,
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


# ══════════════════════════════════════════════════════════════
# CHECK QUÁ HẠN (4 FILE THÔ JD WMS TRONG 1 FOLDER -> AUTO-DETECT)
# ══════════════════════════════════════════════════════════════
CHECK_QUA_HAN_FOLDER_ID = "1fNswoPcRJvd51oLfaICQ1QntEmizsWBx"
CHECK_QUA_HAN_FOLDER_NAME = "CHECK_QUA_HAN"

CHECK_QUA_HAN_TYPES = {
    "DIFF_CENTER": {
        "stg_table": "stg_diff_center",
        "fact_table": "fact_diff_center",
        "mode": "truncate",
        "required_cols": {"differencetype", "differencestatus", "untreateddifferenceqty"},
        "datetime_cols": ["create_time"],
        "int_cols": ["reported_qty", "untreated_qty"],
        "column_mapping": {
            "Goods NO.": "goods_no",
            "Owner": "owner_code",
            "Owner Name": "owner_name",
            "Cell": "cell",
            "Goods Name": "goods_name",
            "Reported Difference QTY.": "reported_qty",
            "Untreated Difference QTY.": "untreated_qty",
            "Difference Type": "difference_type",
            "Difference Status": "difference_status",
            "Operation Type": "operation_type",
            "Task NO.": "task_no",
            "Operator": "operator",
            "Create Time": "create_time",
        },
    },
    "PUTAWAY_BACKLOG": {
        "stg_table": "stg_putaway_backlog",
        "fact_table": "fact_putaway_backlog",
        "mode": "truncate",
        "required_cols": {"putawaycontainernumber", "containercompletiontime", "pendingqtyforputaway"},
        "datetime_cols": ["start_receiving_time", "container_completion_time"],
        "int_cols": ["receiving_qty", "pending_qty"],
        "column_mapping": {
            "JD Inbound NO.": "order_no",
            "Customer Inbound NO.": "customer_inbound_no",
            "Putaway Container Number": "container_no",
            "Start Receiving Time": "start_receiving_time",
            "Container Completion Time": "container_completion_time",
            "Operator": "operator",
            "Target Storage Zone": "target_zone",
            "Receiving QTY.": "receiving_qty",
            "Pending QTY. For Putaway": "pending_qty",
            "Task Type": "task_type",
            "Owner": "owner_code",
            "Owner Name": "owner_name",
        },
    },
    "RECEIVING_BACKLOG": {
        "stg_table": "stg_receiving_backlog",
        "fact_table": "fact_receiving_backlog",
        "mode": "truncate",
        "required_cols": {"receivedate", "tobereceivedqty", "startreceivingtime"},
        "forbidden_cols": {"putawaycontainernumber"},
        "datetime_cols": ["receive_date", "start_receiving_time"],
        "int_cols": ["total_qty", "to_be_received_qty"],
        "column_mapping": {
            "Receive Date": "receive_date",
            "JD Inbound NO.": "order_no",
            "Customer Inbound NO.": "customer_inbound_no",
            "Total QTY.": "total_qty",
            "To Be Received QTY.": "to_be_received_qty",
            "Task Type": "task_type",
            "Status": "status",
            "Start Receiving Time": "start_receiving_time",
            "Owner": "owner_code",
            "Owner Name": "owner_name",
        },
    },
    "UNLOADING_RESULT": {
        "stg_table": "stg_unloading_result",
        "fact_table": "fact_unloading_result",
        "mode": "merge",
        "merge_key": "order_no",
        "required_cols": {"cargotruckpackageno", "perchaseorderno", "arrivaltype", "unloadtime"},
        "datetime_cols": ["unload_time"],
        "int_cols": [],
        "column_mapping": {
            "Cargo/Truck/Package NO.": "cargo_no",
            "Owner": "owner_code",
            "Owner Name": "owner_name",
            "Perchase Order NO.": "order_no",
            "Purchase Order NO.": "order_no",
            "Customer Inbound NO.": "customer_inbound_no",
            "Arrival Type": "arrival_type",
            "Cargo/Truck Type": "truck_type",
            "Unload Staff ID": "unload_staff_id",
            "Unload Time": "unload_time",
            "Status": "status",
            "Whether Combined": "whether_combined",
            "Master Order": "master_order",
        },
    },
}

_CHECK_SLA_BATCH_STATE = {
    "pending": False,
    "last_sync_ts": 0.0,
    "synced_types": set(),
}


def _norm_col_key(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]', '', str(s)).lower()


def detect_check_qua_han_type(columns) -> str | None:
    """Tự động nhận diện 1 trong 4 file thô JD WMS dựa trên bộ tên cột (không phụ thuộc tên file)."""
    norm_cols = {_norm_col_key(c) for c in columns}
    for type_key, tcfg in CHECK_QUA_HAN_TYPES.items():
        req = tcfg["required_cols"]
        forb = tcfg.get("forbidden_cols", set())
        if req.issubset(norm_cols) and not (forb & norm_cols):
            return type_key
    # Fallback nếu JD WMS sửa lỗi chính tả Perchase -> Purchase
    if {"cargotruckpackageno", "purchaseorderno", "arrivaltype", "unloadtime"}.issubset(norm_cols):
        return "UNLOADING_RESULT"
    return None


def sync_check_qua_han_folder(
    drive_svc,
    bq_client,
    processed_map: dict,
    is_full_sync: bool = False,
    dry_run: bool = False,
    only_folder: str = None,
    send_zalo: bool = True,
    is_daemon: bool = True,
) -> tuple[int, int]:
    """
    Quét thư mục CHECK QUÁ HẠN (chứa 4 file thô JD WMS), tự động nhận diện loại file theo header,
    nạp vào 4 bảng BigQuery tương ứng và tự động gửi 1 tin nhắn cảnh báo SLA tổng hợp vào Zalo
    sau khi batch upload ổn định (batch debounce).
    """
    global _CHECK_SLA_BATCH_STATE

    if only_folder and only_folder.upper() not in ("CHECK_QUA_HAN", "CHECK QUÁ HẠN", "CHECK_SLA", "SLA"):
        return 0, 0

    print("\n📂 CHECK QUÁ HẠN", flush=True)

    all_files = []
    for attempt in range(2):
        try:
            results = drive_svc.files().list(
                q=f"'{CHECK_QUA_HAN_FOLDER_ID}' in parents and trashed=false",
                fields="files(id,name,modifiedTime)",
                orderBy="modifiedTime desc",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=50,
            ).execute()
            all_files = [
                f for f in results.get("files", [])
                if f["name"].lower().endswith((".xlsx", ".xls", ".csv"))
            ]
            break
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
                continue
            print(f"  ❌ Lỗi liệt kê file CHECK QUÁ HẠN: {e}")
            return 0, 0

    # Bảo vệ thư mục rỗng tạm thời (khi nhân sự vừa xóa 4 file cũ, đang kéo 4 file mới lên)
    if not all_files:
        print("  ⏳ Thư mục CHECK QUÁ HẠN đang trống — giữ nguyên dữ liệu BigQuery, chờ batch mới.")
        return 0, 0

    synced, skipped = 0, 0
    synced_types_this_cycle = set()

    for file_meta in all_files:
        fid = file_meta["id"]
        fname = file_meta["name"]
        mtime = file_meta["modifiedTime"]

        if not is_full_sync and processed_map.get(fid) == mtime:
            skipped += 1
            continue

        print(f"  📥 Đọc: {fname} ...", end=" ", flush=True)

        if dry_run:
            print("[DRY RUN]")
            synced += 1
            continue

        try:
            req = drive_svc.files().get_media(fileId=fid)
            fh = io.BytesIO()
            dl = MediaIoBaseDownload(fh, req)
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
            file_type = detect_check_qua_han_type(df.columns)
            if not file_type:
                print("⚠️ Không nhận diện được cấu trúc cột SLA — bỏ qua")
                continue

            tcfg = CHECK_QUA_HAN_TYPES[file_type]

            # Nếu cùng 1 loại snapshot có nhiều file mới trong 1 lần quét, chỉ lấy file mới nhất (modifiedTime desc)
            if tcfg["mode"] == "truncate" and file_type in synced_types_this_cycle:
                print(f"⏭️ Đã nạp bản mới hơn cho {file_type} — ghi nhận bỏ qua bản cũ")
                safe_fname = fname.replace("'", "\\'")
                log_sql = f"""
                MERGE `{LOG_TABLE}` L
                USING (SELECT '{fid}' AS file_id, '{safe_fname}' AS file_name,
                              '{CHECK_QUA_HAN_FOLDER_NAME}' AS folder_name, '{mtime}' AS modified_time,
                              0 AS row_count, CURRENT_TIMESTAMP() AS synced_at) N
                ON L.file_id = N.file_id
                WHEN MATCHED     THEN UPDATE SET L.file_name=N.file_name, L.modified_time=N.modified_time, L.row_count=N.row_count, L.synced_at=N.synced_at
                WHEN NOT MATCHED THEN INSERT ROW
                """
                bq_client.query(log_sql).result()
                processed_map[fid] = mtime
                skipped += 1
                continue

            # Map cột chuẩn theo cấu hình
            norm_map = {_norm_col_key(k): v for k, v in tcfg["column_mapping"].items()}
            rename_dict = {}
            used_targets = set()
            for col in df.columns:
                nk = _norm_col_key(col)
                if nk in norm_map and norm_map[nk] not in used_targets:
                    rename_dict[col] = norm_map[nk]
                    used_targets.add(norm_map[nk])
            df = df.rename(columns=rename_dict)

            fact_ref = f"{PROJECT_ID}.{DATASET_ID}.{tcfg['fact_table']}"
            stg_ref = f"{PROJECT_ID}.{DATASET_ID}.{tcfg['stg_table']}"
            target_tbl = bq_client.get_table(fact_ref)
            target_cols = [f.name for f in target_tbl.schema]
            cols_keep = [c for c in target_cols if c in df.columns]

            if not cols_keep:
                print(f"⚠️ [{file_type}] Không khớp cột đích nào — bỏ qua")
                continue

            df_clean = df[cols_keep].copy()

            # Chuẩn hóa kiểu DATETIME và INTEGER an toàn tuyệt đối
            for dt_col in tcfg.get("datetime_cols", []):
                if dt_col in df_clean.columns:
                    parsed = pd.to_datetime(df_clean[dt_col], errors="coerce")
                    df_clean[dt_col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S").where(parsed.notna(), None)

            for int_col in tcfg.get("int_cols", []):
                if int_col in df_clean.columns:
                    num = pd.to_numeric(df_clean[int_col], errors="coerce")
                    df_clean[int_col] = num.round().astype("Int64")

            # Đưa toàn bộ về chuỗi sạch trước khi đẩy vào stg_table
            df_stg = df_clean.copy()
            for c in df_stg.columns:
                df_stg[c] = df_stg[c].astype(str).replace(
                    {"nan": None, "None": None, "<NA>": None, "NaT": None, "": None}
                )

            schema_map = {f.name: f.field_type for f in target_tbl.schema}
            type_conv = {"FLOAT": "FLOAT64", "INTEGER": "INT64", "BOOLEAN": "BOOL"}
            insert_cols = ", ".join(f"`{c}`" for c in cols_keep)
            insert_vals = ", ".join(
                f"SAFE_CAST(S.`{c}` AS {type_conv.get(schema_map.get(c, 'STRING').upper(), schema_map.get(c, 'STRING'))})"
                for c in cols_keep
            )

            row_count = len(df_stg)

            if tcfg["mode"] == "truncate":
                bq_client.query(f"TRUNCATE TABLE `{fact_ref}`").result()
                if row_count > 0:
                    job = bq_client.load_table_from_dataframe(
                        df_stg, stg_ref,
                        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
                    )
                    job.result()
                    insert_sql = f"""
                    INSERT INTO `{fact_ref}` ({insert_cols})
                    SELECT {insert_vals}
                    FROM `{stg_ref}` S
                    """
                    bq_client.query(insert_sql).result()
            else:
                # MERGE theo order_no (UNLOADING_RESULT: giữ lịch sử hạ cont cũ)
                if row_count > 0:
                    job = bq_client.load_table_from_dataframe(
                        df_stg, stg_ref,
                        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
                    )
                    job.result()
                    mk = tcfg["merge_key"]
                    update_set = ", ".join(
                        f"T.`{c}` = SAFE_CAST(S.`{c}` AS {type_conv.get(schema_map.get(c, 'STRING').upper(), schema_map.get(c, 'STRING'))})"
                        for c in cols_keep if c != mk
                    )
                    merge_sql = f"""
                    MERGE `{fact_ref}` T
                    USING (
                      SELECT * FROM `{stg_ref}`
                      WHERE `{mk}` IS NOT NULL AND TRIM(`{mk}`) != ''
                      QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY `{mk}`
                        ORDER BY SAFE_CAST(`unload_time` AS DATETIME) ASC NULLS LAST
                      ) = 1
                    ) S ON T.`{mk}` = S.`{mk}`
                    WHEN MATCHED THEN UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                    """
                    bq_client.query(merge_sql).result()

            safe_fname = fname.replace("'", "\\'")
            log_sql = f"""
            MERGE `{LOG_TABLE}` L
            USING (SELECT '{fid}' AS file_id, '{safe_fname}' AS file_name,
                          '{CHECK_QUA_HAN_FOLDER_NAME}' AS folder_name, '{mtime}' AS modified_time,
                          {row_count} AS row_count, CURRENT_TIMESTAMP() AS synced_at) N
            ON L.file_id = N.file_id
            WHEN MATCHED     THEN UPDATE SET L.file_name=N.file_name, L.modified_time=N.modified_time, L.row_count=N.row_count, L.synced_at=N.synced_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            bq_client.query(log_sql).result()
            processed_map[fid] = mtime
            synced += 1
            synced_types_this_cycle.add(file_type)
            print(f"✅ [{file_type}] {row_count:,} dòng")

        except Exception as e:
            print(f"❌ Lỗi [{fname}]: {e}")

    # Cơ chế batch debounce để không gửi tin nhắn nửa vời khi nhân sự đang kéo thả lần lượt 4 file
    if send_zalo and not dry_run:
        now_ts = time.time()
        should_send_now = False

        if synced_types_this_cycle:
            _CHECK_SLA_BATCH_STATE["synced_types"].update(synced_types_this_cycle)
            _CHECK_SLA_BATCH_STATE["last_sync_ts"] = now_ts
            _CHECK_SLA_BATCH_STATE["pending"] = True
            if len(_CHECK_SLA_BATCH_STATE["synced_types"]) >= 4 or not is_daemon:
                should_send_now = True
            else:
                print(
                    f"  ⏳ Đã nhận {len(_CHECK_SLA_BATCH_STATE['synced_types'])}/4 file SLA "
                    f"({', '.join(sorted(_CHECK_SLA_BATCH_STATE['synced_types']))}) — chờ chu kỳ kế tiếp để gửi gọn 1 tin Zalo.",
                    flush=True,
                )
        elif _CHECK_SLA_BATCH_STATE["pending"] and (now_ts - _CHECK_SLA_BATCH_STATE["last_sync_ts"] >= 55):
            should_send_now = True

        if should_send_now:
            try:
                import zalo_notifier
                z_res = zalo_notifier.scan_and_alert_warehouse_sla()
                if z_res.get("status") == "sent":
                    print(f"  📢 Zalo SLA Kho: {z_res.get('message')}", flush=True)
                elif z_res.get("status") == "error":
                    print(f"  ⚠️ Zalo SLA Kho: {z_res.get('message')}", flush=True)
            except Exception as ez:
                print(f"  ⚠️ Lỗi gửi cảnh báo Zalo SLA Kho: {ez}", flush=True)
            finally:
                _CHECK_SLA_BATCH_STATE["pending"] = False
                _CHECK_SLA_BATCH_STATE["synced_types"].clear()

    return synced, skipped


def run_sync_cycle(drive_svc, bq_client, is_full, dry_run, folder, is_daemon: bool = False):
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

    # Quét thư mục CHECK QUÁ HẠN (4 file thô JD WMS tự nhận diện + cảnh báo Zalo tự động)
    s_sla, sk_sla = sync_check_qua_han_folder(
        drive_svc, bq_client, processed_map,
        is_full_sync=is_full,
        dry_run=dry_run,
        only_folder=folder,
        send_zalo=True,
        is_daemon=is_daemon,
    )
    total_synced  += s_sla
    total_skipped += sk_sla

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
    parser.add_argument("--interval", type=int, default=60, help="Thời gian delay giữa các lần quét (giây), mặc định 60s")
    args = parser.parse_args()

    creds     = get_credentials()
    drive_svc = build("drive", "v3", credentials=creds)
    bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)

    if not args.daemon:
        run_sync_cycle(drive_svc, bq_client, args.full, args.dry_run, args.folder)
        return

    print(f"🚀 Chế độ Tự Động (Daemon Mode) đã kích hoạt: Quét Drive mỗi {args.interval} giây ({args.interval // 60} phút).")
    print("Nhấn Ctrl + C để dừng script bất kỳ lúc nào.\n")

    # Khởi chạy luồng lắng nghe lệnh Zalo Bot (/backlog) đồng thời
    try:
        import threading
        import zalo_bot_service
        bot_thread = threading.Thread(target=zalo_bot_service.start_bot_listener, daemon=True)
        bot_thread.start()
        print("🤖 [Zalo Bot] Luồng lắng nghe lệnh /backlog đã được kích hoạt chạy ngầm.\n")
    except Exception as e_bot:
        print(f"⚠️ [Zalo Bot] Không thể bật luồng lắng nghe: {e_bot}\n")

    # Nếu chạy trên Cloud Web Service (Render/Railway), tự động mở cổng HTTP Health Check
    port_env = os.environ.get("PORT")
    if port_env:
        try:
            from http.server import HTTPServer, BaseHTTPRequestHandler
            class HealthHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(b"OK - JDL Sync & Zalo Bot Service Active 24/7\n")
                def log_message(self, format, *args):
                    return
            def run_health_server():
                p = int(port_env)
                httpd = HTTPServer(('0.0.0.0', p), HealthHandler)
                print(f"🌐 [Cloud Health Server] Đang mở HTTP port {p} cho Cloud Health Check...\n", flush=True)
                httpd.serve_forever()
            t_health = threading.Thread(target=run_health_server, daemon=True)
            t_health.start()
        except Exception as e_h:
            print(f"⚠️ [Cloud Health Server] Không thể khởi động: {e_h}\n")

    while True:
        try:
            run_sync_cycle(drive_svc, bq_client, args.full, args.dry_run, args.folder, is_daemon=True)
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
