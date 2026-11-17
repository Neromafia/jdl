import os
import math
import datetime
from datetime import timedelta, timezone
import threading
import time
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_option_menu import option_menu
from google.cloud import bigquery
from google.oauth2 import service_account

# ==========================================
# CẤU HÌNH TRANG STREAMLIT (Lệnh Streamlit đầu tiên)
# ==========================================
st.set_page_config(
    page_title="JingDong Logistics Dashboard",
    page_icon="favicon.png" if os.path.exists("favicon.png") else ("favicon.ico" if os.path.exists("favicon.ico") else None),
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==========================================
# HẰNG SỐ DỰ ÁN & QUY TẮC PHÂN LOẠI
# ==========================================
import base64

def get_logo_base64() -> str:
    for path in ["favicon.png", "favicon.ico"]:
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    return base64.b64encode(f.read()).decode("utf-8")
            except Exception:
                pass
    return ""

PROJECT_ID = "jingdong-505914"
DATASET_ID = "wms_data"
CREDENTIALS_FILE = "service_account.json"
VN_TIMEZONE = timezone(timedelta(hours=7))
VN_TZ = VN_TIMEZONE

try:
    if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
        PROJECT_ID = st.secrets["gcp_service_account"].get("project_id", PROJECT_ID)
except Exception:
    pass

# ==========================================
# XÁC THỰC GCP VÀ KHỞI TẠO BIGQUERY CLIENT
# ==========================================
def get_gcp_credentials(scopes=None):
    """
    Lấy thông tin xác thực Google Cloud:
    1. Ưu tiên đọc từ Streamlit Secrets (st.secrets) theo định dạng TOML khi deploy lên Streamlit Cloud.
    2. Fallback đọc file service_account.json tại máy local nếu có.
    """
    try:
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
                proj_id = sec.get("project_id", PROJECT_ID)
                if scopes:
                    return service_account.Credentials.from_service_account_info(sec, scopes=scopes), proj_id, None
                return service_account.Credentials.from_service_account_info(sec), proj_id, None
    except Exception:
        pass

    if os.path.exists(CREDENTIALS_FILE):
        try:
            if scopes:
                return service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes), PROJECT_ID, None
            return service_account.Credentials.from_service_account_file(CREDENTIALS_FILE), PROJECT_ID, None
        except Exception as e_file:
            return None, PROJECT_ID, f"Lỗi đọc file {CREDENTIALS_FILE}: {str(e_file)}"

    return None, PROJECT_ID, "Không tìm thấy cấu hình xác thực trong st.secrets (TOML) hoặc file service_account.json."


@st.cache_resource
def get_bigquery_client():
    """Khởi tạo BigQuery Client sử dụng st.secrets (TOML) hoặc service account file."""
    credentials, proj_id, error = get_gcp_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    if error or credentials is None:
        return None, error

    try:
        client = bigquery.Client(credentials=credentials, project=proj_id or PROJECT_ID)
        return client, None
    except Exception as e:
        return None, f"Lỗi khởi tạo BigQuery Client: {str(e)}"


# Import module đồng bộ từ Google Drive lên BigQuery
try:
    from sync_wms_local import (
        get_credentials as get_sync_credentials,
        run_sync_cycle,
        build as build_service,
        bigquery as bq_module,
        PROJECT_ID as SYNC_PROJECT_ID
    )
except Exception as _e_sync:
    get_sync_credentials = None
    run_sync_cycle = None
    build_service = None
    bq_module = None
    SYNC_PROJECT_ID = PROJECT_ID

# ==========================================
# QUẢN LÝ TIẾN TRÌNH ĐỒNG BỘ CHẠY NGẦM (DAEMON THREAD & CACHE RESOURCE)
# ==========================================
@st.cache_resource
def get_global_sync_state():
    """
    Singleton dictionary lưu trạng thái đồng bộ toàn cục.
    Tồn tại xuyên suốt vòng đời tiến trình, không bị reset khi người dùng thao tác UI hoặc rerun.
    Khởi tạo ngay mốc thời gian đồng bộ gần nhất từ bảng `sync_file_logs` trên BigQuery.
    """
    initial_last_sync = None
    try:
        client, _ = get_bigquery_client()
        if client:
            sql = f"SELECT MAX(synced_at) as last_synced FROM `{PROJECT_ID}.{DATASET_ID}.sync_file_logs`"
            df_log = client.query(sql).to_dataframe()
            if not df_log.empty and pd.notnull(df_log.iloc[0]['last_synced']):
                t_utc = df_log.iloc[0]['last_synced']
                t_vn = t_utc.tz_convert(VN_TZ) if hasattr(t_utc, "tz_convert") else t_utc + timedelta(hours=7)
                initial_last_sync = t_vn.strftime("%H:%M:%S %d/%m/%Y")
    except Exception:
        pass

    return {
        "thread_started": False,
        "is_running": False,
        "last_sync": initial_last_sync,
        "synced_count": 0,
        "skipped_count": 0,
        "data_version": time.time(),
        "error": None
    }


def get_latest_sync_timestamp():
    """Lấy mốc thời gian đồng bộ gần nhất, tự động truy vấn BigQuery nếu state chưa có."""
    sync_state = get_global_sync_state()
    if sync_state.get("last_sync"):
        return sync_state["last_sync"]
    try:
        client, _ = get_bigquery_client()
        if client:
            sql = f"SELECT MAX(synced_at) as last_synced FROM `{PROJECT_ID}.{DATASET_ID}.sync_file_logs`"
            df_log = client.query(sql).to_dataframe()
            if not df_log.empty and pd.notnull(df_log.iloc[0]['last_synced']):
                t_utc = df_log.iloc[0]['last_synced']
                t_vn = t_utc.tz_convert(VN_TZ) if hasattr(t_utc, "tz_convert") else t_utc + timedelta(hours=7)
                ts = t_vn.strftime("%H:%M:%S %d/%m/%Y")
                sync_state["last_sync"] = ts
                return ts
    except Exception:
        pass
    return None

SYNC_LOCK = threading.Lock()
SYNC_TRIGGER_EVENT = threading.Event()

def do_sync_cycle(is_full: bool = False) -> dict:
    """Thực hiện một chu kỳ quét đồng bộ từ Google Drive lên BigQuery."""
    try:
        import importlib
        import sync_wms_local
        importlib.reload(sync_wms_local)
        _get_creds = sync_wms_local.get_credentials
        _run_cycle = sync_wms_local.run_sync_cycle
        _build = sync_wms_local.build
        _bq_mod = sync_wms_local.bigquery
        _sync_proj = sync_wms_local.PROJECT_ID
    except Exception:
        _get_creds = get_sync_credentials
        _run_cycle = run_sync_cycle
        _build = build_service
        _bq_mod = bq_module
        _sync_proj = SYNC_PROJECT_ID

    creds = _get_creds()
    drive_svc = _build("drive", "v3", credentials=creds, cache_discovery=False)
    sync_proj = getattr(creds, "project_id", None) or _sync_proj
    if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
        sync_proj = st.secrets["gcp_service_account"].get("project_id", sync_proj)
    bq_client = _bq_mod.Client(credentials=creds, project=sync_proj)

    return _run_cycle(drive_svc, bq_client, is_full=is_full, dry_run=False, folder=None)

def trigger_manual_sync():
    """Kích hoạt quét đồng bộ Google Drive ngay lập tức khi người dùng bấm nút Làm mới."""
    sync_state = get_global_sync_state()
    if sync_state.get("is_running"):
        return False, "Hệ thống đang trong tiến trình đồng bộ dữ liệu, vui lòng đợi trong giây lát!"

    acquired = SYNC_LOCK.acquire(blocking=False)
    if not acquired:
        return False, "Hệ thống đang quét dữ liệu từ Google Drive, vui lòng đợi trong giây lát!"

    try:
        sync_state["is_running"] = True
        res = do_sync_cycle(is_full=False)
        now_vn = datetime.datetime.now(timezone(timedelta(hours=7)))
        sync_state["last_sync"] = now_vn.strftime("%H:%M:%S %d/%m/%Y")
        st.cache_data.clear()
        sync_state["data_version"] = time.time()
        sync_state["error"] = None
        if isinstance(res, dict):
            sync_state["synced_count"] = res.get("synced", 0)
            sync_state["skipped_count"] = res.get("skipped", 0)
            return True, res
        return True, {"synced": 0, "skipped": 0}
    except Exception as err:
        sync_state["error"] = str(err)
        return False, str(err)
    finally:
        sync_state["is_running"] = False
        SYNC_LOCK.release()

def background_sync_worker():
    """Luồng ngầm định kỳ quét và nạp file mới từ Google Drive lên BigQuery mỗi 300 giây (5 phút)."""
    sync_state = get_global_sync_state()
    if not get_sync_credentials or not run_sync_cycle:
        sync_state["error"] = "Không tìm thấy module sync_wms_local"
        return

    # Nghỉ 10 giây ban đầu để ứng dụng hoàn tất việc nạp dữ liệu và hiển thị trang đầu tiên
    time.sleep(10)

    while True:
        try:
            with SYNC_LOCK:
                sync_state["is_running"] = True
                res = do_sync_cycle(is_full=False)
                now_vn = datetime.datetime.now(timezone(timedelta(hours=7)))
                sync_state["last_sync"] = now_vn.strftime("%H:%M:%S %d/%m/%Y")
                if isinstance(res, dict):
                    sync_state["synced_count"] = res.get("synced", 0)
                    sync_state["skipped_count"] = res.get("skipped", 0)
                    if res.get("synced", 0) > 0:
                        try:
                            st.cache_data.clear()
                        except Exception:
                            pass
                        sync_state["data_version"] = time.time()
                sync_state["error"] = None
        except Exception as err:
            sync_state["error"] = str(err)
        finally:
            sync_state["is_running"] = False

        # Nghỉ đúng 300 giây (5 phút) trước lần quét kế tiếp, có thể đánh thức bằng event
        SYNC_TRIGGER_EVENT.wait(timeout=300)
        SYNC_TRIGGER_EVENT.clear()

def ensure_background_sync_started():
    """Khởi tạo background thread duy nhất cho toàn bộ app."""
    sync_state = get_global_sync_state()
    with SYNC_LOCK:
        if not sync_state["thread_started"]:
            sync_thread = threading.Thread(
                target=background_sync_worker,
                daemon=True,
                name="WMS_Background_Sync_Thread"
            )
            sync_thread.start()
            sync_state["thread_started"] = True

# Tự động kích hoạt luồng đồng bộ ngầm khi ứng dụng khởi chạy
ensure_background_sync_started()

# Quy tắc chuẩn hóa khách hàng:
# B2B: Gộp CHINT, KKV (bao gồm JINHE), ENVISION
B2B_GROUPS = ["CHINT", "KKV", "ENVISION"]

# ==========================================
# DICTIONARY ĐA NGÔN NGỮ (i18n): VN & ZH
# Toàn bộ text chuẩn mực, không icon, không tiếng Anh
# ==========================================
TRANSLATIONS = {
    "VN": {
        "app_title": "Bảng Điều hành Vận hành Kho Vận - JingDong Logistics",
        "brand_title": "JingDong Logistics",
        "lbl_select_warehouse": "Kho vận hành:",
        "stages_header": "CÁC KHÂU VẬN HÀNH",
        "date_range_header": "KHOẢNG THỜI GIAN ĐỐI SOÁT",
        "time_from_header": "TỪ GIỜ",
        "time_to_header": "ĐẾN GIỜ",
        "time_range_header": "KHUNG GIỜ",
        "lbl_from_header": "TỪ (BẮT ĐẦU)",
        "lbl_to_header": "ĐẾN (KẾT THÚC)",
        "lbl_quick_presets": "CHỌN NHANH KHOẢNG THỜI GIAN (PRESETS):",
        "date_range_label": "Chọn khoảng thời gian (Từ ngày - Đến ngày):",
        "btn_today": "Hôm nay",
        "btn_30d": "30 ngày gần đây",
        "btn_refresh": "Làm mới dữ liệu",
        "toast_refresh": "Đã làm mới dữ liệu từ cơ sở dữ liệu!",
        "lang_header": "NGÔN NGỮ",
        "filter_cust_type": "Loại khách hàng:",
        "filter_cust_name": "Tên khách hàng:",
        "all_option": "Tất cả",
        "status_connected": "Đã kết nối dữ liệu",
        "status_sync": "Đồng bộ: 2 giờ / lần",
        "stage_prefix": "Khâu",
        "period_prefix": "Khoảng đối soát",
        "tab_day": "Theo Ngày (Thời gian thực & Khung giờ)",
        "tab_week": "Theo Tuần (So sánh tuần trước)",
        "tab_month": "Theo Tháng (So sánh tháng trước)",
        # Section titles (không icon)
        "sec_hourly": "PHÂN TÍCH SẢN LƯỢNG CHI TIẾT THEO NGÀY & TỪNG KHUNG GIỜ",
        "sec_weekly": "PHÂN TÍCH TĂNG TRƯỞNG TUẦN QUA TUẦN",
        "sec_monthly": "PHÂN TÍCH TĂNG TRƯỞNG THÁNG QUA THÁNG",
        "sec_pipeline": "TỔNG HỢP THÔNG LƯỢNG VẬN HÀNH TOÀN KHO",
        "sec_backlog": "BÁO CÁO DỰ KIẾN VẬN HÀNH & HÀNG TỒN ĐỌNG CHỜ NHẬP",
        # Metric labels
        "lbl_total_qty": "TỔNG SẢN LƯỢNG (SẢN PHẨM)",
        "lbl_total_orders": "TỔNG SỐ ĐƠN HÀNG",
        "lbl_customers_count": "SỐ LƯỢNG KHÁCH HÀNG",
        "lbl_avg_order": "BÌNH QUÂN (SẢN PHẨM / ĐƠN)",
        "lbl_selected_period": "Trong khoảng đã chọn",
        "lbl_orders_recorded": "Đơn hàng đã ghi nhận",
        "lbl_active_partners": "Đối tác phát sinh sản lượng",
        "lbl_avg_size": "Quy mô đơn hàng trung bình",
        # Hourly
        "lbl_choose_date_hourly": "Chọn ngày xem chi tiết từng giờ:",
        "lbl_display_metric": "Hiển thị dữ liệu theo:",
        "opt_qty": "Sản lượng (Sản phẩm)",
        "opt_orders": "Số đơn hàng",
        "lbl_dynamic_table": "Bảng phân bổ sản lượng theo giờ",
        "lbl_total_column": "TỔNG CỘNG",
        "lbl_customer_col": "Tên khách hàng",
        "lbl_chart_hourly": "Phân bổ sản lượng theo từng khung giờ trong ngày",
        "lbl_hour_axis": "Khung giờ",
        "btn_download_hourly": "Tải bảng giờ (.csv)",
        # Weekly
        "lbl_week_this": "SẢN LƯỢNG TUẦN NÀY",
        "lbl_week_prev": "SẢN LƯỢNG TUẦN TRƯỚC",
        "lbl_week_growth": "TĂNG TRƯỞNG TUẦN",
        "lbl_week_orders": "TỔNG ĐƠN HÀNG TUẦN NÀY",
        "lbl_week_diff": "Chênh lệch",
        "lbl_week_table": "Bảng tổng hợp lịch sử các tuần",
        "col_week_start": "Bắt đầu tuần",
        "col_week_name": "Tên tuần",
        "col_week_orders": "Tổng đơn hàng",
        "col_week_qty": "Sản lượng (Sản phẩm)",
        "col_week_growth": "Tỷ lệ tăng trưởng tuần",
        "col_week_diff": "Chênh lệch (Sản phẩm)",
        # Monthly
        "lbl_month_this": "SẢN LƯỢNG THÁNG GẦN NHẤT",
        "lbl_month_prev": "SẢN LƯỢNG THÁNG TRƯỚC",
        "lbl_month_growth": "TĂNG TRƯỞNG THÁNG",
        "lbl_month_orders": "TỔNG ĐƠN HÀNG THÁNG NÀY",
        "lbl_month_diff": "Chênh lệch",
        "lbl_month_table": "Bảng tổng hợp lịch sử các tháng",
        "col_month": "Tháng",
        "col_month_orders": "Tổng đơn hàng",
        "col_month_qty": "Sản lượng (Sản phẩm)",
        "col_month_growth": "Tỷ lệ tăng trưởng tháng",
        "col_month_diff": "Chênh lệch (Sản phẩm)",
        # Headcount & Capacity Planning (Không icon)
        "sec_planning": "KẾ HOẠCH ĐỊNH BIÊN NHÂN SỰ & NĂNG LỰC VẬN HÀNH TOÀN KHO",
        "plan_table_title": "KẾ HOẠCH ĐỊNH BIÊN NHÂN SỰ THEO TUẦN (ISO)",
        "plan_select_week": "Chọn tuần vận hành (Tuần ISO):",
        "plan_fixed_header": "Điều chỉnh Định biên Nhân sự các Khâu Cố định (Thứ 2 - Thứ 7)",
        "plan_fixed_note": "Lưu ý: Chủ Nhật mặc định chỉ có Xe nâng (2 người), Bàn giao (2 người) và 2C làm việc; các khâu khác nghỉ.",
        "plan_col_stage": "Khâu vận hành",
        "task_receiving": "Nhập hàng (Receiving)",
        "task_returns": "Hoàn trả (Returns)",
        "task_inventory": "Trong kho (Inventory / Putaway / Cycle count)",
        "task_forklift": "Xe nâng (Forklift)",
        "task_handover": "Bàn giao (Handover)",
        "task_pick_2c": "Nhặt hàng 2C (2C Picking)",
        "task_pack_2c": "Đóng gói 2C (2C Packing)",
        "task_chint_envision": "B2B - CHINT & ENVISION",
        "task_kkv": "B2B - KKV",
        "task_total_hc": "TỔNG CỘNG NHÂN SỰ TOÀN KHO",
        "sec_volume_comparison": "ĐỐI SOÁT SẢN LƯỢNG KẾ HOẠCH VÀ THỰC TẾ",
        "lbl_vol_b2c": "Sản lượng 2C",
        "lbl_vol_b2b": "Sản lượng 2B",
        "lbl_fc_orders": "Đơn dự kiến",
        "lbl_act_orders": "Đơn thực tế",
        "lbl_pct_orders": "Tỉ lệ đạt đơn (%)",
        "lbl_fc_pcs": "PCS dự kiến",
        "lbl_act_pcs": "PCS thực tế",
        "lbl_pct_pcs": "Tỉ lệ đạt PCS (%)",
        "lbl_metric_col": "Khách hàng / Chỉ số",
        "lbl_client_col": "Khách hàng",
        "lbl_total_week": "Tổng tuần",
        "lbl_total_b2c": "TỔNG CỘNG 2C",
        "lbl_total_b2b": "TỔNG CỘNG 2B",
        "lbl_filter_client": "Lọc khách hàng:",
        "plan_unit_people": "Người",
        "plan_unit_orders": "Đơn",
        "plan_unit_pcs": "PCS",
        "lbl_download_csv": "Tải bảng dự kiến nhân sự (.csv)",
        "lbl_download_vol_csv": "Tải bảng đối soát sản lượng (.csv)",
        # Backlog
        "lbl_bl_qty": "TỔNG SẢN PHẨM CHỜ NHẬP",
        "lbl_bl_orders": "SỐ ĐƠN CHỜ TIẾP NHẬN",
        "lbl_bl_partners": "SỐ KHÁCH HÀNG TỒN ĐỌNG",
        "lbl_bl_event": "SỰ KIỆN MUA SẮM LỚN",
        "lbl_bl_chart": "Top 10 Khách hàng có sản lượng chờ xử lý lớn nhất",
        "lbl_bl_event_title": "Lịch Sự kiện Ngày hội Mua sắm (Mega Sales)",
        "col_event_date": "Ngày diễn ra",
        "col_event_name": "Tên sự kiện",
        # Pipeline
        "col_stage_name": "Khâu vận hành",
        "lbl_throughput_chart": "Thông lượng sản lượng giữa các khâu",
        "summary_rec_title": "NHẬP HÀNG (MUA MỚI)",
        "summary_rec_sub": "Đơn & PCS nhập kho NCC",
        "summary_ret_title": "HOÀN TRẢ (VỀ KHO)",
        "summary_ret_sub": "Đơn & PCS hoàn trả về kho",
        "summary_inv_title": "TRONG KHO (KIỂM KÊ)",
        "summary_inv_sub": "Chênh lệch kiểm kê tồn kho",
        "summary_pick_title": "NHẶT HÀNG (PICKING)",
        "summary_pick_sub": "Đơn & PCS nhặt hàng xuất kho",
        "summary_pack_title": "ĐÓNG GÓI (PACKING)",
        "summary_pack_sub": "Đơn & PCS đóng gói xuất kho",
        "summary_hnd_title": "BÀN GIAO VẬN CHUYỂN",
        "summary_hnd_sub": "Tổng số đơn đã bàn giao ĐVVC",
        "lbl_summary_stage_table": "BẢNG THỐNG KÊ TRỌNG TÂM TỪNG KHÂU VẬN HÀNH",
        "lbl_customer_summary_table": "BẢNG ĐỐI SOÁT SẢN LƯỢNG CÁC KHÂU THEO TỪNG KHÁCH HÀNG",
        "lbl_summary_stage_col": "Khâu vận hành",
        "lbl_summary_flow_col": "Phân loại luồng",
        "lbl_summary_orders_col": "Tổng số đơn",
        "lbl_summary_qty_col": "Tổng sản lượng (PCS)",
        "lbl_summary_key_metric": "Chỉ số trọng tâm",
        "lbl_summary_notes": "Diễn giải vận hành",
        "lbl_customer_col": "Khách hàng",
        "lbl_cust_type_col": "Phân loại",
        "lbl_cust_rec_col": "Nhập hàng (Đơn / PCS)",
        "lbl_cust_ret_col": "Hoàn trả (Đơn / PCS)",
        "lbl_cust_pick_col": "Nhặt hàng (Đơn / PCS)",
        "lbl_cust_pack_col": "Đóng gói (Đơn / PCS)",
        "lbl_cust_hnd_col": "Bàn giao (Số đơn)",
        "btn_download_pipeline_csv": "Tải dữ liệu đối soát các khâu (.csv)",
        "no_data": "Không có dữ liệu trong khoảng thời gian đã chọn.",
        "msg_no_hourly_data": "Chưa có phát sinh dữ liệu trong ngày này.",
        # Tỉ trọng & Nhân viên
        "pie_share_orders": "Tỉ trọng Số đơn theo Khách hàng",
        "pie_share_qty": "Tỉ trọng Sản lượng (PCS) theo Khách hàng",
        "sec_hourly_staff": "BẢNG SẢN LƯỢNG THEO TỪNG KHUNG GIỜ THEO NHÂN VIÊN",
        "col_worker": "Nhân viên",
        "lbl_total_row": "TỔNG CỘNG",
        "sec_cust_share_tracking": "THEO DÕI TỈ TRỌNG & TĂNG GIẢM THEO KHÁCH HÀNG",
        "col_share_current": "Tỉ trọng kỳ này (%)",
        "col_share_prev": "Tỉ trọng kỳ trước (%)",
        "col_share_delta": "Tăng/giảm tỉ trọng (Điểm %)",
        "col_growth_rate": "Tăng trưởng (%)",
        "col_period_current": "Kỳ này",
        "col_period_prev": "Kỳ trước",
        "col_period_delta": "Chênh lệch (+/-)",
        "lbl_chart_share_compare": "So sánh tỉ trọng hai kỳ liên tiếp theo khách hàng",
        "btn_download_staff_hourly": "Tải bảng giờ theo nhân viên (.csv)",
        "btn_download_share_csv": "Tải bảng tỉ trọng khách hàng (.csv)",
        "sync_auto_badge": "Tự động đồng bộ mỗi 5 phút",
        "sync_last_time": "Cập nhật gần nhất:",
        "sync_running": "Đang đồng bộ từ Drive...",
        "sync_init": "Đang khởi động đồng bộ ngầm...",
        "sync_new_files": "file mới",
        "sync_error": "Lỗi đồng bộ:",
        # Inventory tab translations
        "inv_title": "BÁO CÁO KIỂM KÊ & ĐỐI SOÁT TỒN KHO",
        "inv_metric_tasks": "NHIỆM VỤ KIỂM KÊ",
        "inv_metric_skus": "SỐ MÃ HÀNG (SKU)",
        "inv_metric_system_qty": "TỔNG SL HỆ THỐNG",
        "inv_metric_actual_qty": "TỔNG SL THỰC TẾ",
        "inv_metric_diff_qty": "CHÊNH LỆCH PCS",
        "inv_metric_accuracy": "ĐỘ CHÍNH XÁC KIỂM KÊ",
        "inv_sub_tasks": "Số nhiệm vụ đã thực hiện",
        "inv_sub_skus": "Số mã SKU phát sinh kiểm kê",
        "inv_sub_sys": "Tồn kho sổ sách WMS",
        "inv_sub_act": "Sản lượng đếm thực tế",
        "inv_sub_diff": "Chênh lệch giữa System & Actual",
        "inv_tab_daily": "Theo Ngày (Tiến độ & Chênh lệch)",
        "inv_tab_customer": "Theo Khách hàng (Đối soát chi tiết)",
        "inv_tab_tasks": "Nhật ký nhiệm vụ kiểm kê",
        "inv_chart_daily_title": "Biểu đồ tiến độ kiểm kê và chênh lệch PCS theo ngày",
        "inv_chart_customer_share": "Tỉ trọng sản lượng kiểm kê theo khách hàng",
        "inv_chart_customer_diff": "Khách hàng có chênh lệch PCS kiểm kê lớn nhất",
        "inv_col_date": "Ngày",
        "inv_col_dow": "Thứ",
        "inv_col_task_count": "Số nhiệm vụ",
        "inv_col_sku_count": "Số SKU",
        "inv_col_sys_qty": "SL Hệ thống (PCS)",
        "inv_col_act_qty": "SL Thực tế (PCS)",
        "inv_col_diff_qty": "Chênh lệch (PCS)",
        "inv_col_accuracy": "% Khớp",
        "inv_col_status": "Trạng thái",
        "inv_col_cust_name": "Tên khách hàng",
        "inv_col_cust_type": "Phân loại",
        "inv_col_share_pct": "% Tỉ trọng",
        "inv_col_task_no": "Mã nhiệm vụ kiểm kê",
        "inv_col_prod_name": "Tên sản phẩm",
        "inv_col_location": "Vị trí / Kệ",
        "inv_col_worker": "Nhân viên kiểm",
        "inv_status_perfect": "Khớp 100%",
        "inv_status_diff": "Lệch tồn",
        "inv_btn_download_daily": "Tải bảng kiểm kê theo ngày (.csv)",
        "inv_btn_download_cust": "Tải bảng đối soát khách hàng (.csv)",
        "inv_btn_download_tasks": "Tải nhật ký nhiệm vụ (.csv)",
        "developed_by": "Phát triển bởi bộ phận QC",
        # Packing Backlog integration
        "card_backlog_label": "TỒN ĐỌNG (BACKLOG)",
        "unit_order": "Đơn",
        "unit_pcs_backlog": "PCS tồn đọng",
        "sub_done_backlog_qty": "Đã đóng / Tổng PCS cần xử lý",
        "sub_done_backlog_orders": "Đã đóng / Tổng đơn cần xử lý",
        "sec_backlog_table": "DANH SÁCH TỒN ĐỌNG BACKLOG THEO KHÁCH HÀNG",
        "col_customer_name": "Tên khách hàng",
        "col_customer_type": "Phân loại",
        "col_backlog_orders": "Số đơn tồn đọng",
        "col_backlog_pcs": "Sản lượng tồn đọng (PCS)",
        "col_backlog_share_orders": "% Tỉ trọng đơn",
        "col_backlog_share_pcs": "% Tỉ trọng PCS",
        "btn_download_backlog": "Tải dữ liệu tồn đọng Backlog (.csv)",
        "msg_no_backlog": "Hiện không có đơn hàng tồn đọng backlog.",
        # Packing Customer Share Table
        "sec_packing_share_table": "BẢNG TỔNG HỢP TỈ TRỌNG SỐ ĐƠN & SẢN LƯỢNG (PCS) THEO KHÁCH HÀNG",
        "col_packing_orders": "Số đơn đóng gói",
        "col_packing_qty": "Sản lượng đóng gói (PCS)",
        "btn_download_packing_share": "Tải bảng tỉ trọng đóng gói (.csv)"
    }
}
TRANSLATIONS["ZH"] = TRANSLATIONS["VN"]

# Bảng tra cứu tên thứ trong tuần
DOW_NAMES = {
    "VN": {
        1: "Chủ Nhật",
        2: "Thứ Hai",
        3: "Thứ Ba",
        4: "Thứ Tư",
        5: "Thứ Năm",
        6: "Thứ Sáu",
        7: "Thứ Bảy"
    }
}
DOW_NAMES["ZH"] = DOW_NAMES["VN"]

# Cấu hình 9 khâu vận hành (không icon)
STAGE_CONFIGS = {
    "TỔNG HỢP": {
        "name_vn": "TỔNG HỢP",
        "name_zh": "",
        "title_vn": "TỔNG HỢP VẬN HÀNH TOÀN KHO",
        "title_zh": "",
        "desc_vn": "Bức tranh toàn cảnh luồng hàng hóa qua tất cả các mắt xích kho vận",
        "desc_zh": "",
        "type": "summary"
    },
    "NHẬP HÀNG": {
        "name_vn": "NHẬP HÀNG",
        "name_zh": "",
        "title_vn": "BÁO CÁO NHẬP HÀNG (MUA MỚI)",
        "title_zh": "",
        "desc_vn": "Theo dõi tiến độ nhận hàng nhà cung cấp và nhập kho lưu trữ",
        "desc_zh": "",
        "table": "fact_receiving",
        "where": "cargo_type = 'Purchase Inbound' AND receiving_time IS NOT NULL",
        "time_col": "receiving_time",
        "order_col": "order_no",
        "qty_col": "receiving_qty",
        "qty_label_vn": "Sản lượng nhập (Sản phẩm)",
        "qty_label_zh": "",
        "order_label_vn": "Số đơn nhập",
        "order_label_zh": ""
    },
    "HOÀN TRẢ": {
        "name_vn": "HOÀN TRẢ",
        "name_zh": "",
        "title_vn": "BÁO CÁO HOÀN TRẢ NHẬP KHO",
        "title_zh": "",
        "desc_vn": "Theo dõi tiếp nhận và kiểm đếm các đơn hàng hoàn/trả về kho",
        "desc_zh": "",
        "table": "fact_receiving",
        "where": "cargo_type = 'Return Inbound' AND receiving_time IS NOT NULL",
        "time_col": "receiving_time",
        "order_col": "order_no",
        "qty_col": "receiving_qty",
        "qty_label_vn": "Sản lượng hoàn (Sản phẩm)",
        "qty_label_zh": "",
        "order_label_vn": "Số đơn hoàn",
        "order_label_zh": ""
    },
    "TRONG KHO": {
        "name_vn": "TRONG KHO",
        "name_zh": "",
        "title_vn": "BÁO CÁO KIỂM KÊ & ĐỐI SOÁT TỒN KHO",
        "title_zh": "",
        "desc_vn": "Theo dõi số lượng nhiệm vụ kiểm kê, số lượng SKU, sản lượng thực tế và đối soát chênh lệch tồn kho",
        "desc_zh": "",
        "type": "inventory",
        "table": "fact_inventory",
        "where": "inventory_time IS NOT NULL",
        "time_col": "inventory_time",
        "order_col": "inventory_check_no",
        "qty_col": "actual_qty",
        "qty_label_vn": "Sản lượng đếm thực tế (Sản phẩm)",
        "qty_label_zh": "",
        "order_label_vn": "Số nhiệm vụ kiểm kê",
        "order_label_zh": ""
    },
    "LÊN KỆ": {
        "name_vn": "LÊN KỆ",
        "name_zh": "",
        "title_vn": "BÁO CÁO LÊN KỆ",
        "title_zh": "",
        "desc_vn": "Theo dõi tiến độ cất hàng lên kệ từ khu vực đệm tiếp nhận",
        "desc_zh": "",
        "table": "fact_putaway",
        "where": "putaway_time IS NOT NULL",
        "time_col": "putaway_time",
        "order_col": "order_no",
        "qty_col": "putaway_qty",
        "qty_label_vn": "Sản lượng lên kệ (Sản phẩm)",
        "qty_label_zh": "",
        "order_label_vn": "Số lệnh lên kệ",
        "order_label_zh": ""
    },
    "NHẶT HÀNG": {
        "name_vn": "NHẶT HÀNG",
        "name_zh": "",
        "title_vn": "BÁO CÁO NHẶT HÀNG",
        "title_zh": "",
        "desc_vn": "Theo dõi hiệu suất lấy hàng từ kệ chuẩn bị cho khâu đóng gói",
        "desc_zh": "",
        "table": "fact_picking",
        "where": "COALESCE(end_time, start_time) IS NOT NULL",
        "time_col": "COALESCE(end_time, start_time)",
        "order_col": "order_no",
        "qty_col": "COALESCE(actual_qty, expected_qty)",
        "qty_label_vn": "Sản lượng nhặt (Sản phẩm)",
        "qty_label_zh": "",
        "order_label_vn": "Số đơn nhặt",
        "order_label_zh": ""
    },
    "ĐÓNG GÓI": {
        "name_vn": "ĐÓNG GÓI",
        "name_zh": "",
        "title_vn": "BÁO CÁO ĐÓNG GÓI XUẤT KHO",
        "title_zh": "",
        "desc_vn": "Giám sát sản lượng đóng gói đơn hàng xuất kho theo thời gian thực",
        "desc_zh": "",
        "table": "fact_packing",
        "where": "pack_time IS NOT NULL",
        "time_col": "pack_time",
        "order_col": "order_no",
        "qty_col": "packing_qty",
        "qty_label_vn": "Sản lượng đóng gói (Sản phẩm)",
        "qty_label_zh": "",
        "order_label_vn": "Số đơn đóng gói",
        "order_label_zh": ""
    },
    "BÀN GIAO": {
        "name_vn": "BÀN GIAO",
        "name_zh": "",
        "title_vn": "BÁO CÁO BÀN GIAO VẬN CHUYỂN",
        "title_zh": "",
        "desc_vn": "Theo dõi bàn giao kiện hàng cho các đơn vị vận chuyển",
        "desc_zh": "",
        "type": "handover",
        "table": "fact_handover",
        "where": "handover_time IS NOT NULL AND (UPPER(COALESCE(carrier_name, '')) LIKE '%J&T%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%SHOPEE%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%SPX%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%GHN%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%VIETTEL%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%VTP%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%BEST%')",
        "time_col": "handover_time",
        "order_col": "order_no",
        "qty_col": "1",
        "qty_label_vn": "Số kiện bàn giao (Kiện)",
        "qty_label_zh": "",
        "order_label_vn": "Số đơn bàn giao",
        "order_label_zh": ""
    },
    "DỰ KIẾN": {
        "name_vn": "DỰ KIẾN",
        "name_zh": "",
        "title_vn": "KẾ HOẠCH ĐỊNH BIÊN NHÂN SỰ & NĂNG LỰC VẬN HÀNH",
        "title_zh": "",
        "desc_vn": "Mô phỏng nhu cầu nhân sự dựa trên sản lượng dự kiến và năng suất thực tế",
        "desc_zh": "",
        "type": "planning"
    }
}

STAGE_KEYS = list(STAGE_CONFIGS.keys())

# ==========================================
# CUSTOM CSS: ANIMATION MƯỢT MÀ & DARK THEME CHUẨN MỰC
# ==========================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Hiệu ứng chuyển động nhẹ nhàng (Smooth Fade-in Animation) */
    @keyframes smoothFadeIn {
        0% {
            opacity: 0;
            transform: translateY(5px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }

    /* Áp dụng animation cho nội dung các Tab và Panels */
    div[data-testid="stTabs"] > div[role="tabpanel"] {
        animation: smoothFadeIn 0.32s cubic-bezier(0.16, 1, 0.3, 1) forwards;
    }

    /* WMS Unified DateTime Range Picker Styling */
    div[data-testid="stPopover"] {
        width: 100% !important;
    }
    div[data-testid="stPopover"] > button {
        background-color: #1e293b !important;
        border: 1px solid #475569 !important;
        border-radius: 6px !important;
        color: #f8fafc !important;
        font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, Courier, monospace, 'Inter', sans-serif !important;
        font-size: 13.5px !important;
        font-weight: 600 !important;
        padding: 8px 14px !important;
        min-height: 42px !important;
        width: 100% !important;
        display: flex !important;
        justify-content: space-between !important;
        align-items: center !important;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.25) !important;
        transition: all 0.2s ease !important;
    }
    div[data-testid="stPopover"] > button:hover {
        border-color: #38bdf8 !important;
        background-color: #24344d !important;
        box-shadow: 0 0 0 1px #38bdf8 !important;
        color: #ffffff !important;
    }
    div[data-testid="stPopoverBody"] {
        background-color: #0f172a !important;
        border: 1px solid #334155 !important;
        border-radius: 10px !important;
        padding: 16px !important;
        box-shadow: 0 16px 36px -4px rgba(0, 0, 0, 0.6) !important;
        min-width: 520px !important;
        max-width: 600px !important;
    }
    div[data-testid="stPopoverBody"] button {
        font-size: 12px !important;
        padding: 4px 8px !important;
        min-height: 32px !important;
    }

    .header-box, .kpi-card, .chart-title-banner, div[data-testid="stPlotlyChart"], div[data-testid="stDataFrame"] {
        animation: smoothFadeIn 0.3s cubic-bezier(0.16, 1, 0.3, 1) forwards;
    }

    /* Nút bấm tab và action có phản hồi mượt */
    div[data-baseweb="tab-list"] button {
        transition: all 0.25s ease-in-out !important;
    }
    div[data-baseweb="tab-list"] button:active {
        transform: scale(0.97) !important;
    }

    button, div.stButton > button {
        transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1) !important;
    }
    button:active, div.stButton > button:active {
        transform: scale(0.98) !important;
    }

    /* ========================================================
       SIDEBAR & MAIN THEME: SEAMLESS HARMONIOUS PALETTE & ANIMATED NAV
       ======================================================== */
    .stApp {
        background-color: #0b1120 !important;
        color: #f1f5f9;
    }
    
    section[data-testid="stSidebar"] {
        background-color: #0d1527 !important;
        background-image: linear-gradient(180deg, #0d1527 0%, #090e1a 100%) !important;
        border-right: 1px solid rgba(51, 65, 85, 0.35) !important;
        box-shadow: 4px 0 24px rgba(0, 0, 0, 0.25);
    }

    .sidebar-brand-container {
        padding: 6px 4px 12px 4px;
        border-bottom: 1px solid rgba(51, 65, 85, 0.4);
        margin-bottom: 12px;
    }
    .sidebar-brand-top {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 8px;
    }
    .sidebar-logo-img {
        width: 36px;
        height: 36px;
        border-radius: 8px;
        box-shadow: 0 4px 14px rgba(0, 0, 0, 0.4), 0 0 10px rgba(56, 189, 248, 0.3);
        transition: transform 0.35s cubic-bezier(0.34, 1.56, 0.64, 1), box-shadow 0.35s ease;
    }
    .sidebar-logo-img:hover {
        transform: scale(1.1) rotate(3deg);
        box-shadow: 0 6px 18px rgba(0, 0, 0, 0.5), 0 0 18px rgba(56, 189, 248, 0.55);
    }
    .sidebar-brand-text {
        display: flex;
        flex-direction: column;
    }
    .brand-title {
        font-size: 19px;
        font-weight: 800;
        background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 40%, #38bdf8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: 0.5px;
        line-height: 1.2;
    }
    .warehouse-badge-row {
        display: flex;
        align-items: center;
        gap: 6px;
        margin-top: 4px;
    }
    .wh-status-dot {
        width: 7px;
        height: 7px;
        background-color: #10b981;
        border-radius: 50%;
        display: inline-block;
        box-shadow: 0 0 8px #10b981;
        animation: pulseGreenDot 2s infinite ease-in-out;
    }
    @keyframes pulseGreenDot {
        0%, 100% { transform: scale(1); opacity: 1; box-shadow: 0 0 6px #10b981; }
        50% { transform: scale(1.35); opacity: 0.7; box-shadow: 0 0 14px #34d399; }
    }
    .warehouse-select-label {
        font-size: 11px;
        font-weight: 700;
        color: #94a3b8;
        letter-spacing: 0.6px;
        text-transform: uppercase;
    }
    .sidebar-section-title {
        color: #94a3b8;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.8px;
        text-transform: uppercase;
        margin-top: 14px;
        margin-bottom: 8px;
    }
    .sidebar-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(51, 65, 85, 0.6) 50%, transparent);
        margin: 14px 0;
    }

    /* WAREHOUSE SELECTOR SINGLE-CONTAINER ROW: 100% PERFECT HORIZONTAL & VERTICAL ALIGNMENT */
    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] {
        display: flex !important;
        flex-direction: row !important;
        align-items: center !important;
        justify-content: space-between !important;
        gap: 10px !important;
        margin-top: 4px !important;
        margin-bottom: 10px !important;
        width: 100% !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] label[data-testid="stWidgetLabel"] {
        display: inline-flex !important;
        align-items: center !important;
        margin: 0 !important;
        padding: 0 !important;
        min-height: unset !important;
        height: auto !important;
        flex: 0 0 auto !important;
        white-space: nowrap !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] label[data-testid="stWidgetLabel"]::before {
        content: "" !important;
        display: inline-block !important;
        width: 7px !important;
        height: 7px !important;
        border-radius: 50% !important;
        background-color: #10b981 !important;
        box-shadow: 0 0 8px rgba(16, 185, 129, 0.8) !important;
        margin-right: 7px !important;
        flex-shrink: 0 !important;
        animation: statusPulse 2s infinite alternate ease-in-out !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] label[data-testid="stWidgetLabel"] div[data-testid="stMarkdownContainer"],
    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] label[data-testid="stWidgetLabel"] p {
        font-size: 11.5px !important;
        font-weight: 700 !important;
        color: #94a3b8 !important;
        letter-spacing: 0.5px !important;
        text-transform: uppercase !important;
        margin: 0 !important;
        padding: 0 !important;
        line-height: 1 !important;
        display: inline-block !important;
        vertical-align: middle !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] {
        flex: 1 1 0 !important;
        min-width: 120px !important;
        border-radius: 8px !important;
        background-color: #152238 !important;
        border: 1px solid rgba(51, 65, 85, 0.7) !important;
        min-height: 36px !important;
        height: 36px !important;
        box-sizing: border-box !important;
        transition: all 0.25s ease !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"]:hover {
        border-color: #38bdf8 !important;
        box-shadow: 0 0 10px rgba(56, 189, 248, 0.25) !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
        min-height: 36px !important;
        height: 36px !important;
        padding-top: 0 !important;
        padding-bottom: 0 !important;
        padding-left: 10px !important;
        padding-right: 8px !important;
        display: flex !important;
        align-items: center !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] * {
        color: #f1f5f9 !important;
        font-weight: 600 !important;
        font-size: 13px !important;
    }

    /* NATIVE SIDEBAR BUTTONS: SMOOTH ANIMATIONS & HOVER */
    section[data-testid="stSidebar"] div.stButton {
        margin-bottom: 2px !important;
    }

    /* INACTIVE BUTTON */
    section[data-testid="stSidebar"] div.stButton > button[kind="secondary"],
    section[data-testid="stSidebar"] div.stButton > button:not([kind="primary"]) {
        background: rgba(21, 34, 56, 0.45) !important;
        color: #94a3b8 !important;
        border: 1px solid rgba(51, 65, 85, 0.3) !important;
        border-left: 3px solid transparent !important;
        border-radius: 10px !important;
        text-align: left !important;
        justify-content: flex-start !important;
        padding: 9px 14px !important;
        font-size: 13px !important;
        font-weight: 600 !important;
        letter-spacing: 0.3px !important;
        transition: all 0.25s cubic-bezier(0.2, 0.8, 0.2, 1) !important;
        box-shadow: none !important;
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
    }

    /* HOVER ANIMATION FOR INACTIVE BUTTON */
    section[data-testid="stSidebar"] div.stButton > button[kind="secondary"]:hover,
    section[data-testid="stSidebar"] div.stButton > button:not([kind="primary"]):hover {
        background: rgba(30, 41, 59, 0.95) !important;
        color: #ffffff !important;
        border-color: rgba(56, 189, 248, 0.45) !important;
        border-left: 4px solid #38bdf8 !important;
        transform: translateX(6px) !important;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3), 0 0 10px rgba(56, 189, 248, 0.25) !important;
    }

    /* ACTIVE BUTTON */
    section[data-testid="stSidebar"] div.stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #1d4ed8 0%, #2563eb 100%) !important;
        color: #ffffff !important;
        border: 1px solid #3b82f6 !important;
        border-left: 4px solid #67e8f9 !important;
        border-radius: 10px !important;
        text-align: left !important;
        justify-content: flex-start !important;
        padding: 9px 14px !important;
        font-size: 13px !important;
        font-weight: 700 !important;
        letter-spacing: 0.4px !important;
        transform: translateX(4px) !important;
        box-shadow: 0 4px 18px rgba(37, 99, 235, 0.5), inset 0 1px 1px rgba(255, 255, 255, 0.25) !important;
        animation: activeNavPulse 3s infinite alternate ease-in-out !important;
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
    }

    @keyframes activeNavPulse {
        0% { box-shadow: 0 4px 16px rgba(37, 99, 235, 0.45), 0 0 8px rgba(103, 232, 249, 0.3); }
        100% { box-shadow: 0 4px 22px rgba(37, 99, 235, 0.7), 0 0 16px rgba(103, 232, 249, 0.6); }
    }

    section[data-testid="stSidebar"] div.stButton > button[kind="primary"]:hover {
        transform: translateX(6px) !important;
        box-shadow: 0 6px 22px rgba(37, 99, 235, 0.65), 0 0 14px rgba(103, 232, 249, 0.5) !important;
    }

    /* CLICK PRESS TACTILE FEEDBACK */
    section[data-testid="stSidebar"] div.stButton > button:active {
        transform: scale(0.97) translateX(3px) !important;
        transition: transform 0.08s ease !important;
    }


    /* Header Box */
    .header-box {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 14px;
        padding: 16px 22px;
        box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.4);
    }
    .header-title {
        color: #f8fafc;
        font-size: 21px;
        font-weight: 700;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
    }
    .header-subtitle {
        color: #94a3b8;
        font-size: 13px;
        margin-bottom: 0px;
    }

    /* Badges */
    .badge {
        display: inline-block;
        padding: 3px 9px;
        border-radius: 6px;
        font-size: 11.5px;
        font-weight: 600;
        margin-right: 6px;
    }
    .badge-primary { background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid #1d4ed8; }
    .badge-success { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid #059669; }
    .badge-purple { background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid #7e22ce; }

    /* KPI Cards - Clean & Instant Rendering */
    .kpi-card {
        background: #152238;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 16px 18px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        border-color: #38bdf8;
        box-shadow: 0 8px 16px -2px rgba(0, 0, 0, 0.35);
    }
    .kpi-label {
        font-size: 11px;
        font-weight: 600;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }
    .kpi-value {
        font-size: 26px;
        font-weight: 700;
        color: #f8fafc;
        line-height: 1.2;
        margin-bottom: 4px;
    }
    
    /* warehouse badge styled in header section */
    .kpi-delta {
        font-size: 12px;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 5px;
    }
    .delta-positive { color: #10b981; }
    .delta-negative { color: #ef4444; }
    .delta-neutral { color: #94a3b8; }

    .section-title {
        color: #f1f5f9;
        font-size: 15.5px;
        font-weight: 700;
        margin-top: 18px;
        margin-bottom: 12px;
        display: flex;
        align-items: center;
        border-bottom: 1px solid #334155;
        padding-bottom: 6px;
    }

    .chart-title-banner {
        color: #f8fafc;
        font-size: 14.5px;
        font-weight: 600;
        margin-bottom: 6px;
    }

    /* Author Footer */
    .author-footer {
        padding: 12px 8px 6px 8px;
        text-align: center;
        color: #94a3b8;
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.3px;
        border-top: 1px solid #334155;
        margin-top: 16px;
        line-height: 1.5;
    }

    /* KHỐI TRẠNG THÁI ĐỒNG BỘ */
    .sync-status-box {
        margin-top: 14px;
        padding: 8px 10px;
        background: rgba(15, 23, 42, 0.65);
        border: 1px solid #334155;
        border-radius: 8px;
        text-align: center;
    }
    .sync-status-badge {
        font-size: 11.5px;
        font-weight: 700;
        color: #34d399;
        margin-bottom: 2px;
    }
    .sync-status-time {
        font-size: 10.5px;
        color: #94a3b8;
        font-weight: 500;
    }

    /* CĂN GIỮA LANGUAGE SWITCHER HOÀN TOÀN */
    .lang-center-title {
        font-size: 11px;
        font-weight: 600;
        color: #64748b;
        text-align: center;
        margin-top: 14px;
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        width: 100%;
        display: block;
    }
    
    [data-testid="stSidebar"] div[data-testid="stButtonGroup"],
    [data-testid="stSidebar"] div[data-testid="stPills"] {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        width: 100% !important;
        margin: 0 auto !important;
    }
    [data-testid="stSidebar"] div[data-testid="stButtonGroup"] > div,
    [data-testid="stSidebar"] div[data-testid="stButtonGroup"] [role="radiogroup"],
    [data-testid="stSidebar"] div[data-testid="stPills"] > div {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        margin: 0 auto !important;
        gap: 8px !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] div[data-testid="stButtonGroup"] button,
    [data-testid="stSidebar"] div[data-testid="stPills"] button {
        border-radius: 18px !important;
        padding: 4px 16px !important;
        font-size: 12px !important;
        min-width: 52px !important;
        text-align: center !important;
        flex: 1 1 0 !important;
    }
    [data-testid="stSidebar"] div:has(> div[data-testid="stButtonGroup"]) {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
# TRUY VẤN DỮ LIỆU ĐA KHÂU TỐI ƯU
# ==========================================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_inventory_data(cache_version: float = 0.0):
    """
    Truy vấn dữ liệu chi tiết kiểm kê trong kho từ BigQuery.
    - inventory_check_no: Mã nhiệm vụ kiểm kê
    - inv_date: Ngày kiểm kê
    - owner_name: Tên khách hàng (chuẩn hóa B2B: CHINT, KKV, ENVISION)
    - customer_type: B2C / B2B
    - sku: Mã hàng
    - product_name: Tên sản phẩm
    - location: Vị trí kệ
    - worker_id: Nhân viên kiểm
    - system_qty: SL hệ thống (PCS)
    - actual_qty: SL thực tế kiểm kê (PCS)
    - difference_qty: Chênh lệch PCS giữa hệ thống và thực tế
    - Cache 5 phút.
    """
    client, error = get_bigquery_client()
    if error or client is None:
        return pd.DataFrame()

    sql_query = f"""
    SELECT 
        DATE(inventory_time) as inv_date,
        COALESCE(inventory_check_no, 'Chưa có mã') as inventory_check_no,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' THEN 'CHINT'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN 'KKV'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'ENVISION'
            ELSE COALESCE(owner_name, 'Chưa xác định')
        END as owner_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%'
              OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'B2B'
            ELSE 'B2C'
        END as customer_type,
        sku,
        COALESCE(product_name, '') as product_name,
        COALESCE(location, '') as location,
        COALESCE(worker_id, '') as worker_id,
        inventory_time,
        COALESCE(system_qty, 0) as system_qty,
        COALESCE(actual_qty, 0) as actual_qty,
        COALESCE(difference_qty, 0) as difference_qty
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_inventory`
    WHERE inventory_time IS NOT NULL
    """
    df = client.query(sql_query).to_dataframe()
    if not df.empty:
        df['inv_date'] = pd.to_datetime(df['inv_date']).dt.date
        df['system_qty'] = pd.to_numeric(df['system_qty'], errors='coerce').fillna(0).astype(int)
        df['actual_qty'] = pd.to_numeric(df['actual_qty'], errors='coerce').fillna(0).astype(int)
        df['difference_qty'] = pd.to_numeric(df['difference_qty'], errors='coerce').fillna(0).astype(int)

    return df


@st.cache_data(ttl=300, show_spinner=False)
def fetch_stage_data(stage_key: str, cache_version: float = 0.0):
    """
    Truy vấn gom nhóm dữ liệu theo từng khâu trực tiếp từ BigQuery.
    - Chuẩn hóa khách hàng B2B: gộp trùng tên CHINT, KKV (bao gồm JINHE), ENVISION.
    - Gom nhóm theo ngày, giờ, khách hàng, loại khách hàng.
    - Cache 5 phút.
    """
    cfg = STAGE_CONFIGS.get(stage_key)
    if not cfg or "table" not in cfg:
        return pd.DataFrame()

    client, error = get_bigquery_client()
    if error or client is None:
        raise RuntimeError(error or "Không thể kết nối BigQuery")

    table_name = cfg["table"]
    where_clause = cfg["where"]
    time_col = cfg["time_col"]
    order_col = cfg["order_col"]
    qty_col = cfg["qty_col"]

    sql_query = f"""
    SELECT 
        DATE({time_col}) as op_date,
        EXTRACT(HOUR FROM {time_col}) as op_hour,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' THEN 'CHINT'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN 'KKV'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'ENVISION'
            ELSE COALESCE(owner_name, 'Chưa xác định')
        END as owner_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%'
              OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'B2B'
            ELSE 'B2C'
        END as customer_type,
        COUNT(DISTINCT {order_col}) as total_orders,
        SUM(SAFE_CAST({qty_col} AS FLOAT64)) as total_qty
    FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
    WHERE {where_clause}
    GROUP BY op_date, op_hour, owner_name, customer_type
    ORDER BY op_date DESC, op_hour ASC
    """
    query_job = client.query(sql_query)
    df = query_job.to_dataframe()

    if not df.empty:
        df['op_date'] = pd.to_datetime(df['op_date']).dt.date
        df['op_hour'] = df['op_hour'].fillna(0).astype(int)
        df['total_qty'] = df['total_qty'].fillna(0.0)
        df['total_orders'] = df['total_orders'].fillna(0).astype(int)

    return df


TARGET_CARRIERS = [
    'J&T Express',
    'Shopee Xpress',
    'GHN',
    'BEST Express',
    'Viettel Post',
    'J&T Cargo'
]

CARRIER_COLORS = {
    'J&T Express': '#ef4444',
    'Shopee Xpress': '#f97316',
    'GHN': '#10b981',
    'BEST Express': '#3b82f6',
    'Viettel Post': '#dc2626',
    'J&T Cargo': '#8b5cf6',
}

@st.cache_data(ttl=300, show_spinner=False)
def fetch_handover_data(cache_version: float = 0.0) -> pd.DataFrame:
    """Truy vấn dữ liệu bàn giao gom nhóm theo ngày, giờ, carrier chuẩn hóa, khách hàng từ BigQuery."""
    client, error = get_bigquery_client()
    if error or client is None:
        return pd.DataFrame()

    sql = f"""
    SELECT 
        DATE(handover_time) as op_date,
        EXTRACT(HOUR FROM handover_time) as op_hour,
        CASE 
            WHEN UPPER(COALESCE(carrier_name, '')) LIKE '%J&T%' AND UPPER(COALESCE(carrier_name, '')) LIKE '%CARGO%' THEN 'J&T Cargo'
            WHEN UPPER(COALESCE(carrier_name, '')) LIKE '%J&T%' THEN 'J&T Express'
            WHEN UPPER(COALESCE(carrier_name, '')) LIKE '%SHOPEE%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%SPX%' THEN 'Shopee Xpress'
            WHEN UPPER(COALESCE(carrier_name, '')) LIKE '%GHN%' THEN 'GHN'
            WHEN UPPER(COALESCE(carrier_name, '')) LIKE '%VIETTEL%' OR UPPER(COALESCE(carrier_name, '')) LIKE '%VTP%' THEN 'Viettel Post'
            WHEN UPPER(COALESCE(carrier_name, '')) LIKE '%BEST%' THEN 'BEST Express'
            ELSE NULL
        END as carrier_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' THEN 'CHINT'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN 'KKV'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'ENVISION'
            ELSE COALESCE(owner_name, 'Chưa xác định')
        END as owner_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'B2B'
            ELSE 'B2C'
        END as customer_type,
        COUNT(DISTINCT order_no) as total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_handover`
    WHERE handover_time IS NOT NULL 
      AND (
          UPPER(COALESCE(carrier_name, '')) LIKE '%J&T%'
          OR UPPER(COALESCE(carrier_name, '')) LIKE '%SHOPEE%'
          OR UPPER(COALESCE(carrier_name, '')) LIKE '%SPX%'
          OR UPPER(COALESCE(carrier_name, '')) LIKE '%GHN%'
          OR UPPER(COALESCE(carrier_name, '')) LIKE '%VIETTEL%'
          OR UPPER(COALESCE(carrier_name, '')) LIKE '%VTP%'
          OR UPPER(COALESCE(carrier_name, '')) LIKE '%BEST%'
      )
    GROUP BY op_date, op_hour, carrier_name, owner_name, customer_type
    HAVING carrier_name IS NOT NULL
    ORDER BY op_date DESC, op_hour ASC
    """
    try:
        df = client.query(sql).to_dataframe()
        if not df.empty:
            df['op_date'] = pd.to_datetime(df['op_date']).dt.date
            df['op_hour'] = df['op_hour'].fillna(0).astype(int)
            df['total_orders'] = df['total_orders'].fillna(0).astype(int)
            df['total_qty'] = df['total_orders']
        return df
    except Exception:
        return pd.DataFrame()


# Bảng benchmark UPH theo từng Thứ trong tuần (DOW)
# 2C Pack UPH được hiệu chỉnh 50.0 orders/h theo yêu cầu vận hành
DOW_BENCHMARK_UPH = {
    1: {"pick_uph": 81.0, "pack_uph": 50.0},  # CN
    2: {"pick_uph": 86.9, "pack_uph": 50.0},  # T2
    3: {"pick_uph": 56.4, "pack_uph": 50.0},  # T3
    4: {"pick_uph": 53.5, "pack_uph": 50.0},  # T4
    5: {"pick_uph": 72.2, "pack_uph": 50.0},  # T5
    6: {"pick_uph": 57.0, "pack_uph": 50.0},  # T6
    7: {"pick_uph": 55.0, "pack_uph": 50.0},  # T7
}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_weekly_all_clients(start_date_str: str, end_date_str: str, cache_version: float = 0.0) -> dict:
    """Truy vấn sản lượng đơn và PCS thực tế theo ngày cho tất cả khách hàng (không viết tắt)."""
    client, error = get_bigquery_client()
    if error or client is None:
        return {}

    sql = f"""
    SELECT 
        DATE(pack_time) as p_date,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'CHINT & ENVISION VIETNAM'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN 'CÔNG TY TNHH CHUỖI CUNG ỨNG JINHE (KKV)'
            ELSE TRIM(COALESCE(owner_name, 'Chưa xác định'))
        END as client_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN '2B'
            ELSE '2C'
        END as client_type,
        COUNT(DISTINCT order_no) as orders,
        SUM(SAFE_CAST(packing_qty AS FLOAT64)) as pcs
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_packing`
    WHERE pack_time IS NOT NULL 
      AND DATE(pack_time) BETWEEN '{start_date_str}' AND '{end_date_str}'
    GROUP BY p_date, client_name, client_type
    """
    try:
        df = client.query(sql).to_dataframe()
        res = {}
        for _, row in df.iterrows():
            d_key = str(row['p_date'])
            c_key = str(row['client_name'])
            c_type = str(row['client_type'])
            res[(d_key, c_key)] = {
                "orders": int(row['orders']) if pd.notnull(row['orders']) else 0,
                "pcs": float(row['pcs']) if pd.notnull(row['pcs']) else 0.0,
                "type": c_type
            }
        return res
    except Exception:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_dow_baselines(cache_version: float = 0.0) -> dict:
    """Truy vấn trung bình đơn & PCS theo từng Thứ trong tuần (DOW 1..7) cho tất cả khách hàng (không viết tắt)."""
    client, error = get_bigquery_client()
    if error or client is None:
        return {}

    sql = f"""
    WITH daily_agg AS (
        SELECT 
            DATE(pack_time) as p_date,
            EXTRACT(DAYOFWEEK FROM pack_time) as dow,
            CASE 
                WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'CHINT & ENVISION VIETNAM'
                WHEN UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN 'CÔNG TY TNHH CHUỖI CUNG ỨNG JINHE (KKV)'
                ELSE TRIM(COALESCE(owner_name, 'Chưa xác định'))
            END as client_name,
            CASE 
                WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' 
                  OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' 
                  OR UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' 
                  OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN '2B'
                ELSE '2C'
            END as client_type,
            COUNT(DISTINCT order_no) as orders,
            SUM(SAFE_CAST(packing_qty AS FLOAT64)) as pcs
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_packing`
        WHERE pack_time IS NOT NULL 
          AND DATE(pack_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        GROUP BY p_date, dow, client_name, client_type
    )
    SELECT 
        client_name,
        client_type,
        dow,
        ROUND(AVG(orders), 0) as avg_orders,
        ROUND(AVG(pcs), 0) as avg_pcs
    FROM daily_agg
    GROUP BY client_name, client_type, dow
    """
    try:
        df = client.query(sql).to_dataframe()
        res = {}
        for _, row in df.iterrows():
            c_name = str(row['client_name'])
            c_type = str(row['client_type'])
            d = int(row['dow'])
            o = int(row['avg_orders']) if pd.notnull(row['avg_orders']) else 0
            p = float(row['avg_pcs']) if pd.notnull(row['avg_pcs']) else 0.0
            if c_name not in res:
                res[c_name] = {"type": c_type, "dows": {}}
            res[c_name]["dows"][d] = (o, p)
        return res
    except Exception:
        return {}


@st.cache_data(ttl=120, show_spinner=False)
def fetch_backlog_data(cache_version: float = 0.0):
    """Truy vấn dữ liệu tồn đọng backlog & lịch sự kiện cho khâu DỰ KIẾN và ĐÓNG GÓI."""
    client, error = get_bigquery_client()
    if error or client is None:
        raise RuntimeError(error or "Không thể kết nối BigQuery")

    sql_backlog = f"""
    SELECT 
        order_no,
        owner_name,
        customer_type,
        SAFE_CAST(piece_qty AS FLOAT64) as piece_qty,
        received_time,
        production_end_time
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_backlog`
    """
    try:
        df_backlog = client.query(sql_backlog).to_dataframe()
    except Exception as e:
        df_backlog = pd.DataFrame(columns=['order_no', 'owner_name', 'customer_type', 'piece_qty', 'received_time', 'production_end_time'])

    if not df_backlog.empty:
        df_backlog['piece_qty'] = pd.to_numeric(df_backlog['piece_qty'], errors='coerce').fillna(0.0)
        df_backlog['backlog_qty'] = df_backlog['piece_qty']
        df_backlog['backlog_orders'] = 1
        df_backlog['cargo_type'] = df_backlog['customer_type']
        if 'production_end_time' in df_backlog.columns:
            df_backlog['prod_dt'] = pd.to_datetime(df_backlog['production_end_time'], errors='coerce')
        else:
            df_backlog['prod_dt'] = pd.NaT
    else:
        df_backlog = pd.DataFrame(columns=['order_no', 'owner_name', 'customer_type', 'piece_qty', 'received_time', 'production_end_time', 'backlog_qty', 'backlog_orders', 'cargo_type', 'prod_dt'])

    sql_events = f"""
    SELECT event_date, event_name 
    FROM `{PROJECT_ID}.{DATASET_ID}.double_day_calendar`
    ORDER BY event_date ASC
    """
    try:
        df_events = client.query(sql_events).to_dataframe()
    except Exception:
        df_events = pd.DataFrame()

    return df_backlog, df_events


# ==========================================
# LỌC DỮ LIỆU & TÍNH TOÁN
# ==========================================
def filter_by_customer(df: pd.DataFrame, cust_type: str, cust_name: str) -> pd.DataFrame:
    """Lọc dữ liệu theo Loại khách hàng và Tên khách hàng (đã chuẩn hóa)."""
    if df.empty:
        return df
    res = df.copy()
    if cust_type != "Tất cả":
        res = res[res['customer_type'] == cust_type]
    if cust_name != "Tất cả":
        res = res[res['owner_name'] == cust_name]
    return res


def filter_by_date_range(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date, start_time=datetime.time(0, 0), end_time=datetime.time(23, 59)) -> pd.DataFrame:
    """
    Lọc dữ liệu chính xác theo ngày và giờ (hỗ trợ đầy đủ ca ngày lẫn ca đêm qua ngày 22:00 -> 06:00).
    """
    if df.empty:
        return df

    # Chuẩn hóa start_time và end_time nếu tham số truyền vào là int (giờ)
    if isinstance(start_time, int):
        s_h = max(0, min(23, start_time))
        start_time = datetime.time(s_h, 0)
    else:
        s_h = start_time.hour

    if isinstance(end_time, int):
        e_h = max(0, min(23, end_time))
        end_time = datetime.time(e_h, 59)
    else:
        e_h = end_time.hour

    date_col = 'op_date' if 'op_date' in df.columns else ('inv_date' if 'inv_date' in df.columns else None)
    if not date_col:
        return df

    if 'op_hour' in df.columns:
        hour_series = df['op_hour'].fillna(0).astype(int)
    elif 'inventory_time' in df.columns:
        hour_series = pd.to_datetime(df['inventory_time']).dt.hour
    else:
        hour_series = None

    if hour_series is None or (s_h == 0 and e_h == 23 and start_date <= end_date):
        return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)].copy()

    # Trường hợp 1: Cùng ngày (start_date == end_date)
    if start_date == end_date:
        if s_h <= e_h:
            mask = (df[date_col] == start_date) & (hour_series >= s_h) & (hour_series <= e_h)
        else:
            mask = (df[date_col] == start_date) & ((hour_series >= s_h) | (hour_series <= e_h))
        return df[mask].copy()

    # Trường hợp 2: Khác ngày (start_date < end_date, ví dụ 10/11 22h -> 11/11 6h)
    mask_start = (df[date_col] == start_date) & (hour_series >= s_h)
    mask_end = (df[date_col] == end_date) & (hour_series <= e_h)
    mask_mid = (df[date_col] > start_date) & (df[date_col] < end_date)
    return df[mask_start | mask_mid | mask_end].copy()


def get_shift_ordered_hours(hours_list, s_h, e_h):
    """
    Sắp xếp danh sách khung giờ phát sinh theo trình tự ca vận hành:
    - Nếu s_h <= e_h: sắp xếp tăng dần bình thường (ví dụ: 8h -> 17h)
    - Nếu s_h > e_h (ca đêm): giờ tối (>= s_h) trước, giờ sáng (<= e_h) sau (ví dụ: 22h -> 23h -> 0h -> 6h)
    """
    if not hours_list:
        return []
    unique_hours = list(set(hours_list))
    if s_h <= e_h:
        return sorted([h for h in unique_hours if s_h <= h <= e_h])
    else:
        evening = sorted([h for h in unique_hours if h >= s_h])
        morning = sorted([h for h in unique_hours if h <= e_h])
        mid = sorted([h for h in unique_hours if h < s_h and h > e_h])
        return evening + morning + mid



@st.cache_data(ttl=300, show_spinner=False)
def fetch_hourly_worker_data(stage_key: str, start_date: datetime.date, end_date: datetime.date, cache_version: float = 0.0):
    """
    Truy vấn sản lượng theo giờ và theo nhân viên cho khâu Picking và Packing.
    """
    cfg = STAGE_CONFIGS.get(stage_key)
    if not cfg or "table" not in cfg:
        return pd.DataFrame()

    table_name = cfg["table"]
    if table_name not in ["fact_picking", "fact_packing"]:
        return pd.DataFrame()

    client, error = get_bigquery_client()
    if error or client is None:
        return pd.DataFrame()

    time_col = cfg["time_col"]
    order_col = cfg["order_col"]
    qty_col = cfg["qty_col"]
    where_clause = cfg["where"]
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    sql_query = f"""
    SELECT 
        DATE({time_col}) as op_date,
        EXTRACT(HOUR FROM {time_col}) as op_hour,
        COALESCE(NULLIF(TRIM(worker_name), ''), 'Chưa xác định') as worker_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' THEN 'CHINT'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%' THEN 'KKV'
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'ENVISION'
            ELSE COALESCE(owner_name, 'Chưa xác định')
        END as owner_name,
        CASE 
            WHEN UPPER(COALESCE(owner_name, '')) LIKE '%CHINT%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%JINHE%' 
              OR UPPER(COALESCE(owner_name, '')) LIKE '%KKV%'
              OR UPPER(COALESCE(owner_name, '')) LIKE '%ENVISION%' THEN 'B2B'
            ELSE 'B2C'
        END as customer_type,
        COUNT(DISTINCT {order_col}) as total_orders,
        SUM(SAFE_CAST({qty_col} AS FLOAT64)) as total_qty
    FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
    WHERE {where_clause} AND DATE({time_col}) BETWEEN '{start_str}' AND '{end_str}'
    GROUP BY op_date, op_hour, worker_name, owner_name, customer_type
    ORDER BY op_date DESC, op_hour ASC
    """
    try:
        query_job = client.query(sql_query)
        df = query_job.to_dataframe()
        if not df.empty:
            df['op_date'] = pd.to_datetime(df['op_date']).dt.date
            df['op_hour'] = df['op_hour'].fillna(0).astype(int)
            df['total_qty'] = df['total_qty'].fillna(0.0)
            df['total_orders'] = df['total_orders'].fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()

# ==========================================
# TAB 1: NGÀY (REAL-TIME & HOURLY BREAKDOWN)
# ==========================================
def render_day_tab(df_filtered: pd.DataFrame, start_date: datetime.date, end_date: datetime.date, cfg: dict, stage_key: str, selected_cust_type: str, selected_cust_name: str, t: dict, lang: str, start_time: datetime.time = datetime.time(0, 0), end_time: datetime.time = datetime.time(23, 59)):
    st.markdown(f'<div class="section-title">{t["sec_hourly"]}</div>', unsafe_allow_html=True)

    df_range = filter_by_date_range(df_filtered, start_date, end_date, start_time, end_time)

    tot_qty = df_range['total_qty'].sum() if not df_range.empty else 0.0
    tot_orders = df_range['total_orders'].sum() if not df_range.empty else 0
    num_cust = df_range['owner_name'].nunique() if not df_range.empty else 0
    avg_qty_order = (tot_qty / tot_orders) if tot_orders > 0 else 0.0

    backlog_orders = 0
    backlog_qty = 0.0
    df_bl_filtered = pd.DataFrame()
    if stage_key == "ĐÓNG GÓI":
        try:
            sync_st = get_global_sync_state()
            cur_v = sync_st.get("data_version", 0.0)
            df_backlog_all, _ = fetch_backlog_data(cache_version=cur_v)
            df_bl_filtered = filter_by_customer(df_backlog_all, selected_cust_type, selected_cust_name)
            if not df_bl_filtered.empty:
                backlog_orders = int(df_bl_filtered['order_no'].nunique())
                backlog_qty = float(df_bl_filtered['piece_qty'].sum())
        except Exception as e:
            st.error(f"Lỗi tải Backlog: {e}")

    if df_range.empty and stage_key != "ĐÓNG GÓI":
        st.warning(f"{t['no_data']}")
        return

    qty_label = cfg.get(f"qty_label_{lang.lower()}", t["lbl_total_qty"])
    order_label = cfg.get(f"order_label_{lang.lower()}", t["lbl_total_orders"])

    if stage_key == "ĐÓNG GÓI":
        total_combined_qty = tot_qty + backlog_qty
        pct_done_qty = (tot_qty / total_combined_qty * 100.0) if total_combined_qty > 0 else 0.0
        c1_val = f"{tot_qty:,.0f} / {total_combined_qty:,.0f} ({pct_done_qty:.1f}%)"
        c1_delta = f"• {t['sub_done_backlog_qty']}"

        total_combined_orders = tot_orders + backlog_orders
        pct_done_orders = (tot_orders / total_combined_orders * 100.0) if total_combined_orders > 0 else 0.0
        c2_val = f"{tot_orders:,} / {total_combined_orders:,} ({pct_done_orders:.1f}%)"
        c2_delta = f"• {t['sub_done_backlog_orders']}"

        c3_label = t["card_backlog_label"]
        c3_val = f"{backlog_orders:,} {t['unit_order']}"
        c3_delta = f"• {backlog_qty:,.0f} {t['unit_pcs_backlog']}"
        c3_val_style = 'style="color: #f59e0b;"'
    else:
        c1_val = f"{tot_qty:,.0f}"
        c1_delta = f"• {t['lbl_selected_period']}"
        c2_val = f"{tot_orders:,}"
        c2_delta = f"• {t['lbl_orders_recorded']}"
        c3_label = t['lbl_customers_count']
        c3_val = f"{num_cust}"
        c3_delta = f"• {t['lbl_active_partners']}"
        c3_val_style = ""

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{qty_label}</div>
            <div class="kpi-value">{c1_val}</div>
            <div class="kpi-delta delta-neutral">{c1_delta}</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{order_label}</div>
            <div class="kpi-value">{c2_val}</div>
            <div class="kpi-delta delta-neutral">{c2_delta}</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{c3_label}</div>
            <div class="kpi-value" {c3_val_style}>{c3_val}</div>
            <div class="kpi-delta delta-neutral">{c3_delta}</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_avg_order']}</div>
            <div class="kpi-value">{avg_qty_order:,.1f}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_avg_size']}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # 1. BIỂU ĐỒ HOẶC BẢNG TỈ TRỌNG THEO ĐƠN VÀ THEO PCS
    if stage_key == "ĐÓNG GÓI":
        st.markdown(f'<div class="chart-title-banner">{t["sec_packing_share_table"]}</div>', unsafe_allow_html=True)
        if not df_range.empty:
            df_cust_share = df_range.groupby(['owner_name'], as_index=False).agg({
                'total_orders': 'sum',
                'total_qty': 'sum'
            })
            tot_tbl_orders = int(df_cust_share['total_orders'].sum())
            tot_tbl_qty = float(df_cust_share['total_qty'].sum())

            df_cust_share['order_share_pct'] = (df_cust_share['total_orders'] / tot_tbl_orders * 100).fillna(0) if tot_tbl_orders > 0 else 0
            df_cust_share['qty_share_pct'] = (df_cust_share['total_qty'] / tot_tbl_qty * 100).fillna(0) if tot_tbl_qty > 0 else 0

            df_cust_share = df_cust_share.sort_values(by='total_orders', ascending=False)

            total_share_row = pd.DataFrame([{
                'owner_name': t['lbl_total_row'],
                'total_orders': tot_tbl_orders,
                'order_share_pct': 100.0 if tot_tbl_orders > 0 else 0,
                'total_qty': tot_tbl_qty,
                'qty_share_pct': 100.0 if tot_tbl_qty > 0 else 0
            }])
            df_cust_share_with_tot = pd.concat([df_cust_share, total_share_row], ignore_index=True)

            df_cust_disp = df_cust_share_with_tot.copy()
            df_cust_disp['total_orders'] = df_cust_disp['total_orders'].apply(lambda x: f"{x:,.0f}")
            df_cust_disp['order_share_pct'] = df_cust_disp['order_share_pct'].apply(lambda x: f"{x:.1f}%")
            df_cust_disp['total_qty'] = df_cust_disp['total_qty'].apply(lambda x: f"{x:,.0f}")
            df_cust_disp['qty_share_pct'] = df_cust_disp['qty_share_pct'].apply(lambda x: f"{x:.1f}%")

            df_cust_disp.rename(columns={
                'owner_name': t['col_customer_name'],
                'total_orders': t['col_packing_orders'],
                'order_share_pct': t['col_backlog_share_orders'],
                'total_qty': t['col_packing_qty'],
                'qty_share_pct': t['col_backlog_share_pcs']
            }, inplace=True)

            st.dataframe(df_cust_disp, width="stretch", height=280, hide_index=True)

            col_csv_share, _ = st.columns([1.5, 2.5])
            with col_csv_share:
                csv_share_bytes = df_cust_disp.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label=t['btn_download_packing_share'],
                    data=csv_share_bytes,
                    file_name=f"packing_customer_share_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key=f"btn_dl_packing_share_{stage_key}"
                )
        else:
            st.info(t["no_data"])
    else:
        df_cust_share = df_range.groupby('owner_name', as_index=False).agg({
            'total_orders': 'sum',
            'total_qty': 'sum'
        })
        
        col_pie1, col_pie2 = st.columns(2)
        with col_pie1:
            st.markdown(f'<div class="chart-title-banner">{t["pie_share_orders"]}</div>', unsafe_allow_html=True)
            if not df_cust_share.empty and df_cust_share['total_orders'].sum() > 0:
                fig_p1 = px.pie(
                    df_cust_share,
                    values='total_orders',
                    names='owner_name',
                    hole=0.45,
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                fig_p1.update_traces(textposition='inside', textinfo='percent+label', insidetextorientation='radial')
                fig_p1.update_layout(
                    title=None,
                    paper_bgcolor="#1e293b",
                    plot_bgcolor="#0f172a",
                    margin=dict(l=20, r=20, t=10, b=30),
                    legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5, font=dict(color="#94a3b8", size=10)),
                    height=320
                )
                st.plotly_chart(fig_p1, use_container_width=True)
            else:
                st.info(t["no_data"])

        with col_pie2:
            st.markdown(f'<div class="chart-title-banner">{t["pie_share_qty"]}</div>', unsafe_allow_html=True)
            if not df_cust_share.empty and df_cust_share['total_qty'].sum() > 0:
                fig_p2 = px.pie(
                    df_cust_share,
                    values='total_qty',
                    names='owner_name',
                    hole=0.45,
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.qualitative.Safe
                )
                fig_p2.update_traces(textposition='inside', textinfo='percent+label', insidetextorientation='radial')
                fig_p2.update_layout(
                    title=None,
                    paper_bgcolor="#1e293b",
                    plot_bgcolor="#0f172a",
                    margin=dict(l=20, r=20, t=10, b=30),
                    legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5, font=dict(color="#94a3b8", size=10)),
                    height=320
                )
                st.plotly_chart(fig_p2, use_container_width=True)
            else:
                st.info(t["no_data"])

    st.markdown("<br>", unsafe_allow_html=True)

    # 2. CHUYỂN ĐỔI CHỈ SỐ CHO BẢNG GIỜ (MẶC ĐỊNH LÀ SỐ ĐƠN HÀNG)
    metric_choice = st.radio(
        t['lbl_display_metric'],
        options=[t["opt_orders"], t["opt_qty"]],
        horizontal=True,
        key=f"day_tab_metric_choice_{stage_key}"
    )

    val_col = 'total_orders' if metric_choice == t["opt_orders"] else 'total_qty'
    val_title = t["opt_orders"] if metric_choice == t["opt_orders"] else t["opt_qty"]

    # 3. BẢNG PHÂN BỔ SẢN LƯỢNG THEO KHUNG GIỜ (THEO KHÁCH HÀNG)
    fmt_d1 = start_date.strftime('%d/%m/%Y') if lang == "VN" else start_date.strftime('%Y-%m-%d')
    fmt_d2 = end_date.strftime('%d/%m/%Y') if lang == "VN" else end_date.strftime('%Y-%m-%d')
    date_range_label = fmt_d1 if start_date == end_date else f"{fmt_d1} — {fmt_d2}"
    time_range_label = f" ({start_time.strftime('%H:%M')} — {end_time.strftime('%H:%M')})"

    st.markdown(f'<div class="chart-title-banner">{t["lbl_dynamic_table"]} — {date_range_label}{time_range_label}</div>', unsafe_allow_html=True)

    pivot_cust = df_range.pivot_table(
        index='owner_name',
        columns='op_hour',
        values=val_col,
        aggfunc='sum',
        fill_value=0
    )
    raw_hour_cols = [c for c in pivot_cust.columns if isinstance(c, (int, float, np.integer)) or str(c).isdigit()]
    active_hours = [int(h) for h in raw_hour_cols if pivot_cust[h].sum() > 0]
    display_hours = get_shift_ordered_hours(active_hours, start_time.hour, end_time.hour)

    if display_hours:
        pivot_cust = pivot_cust.reindex(columns=display_hours, fill_value=0)
        pivot_cust[t['lbl_total_column']] = pivot_cust.sum(axis=1)
        pivot_cust = pivot_cust.sort_values(t['lbl_total_column'], ascending=False)

        total_row = pivot_cust.sum(axis=0)
        total_row.name = t['lbl_total_row']
        pivot_cust_with_total = pd.concat([pivot_cust, pd.DataFrame([total_row])])

        rename_cols = {h: f"{h:02d}:00" for h in display_hours}
        display_cust_pivot = pivot_cust_with_total.rename(columns=rename_cols).reset_index()
        display_cust_pivot.rename(columns={'index': t['lbl_customer_col']}, inplace=True)

        formatted_cust_pivot = display_cust_pivot.copy()
        for col in formatted_cust_pivot.columns:
            if col != t['lbl_customer_col']:
                formatted_cust_pivot[col] = formatted_cust_pivot[col].apply(
                    lambda x: f"{x:,.0f}" if isinstance(x, (int, float, np.number)) and x > 0 else ("-" if isinstance(x, (int, float, np.number)) else str(x))
                )

        st.dataframe(formatted_cust_pivot, width="stretch", height=280, hide_index=True)

        col_csv1, _ = st.columns([1.5, 2.5])
        with col_csv1:
            csv_bytes = display_cust_pivot.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                label=t['btn_download_hourly'],
                data=csv_bytes,
                file_name=f"hourly_customers_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{start_time.strftime('%H%M')}_{end_time.strftime('%H%M')}.csv",
                mime="text/csv",
                use_container_width=True,
                key=f"btn_dl_hourly_cust_{stage_key}"
            )
    else:
        st.info(t["msg_no_hourly_data"])

    # 4. BẢNG SẢN LƯỢNG THEO TỪNG KHUNG GIỜ THEO NHÂN VIÊN (Áp dụng cho Nhặt hàng & Đóng gói)
    if cfg.get("table") in ["fact_picking", "fact_packing"]:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(f'<div class="chart-title-banner">{t["sec_hourly_staff"]} — {date_range_label}{time_range_label}</div>', unsafe_allow_html=True)

        sync_st = get_global_sync_state()
        cur_v = sync_st.get("data_version", 0.0)
        with st.spinner("..."):
            df_workers_raw = fetch_hourly_worker_data(stage_key, start_date, end_date, cache_version=cur_v)

        df_workers = filter_by_customer(df_workers_raw, selected_cust_type, selected_cust_name)
        df_workers = filter_by_date_range(df_workers, start_date, end_date, start_time, end_time)

        if not df_workers.empty:
            pivot_worker = df_workers.pivot_table(
                index='worker_name',
                columns='op_hour',
                values=val_col,
                aggfunc='sum',
                fill_value=0
            )
            raw_worker_hours = [c for c in pivot_worker.columns if isinstance(c, (int, float, np.integer)) or str(c).isdigit()]
            active_worker_hours = [int(h) for h in raw_worker_hours if pivot_worker[h].sum() > 0]
            display_worker_hours = get_shift_ordered_hours(active_worker_hours, start_time.hour, end_time.hour)

            if display_worker_hours:
                pivot_worker = pivot_worker.reindex(columns=display_worker_hours, fill_value=0)
                pivot_worker[t['lbl_total_column']] = pivot_worker.sum(axis=1)
                pivot_worker = pivot_worker.sort_values(t['lbl_total_column'], ascending=False)

                worker_total_row = pivot_worker.sum(axis=0)
                worker_total_row.name = t['lbl_total_row']
                pivot_worker_with_total = pd.concat([pivot_worker, pd.DataFrame([worker_total_row])])

                rename_cols_worker = {h: f"{h:02d}:00" for h in display_worker_hours}
                display_worker_pivot = pivot_worker_with_total.rename(columns=rename_cols_worker).reset_index()
                display_worker_pivot.rename(columns={'index': t['col_worker']}, inplace=True)

                formatted_worker_pivot = display_worker_pivot.copy()
                for col in formatted_worker_pivot.columns:
                    if col != t['col_worker']:
                        formatted_worker_pivot[col] = formatted_worker_pivot[col].apply(
                            lambda x: f"{x:,.0f}" if isinstance(x, (int, float, np.number)) and x > 0 else ("-" if isinstance(x, (int, float, np.number)) else str(x))
                        )

                st.dataframe(formatted_worker_pivot, width="stretch", height=320, hide_index=True)

                col_csv2, _ = st.columns([1.5, 2.5])
                with col_csv2:
                    csv_worker_bytes = display_worker_pivot.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label=t['btn_download_staff_hourly'],
                        data=csv_worker_bytes,
                        file_name=f"hourly_staff_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{start_time.strftime('%H%M')}_{end_time.strftime('%H%M')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"btn_dl_hourly_staff_{stage_key}"
                    )
            else:
                st.info(t["msg_no_hourly_data"])
        else:
            st.info(t["msg_no_hourly_data"])

    # 5. TIẾN ĐỘ & RỦI RO SLA ĐƠN TỒN THEO HẠN SẢN XUẤT (Áp dụng riêng cho ĐÓNG GÓI)
    if stage_key == "ĐÓNG GÓI":
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="chart-title-banner">TIẾN ĐỘ & RỦI RO SLA ĐƠN TỒN THEO HẠN SẢN XUẤT (PRODUCTION END TIME)</div>', unsafe_allow_html=True)

        if not df_bl_filtered.empty:
            now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
            today_vn = now_vn.date()
            target_date = end_date if end_date else today_vn

            # Lọc các đơn có hạn sản xuất trong ngày mục tiêu (hoặc hôm nay)
            has_prod_dt = 'prod_dt' in df_bl_filtered.columns and df_bl_filtered['prod_dt'].notnull().any()
            if has_prod_dt:
                df_bl_today = df_bl_filtered[df_bl_filtered['prod_dt'].dt.date == target_date].copy()
                if df_bl_today.empty and target_date != today_vn:
                    df_bl_today = df_bl_filtered[df_bl_filtered['prod_dt'].dt.date == today_vn].copy()
                    if not df_bl_today.empty:
                        target_date = today_vn
            else:
                df_bl_today = pd.DataFrame()

            urgent_delta = datetime.timedelta(hours=2)
            urgent_time = now_vn + urgent_delta

            if not df_bl_today.empty:
                # Trích xuất khung giờ hạn dạng chuỗi HH:MM:SS
                df_bl_today['end_time_str'] = df_bl_today['prod_dt'].dt.strftime('%H:%M:%S')

                # Phân loại SLA
                def classify_sla(dt):
                    if pd.isnull(dt):
                        return 'Không có hạn'
                    if dt < now_vn:
                        return 'Đã trễ'
                    elif dt <= urgent_time:
                        return 'Sắp trễ'
                    else:
                        return 'Trong hạn'

                df_bl_today['sla_status'] = df_bl_today['prod_dt'].apply(classify_sla)

                tot_sla_orders = int(df_bl_today['order_no'].nunique())
                late_cnt = int((df_bl_today['sla_status'] == 'Đã trễ').sum())
                urgent_cnt = int((df_bl_today['sla_status'] == 'Sắp trễ').sum())
                ontime_cnt = int((df_bl_today['sla_status'] == 'Trong hạn').sum())

                # 4 KPI Cards tập trung vào Số Đơn Hàng
                c_sla1, c_sla2, c_sla3, c_sla4 = st.columns(4)
                with c_sla1:
                    st.markdown(f"""
                    <div class="kpi-card">
                        <div class="kpi-label">HẠN HÔM NAY ({target_date.strftime('%d/%m')})</div>
                        <div class="kpi-value" style="color: #38bdf8;">{tot_sla_orders:,} <span style="font-size: 13px; color: #94a3b8;">đơn</span></div>
                        <div class="kpi-delta delta-neutral">• Toàn bộ đơn cần xong hôm nay</div>
                    </div>
                    """, unsafe_allow_html=True)
                with c_sla2:
                    late_color = "#ef4444" if late_cnt > 0 else "#94a3b8"
                    late_tag = "🚨 CẦN XỬ LÝ GẤP" if late_cnt > 0 else "✅ 0 đơn trễ"
                    st.markdown(f"""
                    <div class="kpi-card" style="{'border-left: 3px solid #ef4444;' if late_cnt > 0 else ''}">
                        <div class="kpi-label" style="color: {late_color};">ĐÃ TRỄ HẠN (OVERDUE)</div>
                        <div class="kpi-value" style="color: {late_color};">{late_cnt:,} <span style="font-size: 13px;">đơn</span></div>
                        <div class="kpi-delta" style="color: {late_color};">• {late_tag}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with c_sla3:
                    urg_color = "#f59e0b" if urgent_cnt > 0 else "#94a3b8"
                    urg_tag = "⏳ Cận hạn ≤ 2h" if urgent_cnt > 0 else "• Không có đơn cận hạn"
                    st.markdown(f"""
                    <div class="kpi-card" style="{'border-left: 3px solid #f59e0b;' if urgent_cnt > 0 else ''}">
                        <div class="kpi-label" style="color: {urg_color};">SẮP TRỄ HẠN (CẬN HẠN)</div>
                        <div class="kpi-value" style="color: {urg_color};">{urgent_cnt:,} <span style="font-size: 13px;">đơn</span></div>
                        <div class="kpi-delta" style="color: {urg_color};">• {urg_tag}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with c_sla4:
                    ontime_color = "#10b981"
                    pct_ontime = (ontime_cnt / tot_sla_orders * 100) if tot_sla_orders > 0 else 100.0
                    st.markdown(f"""
                    <div class="kpi-card">
                        <div class="kpi-label" style="color: {ontime_color};">TRONG HẠN AN TOÀN</div>
                        <div class="kpi-value" style="color: {ontime_color};">{ontime_cnt:,} <span style="font-size: 13px;">đơn</span></div>
                        <div class="kpi-delta" style="color: {ontime_color};">• {pct_ontime:.1f}% an toàn SLA</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
                col_zalo_sla, col_zalo_pack, col_zalo_space = st.columns([1, 1, 1])
                with col_zalo_sla:
                    if st.button("📢 Gửi cảnh báo SLA vào Nhóm Zalo", key="btn_send_zalo_sla", use_container_width=True):
                        try:
                            import zalo_notifier
                            with st.spinner("Đang kiểm tra và gửi cảnh báo vào Nhóm Zalo..."):
                                z_res = zalo_notifier.scan_and_alert_backlog_sla(df_bl_today, force_send=True, custom_urgent_hours=1.0)
                                if z_res.get("status") == "sent":
                                    st.success(z_res.get("message"))
                                elif z_res.get("status") == "not_configured":
                                    st.warning("⚠️ Chưa cấu hình Zalo Bot (Webhook URL hoặc Token trong secrets.toml).")
                                    with st.expander("👀 Xem trước nội dung tin nhắn sẽ gửi vào Zalo"):
                                        st.code(z_res.get("preview_message"), language="text")
                                elif z_res.get("status") == "safe":
                                    st.info("✅ Hiện không có đơn nào cận hạn (≤ 1h) hoặc trễ hạn SLA.")
                                else:
                                    st.error(f"Lỗi: {z_res.get('message')}")
                                    if z_res.get("preview_message"):
                                        with st.expander("👀 Xem trước nội dung tin nhắn"):
                                            st.code(z_res.get("preview_message"), language="text")
                        except Exception as ex_z:
                            st.error(f"Lỗi gửi tin: {ex_z}")
                with col_zalo_pack:
                    if st.button("📦 Gửi tiến độ Đóng Gói vào Nhóm Zalo", key="btn_send_zalo_pack", use_container_width=True):
                        try:
                            import zalo_notifier
                            with st.spinner("Đang tổng hợp và gửi tiến độ Đóng Gói vào Nhóm Zalo..."):
                                z_res = zalo_notifier.scan_and_alert_packing_progress(force_send=True)
                                if z_res.get("status") == "sent":
                                    st.success(z_res.get("message"))
                                else:
                                    st.error(f"Lỗi: {z_res.get('message')}")
                        except Exception as ex_p:
                            st.error(f"Lỗi gửi tin: {ex_p}")
                            st.error(f"Lỗi gửi Zalo: {ex_z}")

                st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

                # ==========================================
                # CHI TIẾT SỐ ĐƠN CỦA TỪNG KHÁCH HÀNG THEO TỪNG MỐC HẠN SẢN XUẤT (MA TRẬN KHÁCH HÀNG X END TIME)
                # ==========================================
                st.markdown('<div class="chart-title-banner">CHI TIẾT SỐ ĐƠN TỒN CỦA TỪNG KHÁCH HÀNG THEO TỪNG KHUNG GIỜ HẠN (PRODUCTION END TIME)</div>', unsafe_allow_html=True)

                pivot_orders = df_bl_today.pivot_table(
                    index='owner_name',
                    columns='end_time_str',
                    values='order_no',
                    aggfunc='nunique',
                    fill_value=0
                )
                end_time_cols = sorted(pivot_orders.columns.tolist())
                pivot_orders['TỔNG ĐƠN TỒN'] = pivot_orders[end_time_cols].sum(axis=1)
                pivot_orders = pivot_orders.sort_values(by='TỔNG ĐƠN TỒN', ascending=False).reset_index()

                tot_dict = {'owner_name': 'TỔNG CỘNG'}
                for c in end_time_cols:
                    tot_dict[c] = int(pivot_orders[c].sum())
                tot_dict['TỔNG ĐƠN TỒN'] = int(pivot_orders['TỔNG ĐƠN TỒN'].sum())

                pivot_display = pd.concat([pivot_orders, pd.DataFrame([tot_dict])], ignore_index=True)
                pivot_display.rename(columns={'owner_name': 'Khách hàng'}, inplace=True)

                for c in end_time_cols + ['TỔNG ĐƠN TỒN']:
                    pivot_display[c] = pivot_display[c].apply(lambda x: f"{x:,.0f}" if x > 0 else "-")

                st.dataframe(pivot_display, width="stretch", height=min(450, 42 * len(pivot_display) + 40), hide_index=True)

                col_pivot_dl, _ = st.columns([1.5, 2.5])
                with col_pivot_dl:
                    csv_pivot_bytes = pivot_display.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Tải bảng chi tiết khách hàng theo khung giờ (.csv)",
                        data=csv_pivot_bytes,
                        file_name=f"packing_cust_x_end_time_{today_vn.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="btn_dl_packing_cust_end_time"
                    )

                # ==========================================
                # EXPANDER: DANH SÁCH ĐƠN ĐÃ TRỄ & SẮP TRỄ CẦN ĐÓNG GẤP (ẨN CỘT PHÂN LOẠI)
                # ==========================================
                df_urgent_detail = df_bl_today[df_bl_today['sla_status'].isin(['Đã trễ', 'Sắp trễ'])].copy()
                if not df_urgent_detail.empty:
                    with st.expander(f"🚨 Danh sách chi tiết {len(df_urgent_detail):,} đơn ĐÃ TRỄ & SẮP TRỄ cần xử lý gấp", expanded=(late_cnt > 0)):
                        df_u_show = df_urgent_detail[['order_no', 'owner_name', 'production_end_time', 'sla_status', 'piece_qty']].sort_values(by=['sla_status', 'production_end_time'], ascending=[True, True])
                        df_u_show['piece_qty'] = df_u_show['piece_qty'].apply(lambda x: f"{x:,.0f}")
                        df_u_show.columns = ["Mã đơn hàng", "Khách hàng", "Hạn sản xuất (Production End Time)", "Trạng thái SLA", "Số lượng (PCS)"]
                        st.dataframe(df_u_show, width="stretch", height=250, hide_index=True)
            else:
                st.info(f"Không có đơn tồn đọng nào có Hạn sản xuất (Production End Time) trong ngày {target_date.strftime('%d/%m/%Y')}.")

            # ==========================================
            # BẢNG 3: TỔNG HỢP TOÀN BỘ ĐƠN TỒN BACKLOG THEO KHÁCH HÀNG (TẤT CẢ CÁC HẠN - ẨN CỘT PHÂN LOẠI)
            # ==========================================
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<div class="chart-title-banner">TỔNG HỢP TOÀN BỘ ĐƠN TỒN BACKLOG THEO KHÁCH HÀNG (TẤT CẢ CÁC HẠN)</div>', unsafe_allow_html=True)
            df_bl_summary = df_bl_filtered.groupby(['owner_name'], as_index=False).agg(
                backlog_orders=('order_no', 'nunique'),
                backlog_qty=('piece_qty', 'sum')
            ).sort_values(by='backlog_orders', ascending=False)

            tot_bl_tbl_orders = int(df_bl_summary['backlog_orders'].sum())
            tot_bl_tbl_qty = float(df_bl_summary['backlog_qty'].sum())

            df_bl_summary['share_orders'] = (df_bl_summary['backlog_orders'] / tot_bl_tbl_orders * 100) if tot_bl_tbl_orders > 0 else 0.0
            df_bl_summary['share_qty'] = (df_bl_summary['backlog_qty'] / tot_bl_tbl_qty * 100) if tot_bl_tbl_qty > 0 else 0.0

            total_row = pd.DataFrame([{
                'owner_name': 'TỔNG CỘNG',
                'backlog_orders': tot_bl_tbl_orders,
                'share_orders': 100.0,
                'backlog_qty': tot_bl_tbl_qty,
                'share_qty': 100.0
            }])
            df_bl_display = pd.concat([df_bl_summary, total_row], ignore_index=True)

            df_bl_formatted = df_bl_display.copy()
            df_bl_formatted['backlog_orders'] = df_bl_formatted['backlog_orders'].apply(lambda x: f"{x:,.0f}")
            df_bl_formatted['share_orders'] = df_bl_formatted['share_orders'].apply(lambda x: f"{x:.1f}%")
            df_bl_formatted['backlog_qty'] = df_bl_formatted['backlog_qty'].apply(lambda x: f"{x:,.0f}")
            df_bl_formatted['share_qty'] = df_bl_formatted['share_qty'].apply(lambda x: f"{x:.1f}%")

            df_bl_formatted.columns = [
                "Tên khách hàng",
                "Số đơn tồn đọng",
                "% Tỉ trọng đơn",
                "Sản lượng tồn đọng (PCS)",
                "% Tỉ trọng PCS"
            ]

            st.dataframe(df_bl_formatted, width="stretch", height=320, hide_index=True)

            col_bl_csv, _ = st.columns([1.5, 2.5])
            with col_bl_csv:
                csv_bl_bytes = df_bl_display.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label="Tải bảng tồn đọng chung (.csv)",
                    data=csv_bl_bytes,
                    file_name=f"packing_backlog_all_{datetime.date.today().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="btn_dl_packing_backlog_all"
                )
        else:
            st.info("Hiện không có đơn hàng tồn đọng nào cần xử lý.")

# ==========================================
# TAB 2: TUẦN (WEEK-OVER-WEEK - WoW ANALYSIS)
# ==========================================
def render_week_tab(df_filtered: pd.DataFrame, cfg: dict, t: dict, lang: str):
    st.markdown(f'<div class="section-title">{t["sec_weekly"]}</div>', unsafe_allow_html=True)

    if df_filtered.empty:
        st.warning(f"{t['no_data']}")
        return

    df = df_filtered.copy()
    df['op_date'] = pd.to_datetime(df['op_date'])
    df['week_start'] = df['op_date'].dt.to_period('W-SUN').dt.start_time.dt.date

    weekly_group = df.groupby(['week_start', 'customer_type', 'owner_name'], as_index=False).agg({
        'total_orders': 'sum',
        'total_qty': 'sum'
    }).sort_values('week_start')

    weekly_total = weekly_group.groupby('week_start', as_index=False).agg({
        'total_orders': 'sum',
        'total_qty': 'sum'
    }).sort_values('week_start')

    weekly_total['qty_prev_week'] = weekly_total['total_qty'].shift(1)
    weekly_total['orders_prev_week'] = weekly_total['total_orders'].shift(1)
    weekly_total['wow_qty_pct'] = (
        (weekly_total['total_qty'] - weekly_total['qty_prev_week']) / weekly_total['qty_prev_week'] * 100
    )
    weekly_total['wow_qty_delta'] = weekly_total['total_qty'] - weekly_total['qty_prev_week']

    if len(weekly_total) >= 2:
        latest_w = weekly_total.iloc[-1]
        prev_w = weekly_total.iloc[-2]
        wow_pct = latest_w['wow_qty_pct']
        delta_q = latest_w['wow_qty_delta']
    else:
        latest_w = weekly_total.iloc[-1]
        prev_w = None
        wow_pct = 0.0
        delta_q = 0.0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_week_this']}</div>
            <div class="kpi-value">{latest_w['total_qty']:,.0f}</div>
            <div class="kpi-delta delta-neutral">• {latest_w['week_start'].strftime('%d/%m/%Y')}</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        prev_val = prev_w['total_qty'] if prev_w is not None else 0
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_week_prev']}</div>
            <div class="kpi-value">{prev_val:,.0f}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_week_diff']}</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        wow_class = "delta-positive" if wow_pct > 0 else ("delta-negative" if wow_pct < 0 else "delta-neutral")
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_week_growth']}</div>
            <div class="kpi-value" style="color: {'#10b981' if wow_pct > 0 else ('#ef4444' if wow_pct < 0 else '#f8fafc')};">{wow_pct:+.1f}%</div>
            <div class="kpi-delta {wow_class}">{t['lbl_week_diff']}: {delta_q:+,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_week_orders']}</div>
            <div class="kpi-value">{latest_w['total_orders']:,}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_orders_recorded']}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f'<div class="chart-title-banner">{t["sec_weekly"]}</div>', unsafe_allow_html=True)

    prefix_w = "Tuần "
    suffix_w = ""
    weekly_total['week_label'] = weekly_total['week_start'].apply(lambda d: f"{prefix_w}{d.strftime('%W')}{suffix_w} ({d.strftime('%d/%m')})")
    
    fig_w = go.Figure()
    fig_w.add_trace(go.Bar(
        x=weekly_total['week_label'],
        y=weekly_total['total_qty'],
        name=t["lbl_total_qty"],
        marker_color="#3b82f6",
        yaxis="y",
        hovertemplate="<b>%{x}</b><br>%{y:,.0f}<extra></extra>"
    ))
    fig_w.add_trace(go.Scatter(
        x=weekly_total['week_label'],
        y=weekly_total['wow_qty_pct'],
        name=t["lbl_week_growth"],
        mode="lines+markers",
        line=dict(color="#10b981", width=2.5),
        marker=dict(size=7, color="#34d399"),
        yaxis="y2",
        hovertemplate="<b>%{x}</b><br>%{y:+.1f}%<extra></extra>"
    ))
    fig_w.update_layout(
        title=None,
        template="plotly_dark",
        paper_bgcolor="#1e293b",
        plot_bgcolor="#0f172a",
        margin=dict(l=30, r=30, t=10, b=80),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(color="#94a3b8")),
        yaxis=dict(title=dict(text=t["lbl_total_qty"], font=dict(color="#60a5fa")), color="#60a5fa"),
        yaxis2=dict(title=dict(text=t["lbl_week_growth"], font=dict(color="#34d399")), color="#34d399", overlaying="y", side="right"),
        hovermode="x unified"
    )
    st.plotly_chart(fig_w, use_container_width=True)

    st.markdown(f'<div class="chart-title-banner">{t["lbl_week_table"]}</div>', unsafe_allow_html=True)
    tbl_week = weekly_total.sort_values('week_start', ascending=False).copy()
    display_w = pd.DataFrame({
        t['col_week_start']: tbl_week['week_start'].apply(lambda d: d.strftime('%d/%m/%Y') if lang == "VN" else d.strftime('%Y-%m-%d')),
        t['col_week_name']: tbl_week['week_label'],
        t['col_week_orders']: tbl_week['total_orders'].apply(lambda v: f"{v:,}"),
        t['col_week_qty']: tbl_week['total_qty'].apply(lambda v: f"{v:,.0f}"),
        t['col_week_growth']: tbl_week['wow_qty_pct'].apply(
            lambda v: f"+{v:.2f}%" if pd.notnull(v) and v > 0 else (f"{v:.2f}%" if pd.notnull(v) and v < 0 else (f"{v:.2f}%" if pd.notnull(v) else "—"))
        ),
        t['col_week_diff']: tbl_week['wow_qty_delta'].apply(lambda v: f"{v:+,.0f}" if pd.notnull(v) else "—")
    })
    st.dataframe(display_w, width="stretch", hide_index=True)

    # ==========================================
    # THEO DÕI TỈ TRỌNG & TĂNG GIẢM THEO KHÁCH HÀNG (WoW)
    # ==========================================
    weeks = sorted(weekly_group['week_start'].unique())
    if len(weeks) >= 2:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(f'<div class="chart-title-banner">{t["sec_cust_share_tracking"]} (WoW)</div>', unsafe_allow_html=True)

        col_w_sel, col_w_metric = st.columns([1.5, 2.5])
        with col_w_sel:
            sel_week_idx = len(weeks) - 1
            chosen_week = st.selectbox(
                t['col_week_name'],
                options=weeks,
                index=sel_week_idx,
                format_func=lambda d: f"Tuần {d.strftime('%W')} ({d.strftime('%d/%m/%Y')})",
                key="sb_week_share"
            )
        with col_w_metric:
            w_metric_choice = st.radio(
                t['lbl_display_metric'],
                options=[t["opt_orders"], t["opt_qty"]],
                horizontal=True,
                key="rb_week_share_metric"
            )

        chosen_idx = weeks.index(chosen_week)
        w_curr = chosen_week
        w_prev = weeks[chosen_idx - 1] if chosen_idx > 0 else None

        df_w_curr = weekly_group[weekly_group['week_start'] == w_curr].copy()
        df_w_prev = weekly_group[weekly_group['week_start'] == w_prev].copy() if w_prev else pd.DataFrame()

        tot_orders_curr = df_w_curr['total_orders'].sum()
        tot_qty_curr = df_w_curr['total_qty'].sum()
        tot_orders_prev = df_w_prev['total_orders'].sum() if not df_w_prev.empty else 0
        tot_qty_prev = df_w_prev['total_qty'].sum() if not df_w_prev.empty else 0

        df_w_curr['share_orders_curr'] = (df_w_curr['total_orders'] / tot_orders_curr * 100) if tot_orders_curr > 0 else 0
        df_w_curr['share_qty_curr'] = (df_w_curr['total_qty'] / tot_qty_curr * 100) if tot_qty_curr > 0 else 0

        if not df_w_prev.empty:
            df_w_prev['share_orders_prev'] = (df_w_prev['total_orders'] / tot_orders_prev * 100) if tot_orders_prev > 0 else 0
            df_w_prev['share_qty_prev'] = (df_w_prev['total_qty'] / tot_qty_prev * 100) if tot_qty_prev > 0 else 0
            m_week = pd.merge(
                df_w_curr[['owner_name', 'customer_type', 'total_orders', 'total_qty', 'share_orders_curr', 'share_qty_curr']],
                df_w_prev[['owner_name', 'total_orders', 'total_qty', 'share_orders_prev', 'share_qty_prev']],
                on='owner_name',
                how='outer',
                suffixes=('_curr', '_prev')
            ).fillna(0)
        else:
            m_week = df_w_curr.copy()
            m_week['total_orders_prev'] = 0
            m_week['total_qty_prev'] = 0
            m_week['share_orders_prev'] = 0.0
            m_week['share_qty_prev'] = 0.0
            m_week.rename(columns={'total_orders': 'total_orders_curr', 'total_qty': 'total_qty_curr'}, inplace=True)

        m_week['delta_share_orders'] = m_week['share_orders_curr'] - m_week['share_orders_prev']
        m_week['delta_share_qty'] = m_week['share_qty_curr'] - m_week['share_qty_prev']
        m_week['growth_orders'] = ((m_week['total_orders_curr'] - m_week['total_orders_prev']) / m_week['total_orders_prev'].replace(0, pd.NA) * 100).fillna(0)
        m_week['growth_qty'] = ((m_week['total_qty_curr'] - m_week['total_qty_prev']) / m_week['total_qty_prev'].replace(0, pd.NA) * 100).fillna(0)

        is_qty = (w_metric_choice == t["opt_qty"])
        val_curr_col = 'total_qty_curr' if is_qty else 'total_orders_curr'
        val_prev_col = 'total_qty_prev' if is_qty else 'total_orders_prev'
        share_curr_col = 'share_qty_curr' if is_qty else 'share_orders_curr'
        share_prev_col = 'share_qty_prev' if is_qty else 'share_orders_prev'
        delta_share_col = 'delta_share_qty' if is_qty else 'delta_share_orders'
        growth_col = 'growth_qty' if is_qty else 'growth_orders'

        m_week = m_week.sort_values(val_curr_col, ascending=False)

        # Biểu đồ so sánh tỉ trọng kỳ này vs kỳ trước cho Top khách hàng
        top_share = m_week.head(10).copy()
        fig_share_w = go.Figure()
        fig_share_w.add_trace(go.Bar(
            x=top_share['owner_name'],
            y=top_share[share_curr_col],
            name=f"{t['col_share_current']} ({w_curr.strftime('%d/%m')})",
            marker_color="#3b82f6"
        ))
        if w_prev:
            fig_share_w.add_trace(go.Bar(
                x=top_share['owner_name'],
                y=top_share[share_prev_col],
                name=f"{t['col_share_prev']} ({w_prev.strftime('%d/%m')})",
                marker_color="#64748b"
            ))
        fig_share_w.update_layout(
            barmode='group',
            title=None,
            template="plotly_dark",
            paper_bgcolor="#1e293b",
            plot_bgcolor="#0f172a",
            margin=dict(l=30, r=30, t=10, b=80),
            legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(color="#94a3b8")),
            yaxis=dict(title=t['col_share_current'], color="#94a3b8"),
            xaxis=dict(color="#94a3b8")
        )
        st.plotly_chart(fig_share_w, use_container_width=True)

        # Bảng dữ liệu chi tiết
        display_m_week = pd.DataFrame({
            t['lbl_customer_col']: m_week['owner_name'],
            t['col_period_current']: m_week[val_curr_col].apply(lambda v: f"{v:,.0f}"),
            t['col_share_current']: m_week[share_curr_col].apply(lambda v: f"{v:.2f}%"),
            t['col_period_prev']: m_week[val_prev_col].apply(lambda v: f"{v:,.0f}"),
            t['col_share_prev']: m_week[share_prev_col].apply(lambda v: f"{v:.2f}%"),
            t['col_share_delta']: m_week[delta_share_col].apply(lambda v: f"+{v:.2f}%" if v > 0 else f"{v:.2f}%"),
            t['col_growth_rate']: m_week[growth_col].apply(lambda v: f"+{v:.1f}%" if v > 0 else (f"{v:.1f}%" if v < 0 else "0.0%")),
        })

        tot_curr = m_week[val_curr_col].sum()
        tot_prev = m_week[val_prev_col].sum()
        tot_growth = ((tot_curr - tot_prev) / tot_prev * 100) if tot_prev > 0 else 0
        summary_row = {
            t['lbl_customer_col']: t['lbl_total_row'],
            t['col_period_current']: f"{tot_curr:,.0f}",
            t['col_share_current']: "100.00%",
            t['col_period_prev']: f"{tot_prev:,.0f}",
            t['col_share_prev']: "100.00%",
            t['col_share_delta']: "0.00%",
            t['col_growth_rate']: f"{tot_growth:+.1f}%"
        }
        display_m_week = pd.concat([display_m_week, pd.DataFrame([summary_row])], ignore_index=True)

        st.dataframe(display_m_week, width="stretch", hide_index=True)

        col_dl_share, _ = st.columns([1.5, 2.5])
        with col_dl_share:
            csv_share_bytes = display_m_week.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                label=t['btn_download_share_csv'],
                data=csv_share_bytes,
                file_name=f"customer_share_week_{w_curr.strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
                key="btn_dl_week_share"
            )

# ==========================================
# TAB 3: THÁNG (MONTH-OVER-MONTH - MoM ANALYSIS)
# ==========================================
def render_month_tab(df_filtered: pd.DataFrame, cfg: dict, t: dict, lang: str):
    st.markdown(f'<div class="section-title">{t["sec_monthly"]}</div>', unsafe_allow_html=True)

    if df_filtered.empty:
        st.warning(f"{t['no_data']}")
        return

    df = df_filtered.copy()
    df['op_date'] = pd.to_datetime(df['op_date'])
    df['month_key'] = df['op_date'].dt.strftime('%Y-%m')

    monthly_group = df.groupby(['month_key', 'customer_type', 'owner_name'], as_index=False).agg({
        'total_orders': 'sum',
        'total_qty': 'sum'
    }).sort_values('month_key')

    monthly_total = monthly_group.groupby('month_key', as_index=False).agg({
        'total_orders': 'sum',
        'total_qty': 'sum'
    }).sort_values('month_key')

    monthly_total['qty_prev_month'] = monthly_total['total_qty'].shift(1)
    monthly_total['mom_qty_pct'] = (
        (monthly_total['total_qty'] - monthly_total['qty_prev_month']) / monthly_total['qty_prev_month'] * 100
    )
    monthly_total['mom_qty_delta'] = monthly_total['total_qty'] - monthly_total['qty_prev_month']

    if len(monthly_total) >= 2:
        latest_m = monthly_total.iloc[-1]
        prev_m = monthly_total.iloc[-2]
        mom_pct = latest_m['mom_qty_pct']
        delta_m = latest_m['mom_qty_delta']
    else:
        latest_m = monthly_total.iloc[-1]
        prev_m = None
        mom_pct = 0.0
        delta_m = 0.0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_month_this']}</div>
            <div class="kpi-value">{latest_m['total_qty']:,.0f}</div>
            <div class="kpi-delta delta-neutral">• {latest_m['month_key']}</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        prev_m_val = prev_m['total_qty'] if prev_m is not None else 0
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_month_prev']}</div>
            <div class="kpi-value">{prev_m_val:,.0f}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_month_diff']}</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        mom_class = "delta-positive" if mom_pct > 0 else ("delta-negative" if mom_pct < 0 else "delta-neutral")
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_month_growth']}</div>
            <div class="kpi-value" style="color: {'#10b981' if mom_pct > 0 else ('#ef4444' if mom_pct < 0 else '#f8fafc')};">{mom_pct:+.1f}%</div>
            <div class="kpi-delta {mom_class}">{t['lbl_month_diff']}: {delta_m:+,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_month_orders']}</div>
            <div class="kpi-value">{latest_m['total_orders']:,}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_orders_recorded']}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f'<div class="chart-title-banner">{t["sec_monthly"]}</div>', unsafe_allow_html=True)

    fig_m = go.Figure()
    fig_m.add_trace(go.Bar(
        x=monthly_total['month_key'],
        y=monthly_total['total_qty'],
        name=t["lbl_total_qty"],
        marker_color="#8b5cf6",
        yaxis="y",
        hovertemplate="<b>%{x}</b><br>%{y:,.0f}<extra></extra>"
    ))
    fig_m.add_trace(go.Scatter(
        x=monthly_total['month_key'],
        y=monthly_total['mom_qty_pct'],
        name=t["lbl_month_growth"],
        mode="lines+markers",
        line=dict(color="#f59e0b", width=2.5),
        marker=dict(size=7, color="#fbbf24"),
        yaxis="y2",
        hovertemplate="<b>%{x}</b><br>%{y:+.1f}%<extra></extra>"
    ))
    fig_m.update_layout(
        title=None,
        template="plotly_dark",
        paper_bgcolor="#1e293b",
        plot_bgcolor="#0f172a",
        margin=dict(l=30, r=30, t=10, b=80),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(color="#94a3b8")),
        yaxis=dict(title=dict(text=t["lbl_total_qty"], font=dict(color="#a78bfa")), color="#a78bfa"),
        yaxis2=dict(title=dict(text=t["lbl_month_growth"], font=dict(color="#fbbf24")), color="#fbbf24", overlaying="y", side="right"),
        hovermode="x unified"
    )
    st.plotly_chart(fig_m, use_container_width=True)

    st.markdown(f'<div class="chart-title-banner">{t["lbl_month_table"]}</div>', unsafe_allow_html=True)
    tbl_month = monthly_total.sort_values('month_key', ascending=False).copy()
    display_m = pd.DataFrame({
        t['col_month']: tbl_month['month_key'],
        t['col_month_orders']: tbl_month['total_orders'].apply(lambda v: f"{v:,}"),
        t['col_month_qty']: tbl_month['total_qty'].apply(lambda v: f"{v:,.0f}"),
        t['col_month_growth']: tbl_month['mom_qty_pct'].apply(
            lambda v: f"+{v:.2f}%" if pd.notnull(v) and v > 0 else (f"{v:.2f}%" if pd.notnull(v) and v < 0 else (f"{v:.2f}%" if pd.notnull(v) else "—"))
        ),
        t['col_month_diff']: tbl_month['mom_qty_delta'].apply(lambda v: f"{v:+,.0f}" if pd.notnull(v) else "—")
    })
    st.dataframe(display_m, width="stretch", hide_index=True)

    # ==========================================
    # THEO DÕI TỈ TRỌNG & TĂNG GIẢM THEO KHÁCH HÀNG (MoM)
    # ==========================================
    months = sorted(monthly_group['month_key'].unique())
    if len(months) >= 2:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(f'<div class="chart-title-banner">{t["sec_cust_share_tracking"]} (MoM)</div>', unsafe_allow_html=True)

        col_m_sel, col_m_metric = st.columns([1.5, 2.5])
        with col_m_sel:
            sel_month_idx = len(months) - 1
            chosen_month = st.selectbox(
                t['col_month'],
                options=months,
                index=sel_month_idx,
                key="sb_month_share"
            )
        with col_m_metric:
            m_metric_choice = st.radio(
                t['lbl_display_metric'],
                options=[t["opt_orders"], t["opt_qty"]],
                horizontal=True,
                key="rb_month_share_metric"
            )

        chosen_m_idx = months.index(chosen_month)
        m_curr = chosen_month
        m_prev = months[chosen_m_idx - 1] if chosen_m_idx > 0 else None

        df_m_curr = monthly_group[monthly_group['month_key'] == m_curr].copy()
        df_m_prev = monthly_group[monthly_group['month_key'] == m_prev].copy() if m_prev else pd.DataFrame()

        tot_orders_curr = df_m_curr['total_orders'].sum()
        tot_qty_curr = df_m_curr['total_qty'].sum()
        tot_orders_prev = df_m_prev['total_orders'].sum() if not df_m_prev.empty else 0
        tot_qty_prev = df_m_prev['total_qty'].sum() if not df_m_prev.empty else 0

        df_m_curr['share_orders_curr'] = (df_m_curr['total_orders'] / tot_orders_curr * 100) if tot_orders_curr > 0 else 0
        df_m_curr['share_qty_curr'] = (df_m_curr['total_qty'] / tot_qty_curr * 100) if tot_qty_curr > 0 else 0

        if not df_m_prev.empty:
            df_m_prev['share_orders_prev'] = (df_m_prev['total_orders'] / tot_orders_prev * 100) if tot_orders_prev > 0 else 0
            df_m_prev['share_qty_prev'] = (df_m_prev['total_qty'] / tot_qty_prev * 100) if tot_qty_prev > 0 else 0
            m_month = pd.merge(
                df_m_curr[['owner_name', 'customer_type', 'total_orders', 'total_qty', 'share_orders_curr', 'share_qty_curr']],
                df_m_prev[['owner_name', 'total_orders', 'total_qty', 'share_orders_prev', 'share_qty_prev']],
                on='owner_name',
                how='outer',
                suffixes=('_curr', '_prev')
            ).fillna(0)
        else:
            m_month = df_m_curr.copy()
            m_month['total_orders_prev'] = 0
            m_month['total_qty_prev'] = 0
            m_month['share_orders_prev'] = 0.0
            m_month['share_qty_prev'] = 0.0
            m_month.rename(columns={'total_orders': 'total_orders_curr', 'total_qty': 'total_qty_curr'}, inplace=True)

        m_month['delta_share_orders'] = m_month['share_orders_curr'] - m_month['share_orders_prev']
        m_month['delta_share_qty'] = m_month['share_qty_curr'] - m_month['share_qty_prev']
        m_month['growth_orders'] = ((m_month['total_orders_curr'] - m_month['total_orders_prev']) / m_month['total_orders_prev'].replace(0, pd.NA) * 100).fillna(0)
        m_month['growth_qty'] = ((m_month['total_qty_curr'] - m_month['total_qty_prev']) / m_month['total_qty_prev'].replace(0, pd.NA) * 100).fillna(0)

        is_qty = (m_metric_choice == t["opt_qty"])
        val_curr_col = 'total_qty_curr' if is_qty else 'total_orders_curr'
        val_prev_col = 'total_qty_prev' if is_qty else 'total_orders_prev'
        share_curr_col = 'share_qty_curr' if is_qty else 'share_orders_curr'
        share_prev_col = 'share_qty_prev' if is_qty else 'share_orders_prev'
        delta_share_col = 'delta_share_qty' if is_qty else 'delta_share_orders'
        growth_col = 'growth_qty' if is_qty else 'growth_orders'

        m_month = m_month.sort_values(val_curr_col, ascending=False)

        # Biểu đồ so sánh tỉ trọng kỳ này vs kỳ trước cho Top khách hàng
        top_share_m = m_month.head(10).copy()
        fig_share_m = go.Figure()
        fig_share_m.add_trace(go.Bar(
            x=top_share_m['owner_name'],
            y=top_share_m[share_curr_col],
            name=f"{t['col_share_current']} ({m_curr})",
            marker_color="#8b5cf6"
        ))
        if m_prev:
            fig_share_m.add_trace(go.Bar(
                x=top_share_m['owner_name'],
                y=top_share_m[share_prev_col],
                name=f"{t['col_share_prev']} ({m_prev})",
                marker_color="#64748b"
            ))
        fig_share_m.update_layout(
            barmode='group',
            title=None,
            template="plotly_dark",
            paper_bgcolor="#1e293b",
            plot_bgcolor="#0f172a",
            margin=dict(l=30, r=30, t=10, b=80),
            legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(color="#94a3b8")),
            yaxis=dict(title=t['col_share_current'], color="#94a3b8"),
            xaxis=dict(color="#94a3b8")
        )
        st.plotly_chart(fig_share_m, use_container_width=True)

        # Bảng dữ liệu chi tiết
        display_m_month = pd.DataFrame({
            t['lbl_customer_col']: m_month['owner_name'],
            t['col_period_current']: m_month[val_curr_col].apply(lambda v: f"{v:,.0f}"),
            t['col_share_current']: m_month[share_curr_col].apply(lambda v: f"{v:.2f}%"),
            t['col_period_prev']: m_month[val_prev_col].apply(lambda v: f"{v:,.0f}"),
            t['col_share_prev']: m_month[share_prev_col].apply(lambda v: f"{v:.2f}%"),
            t['col_share_delta']: m_month[delta_share_col].apply(lambda v: f"+{v:.2f}%" if v > 0 else f"{v:.2f}%"),
            t['col_growth_rate']: m_month[growth_col].apply(lambda v: f"+{v:.1f}%" if v > 0 else (f"{v:.1f}%" if v < 0 else "0.0%")),
        })

        tot_m_curr = m_month[val_curr_col].sum()
        tot_m_prev = m_month[val_prev_col].sum()
        tot_m_growth = ((tot_m_curr - tot_m_prev) / tot_m_prev * 100) if tot_m_prev > 0 else 0
        summary_m_row = {
            t['lbl_customer_col']: t['lbl_total_row'],
            t['col_period_current']: f"{tot_m_curr:,.0f}",
            t['col_share_current']: "100.00%",
            t['col_period_prev']: f"{tot_m_prev:,.0f}",
            t['col_share_prev']: "100.00%",
            t['col_share_delta']: "0.00%",
            t['col_growth_rate']: f"{tot_m_growth:+.1f}%"
        }
        display_m_month = pd.concat([display_m_month, pd.DataFrame([summary_m_row])], ignore_index=True)

        st.dataframe(display_m_month, width="stretch", hide_index=True)

        col_dl_m_share, _ = st.columns([1.5, 2.5])
        with col_dl_m_share:
            csv_m_share_bytes = display_m_month.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                label=t['btn_download_share_csv'],
                data=csv_m_share_bytes,
                file_name=f"customer_share_month_{m_curr}.csv",
                mime="text/csv",
                use_container_width=True,
                key="btn_dl_month_share"
            )
    else:
        st.info(t['no_data'])

# ==========================================
# KHÂU DỰ KIẾN (BACKLOG & PLANNING)
# ==========================================
def render_backlog_report(cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str):
    st.markdown(f'<div class="section-title">{t["sec_backlog"]}</div>', unsafe_allow_html=True)
    
    with st.spinner("..."):
        try:
            sync_st = get_global_sync_state()
            cur_v = sync_st.get("data_version", 0.0)
            df_backlog, df_events = fetch_backlog_data(cache_version=cur_v)
        except Exception as e:
            st.error(f"Lỗi tải dữ liệu Backlog: {e}")
            return

    df_bl_filtered = filter_by_customer(df_backlog, cust_type, cust_name)

    tot_bl_qty = df_bl_filtered['backlog_qty'].sum() if not df_bl_filtered.empty else 0
    tot_bl_orders = df_bl_filtered['backlog_orders'].sum() if not df_bl_filtered.empty else 0
    num_bl_owners = df_bl_filtered['owner_name'].nunique() if not df_bl_filtered.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_bl_qty']}</div>
            <div class="kpi-value" style="color: #f59e0b;">{tot_bl_qty:,.0f}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_selected_period']}</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_bl_orders']}</div>
            <div class="kpi-value">{tot_bl_orders:,}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_orders_recorded']}</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_bl_partners']}</div>
            <div class="kpi-value">{num_bl_owners}</div>
            <div class="kpi-delta delta-neutral">• {t['lbl_active_partners']}</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{t['lbl_bl_event']}</div>
            <div class="kpi-value" style="color: #c084fc;">Double Day</div>
            <div class="kpi-delta delta-neutral">• Mega Campaign</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col_bl_chart, col_bl_events = st.columns([2, 1])

    with col_bl_chart:
        st.markdown(f'<div class="chart-title-banner">{t["lbl_bl_chart"]}</div>', unsafe_allow_html=True)
        if not df_bl_filtered.empty:
            bl_by_cust = df_bl_filtered.groupby(['owner_name', 'cargo_type'], as_index=False).agg({
                'backlog_qty': 'sum'
            }).sort_values('backlog_qty', ascending=False).head(10)

            fig_bl = px.bar(
                bl_by_cust,
                x='owner_name',
                y='backlog_qty',
                color='cargo_type',
                labels={'owner_name': t['lbl_customer_col'], 'backlog_qty': t['lbl_total_qty'], 'cargo_type': t['filter_cust_type']},
                template="plotly_dark"
            )
            fig_bl.update_layout(
                title=None,
                paper_bgcolor="#1e293b",
                plot_bgcolor="#0f172a",
                margin=dict(l=30, r=30, t=10, b=80),
                legend=dict(orientation="h", yanchor="top", y=-0.22, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_bl, use_container_width=True)
        else:
            st.info(t["no_data"])

    with col_bl_events:
        st.markdown(f'<div class="chart-title-banner">{t["lbl_bl_event_title"]}</div>', unsafe_allow_html=True)
        if not df_events.empty:
            df_ev_display = df_events.copy()
            df_ev_display.columns = [t['col_event_date'], t['col_event_name']]
            st.dataframe(df_ev_display, width="stretch", hide_index=True)
        else:
            st.info(t["no_data"])


# ==========================================
# KHÂU DỰ KIẾN (HEADCOUNT & CAPACITY PLANNING)
# ==========================================
def render_headcount_planning(cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str):
    # Tiêu đề bảng
    st.markdown(f'<div style="text-align: center; font-size: 26px; font-weight: 800; color: #ffffff; letter-spacing: 2px; text-transform: uppercase; margin: 10px 0 20px 0;">{t["plan_table_title"]}</div>', unsafe_allow_html=True)

    # 1. BỘ CHỌN TUẦN ISO: BAO GỒM CẢ TUẦN TƯƠNG LAI, HIỆN TẠI VÀ QUÁ KHỨ (KHÔNG CÓ NÚT LÀM MỚI)
    today_vn = datetime.datetime.now(VN_TIMEZONE).date()
    monday_curr = today_vn - datetime.timedelta(days=today_vn.weekday())

    iso_weeks_options = []
    default_idx = 4  # Mặc định trỏ vào Tuần hiện tại
    for i in range(-4, 9):
        m_start = monday_curr - datetime.timedelta(weeks=i)
        s_end = m_start + datetime.timedelta(days=6)
        iso_year, iso_week, _ = m_start.isocalendar()
        if i == 0:
            tag = " [Tuần này]"
        elif i < 0:
            tag = " [Tương lai]"
        else:
            tag = ""
        lbl = f"Tuần {iso_week} ({m_start.strftime('%d/%m')} - {s_end.strftime('%d/%m/%Y')}){tag}"
        iso_weeks_options.append((lbl, m_start))

    selected_week_tuple = st.selectbox(
        t["plan_select_week"],
        options=iso_weeks_options,
        index=default_idx,
        format_func=lambda x: x[0],
        key="iso_week_selector"
    )
    selected_monday = selected_week_tuple[1]

    # 7 ngày liên tiếp từ Thứ 2 đến Chủ Nhật của tuần ISO được chọn
    week_dates = [selected_monday + datetime.timedelta(days=i) for i in range(7)]
    start_str = week_dates[0].strftime('%Y-%m-%d')
    end_str = week_dates[-1].strftime('%Y-%m-%d')

    dow_names = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
    date_cols = [f"{dow_names[i]} {week_dates[i].strftime('%d/%m')}" for i in range(7)]

    # 2. TRUY VẤN DỮ LIỆU THỰC TẾ & BASELINE TỪ BIGQUERY
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    with st.spinner("..."):
        try:
            weekly_actual = fetch_weekly_all_clients(start_str, end_str, cache_version=cur_v)
            dow_baselines = fetch_dow_baselines(cache_version=cur_v)
        except Exception:
            weekly_actual = {}
            dow_baselines = {}

    # BẢNG THIẾT LẬP ĐỊNH BIÊN CÁC KHÂU NGOÀI 2C (SỐ CỐ ĐỊNH MỚI: 12, 12, 15, 10, 6, 6, 12)
    with st.expander(t["plan_fixed_header"], expanded=False):
        st.caption(t["plan_fixed_note"])
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"**Kho & Nhập hàng**")
            hc_rec = st.number_input(t['task_receiving'], min_value=0, value=12, step=1, key="hc_iso_rec")
            hc_ret = st.number_input(t['task_returns'], min_value=0, value=12, step=1, key="hc_iso_ret")
        with c2:
            st.markdown(f"**Tồn kho & Vận hành**")
            hc_inv = st.number_input(t['task_inventory'], min_value=0, value=15, step=1, key="hc_iso_inv")
            hc_frk = st.number_input(t['task_forklift'], min_value=0, value=10, step=1, key="hc_iso_frk")
        with c3:
            st.markdown(f"**Bàn giao & Dự án B2B**")
            hc_hnd = st.number_input(t['task_handover'], min_value=0, value=6, step=1, key="hc_iso_hnd")
            hc_chint_env = st.number_input(t['task_chint_envision'], min_value=0, value=6, step=1, key="hc_iso_chint_env")
        with c4:
            st.markdown(f"**Khách hàng KKV**")
            hc_kkv = st.number_input(t['task_kkv'], min_value=0, value=12, step=1, key="hc_iso_kkv")

        st.markdown("<div style='height: 1px; background: #334155; margin: 12px 0 8px 0;'></div>", unsafe_allow_html=True)
        st.markdown(f"**Quy chuẩn Định biên Nhặt hàng & Đóng gói B2C**")
        cp1, cp2, cp3 = st.columns(3)
        with cp1:
            max_pack_hc = st.number_input(
                "Trần nhân sự Đóng gói tối đa (Max Pack HC)",
                min_value=1,
                value=25,
                step=1,
                key="hc_max_pack",
                help="Định biên nhân sự cứng tối đa của khâu Pack (thường không quá 25, nếu thiếu sẽ mượn khâu khác)"
            )
        with cp2:
            pack_ratio_pct = st.number_input(
                "Tỉ lệ Pack cao hơn Pick (%)",
                min_value=0,
                value=20,
                step=5,
                key="hc_pack_ratio",
                help="Pack thường cao hơn Pick 20% (ví dụ: pack 24 thì pick 20, pack 25 thì pick 21)"
            )
        with cp3:
            pack_base_uph = st.number_input(
                "Định mức UPH Đóng gói cơ sở (đơn/h)",
                min_value=10.0,
                value=54.0,
                step=1.0,
                key="hc_pack_uph",
                help="Định mức UPH cơ sở tính cho ca 8 tiếng trước khi áp dụng trần"
            )

    # 3. TẬP HỢP TẤT CẢ KHÁCH HÀNG 2C VÀ 2B
    all_client_names = set()
    for (d_str, c_name) in weekly_actual.keys():
        all_client_names.add(c_name)
    for c_name in dow_baselines.keys():
        all_client_names.add(c_name)

    b2c_clients = []
    b2b_clients = []
    for c_name in all_client_names:
        c_type = "2C"
        if c_name in dow_baselines:
            c_type = dow_baselines[c_name].get("type", "2C")
        elif any(k[1] == c_name and weekly_actual[k].get("type") == "2B" for k in weekly_actual):
            c_type = "2B"
        
        if c_type == "2B":
            b2b_clients.append(c_name)
        else:
            b2c_clients.append(c_name)

    # Tính tổng sản lượng dự kiến tuần để sắp xếp khách hàng lớn lên trên
    def get_client_weekly_fc(c_name):
        tot_fc = 0
        b_info = dow_baselines.get(c_name, {}).get("dows", {})
        for i, d in enumerate(week_dates):
            dow = (d.weekday() + 1) % 7 + 1
            tot_fc += b_info.get(dow, (0, 0.0))[0]
        return tot_fc

    b2c_clients = sorted(b2c_clients, key=lambda c: get_client_weekly_fc(c), reverse=True)
    b2b_clients = sorted(b2b_clients, key=lambda c: get_client_weekly_fc(c), reverse=True)

    # Cấu trúc dữ liệu theo từng khách hàng
    clients_data = {c: {"fc_ords": [], "act_ords": [], "fc_pcs": [], "act_pcs": []} for c in all_client_names}

    daily_total_2c_ords_act = []
    daily_total_2c_ords_fc = []

    daily_pick_2c = []
    daily_pack_2c = []
    daily_borrow_2c = []

    ratio_factor = 1.0 + (pack_ratio_pct / 100.0)

    for i, d in enumerate(week_dates):
        d_str = d.strftime('%Y-%m-%d')
        dow = (d.weekday() + 1) % 7 + 1

        day_tot_2c_fc_o = 0
        day_tot_2c_act_o = 0
        has_any_2c_act = False

        for c in all_client_names:
            b_info = dow_baselines.get(c, {}).get("dows", {})
            base_o, base_p = b_info.get(dow, (0, 0.0))
            clients_data[c]["fc_ords"].append(base_o)
            clients_data[c]["fc_pcs"].append(base_p)

            if c in b2c_clients:
                day_tot_2c_fc_o += base_o

            act_info = weekly_actual.get((d_str, c))
            if act_info is not None and act_info["orders"] > 0:
                clients_data[c]["act_ords"].append(act_info["orders"])
                clients_data[c]["act_pcs"].append(act_info["pcs"])
                if c in b2c_clients:
                    day_tot_2c_act_o += act_info["orders"]
                    has_any_2c_act = True
            else:
                clients_data[c]["act_ords"].append(None)
                clients_data[c]["act_pcs"].append(None)

        if has_any_2c_act:
            daily_total_2c_ords_act.append(day_tot_2c_act_o)
            ord_for_hc = day_tot_2c_act_o
        else:
            daily_total_2c_ords_act.append(None)
            ord_for_hc = day_tot_2c_fc_o

        daily_total_2c_ords_fc.append(day_tot_2c_fc_o)

        # Tính toán định biên Pick & Pack 2C theo quy tắc vận hành thực tế:
        # 1. Nhu cầu Pack theo UPH cơ sở (mặc định 54 đơn/giờ)
        raw_pack_hc = math.ceil(ord_for_hc / (pack_base_uph * 8.0)) if ord_for_hc > 0 else 0
        
        # 2. Áp dụng trần Pack tối đa (mặc định 25, không vượt quá)
        pack_hc = min(max_pack_hc, raw_pack_hc)
        
        # 3. Pack thường cao hơn Pick 20% (ví dụ pack 24 thì pick 20, pack 25 thì pick 21)
        pick_hc = round(pack_hc / ratio_factor) if pack_hc > 0 else 0
        
        # 4. Ghi nhận số nhân sự thiếu cần mượn ngoài vào ngày cao điểm
        borrow_hc = max(0, raw_pack_hc - max_pack_hc)
        daily_borrow_2c.append(borrow_hc)

        daily_pick_2c.append(pick_hc)
        daily_pack_2c.append(pack_hc)

    # 4. QUY TẮC NHÂN SỰ CHỦ NHẬT:
    # "các ngày chủ nhật thông thường chỉ có xe nâng (2 người), 2C, bàn giao (2 người) đi làm thôi"
    vals_rec = [hc_rec]*6 + [0]
    vals_ret = [hc_ret]*6 + [0]
    vals_inv = [hc_inv]*6 + [0]
    vals_frk = [hc_frk]*6 + [2]
    vals_hnd = [hc_hnd]*6 + [2]
    vals_chint_env = [hc_chint_env]*6 + [0]
    vals_kkv = [hc_kkv]*6 + [0]

    daily_total_hcs = [
        vals_rec[i] + vals_ret[i] + vals_inv[i] + vals_frk[i] + vals_hnd[i] +
        daily_pick_2c[i] + daily_pack_2c[i] + vals_chint_env[i] + vals_kkv[i]
        for i in range(7)
    ]

    # ĐÃ BỎ 4 BOX KPI PHÍA DƯỚI THEO YÊU CẦU NGƯỜI DÙNG

    # 5. BẢNG ĐỊNH BIÊN NHÂN SỰ THEO TUẦN (BỎ CỘT BÌNH QUÂN)
    def make_cell(val, highlight=None):
        hl = ""
        if highlight == "pick":
            hl = "color: #60a5fa; font-weight: 700; background: rgba(59, 130, 246, 0.1);"
        elif highlight == "pack":
            hl = "color: #34d399; font-weight: 700; background: rgba(16, 185, 129, 0.1);"
        elif highlight == "total":
            hl = "color: #38bdf8; font-weight: 800; background: rgba(37, 99, 235, 0.25); font-size: 14px;"
        else:
            hl = "color: #f1f5f9; font-weight: 500;"
        return f'<td style="padding: 10px 12px; text-align: center; border: 1px solid #334155; {hl}">{val}</td>'

    stages_data = [
        {"name": t['task_receiving'], "values": vals_rec, "hl": None},
        {"name": t['task_returns'], "values": vals_ret, "hl": None},
        {"name": t['task_inventory'], "values": vals_inv, "hl": None},
        {"name": t['task_forklift'], "values": vals_frk, "hl": None},
        {"name": t['task_handover'], "values": vals_hnd, "hl": None},
        {"name": t['task_pick_2c'], "values": daily_pick_2c, "hl": "pick"},
        {"name": t['task_pack_2c'], "values": daily_pack_2c, "hl": "pack"},
        {"name": t['task_chint_envision'], "values": vals_chint_env, "hl": None},
        {"name": t['task_kkv'], "values": vals_kkv, "hl": None},
    ]

    th_headers = "".join([f'<th style="background:#1d4ed8; color:#ffffff; padding:11px 12px; text-align:center; font-weight:700; border:1px solid #334155; min-width:85px;">{dc}</th>' for dc in date_cols])

    rows_html = ""
    for stg in stages_data:
        vals = stg["values"]
        row_cells = "".join([make_cell(v, stg["hl"]) for v in vals])
        stg_name_style = "color: #60a5fa; font-weight: 700;" if stg["hl"] == "pick" else ("color: #34d399; font-weight: 700;" if stg["hl"] == "pack" else "color: #f8fafc; font-weight: 600;")
        rows_html += f"""<tr>
<td style="background: #1e293b; {stg_name_style} padding: 10px 14px; border: 1px solid #334155;">{stg['name']}</td>
{row_cells}
</tr>"""

    tot_cells = "".join([make_cell(daily_total_hcs[i], "total") for i in range(7)])

    table_html = f"""<div style="overflow-x: auto; border-radius: 8px; border: 1px solid #334155; box-shadow: 0 4px 16px rgba(0,0,0,0.3); margin-top: 10px;">
<table style="width: 100%; border-collapse: collapse; background: #1e293b; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 13px;">
<thead>
<tr style="background: #1d4ed8; color: #ffffff;">
<th style="padding: 11px 14px; text-align: left; font-weight: 700; border: 1px solid #334155; min-width: 240px;">{t['plan_col_stage']}</th>
{th_headers}
</tr>
</thead>
<tbody>
{rows_html}
<tr style="border-top: 2px solid #2563eb;">
<td style="background: #1e3a8a; color: #38bdf8; font-weight: 800; font-size: 14px; padding: 12px 14px; border: 1px solid #334155; text-align: left;">{t['task_total_hc']}</td>
{tot_cells}
</tr>
</tbody>
</table>
</div>"""

    st.html(table_html)

    # 6. BẢNG SẢN LƯỢNG KẾ HOẠCH VÀ THỰC TẾ (LUÔN HIỂN THỊ CHI TIẾT)
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f'<div style="font-size: 18px; font-weight: 700; color: #cbd5e1; margin-top: 6px; margin-bottom: 12px;">{t["sec_volume_comparison"]}</div>', unsafe_allow_html=True)

    # SẮP XẾP LẠI BUTTON GỌN GÀNG: LOẠI SẢN LƯỢNG (2C / 2B) VÀ BỘ LỌC KHÁCH HÀNG
    col_btn_type, col_filter_client = st.columns([1.1, 2.9])
    with col_btn_type:
        st.markdown("<div style='margin-top: 4px;'>", unsafe_allow_html=True)
        vol_view = st.radio(
            "Loại sản lượng",
            options=["2C", "2B"],
            horizontal=True,
            label_visibility="collapsed",
            key="vol_toggle_2c_2b"
        )
        st.markdown("</div>", unsafe_allow_html=True)

    active_target_clients = b2c_clients if vol_view == "2C" else b2b_clients
    total_label = t['lbl_total_b2c'] if vol_view == "2C" else t['lbl_total_b2b']

    with col_filter_client:
        selected_filter_client = st.selectbox(
            t['lbl_filter_client'],
            options=[t['all_option']] + active_target_clients,
            key="vol_dynamic_client_filter"
        )

    # Lọc danh sách khách hàng hiển thị
    display_clients = [selected_filter_client] if selected_filter_client != t['all_option'] else active_target_clients

    vol_th_headers = "".join([f'<th style="background:#0f172a; color:#ffffff; padding:10px 12px; text-align:center; font-weight:700; border:1px solid #334155; min-width:85px; position:sticky; top:0; z-index:10;">{dc}</th>' for dc in date_cols])

    def render_vol_cell(val, is_pcs=False, is_pct=False):
        if val is None or val == "-":
            return '<td style="padding: 7px 12px; text-align: center; border: 1px solid #334155; color: #64748b;">-</td>'
        if is_pct:
            pct_val = float(val)
            color = "#34d399" if pct_val >= 95.0 else ("#fbbf24" if pct_val >= 80.0 else "#f87171")
            return f'<td style="padding: 7px 12px; text-align: center; border: 1px solid #334155; color: {color}; font-weight: 700;">{pct_val:.1f}%</td>'
        fmt = f"{val:,.0f}" if is_pcs else f"{val:,}"
        return f'<td style="padding: 7px 12px; text-align: center; border: 1px solid #334155; color: #f1f5f9; font-weight: 500;">{fmt}</td>'

    def make_vol_row(label, daily_vals, total_val, is_pcs=False, is_pct=False, row_bg="#1e293b", lbl_color="#94a3b8"):
        cells = "".join([render_vol_cell(v, is_pcs, is_pct) for v in daily_vals])
        tot_cell = render_vol_cell(total_val, is_pcs, is_pct)
        return f"""<tr>
<td style="background: {row_bg}; color: {lbl_color}; font-weight: 600; padding: 7px 14px; border: 1px solid #334155;">{label}</td>
{cells}
{tot_cell}
</tr>"""

    def render_detailed_client_block(c_name, c_data, header_bg="#1e3a8a"):
        fc_o, act_o = c_data["fc_ords"], c_data["act_ords"]
        fc_p, act_p = c_data["fc_pcs"], c_data["act_pcs"]

        pct_o = [(act_o[i] / fc_o[i] * 100) if (act_o[i] is not None and fc_o[i] > 0) else None for i in range(7)]
        pct_p = [(act_p[i] / fc_p[i] * 100) if (act_p[i] is not None and fc_p[i] > 0) else None for i in range(7)]

        valid_o = [v for v in act_o if v is not None]
        tot_act_o = sum(valid_o) if valid_o else None
        valid_p = [v for v in act_p if v is not None]
        tot_act_p = sum(valid_p) if valid_p else None

        tot_fc_o = sum(fc_o)
        tot_fc_p = sum(fc_p)

        tot_pct_o = (tot_act_o / tot_fc_o * 100) if (tot_act_o is not None and tot_fc_o > 0) else None
        tot_pct_p = (tot_act_p / tot_fc_p * 100) if (tot_act_p is not None and tot_fc_p > 0) else None

        return (
            f'<tr><td colspan="9" style="background:{header_bg}; color:#ffffff; font-weight:700; padding:8px 14px; font-size:13px; border: 1px solid #334155;">{c_name}</td></tr>' +
            make_vol_row(t['lbl_fc_orders'], fc_o, tot_fc_o, False, False, "#1e293b", "#38bdf8") +
            make_vol_row(t['lbl_act_orders'], act_o, tot_act_o, False, False, "#1e293b", "#f1f5f9") +
            make_vol_row(t['lbl_pct_orders'], pct_o, tot_pct_o, False, True, "#0f172a", "#fbbf24") +
            make_vol_row(t['lbl_fc_pcs'], fc_p, tot_fc_p, True, False, "#1e293b", "#60a5fa") +
            make_vol_row(t['lbl_act_pcs'], act_p, tot_act_p, True, False, "#1e293b", "#f1f5f9") +
            make_vol_row(t['lbl_pct_pcs'], pct_p, tot_pct_p, False, True, "#0f172a", "#fbbf24")
        )

    # TÍNH TOÁN DÒNG TỔNG CỘNG TOÀN BỘ CHO 2C HOẶC 2B
    tot_fc_ords = [sum([clients_data[c]["fc_ords"][i] for c in active_target_clients]) for i in range(7)]
    tot_fc_pcss = [sum([clients_data[c]["fc_pcs"][i] for c in active_target_clients]) for i in range(7)]

    tot_act_ords = []
    tot_act_pcss = []
    for i in range(7):
        acts_o = [clients_data[c]["act_ords"][i] for c in active_target_clients if clients_data[c]["act_ords"][i] is not None]
        acts_p = [clients_data[c]["act_pcs"][i] for c in active_target_clients if clients_data[c]["act_pcs"][i] is not None]
        tot_act_ords.append(sum(acts_o) if acts_o else None)
        tot_act_pcss.append(sum(acts_p) if acts_p else None)

    tot_pct_ords = [(tot_act_ords[i] / tot_fc_ords[i] * 100) if (tot_act_ords[i] is not None and tot_fc_ords[i] > 0) else None for i in range(7)]
    tot_pct_pcss = [(tot_act_pcss[i] / tot_fc_pcss[i] * 100) if (tot_act_pcss[i] is not None and tot_fc_pcss[i] > 0) else None for i in range(7)]

    grand_act_o = sum([v for v in tot_act_ords if v is not None]) if any(v is not None for v in tot_act_ords) else None
    grand_act_p = sum([v for v in tot_act_pcss if v is not None]) if any(v is not None for v in tot_act_pcss) else None
    grand_fc_o = sum(tot_fc_ords)
    grand_fc_p = sum(tot_fc_pcss)

    grand_pct_o = (grand_act_o / grand_fc_o * 100) if (grand_act_o is not None and grand_fc_o > 0) else None
    grand_pct_p = (grand_act_p / grand_fc_p * 100) if (grand_act_p is not None and grand_fc_p > 0) else None

    # LUÔN HIỂN THỊ CHI TIẾT TẤT CẢ KHÁCH HÀNG (MỖI KHÁCH 6 DÒNG)
    detail_rows_html = ""
    for c_name in display_clients:
        detail_rows_html += render_detailed_client_block(c_name, clients_data[c_name], header_bg="#1e3a8a")

    total_block_html = (
        f'<tr><td colspan="9" style="background:#1d4ed8; color:#ffffff; font-weight:800; padding:10px 14px; font-size:14px; border: 1px solid #334155; border-bottom: 2px solid #2563eb;">{total_label}</td></tr>' +
        make_vol_row(t['lbl_fc_orders'], tot_fc_ords, grand_fc_o, False, False, "#1e293b", "#38bdf8") +
        make_vol_row(t['lbl_act_orders'], tot_act_ords, grand_act_o, False, False, "#1e293b", "#f1f5f9") +
        make_vol_row(t['lbl_pct_orders'], tot_pct_ords, grand_pct_o, False, True, "#0f172a", "#fbbf24") +
        make_vol_row(t['lbl_fc_pcs'], tot_fc_pcss, grand_fc_p, True, False, "#1e293b", "#60a5fa") +
        make_vol_row(t['lbl_act_pcs'], tot_act_pcss, grand_act_p, True, False, "#1e293b", "#f1f5f9") +
        make_vol_row(t['lbl_pct_pcs'], tot_pct_pcss, grand_pct_p, False, True, "#0f172a", "#fbbf24")
    )
    vol_table_body = total_block_html + detail_rows_html

    vol_table_html = f"""<div style="overflow-x: auto; max-height: 600px; overflow-y: auto; border-radius: 8px; border: 1px solid #334155; box-shadow: 0 4px 16px rgba(0,0,0,0.3); margin-top: 6px;">
<table style="width: 100%; border-collapse: collapse; background: #1e293b; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; font-size: 13px;">
<thead>
<tr style="background: #0f172a; color: #ffffff;">
<th style="padding: 10px 14px; text-align: left; font-weight: 700; border: 1px solid #334155; min-width: 240px; position: sticky; top: 0; z-index: 10; background: #0f172a;">{t['lbl_metric_col']}</th>
{vol_th_headers}
<th style="padding: 10px 12px; text-align: center; font-weight: 700; border: 1px solid #334155; min-width: 110px; background: #1e3a8a; position: sticky; top: 0; z-index: 10;">{t['lbl_total_week']}</th>
</tr>
</thead>
<tbody>
{vol_table_body}
</tbody>
</table>
</div>"""

    st.html(vol_table_html)

    # 7. NÚT XUẤT CSV
    st.markdown("<div style='margin-top: 16px;'></div>", unsafe_allow_html=True)
    c_dl1, c_dl2 = st.columns([1, 1])
    with c_dl1:
        csv_rows = []
        for stg in stages_data:
            csv_rows.append({"Khâu vận hành": stg["name"], **{date_cols[i]: stg["values"][i] for i in range(7)}})
        csv_rows.append({"Khâu vận hành": t['task_total_hc'], **{date_cols[i]: daily_total_hcs[i] for i in range(7)}})
        df_export_hc = pd.DataFrame(csv_rows)
        csv_bytes_hc = df_export_hc.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label=t['lbl_download_csv'],
            data=csv_bytes_hc,
            file_name=f"WMS_Headcount_ISO_Week_{start_str}_to_{end_str}.csv",
            mime="text/csv",
            use_container_width=True
        )
    with c_dl2:
        vol_export_rows = []
        vol_export_rows.append({"Khách hàng": total_label, "Chỉ số": t['lbl_fc_orders'], **{date_cols[i]: tot_fc_ords[i] for i in range(7)}, t['lbl_total_week']: grand_fc_o})
        vol_export_rows.append({"Khách hàng": total_label, "Chỉ số": t['lbl_act_orders'], **{date_cols[i]: (tot_act_ords[i] if tot_act_ords[i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (grand_act_o if grand_act_o is not None else "-")})
        vol_export_rows.append({"Khách hàng": total_label, "Chỉ số": t['lbl_fc_pcs'], **{date_cols[i]: tot_fc_pcss[i] for i in range(7)}, t['lbl_total_week']: grand_fc_p})
        vol_export_rows.append({"Khách hàng": total_label, "Chỉ số": t['lbl_act_pcs'], **{date_cols[i]: (tot_act_pcss[i] if tot_act_pcss[i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (grand_act_p if grand_act_p is not None else "-")})
        for c in display_clients:
            vol_export_rows.append({"Khách hàng": c, "Chỉ số": t['lbl_fc_orders'], **{date_cols[i]: clients_data[c]["fc_ords"][i] for i in range(7)}, t['lbl_total_week']: sum(clients_data[c]["fc_ords"])})
            vol_export_rows.append({"Khách hàng": c, "Chỉ số": t['lbl_act_orders'], **{date_cols[i]: (clients_data[c]["act_ords"][i] if clients_data[c]["act_ords"][i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (sum([v for v in clients_data[c]["act_ords"] if v is not None]) if any(v is not None for v in clients_data[c]["act_ords"]) else "-")})
            vol_export_rows.append({"Khách hàng": c, "Chỉ số": t['lbl_fc_pcs'], **{date_cols[i]: clients_data[c]["fc_pcs"][i] for i in range(7)}, t['lbl_total_week']: sum(clients_data[c]["fc_pcs"])})
            vol_export_rows.append({"Khách hàng": c, "Chỉ số": t['lbl_act_pcs'], **{date_cols[i]: (clients_data[c]["act_pcs"][i] if clients_data[c]["act_pcs"][i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (sum([v for v in clients_data[c]["act_pcs"] if v is not None]) if any(v is not None for v in clients_data[c]["act_pcs"]) else "-")})
        df_export_vol = pd.DataFrame(vol_export_rows)
        csv_bytes_vol = df_export_vol.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label=t['lbl_download_vol_csv'],
            data=csv_bytes_vol,
            file_name=f"WMS_{vol_view}_Volume_Breakdown_ISO_Week_{start_str}_to_{end_str}.csv",
            mime="text/csv",
            use_container_width=True
        )


# ==========================================
# KHÂU TỔNG HỢP (PIPELINE SUMMARY)
# ==========================================
def render_pipeline_summary(cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str, start_time: datetime.time = datetime.time(0, 0), end_time: datetime.time = datetime.time(23, 59)):
    st.markdown(f'<div class="section-title">{t["sec_pipeline"]}</div>', unsafe_allow_html=True)
    
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)

    # 1. Fetch và tính toán dữ liệu các khâu trọng tâm (Skip khâu Lên Kệ theo yêu cầu)
    # Khâu NHẬP HÀNG (Purchase Inbound)
    df_rec = pd.DataFrame()
    try:
        raw_rec = fetch_stage_data("NHẬP HÀNG", cache_version=cur_v)
        df_rec = filter_by_date_range(filter_by_customer(raw_rec, cust_type, cust_name), start_date, end_date, start_time, end_time)
    except Exception:
        pass
    rec_orders = int(df_rec['total_orders'].sum()) if not df_rec.empty else 0
    rec_qty = float(df_rec['total_qty'].sum()) if not df_rec.empty else 0.0

    # Khâu HOÀN TRẢ (Return Inbound)
    df_ret = pd.DataFrame()
    try:
        raw_ret = fetch_stage_data("HOÀN TRẢ", cache_version=cur_v)
        df_ret = filter_by_date_range(filter_by_customer(raw_ret, cust_type, cust_name), start_date, end_date, start_time, end_time)
    except Exception:
        pass
    ret_orders = int(df_ret['total_orders'].sum()) if not df_ret.empty else 0
    ret_qty = float(df_ret['total_qty'].sum()) if not df_ret.empty else 0.0

    # Khâu TRONG KHO (Kiểm kê tồn kho & đối soát chênh lệch)
    df_inv = pd.DataFrame()
    try:
        raw_inv = fetch_inventory_data(cache_version=cur_v)
        if not raw_inv.empty:
            df_inv = filter_by_customer(raw_inv, cust_type, cust_name)
            df_inv = filter_by_date_range(df_inv, start_date, end_date, start_time, end_time)
    except Exception:
        pass
    inv_diff_qty = int(df_inv['difference_qty'].sum()) if not df_inv.empty else 0
    inv_actual_qty = int(df_inv['actual_qty'].sum()) if not df_inv.empty else 0
    inv_system_qty = int(df_inv['system_qty'].sum()) if not df_inv.empty else 0
    inv_tasks = df_inv[df_inv['inventory_check_no'] != 'Chưa có mã']['inventory_check_no'].nunique() if not df_inv.empty else 0

    # Khâu NHẶT HÀNG (Picking)
    df_pick = pd.DataFrame()
    try:
        raw_pick = fetch_stage_data("NHẶT HÀNG", cache_version=cur_v)
        df_pick = filter_by_date_range(filter_by_customer(raw_pick, cust_type, cust_name), start_date, end_date, start_time, end_time)
    except Exception:
        pass
    pick_orders = int(df_pick['total_orders'].sum()) if not df_pick.empty else 0
    pick_qty = float(df_pick['total_qty'].sum()) if not df_pick.empty else 0.0

    # Khâu ĐÓNG GÓI (Packing)
    df_pack = pd.DataFrame()
    try:
        raw_pack = fetch_stage_data("ĐÓNG GÓI", cache_version=cur_v)
        df_pack = filter_by_date_range(filter_by_customer(raw_pack, cust_type, cust_name), start_date, end_date, start_time, end_time)
    except Exception:
        pass
    pack_orders = int(df_pack['total_orders'].sum()) if not df_pack.empty else 0
    pack_qty = float(df_pack['total_qty'].sum()) if not df_pack.empty else 0.0

    # Khâu BÀN GIAO (Carrier Handover)
    df_hnd = pd.DataFrame()
    try:
        raw_hnd = fetch_stage_data("BÀN GIAO", cache_version=cur_v)
        df_hnd = filter_by_date_range(filter_by_customer(raw_hnd, cust_type, cust_name), start_date, end_date, start_time, end_time)
    except Exception:
        pass
    hnd_orders = int(df_hnd['total_orders'].sum()) if not df_hnd.empty else 0

    # ==========================================
    # 2. KHỐI 6 THẺ KPI TRỌNG TÂM CÁC KHÂU
    # ==========================================
    # Hàng 1: Luồng Nhập & Tồn Kho (Inbound & Inventory)
    c1, c2, c3 = st.columns(3)
    with c1:
        avg_rec_str = f" • {rec_qty/rec_orders:.1f} PCS/đơn" if rec_orders > 0 else ""
        st.markdown(f"""
        <div class="kpi-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="kpi-label">{t['summary_rec_title']}</span>
                <span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(59, 130, 246, 0.18); color: #60a5fa;">Inbound NCC</span>
            </div>
            <div class="kpi-value">{rec_qty:,.0f} <span style="font-size: 14px; font-weight: 500; color: #94a3b8;">PCS</span></div>
            <div style="font-size: 12px; color: #cbd5e1; margin-bottom: 4px;">
                Tổng đơn nhập: <b>{rec_orders:,}</b> đơn{avg_rec_str}
            </div>
            <div class="kpi-delta delta-neutral">• {t['summary_rec_sub']}</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        avg_ret_str = f" • {ret_qty/ret_orders:.1f} PCS/đơn" if ret_orders > 0 else ""
        st.markdown(f"""
        <div class="kpi-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="kpi-label">{t['summary_ret_title']}</span>
                <span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(245, 158, 11, 0.18); color: #fbbf24;">Reverse</span>
            </div>
            <div class="kpi-value">{ret_qty:,.0f} <span style="font-size: 14px; font-weight: 500; color: #94a3b8;">PCS</span></div>
            <div style="font-size: 12px; color: #cbd5e1; margin-bottom: 4px;">
                Tổng đơn hoàn: <b>{ret_orders:,}</b> đơn{avg_ret_str}
            </div>
            <div class="kpi-delta delta-neutral">• {t['summary_ret_sub']}</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        if inv_diff_qty > 0:
            diff_display = f'<span style="color: #ef4444;">+{inv_diff_qty:,}</span>'
            diff_badge = '<span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(239, 68, 68, 0.18); color: #f87171;">Lệch tồn</span>'
        elif inv_diff_qty < 0:
            diff_display = f'<span style="color: #ef4444;">{inv_diff_qty:,}</span>'
            diff_badge = '<span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(239, 68, 68, 0.18); color: #f87171;">Lệch tồn</span>'
        else:
            diff_display = '<span style="color: #10b981;">0</span>'
            diff_badge = '<span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(16, 185, 129, 0.18); color: #34d399;">Khớp tồn</span>'

        inv_detail_str = f"Thực tế: <b>{inv_actual_qty:,}</b> | Sổ sách: <b>{inv_system_qty:,}</b> PCS"
        inv_sub_str = f"• {inv_tasks:,} nhiệm vụ kiểm kê"

        st.markdown(f"""
        <div class="kpi-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="kpi-label">{t['summary_inv_title']}</span>
                {diff_badge}
            </div>
            <div class="kpi-value">{diff_display} <span style="font-size: 14px; font-weight: 500; color: #94a3b8;">PCS</span></div>
            <div style="font-size: 12px; color: #cbd5e1; margin-bottom: 4px;">{inv_detail_str}</div>
            <div class="kpi-delta delta-neutral">{inv_sub_str}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)

    # Hàng 2: Luồng Xuất & Bàn Giao (Outbound & Handover)
    c4, c5, c6 = st.columns(3)
    with c4:
        avg_pick_str = f" • {pick_qty/pick_orders:.1f} PCS/đơn" if pick_orders > 0 else ""
        st.markdown(f"""
        <div class="kpi-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="kpi-label">{t['summary_pick_title']}</span>
                <span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(16, 185, 129, 0.18); color: #34d399;">Wave Picking</span>
            </div>
            <div class="kpi-value">{pick_qty:,.0f} <span style="font-size: 14px; font-weight: 500; color: #94a3b8;">PCS</span></div>
            <div style="font-size: 12px; color: #cbd5e1; margin-bottom: 4px;">
                Tổng đơn nhặt: <b>{pick_orders:,}</b> đơn{avg_pick_str}
            </div>
            <div class="kpi-delta delta-neutral">• {t['summary_pick_sub']}</div>
        </div>
        """, unsafe_allow_html=True)

    with c5:
        avg_pack_str = f" • {pack_qty/pack_orders:.1f} PCS/đơn" if pack_orders > 0 else ""
        st.markdown(f"""
        <div class="kpi-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="kpi-label">{t['summary_pack_title']}</span>
                <span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(139, 92, 246, 0.18); color: #a78bfa;">Packing</span>
            </div>
            <div class="kpi-value">{pack_qty:,.0f} <span style="font-size: 14px; font-weight: 500; color: #94a3b8;">PCS</span></div>
            <div style="font-size: 12px; color: #cbd5e1; margin-bottom: 4px;">
                Tổng đơn đóng: <b>{pack_orders:,}</b> đơn{avg_pack_str}
            </div>
            <div class="kpi-delta delta-neutral">• {t['summary_pack_sub']}</div>
        </div>
        """, unsafe_allow_html=True)

    with c6:
        hnd_rate_str = f"Tỉ lệ bàn giao/đóng: <b>{hnd_orders/pack_orders*100:.1f}%</b>" if pack_orders > 0 else "Hoàn tất bàn giao"
        st.markdown(f"""
        <div class="kpi-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                <span class="kpi-label">{t['summary_hnd_title']}</span>
                <span style="font-size: 10.5px; font-weight: 600; padding: 2px 7px; border-radius: 4px; background: rgba(236, 72, 153, 0.18); color: #f472b6;">Handover</span>
            </div>
            <div class="kpi-value">{hnd_orders:,} <span style="font-size: 14px; font-weight: 500; color: #94a3b8;">ĐƠN</span></div>
            <div style="font-size: 12px; color: #cbd5e1; margin-bottom: 4px;">{hnd_rate_str}</div>
            <div class="kpi-delta delta-neutral">• {t['summary_hnd_sub']}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # 3. BẢNG 1: TỔNG HỢP TRỌNG TÂM TỪNG KHÂU VẬN HÀNH (KHÔNG CHART)
    # ==========================================
    st.markdown(f'<div class="section-title">{t["lbl_summary_stage_table"]}</div>', unsafe_allow_html=True)

    summary_stages_rows = [
        {
            t['lbl_summary_stage_col']: "NHẬP HÀNG",
            t['lbl_summary_flow_col']: "Inbound (Nhập mới)",
            t['lbl_summary_orders_col']: f"{rec_orders:,}",
            t['lbl_summary_qty_col']: f"{rec_qty:,.0f}",
            t['lbl_summary_key_metric']: f"{rec_orders:,} Đơn | {rec_qty:,.0f} PCS",
            t['lbl_summary_notes']: f"Bình quân: {rec_qty/rec_orders:.1f} PCS/đơn" if rec_orders > 0 else "Không phát sinh"
        },
        {
            t['lbl_summary_stage_col']: "HOÀN TRẢ",
            t['lbl_summary_flow_col']: "Reverse (Hoàn kho)",
            t['lbl_summary_orders_col']: f"{ret_orders:,}",
            t['lbl_summary_qty_col']: f"{ret_qty:,.0f}",
            t['lbl_summary_key_metric']: f"{ret_orders:,} Đơn | {ret_qty:,.0f} PCS",
            t['lbl_summary_notes']: f"Bình quân: {ret_qty/ret_orders:.1f} PCS/đơn" if ret_orders > 0 else "Không phát sinh"
        },
        {
            t['lbl_summary_stage_col']: "TRONG KHO",
            t['lbl_summary_flow_col']: "Internal (Kiểm kê tồn)",
            t['lbl_summary_orders_col']: f"{inv_tasks:,} nhiệm vụ",
            t['lbl_summary_qty_col']: f"{inv_actual_qty:,}",
            t['lbl_summary_key_metric']: (f"Chênh lệch: {inv_diff_qty:+d} PCS" if inv_diff_qty != 0 else "Chênh lệch: 0 PCS (Khớp)"),
            t['lbl_summary_notes']: f"Thực tế: {inv_actual_qty:,} | Sổ sách: {inv_system_qty:,} PCS"
        },
        {
            t['lbl_summary_stage_col']: "NHẶT HÀNG",
            t['lbl_summary_flow_col']: "Outbound (Nhặt hàng)",
            t['lbl_summary_orders_col']: f"{pick_orders:,}",
            t['lbl_summary_qty_col']: f"{pick_qty:,.0f}",
            t['lbl_summary_key_metric']: f"{pick_orders:,} Đơn | {pick_qty:,.0f} PCS",
            t['lbl_summary_notes']: f"Bình quân: {pick_qty/pick_orders:.1f} PCS/đơn" if pick_orders > 0 else "Không phát sinh"
        },
        {
            t['lbl_summary_stage_col']: "ĐÓNG GÓI",
            t['lbl_summary_flow_col']: "Outbound (Đóng gói)",
            t['lbl_summary_orders_col']: f"{pack_orders:,}",
            t['lbl_summary_qty_col']: f"{pack_qty:,.0f}",
            t['lbl_summary_key_metric']: f"{pack_orders:,} Đơn | {pack_qty:,.0f} PCS",
            t['lbl_summary_notes']: f"Bình quân: {pack_qty/pack_orders:.1f} PCS/đơn" if pack_orders > 0 else "Không phát sinh"
        },
        {
            t['lbl_summary_stage_col']: "BÀN GIAO",
            t['lbl_summary_flow_col']: "Handover (Giao ĐVVC)",
            t['lbl_summary_orders_col']: f"{hnd_orders:,}",
            t['lbl_summary_qty_col']: "-",
            t['lbl_summary_key_metric']: f"Bàn giao: {hnd_orders:,} Đơn",
            t['lbl_summary_notes']: f"Tỉ lệ bàn giao/đóng: {hnd_orders/pack_orders*100:.1f}%" if pack_orders > 0 else "Hoàn tất bàn giao"
        }
    ]
    df_summary_stages = pd.DataFrame(summary_stages_rows)
    st.dataframe(df_summary_stages, width="stretch", hide_index=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # 4. BẢNG 2: BẢNG ĐỐI SOÁT SẢN LƯỢNG CÁC KHÂU THEO TỪNG KHÁCH HÀNG
    # ==========================================
    st.markdown(f'<div class="section-title">{t["lbl_customer_summary_table"]}</div>', unsafe_allow_html=True)

    all_owners = set()
    owner_cust_type = {}
    for df_temp in [df_rec, df_ret, df_pick, df_pack, df_hnd]:
        if not df_temp.empty and 'owner_name' in df_temp.columns:
            for _, r in df_temp[['owner_name', 'customer_type']].drop_duplicates().iterrows():
                o = r['owner_name']
                if o and o != 'Chưa xác định':
                    all_owners.add(o)
                    owner_cust_type[o] = r['customer_type']

    if all_owners:
        recon_rows = []
        for o in sorted(all_owners):
            c_type = owner_cust_type.get(o, 'B2C')
            o_rec_ord = int(df_rec[df_rec['owner_name'] == o]['total_orders'].sum()) if not df_rec.empty else 0
            o_rec_q = float(df_rec[df_rec['owner_name'] == o]['total_qty'].sum()) if not df_rec.empty else 0.0

            o_ret_ord = int(df_ret[df_ret['owner_name'] == o]['total_orders'].sum()) if not df_ret.empty else 0
            o_ret_q = float(df_ret[df_ret['owner_name'] == o]['total_qty'].sum()) if not df_ret.empty else 0.0

            o_pick_ord = int(df_pick[df_pick['owner_name'] == o]['total_orders'].sum()) if not df_pick.empty else 0
            o_pick_q = float(df_pick[df_pick['owner_name'] == o]['total_qty'].sum()) if not df_pick.empty else 0.0

            o_pack_ord = int(df_pack[df_pack['owner_name'] == o]['total_orders'].sum()) if not df_pack.empty else 0
            o_pack_q = float(df_pack[df_pack['owner_name'] == o]['total_qty'].sum()) if not df_pack.empty else 0.0

            o_hnd_ord = int(df_hnd[df_hnd['owner_name'] == o]['total_orders'].sum()) if not df_hnd.empty else 0

            rec_val = f"{o_rec_ord:,} đ | {o_rec_q:,.0f} pcs" if (o_rec_ord > 0 or o_rec_q > 0) else "-"
            ret_val = f"{o_ret_ord:,} đ | {o_ret_q:,.0f} pcs" if (o_ret_ord > 0 or o_ret_q > 0) else "-"
            pick_val = f"{o_pick_ord:,} đ | {o_pick_q:,.0f} pcs" if (o_pick_ord > 0 or o_pick_q > 0) else "-"
            pack_val = f"{o_pack_ord:,} đ | {o_pack_q:,.0f} pcs" if (o_pack_ord > 0 or o_pack_q > 0) else "-"
            hnd_val = f"{o_hnd_ord:,} đ" if o_hnd_ord > 0 else "-"

            recon_rows.append({
                t['lbl_customer_col']: o,
                t['lbl_cust_rec_col']: rec_val,
                t['lbl_cust_ret_col']: ret_val,
                t['lbl_cust_pick_col']: pick_val,
                t['lbl_cust_pack_col']: pack_val,
                t['lbl_cust_hnd_col']: hnd_val,
            })

        df_recon = pd.DataFrame(recon_rows)
        st.dataframe(df_recon, width="stretch", hide_index=True)

        # CSV Download Button
        csv_bytes = df_recon.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button(
            label=t["btn_download_pipeline_csv"],
            data=csv_bytes,
            file_name=f"JDL_Doi_Soat_Cac_Khau_{start_date}_{end_date}.csv",
            mime="text/csv",
            key="btn_dl_pipeline_summary"
        )
    else:
        st.info(t["no_data"])

    # ==========================================
    # 5. BẢNG 3: CHI TIẾT SỐ ĐƠN TỒN CỦA TỪNG KHÁCH HÀNG THEO TỪNG KHUNG GIỜ HẠN SẢN XUẤT (PRODUCTION END TIME)
    # ==========================================
    st.markdown("<br>", unsafe_allow_html=True)
    try:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
        today_vn = now_vn.date()
        target_date = end_date if end_date else today_vn

        sync_st = get_global_sync_state()
        cur_v = sync_st.get("data_version", 0.0)
        df_backlog_all, _ = fetch_backlog_data(cache_version=cur_v)
        df_bl_filtered = filter_by_customer(df_backlog_all, cust_type, cust_name)

        has_prod_dt = 'prod_dt' in df_bl_filtered.columns and df_bl_filtered['prod_dt'].notnull().any()
        if has_prod_dt:
            df_bl_today = df_bl_filtered[df_bl_filtered['prod_dt'].dt.date == target_date].copy()
            if df_bl_today.empty and target_date != today_vn:
                df_bl_today = df_bl_filtered[df_bl_filtered['prod_dt'].dt.date == today_vn].copy()
                if not df_bl_today.empty:
                    target_date = today_vn
        else:
            df_bl_today = pd.DataFrame()

        st.markdown(f'<div class="section-title">CHI TIẾT SỐ ĐƠN TỒN CỦA TỪNG KHÁCH HÀNG THEO TỪNG KHUNG GIỜ HẠN (HẠN NGÀY {target_date.strftime("%d/%m")})</div>', unsafe_allow_html=True)

        if not df_bl_today.empty:
            df_bl_today['end_time_str'] = df_bl_today['prod_dt'].dt.strftime('%H:%M:%S')

            pivot_orders = df_bl_today.pivot_table(
                index='owner_name',
                columns='end_time_str',
                values='order_no',
                aggfunc='nunique',
                fill_value=0
            )
            end_time_cols = sorted(pivot_orders.columns.tolist())
            pivot_orders['TỔNG ĐƠN TỒN'] = pivot_orders[end_time_cols].sum(axis=1)
            pivot_orders = pivot_orders.sort_values(by='TỔNG ĐƠN TỒN', ascending=False).reset_index()

            tot_dict = {'owner_name': 'TỔNG CỘNG'}
            for c in end_time_cols:
                tot_dict[c] = int(pivot_orders[c].sum())
            tot_dict['TỔNG ĐƠN TỒN'] = int(pivot_orders['TỔNG ĐƠN TỒN'].sum())

            pivot_display = pd.concat([pivot_orders, pd.DataFrame([tot_dict])], ignore_index=True)
            pivot_display.rename(columns={'owner_name': 'Khách hàng'}, inplace=True)

            for c in end_time_cols + ['TỔNG ĐƠN TỒN']:
                pivot_display[c] = pivot_display[c].apply(lambda x: f"{x:,.0f}" if x > 0 else "-")

            st.dataframe(pivot_display, width="stretch", height=min(450, 42 * len(pivot_display) + 40), hide_index=True)

            col_summary_pivot_dl, _ = st.columns([1.5, 2.5])
            with col_summary_pivot_dl:
                csv_sum_bytes = pivot_display.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label="Tải bảng chi tiết đơn tồn theo khung giờ (.csv)",
                    data=csv_sum_bytes,
                    file_name=f"Tong_Hop_Don_Ton_Khung_Gio_{today_vn.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="btn_dl_summary_end_time_pivot"
                )
        else:
            st.info(f"Không có đơn tồn đọng nào có Hạn sản xuất trong ngày {target_date.strftime('%d/%m/%Y')}.")
    except Exception as e:
        st.error(f"Lỗi tải dữ liệu đơn tồn: {e}")



def render_inventory_dashboard(df_inventory: pd.DataFrame, cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str, start_time: datetime.time = datetime.time(0, 0), end_time: datetime.time = datetime.time(23, 59)):
    """
    Dashboard chuyên biệt cho khâu TRONG KHO (Kiểm kê & Đối soát tồn kho):
    - Đếm số nhiệm vụ kiểm kê (Task NO) mỗi ngày
    - Đếm số SKU kiểm kê mỗi ngày
    - Tổng sản lượng tồn hệ thống (system_qty) và thực tế (actual_qty)
    - Chênh lệch PCS giữa hệ thống và thực tế (difference_qty)
    - Phân tích chi tiết và đối soát theo từng khách hàng
    """
    if df_inventory.empty:
        st.info("Chưa có dữ liệu kiểm kê trong cơ sở dữ liệu.")
        return

    df = df_inventory.copy()

    # 1. Lọc theo khoảng thời gian đối soát
    df = filter_by_date_range(df, start_date, end_date, start_time, end_time)

    # 2. Lọc theo loại khách hàng
    if cust_type and cust_type != t["all_option"]:
        df = df[df['customer_type'] == cust_type]

    # 3. Lọc theo tên khách hàng cụ thể
    if cust_name and cust_name != t["all_option"]:
        df = df[df['owner_name'] == cust_name]

    if df.empty:
        st.warning("Không tìm thấy dữ liệu kiểm kê phù hợp với điều kiện lọc.")
        return

    # Tính toán các chỉ số KPI điều hành
    valid_tasks = df[df['inventory_check_no'] != 'Chưa có mã']['inventory_check_no']
    total_tasks = valid_tasks.nunique() if not valid_tasks.empty else df['inv_date'].nunique()
    total_skus = df['sku'].nunique()
    total_system_qty = int(df['system_qty'].sum())
    total_actual_qty = int(df['actual_qty'].sum())
    total_diff_qty = int(df['difference_qty'].sum())

    if total_system_qty > 0:
        accuracy_pct = max(0.0, 100.0 - (total_diff_qty / total_system_qty * 100.0))
    else:
        accuracy_pct = 100.0

    # Hiển thị 5 Thẻ KPI
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{t['inv_metric_tasks']}</div>
            <div class="metric-value">{total_tasks:,}</div>
            <div class="metric-sub">{t['inv_sub_tasks']}</div>
        </div>
        """, unsafe_allow_html=True)

    with m2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{t['inv_metric_skus']}</div>
            <div class="metric-value">{total_skus:,}</div>
            <div class="metric-sub">{t['inv_sub_skus']}</div>
        </div>
        """, unsafe_allow_html=True)

    with m3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{t['inv_metric_system_qty']}</div>
            <div class="metric-value">{total_system_qty:,}</div>
            <div class="metric-sub">{t['inv_sub_sys']}</div>
        </div>
        """, unsafe_allow_html=True)

    with m4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{t['inv_metric_actual_qty']}</div>
            <div class="metric-value">{total_actual_qty:,}</div>
            <div class="metric-sub">{t['inv_sub_act']}</div>
        </div>
        """, unsafe_allow_html=True)

    with m5:
        diff_color = "#34d399" if total_diff_qty == 0 else "#f87171"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{t['inv_metric_diff_qty']}</div>
            <div class="metric-value" style="color: {diff_color};">{total_diff_qty:,}</div>
            <div class="metric-sub">{t['inv_col_accuracy']}: <strong>{accuracy_pct:.2f}%</strong></div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    # 3 Tab phân tích chuyên sâu
    tab_daily, tab_cust, tab_tasks = st.tabs([
        t['inv_tab_daily'],
        t['inv_tab_customer'],
        t['inv_tab_tasks']
    ])

    # ==========================================
    # TAB 1: THEO NGÀY (TIẾN ĐỘ & CHÊNH LỆCH MỖI NGÀY)
    # ==========================================
    with tab_daily:
        daily_df = df.groupby('inv_date').agg(
            task_count=('inventory_check_no', lambda x: x[x != 'Chưa có mã'].nunique() if (x != 'Chưa có mã').any() else 1),
            sku_count=('sku', 'nunique'),
            system_qty=('system_qty', 'sum'),
            actual_qty=('actual_qty', 'sum'),
            diff_qty=('difference_qty', 'sum')
        ).reset_index().sort_values('inv_date', ascending=True)

        daily_df['accuracy_pct'] = daily_df.apply(
            lambda r: max(0.0, 100.0 - (r['diff_qty'] / r['system_qty'] * 100.0)) if r['system_qty'] > 0 else 100.0,
            axis=1
        )
        daily_df['dow_name'] = daily_df['inv_date'].apply(lambda d: DOW_NAMES[lang][d.isoweekday() % 7 + 1])
        daily_df['status'] = daily_df['diff_qty'].apply(
            lambda d: t['inv_status_perfect'] if d == 0 else f"{t['inv_status_diff']} ({d:,})"
        )

        # Biểu đồ xu hướng Bar + Line
        st.markdown(f'<div class="section-title">{t["inv_chart_daily_title"]}</div>', unsafe_allow_html=True)
        fig_daily = go.Figure()

        # Bar: SL Hệ thống
        fig_daily.add_trace(go.Bar(
            x=[d.strftime("%d/%m") for d in daily_df['inv_date']],
            y=daily_df['system_qty'],
            name=t['inv_col_sys_qty'],
            marker=dict(color='#3b82f6'),
            yaxis='y'
        ))

        # Bar: SL Thực tế
        fig_daily.add_trace(go.Bar(
            x=[d.strftime("%d/%m") for d in daily_df['inv_date']],
            y=daily_df['actual_qty'],
            name=t['inv_col_act_qty'],
            marker=dict(color='#10b981'),
            yaxis='y'
        ))

        # Line: Chênh lệch PCS
        fig_daily.add_trace(go.Scatter(
            x=[d.strftime("%d/%m") for d in daily_df['inv_date']],
            y=daily_df['diff_qty'],
            name=t['inv_col_diff_qty'],
            mode='lines+markers',
            line=dict(color='#f43f5e', width=3),
            marker=dict(size=7, color='#f43f5e'),
            yaxis='y2'
        ))

        fig_daily.update_layout(
            barmode='group',
            paper_bgcolor="#1e293b",
            plot_bgcolor="#0f172a",
            margin=dict(l=30, r=40, t=20, b=40),
            font=dict(color="#e2e8f0"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis=dict(title=t['inv_col_sys_qty'], gridcolor="#334155"),
            yaxis2=dict(
                title=t['inv_col_diff_qty'],
                overlaying='y',
                side='right',
                gridcolor="rgba(0,0,0,0)",
                showgrid=False
            )
        )
        st.plotly_chart(fig_daily, use_container_width=True)

        # Bảng tổng hợp theo ngày
        display_daily = daily_df.copy()
        display_daily['date_str'] = display_daily['inv_date'].apply(lambda d: d.strftime("%d/%m/%Y"))
        
        cols_show = ['date_str', 'dow_name', 'task_count', 'sku_count', 'system_qty', 'actual_qty', 'diff_qty', 'accuracy_pct', 'status']
        table_view = display_daily[cols_show].copy()
        table_view.columns = [
            t['inv_col_date'],
            t['inv_col_dow'],
            t['inv_col_task_count'],
            t['inv_col_sku_count'],
            t['inv_col_sys_qty'],
            t['inv_col_act_qty'],
            t['inv_col_diff_qty'],
            t['inv_col_accuracy'],
            t['inv_col_status']
        ]

        # Format số
        table_view[t['inv_col_task_count']] = table_view[t['inv_col_task_count']].apply(lambda x: f"{x:,}")
        table_view[t['inv_col_sku_count']] = table_view[t['inv_col_sku_count']].apply(lambda x: f"{x:,}")
        table_view[t['inv_col_sys_qty']] = table_view[t['inv_col_sys_qty']].apply(lambda x: f"{x:,}")
        table_view[t['inv_col_act_qty']] = table_view[t['inv_col_act_qty']].apply(lambda x: f"{x:,}")
        table_view[t['inv_col_diff_qty']] = table_view[t['inv_col_diff_qty']].apply(lambda x: f"{x:,}")
        table_view[t['inv_col_accuracy']] = table_view[t['inv_col_accuracy']].apply(lambda x: f"{x:.2f}%")

        # Thêm dòng TỔNG CỘNG
        total_row = pd.DataFrame([{
            t['inv_col_date']: t['lbl_total_column'],
            t['inv_col_dow']: "-",
            t['inv_col_task_count']: f"{total_tasks:,}",
            t['inv_col_sku_count']: f"{total_skus:,}",
            t['inv_col_sys_qty']: f"{total_system_qty:,}",
            t['inv_col_act_qty']: f"{total_actual_qty:,}",
            t['inv_col_diff_qty']: f"{total_diff_qty:,}",
            t['inv_col_accuracy']: f"{accuracy_pct:.2f}%",
            t['inv_col_status']: t['inv_status_perfect'] if total_diff_qty == 0 else f"{t['inv_status_diff']} ({total_diff_qty:,})"
        }])
        table_view_full = pd.concat([table_view, total_row], ignore_index=True)

        st.dataframe(table_view_full, width="stretch", hide_index=True)

        csv_daily = table_view_full.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            t['inv_btn_download_daily'],
            data=csv_daily,
            file_name=f"inventory_daily_{start_date}_{end_date}.csv",
            mime="text/csv"
        )

    # ==========================================
    # TAB 2: THEO KHÁCH HÀNG (ĐỐI SOÁT CHI TIẾT)
    # ==========================================
    with tab_cust:
        cust_df = df.groupby(['owner_name', 'customer_type']).agg(
            task_count=('inventory_check_no', lambda x: x[x != 'Chưa có mã'].nunique() if (x != 'Chưa có mã').any() else 1),
            sku_count=('sku', 'nunique'),
            system_qty=('system_qty', 'sum'),
            actual_qty=('actual_qty', 'sum'),
            diff_qty=('difference_qty', 'sum')
        ).reset_index().sort_values('actual_qty', ascending=False)

        cust_df['share_pct'] = (cust_df['actual_qty'] / total_actual_qty * 100.0) if total_actual_qty > 0 else 0.0
        cust_df['accuracy_pct'] = cust_df.apply(
            lambda r: max(0.0, 100.0 - (r['diff_qty'] / r['system_qty'] * 100.0)) if r['system_qty'] > 0 else 100.0,
            axis=1
        )

        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.markdown(f'<div class="section-title">{t["inv_chart_customer_share"]}</div>', unsafe_allow_html=True)
            top_pie_cust = cust_df.head(8).copy()
            if len(cust_df) > 8:
                other_share = cust_df.iloc[8:]['actual_qty'].sum()
                top_pie_cust = pd.concat([top_pie_cust, pd.DataFrame([{
                    'owner_name': 'Khác',
                    'actual_qty': other_share
                }])], ignore_index=True)
            
            fig_donut = px.pie(
                top_pie_cust,
                values='actual_qty',
                names='owner_name',
                hole=0.45,
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig_donut.update_layout(
                paper_bgcolor="#1e293b",
                plot_bgcolor="#0f172a",
                margin=dict(l=10, r=10, t=10, b=10),
                font=dict(color="#e2e8f0"),
                showlegend=True,
                legend=dict(orientation="h", yanchor="top", y=-0.1)
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_c2:
            st.markdown(f'<div class="section-title">{t["inv_chart_customer_diff"]}</div>', unsafe_allow_html=True)
            top_diff_cust = cust_df.sort_values('diff_qty', ascending=True).tail(8)
            fig_diff_bar = px.bar(
                top_diff_cust,
                x='diff_qty',
                y='owner_name',
                orientation='h',
                color='diff_qty',
                color_continuous_scale='Reds'
            )
            fig_diff_bar.update_layout(
                paper_bgcolor="#1e293b",
                plot_bgcolor="#0f172a",
                margin=dict(l=10, r=20, t=10, b=10),
                font=dict(color="#e2e8f0"),
                xaxis=dict(title=t['inv_col_diff_qty'], gridcolor="#334155"),
                yaxis=dict(title=None),
                coloraxis_showscale=False
            )
            st.plotly_chart(fig_diff_bar, use_container_width=True)

        # Bảng đối soát chi tiết khách hàng
        cust_table = cust_df[[
            'owner_name', 'customer_type', 'task_count', 'sku_count', 
            'system_qty', 'actual_qty', 'diff_qty', 'share_pct', 'accuracy_pct'
        ]].copy()
        
        cust_table.columns = [
            t['inv_col_cust_name'],
            t['inv_col_cust_type'],
            t['inv_col_task_count'],
            t['inv_col_sku_count'],
            t['inv_col_sys_qty'],
            t['inv_col_act_qty'],
            t['inv_col_diff_qty'],
            t['inv_col_share_pct'],
            t['inv_col_accuracy']
        ]

        cust_table[t['inv_col_task_count']] = cust_table[t['inv_col_task_count']].apply(lambda x: f"{x:,}")
        cust_table[t['inv_col_sku_count']] = cust_table[t['inv_col_sku_count']].apply(lambda x: f"{x:,}")
        cust_table[t['inv_col_sys_qty']] = cust_table[t['inv_col_sys_qty']].apply(lambda x: f"{x:,}")
        cust_table[t['inv_col_act_qty']] = cust_table[t['inv_col_act_qty']].apply(lambda x: f"{x:,}")
        cust_table[t['inv_col_diff_qty']] = cust_table[t['inv_col_diff_qty']].apply(lambda x: f"{x:,}")
        cust_table[t['inv_col_share_pct']] = cust_table[t['inv_col_share_pct']].apply(lambda x: f"{x:.2f}%")
        cust_table[t['inv_col_accuracy']] = cust_table[t['inv_col_accuracy']].apply(lambda x: f"{x:.2f}%")

        # Dòng TỔNG CỘNG
        total_cust_row = pd.DataFrame([{
            t['inv_col_cust_name']: t['lbl_total_column'],
            t['inv_col_cust_type']: "-",
            t['inv_col_task_count']: f"{total_tasks:,}",
            t['inv_col_sku_count']: f"{total_skus:,}",
            t['inv_col_sys_qty']: f"{total_system_qty:,}",
            t['inv_col_act_qty']: f"{total_actual_qty:,}",
            t['inv_col_diff_qty']: f"{total_diff_qty:,}",
            t['inv_col_share_pct']: "100.00%",
            t['inv_col_accuracy']: f"{accuracy_pct:.2f}%"
        }])
        cust_table_full = pd.concat([cust_table, total_cust_row], ignore_index=True)

        st.dataframe(cust_table_full, width="stretch", hide_index=True)

        csv_cust = cust_table_full.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            t['inv_btn_download_cust'],
            data=csv_cust,
            file_name=f"inventory_customers_{start_date}_{end_date}.csv",
            mime="text/csv"
        )

    # ==========================================
    # TAB 3: NHẬT KÝ NHIỆM VỤ KIỂM KÊ (DANH SÁCH CHI TIẾT)
    # ==========================================
    with tab_tasks:
        st.markdown(f'<div class="section-title">{t["inv_tab_tasks"]}</div>', unsafe_allow_html=True)
        search_kw = st.text_input(
            "Tìm kiếm theo Mã phiếu hoặc Mã SKU:",
            value="",
            placeholder="Nhập mã phiếu kiểm kê hoặc mã hàng..."
        )

        task_agg = df.groupby(['inventory_check_no', 'inv_date', 'owner_name', 'customer_type']).agg(
            sku_count=('sku', 'nunique'),
            system_qty=('system_qty', 'sum'),
            actual_qty=('actual_qty', 'sum'),
            diff_qty=('difference_qty', 'sum'),
            worker_id=('worker_id', lambda x: str(x.iloc[0]) if len(x) > 0 and pd.notnull(x.iloc[0]) else '-'),
            location_count=('location', 'nunique')
        ).reset_index().sort_values('inv_date', ascending=False)

        if search_kw.strip():
            kw = search_kw.strip().upper()
            matched_tasks = df[
                df['inventory_check_no'].str.upper().str.contains(kw, na=False) |
                df['sku'].str.upper().str.contains(kw, na=False) |
                df['product_name'].str.upper().str.contains(kw, na=False)
            ]['inventory_check_no'].unique()
            task_agg = task_agg[task_agg['inventory_check_no'].isin(matched_tasks)]

        task_agg['date_str'] = task_agg['inv_date'].apply(lambda d: d.strftime("%d/%m/%Y"))
        task_agg['accuracy_pct'] = task_agg.apply(
            lambda r: max(0.0, 100.0 - (r['diff_qty'] / r['system_qty'] * 100.0)) if r['system_qty'] > 0 else 100.0,
            axis=1
        )
        task_agg['status'] = task_agg['diff_qty'].apply(
            lambda d: t['inv_status_perfect'] if d == 0 else f"{t['inv_status_diff']} ({d:,})"
        )

        view_task = task_agg[[
            'inventory_check_no', 'date_str', 'owner_name', 'customer_type',
            'sku_count', 'system_qty', 'actual_qty', 'diff_qty', 'accuracy_pct', 'status', 'worker_id'
        ]].copy()

        view_task.columns = [
            t['inv_col_task_no'],
            t['inv_col_date'],
            t['inv_col_cust_name'],
            t['inv_col_cust_type'],
            t['inv_col_sku_count'],
            t['inv_col_sys_qty'],
            t['inv_col_act_qty'],
            t['inv_col_diff_qty'],
            t['inv_col_accuracy'],
            t['inv_col_status'],
            t['inv_col_worker']
        ]

        view_task[t['inv_col_sku_count']] = view_task[t['inv_col_sku_count']].apply(lambda x: f"{x:,}")
        view_task[t['inv_col_sys_qty']] = view_task[t['inv_col_sys_qty']].apply(lambda x: f"{x:,}")
        view_task[t['inv_col_act_qty']] = view_task[t['inv_col_act_qty']].apply(lambda x: f"{x:,}")
        view_task[t['inv_col_diff_qty']] = view_task[t['inv_col_diff_qty']].apply(lambda x: f"{x:,}")
        view_task[t['inv_col_accuracy']] = view_task[t['inv_col_accuracy']].apply(lambda x: f"{x:.2f}%")

        st.dataframe(view_task, width="stretch", hide_index=True)

        csv_tasks = view_task.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            t['inv_btn_download_tasks'],
            data=csv_tasks,
            file_name=f"inventory_tasks_{start_date}_{end_date}.csv",
            mime="text/csv"
        )



def render_handover_dashboard(df_handover: pd.DataFrame, cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str, start_time: datetime.time = datetime.time(0, 0), end_time: datetime.time = datetime.time(23, 59)):
    """
    Dashboard chuyên biệt cho khâu BÀN GIAO:
    Tập trung vào 6 Đơn vị vận chuyển (Carriers):
    - J&T Express
    - J&T Cargo
    - Shopee Xpress
    - GHN
    - Viettel Post (gộp chung các dịch vụ VTP)
    - BEST Express
    """
    if df_handover.empty:
        st.info("Chưa có dữ liệu bàn giao trong cơ sở dữ liệu.")
        return

    tab_day, tab_week, tab_month = st.tabs([
        t.get('tab_day', 'Ngày'),
        t.get('tab_week', 'Tuần'),
        t.get('tab_month', 'Tháng')
    ])

    with tab_day:
        # Lọc dữ liệu theo điều kiện
        df = df_handover.copy()
        df = filter_by_date_range(df, start_date, end_date, start_time, end_time)
        df = filter_by_customer(df, cust_type, cust_name)

        if df.empty:
            st.warning(t.get('no_data', 'Không tìm thấy dữ liệu bàn giao phù hợp.'))
        else:
            tot_orders = int(df['total_orders'].sum())
            active_carriers = df['carrier_name'].nunique()

            # Xếp hạng Carrier
            carrier_agg = df.groupby('carrier_name')['total_orders'].sum().sort_values(ascending=False)
            top1_carrier = carrier_agg.index[0] if len(carrier_agg) > 0 else "Chưa có"
            top1_val = int(carrier_agg.iloc[0]) if len(carrier_agg) > 0 else 0
            top1_pct = (top1_val / tot_orders * 100) if tot_orders > 0 else 0

            top2_carrier = carrier_agg.index[1] if len(carrier_agg) > 1 else "-"
            top2_val = int(carrier_agg.iloc[1]) if len(carrier_agg) > 1 else 0
            top2_pct = (top2_val / tot_orders * 100) if tot_orders > 0 else 0

            # 4 Thẻ KPI chính
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">TỔNG ĐƠN BÀN GIAO</div>
                    <div class="kpi-value" style="color: #38bdf8;">{tot_orders:,} <span style="font-size: 13px; color: #94a3b8;">đơn</span></div>
                    <div class="kpi-delta delta-neutral">• Đã giao cho đơn vị vận chuyển</div>
                </div>
                """, unsafe_allow_html=True)
            with c2:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">TOP 1: {top1_carrier.upper()}</div>
                    <div class="kpi-value" style="color: #f59e0b;">{top1_val:,} <span style="font-size: 13px; color: #94a3b8;">đơn</span></div>
                    <div class="kpi-delta delta-neutral">• Chiếm {top1_pct:.1f}% tổng sản lượng</div>
                </div>
                """, unsafe_allow_html=True)
            with c3:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">TOP 2: {top2_carrier.upper()}</div>
                    <div class="kpi-value" style="color: #60a5fa;">{top2_val:,} <span style="font-size: 13px; color: #94a3b8;">đơn</span></div>
                    <div class="kpi-delta delta-neutral">• Chiếm {top2_pct:.1f}% tổng sản lượng</div>
                </div>
                """, unsafe_allow_html=True)
            with c4:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">ĐỐI TÁC VẬN CHUYỂN</div>
                    <div class="kpi-value" style="color: #10b981;">{active_carriers} <span style="font-size: 13px; color: #94a3b8;">/ 6 hãng</span></div>
                    <div class="kpi-delta delta-neutral">• Đang phát sinh lấy hàng</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # PHẦN 1: TỔNG HỢP & TỈ TRỌNG SỐ ĐƠN THEO ĐƠN VỊ VẬN CHUYỂN (CARRIER)
            st.markdown('<div class="chart-title-banner">TỔNG HỢP & TỈ TRỌNG BÀN GIAO THEO ĐƠN VỊ VẬN CHUYỂN (CARRIER)</div>', unsafe_allow_html=True)
            col_tbl, col_chart = st.columns([1.3, 1.0])
            with col_tbl:
                df_carrier_summary = carrier_agg.reset_index()
                df_carrier_summary.columns = ['Đơn vị vận chuyển', 'Số đơn bàn giao']
                df_carrier_summary['Tỉ trọng (%)'] = (df_carrier_summary['Số đơn bàn giao'] / tot_orders * 100).apply(lambda x: f"{x:.2f}%")
                
                tot_c_row = pd.DataFrame([{
                    'Đơn vị vận chuyển': 'TỔNG CỘNG',
                    'Số đơn bàn giao': tot_orders,
                    'Tỉ trọng (%)': '100.0%'
                }])
                df_carrier_display = pd.concat([df_carrier_summary, tot_c_row], ignore_index=True)
                df_carrier_formatted = df_carrier_display.copy()
                df_carrier_formatted['Số đơn bàn giao'] = df_carrier_formatted['Số đơn bàn giao'].apply(lambda x: f"{x:,}")
                st.dataframe(df_carrier_formatted, width="stretch", height=min(290, 42 * len(df_carrier_formatted) + 40), hide_index=True)

                col_dl_c, _ = st.columns([2, 1])
                with col_dl_c:
                    csv_c_bytes = df_carrier_display.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Tải bảng tổng hợp carrier (.csv)",
                        data=csv_c_bytes,
                        file_name=f"handover_carrier_summary_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="btn_dl_handover_carrier_sum"
                    )

            with col_chart:
                fig_donut = px.pie(
                    df_carrier_summary,
                    values='Số đơn bàn giao',
                    names='Đơn vị vận chuyển',
                    hole=0.48,
                    template="plotly_dark",
                    color='Đơn vị vận chuyển',
                    color_discrete_map=CARRIER_COLORS
                )
                fig_donut.update_traces(textposition='inside', textinfo='percent+label', insidetextorientation='radial')
                fig_donut.update_layout(
                    title=None,
                    paper_bgcolor="#1e293b",
                    plot_bgcolor="#0f172a",
                    margin=dict(l=10, r=10, t=10, b=30),
                    showlegend=False,
                    height=270
                )
                st.plotly_chart(fig_donut, use_container_width=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # PHẦN 2: MA TRẬN PHÂN BỔ SỐ ĐƠN BÀN GIAO THEO KHUNG GIỜ (CARRIER X GIỜ)
            fmt_d1 = start_date.strftime('%d/%m/%Y')
            fmt_d2 = end_date.strftime('%d/%m/%Y')
            date_range_label = fmt_d1 if start_date == end_date else f"{fmt_d1} — {fmt_d2}"
            time_range_label = f" ({start_time.strftime('%H:%M')} — {end_time.strftime('%H:%M')})"
            st.markdown(f'<div class="chart-title-banner">PHÂN BỔ SỐ ĐƠN BÀN GIAO THEO TỪNG KHUNG GIỜ (THEO ĐƠN VỊ VẬN CHUYỂN) — {date_range_label}{time_range_label}</div>', unsafe_allow_html=True)

            pivot_carrier_hour = df.pivot_table(
                index='carrier_name',
                columns='op_hour',
                values='total_orders',
                aggfunc='sum',
                fill_value=0
            )
            raw_h_cols = [c for c in pivot_carrier_hour.columns if isinstance(c, (int, float, np.integer)) or str(c).isdigit()]
            active_h = [int(h) for h in raw_h_cols if pivot_carrier_hour[h].sum() > 0]
            display_h = get_shift_ordered_hours(active_h, start_time.hour, end_time.hour)

            if display_h:
                pivot_carrier_hour = pivot_carrier_hour.reindex(columns=display_h, fill_value=0)
                pivot_carrier_hour['TỔNG ĐƠN'] = pivot_carrier_hour.sum(axis=1)
                pivot_carrier_hour = pivot_carrier_hour.sort_values(by='TỔNG ĐƠN', ascending=False)

                tot_h_dict = {'carrier_name': 'TỔNG CỘNG'}
                for h in display_h:
                    tot_h_dict[h] = int(pivot_carrier_hour[h].sum())
                tot_h_dict['TỔNG ĐƠN'] = int(pivot_carrier_hour['TỔNG ĐƠN'].sum())

                rename_h = {h: f"{h:02d}:00" for h in display_h}
                pivot_carrier_hour_renamed = pivot_carrier_hour.rename(columns=rename_h).reset_index()
                pivot_carrier_hour_renamed.rename(columns={'carrier_name': 'Đơn vị vận chuyển'}, inplace=True)
                
                tot_row_h = pd.DataFrame([tot_h_dict]).rename(columns=rename_h)
                tot_row_h.rename(columns={'carrier_name': 'Đơn vị vận chuyển'}, inplace=True)
                
                pivot_carrier_hour_disp = pd.concat([pivot_carrier_hour_renamed, tot_row_h], ignore_index=True)
                for col in pivot_carrier_hour_disp.columns:
                    if col != 'Đơn vị vận chuyển':
                        pivot_carrier_hour_disp[col] = pivot_carrier_hour_disp[col].apply(lambda x: f"{x:,}" if x > 0 else "-")

                st.dataframe(pivot_carrier_hour_disp, width="stretch", height=min(320, 42 * len(pivot_carrier_hour_disp) + 40), hide_index=True)

                col_dl_h, _ = st.columns([1.5, 2.5])
                with col_dl_h:
                    csv_h_bytes = pivot_carrier_hour_disp.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Tải bảng bàn giao theo giờ (.csv)",
                        data=csv_h_bytes,
                        file_name=f"handover_carrier_hourly_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="btn_dl_handover_carrier_hourly"
                    )
            else:
                st.info("Không có dữ liệu bàn giao theo giờ trong khoảng thời gian đã chọn.")

            st.markdown("<br>", unsafe_allow_html=True)

            # PHẦN 3: CHI TIẾT SỐ ĐƠN BÀN GIAO CỦA TỪNG KHÁCH HÀNG THEO ĐƠN VỊ VẬN CHUYỂN
            st.markdown('<div class="chart-title-banner">CHI TIẾT SỐ ĐƠN BÀN GIAO CỦA TỪNG KHÁCH HÀNG THEO ĐƠN VỊ VẬN CHUYỂN</div>', unsafe_allow_html=True)
            pivot_cust_c = df.pivot_table(
                index='owner_name',
                columns='carrier_name',
                values='total_orders',
                aggfunc='sum',
                fill_value=0
            )
            c_cols = [c for c in TARGET_CARRIERS if c in pivot_cust_c.columns]
            if c_cols:
                pivot_cust_c = pivot_cust_c[c_cols]
                pivot_cust_c['TỔNG ĐƠN'] = pivot_cust_c.sum(axis=1)
                pivot_cust_c = pivot_cust_c.sort_values(by='TỔNG ĐƠN', ascending=False).reset_index()

                tot_cust_dict = {'owner_name': 'TỔNG CỘNG'}
                for c in c_cols:
                    tot_cust_dict[c] = int(pivot_cust_c[c].sum())
                tot_cust_dict['TỔNG ĐƠN'] = int(pivot_cust_c['TỔNG ĐƠN'].sum())

                pivot_cust_c_disp = pd.concat([pivot_cust_c, pd.DataFrame([tot_cust_dict])], ignore_index=True)
                pivot_cust_c_disp.rename(columns={'owner_name': 'Khách hàng'}, inplace=True)
                for col in c_cols + ['TỔNG ĐƠN']:
                    pivot_cust_c_disp[col] = pivot_cust_c_disp[col].apply(lambda x: f"{x:,}" if x > 0 else "-")

                st.dataframe(pivot_cust_c_disp, width="stretch", height=min(450, 42 * len(pivot_cust_c_disp) + 40), hide_index=True)

                col_dl_cust, _ = st.columns([1.5, 2.5])
                with col_dl_cust:
                    csv_cust_c_bytes = pivot_cust_c_disp.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        label="Tải bảng chi tiết khách hàng x carrier (.csv)",
                        data=csv_cust_c_bytes,
                        file_name=f"handover_customer_x_carrier_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="btn_dl_handover_cust_carrier"
                    )

    with tab_week:
        # PHÂN TÍCH THEO TUẦN CHO 6 CARRIERS
        df_w = df_handover.copy()
        df_w = filter_by_customer(df_w, cust_type, cust_name)

        if df_w.empty:
            st.warning(t.get('no_data', 'Không tìm thấy dữ liệu.'))
        else:
            df_w['op_date'] = pd.to_datetime(df_w['op_date'])
            df_w['week_start'] = df_w['op_date'].dt.to_period('W-SUN').dt.start_time.dt.date

            pivot_w = df_w.pivot_table(
                index='week_start',
                columns='carrier_name',
                values='total_orders',
                aggfunc='sum',
                fill_value=0
            ).sort_index()

            c_cols_w = [c for c in TARGET_CARRIERS if c in pivot_w.columns]
            pivot_w = pivot_w[c_cols_w]
            pivot_w['TỔNG ĐƠN'] = pivot_w.sum(axis=1)

            st.markdown('<div class="chart-title-banner">XU HƯỚNG BÀN GIAO THEO TUẦN (PHÂN BỔ THEO CARRIER)</div>', unsafe_allow_html=True)
            
            fig_w_stack = go.Figure()
            for c in c_cols_w:
                fig_w_stack.add_trace(go.Bar(
                    x=[f"Tuần {d.strftime('%W')} ({d.strftime('%d/%m')})" for d in pivot_w.index],
                    y=pivot_w[c],
                    name=c,
                    marker_color=CARRIER_COLORS.get(c, '#94a3b8')
                ))
            fig_w_stack.update_layout(
                barmode='stack',
                template="plotly_dark",
                paper_bgcolor="#1e293b",
                plot_bgcolor="#0f172a",
                margin=dict(l=30, r=30, t=10, b=50),
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                height=350
            )
            st.plotly_chart(fig_w_stack, use_container_width=True)

            pivot_w_disp = pivot_w.reset_index()
            pivot_w_disp['week_start'] = pivot_w_disp['week_start'].apply(lambda d: f"Tuần {d.strftime('%W')} ({d.strftime('%d/%m/%Y')})")
            pivot_w_disp.rename(columns={'week_start': 'Tuần (Bắt đầu)'}, inplace=True)
            for col in c_cols_w + ['TỔNG ĐƠN']:
                pivot_w_disp[col] = pivot_w_disp[col].apply(lambda x: f"{x:,}")

            st.dataframe(pivot_w_disp, width="stretch", hide_index=True)

    with tab_month:
        # PHÂN TÍCH THEO THÁNG CHO 6 CARRIERS
        df_m = df_handover.copy()
        df_m = filter_by_customer(df_m, cust_type, cust_name)

        if df_m.empty:
            st.warning(t.get('no_data', 'Không tìm thấy dữ liệu.'))
        else:
            df_m['op_date'] = pd.to_datetime(df_m['op_date'])
            df_m['month_label'] = df_m['op_date'].dt.strftime('%m/%Y')
            df_m['month_sort'] = df_m['op_date'].dt.strftime('%Y-%m')

            pivot_m = df_m.pivot_table(
                index=['month_sort', 'month_label'],
                columns='carrier_name',
                values='total_orders',
                aggfunc='sum',
                fill_value=0
            ).sort_index()

            c_cols_m = [c for c in TARGET_CARRIERS if c in pivot_m.columns]
            pivot_m = pivot_m[c_cols_m]
            pivot_m['TỔNG ĐƠN'] = pivot_m.sum(axis=1)

            st.markdown('<div class="chart-title-banner">XU HƯỚNG BÀN GIAO THEO THÁNG (PHÂN BỔ THEO CARRIER)</div>', unsafe_allow_html=True)

            months_x = [idx[1] for idx in pivot_m.index]
            fig_m_stack = go.Figure()
            for c in c_cols_m:
                fig_m_stack.add_trace(go.Bar(
                    x=months_x,
                    y=pivot_m[c],
                    name=c,
                    marker_color=CARRIER_COLORS.get(c, '#94a3b8')
                ))
            fig_m_stack.update_layout(
                barmode='stack',
                template="plotly_dark",
                paper_bgcolor="#1e293b",
                plot_bgcolor="#0f172a",
                margin=dict(l=30, r=30, t=10, b=50),
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                height=350
            )
            st.plotly_chart(fig_m_stack, use_container_width=True)

            pivot_m_disp = pivot_m.reset_index(level=1)
            pivot_m_disp.rename(columns={'month_label': 'Tháng'}, inplace=True)
            for col in c_cols_m + ['TỔNG ĐƠN']:
                pivot_m_disp[col] = pivot_m_disp[col].apply(lambda x: f"{x:,}")

            st.dataframe(pivot_m_disp, width="stretch", hide_index=True)



# ==========================================
# FRAGMENT WRAPPERS (AUTO-REFRESH VỚI SCROLL PRESERVATION)
# ==========================================
@st.fragment(run_every="15s")
def render_sidebar_sync_status_fragment(t):
    sync_state = get_global_sync_state()
    last_sync_time = get_latest_sync_timestamp()
    if sync_state.get("error"):
        sync_time_text = f"{t.get('sync_error', 'Lỗi đồng bộ:')} {sync_state['error'][:40]}"
        badge_icon = "🔴"
    elif sync_state.get("is_running"):
        if last_sync_time:
            sync_time_text = f"{t['sync_last_time']} {last_sync_time} ({t.get('sync_running', 'Đang đồng bộ...')})"
        else:
            sync_time_text = t['sync_running']
        badge_icon = "🟡"
    elif last_sync_time:
        sync_time_text = f"{t['sync_last_time']} {last_sync_time}"
        if sync_state.get("synced_count", 0) > 0:
            sync_time_text += f" ({sync_state['synced_count']} {t['sync_new_files']})"
        badge_icon = "🟢"
    else:
        sync_time_text = t['sync_init']
        badge_icon = "🟢"

    st.markdown(f"""
    <div class="sync-status-box">
        <div class="sync-status-badge">{badge_icon} {t['sync_auto_badge']}</div>
        <div class="sync-status-time">{sync_time_text}</div>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔄 " + t.get("btn_refresh", "Làm mới dữ liệu"), key="btn_sidebar_refresh", use_container_width=True):
        with st.spinner("🔄 Đang quét trực tiếp Google Drive & cập nhật dữ liệu..."):
            success, res = trigger_manual_sync()
            if success:
                synced_cnt = res.get("synced", 0) if isinstance(res, dict) else 0
                if synced_cnt > 0:
                    st.toast(f"✅ Đã quét xong: Nạp mới thành công {synced_cnt} file từ Google Drive!", icon="🎉")
                else:
                    st.toast("⚡ Dữ liệu đã là mới nhất! Không có file mới nào trên Google Drive.", icon="✅")
            else:
                st.toast(f"ℹ️ {res}", icon="⚠️")
        time.sleep(1)
        st.rerun()

@st.fragment(run_every="15s")
def render_day_tab_fragment(stage_key, start_date, end_date, cur_cfg, selected_cust_type, selected_cust_name, t, lang, start_time=datetime.time(0, 0), end_time=datetime.time(23, 59)):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_raw = fetch_stage_data(stage_key, cache_version=cur_v)
    df_filtered = filter_by_customer(df_raw, selected_cust_type, selected_cust_name)
    render_day_tab(df_filtered, start_date, end_date, cur_cfg, stage_key, selected_cust_type, selected_cust_name, t, lang, start_time, end_time)

@st.fragment(run_every="30s")
def render_week_tab_fragment(stage_key, cur_cfg, selected_cust_type, selected_cust_name, t, lang):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_raw = fetch_stage_data(stage_key, cache_version=cur_v)
    df_filtered = filter_by_customer(df_raw, selected_cust_type, selected_cust_name)
    render_week_tab(df_filtered, cur_cfg, t, lang)

@st.fragment(run_every="30s")
def render_month_tab_fragment(stage_key, cur_cfg, selected_cust_type, selected_cust_name, t, lang):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_raw = fetch_stage_data(stage_key, cache_version=cur_v)
    df_filtered = filter_by_customer(df_raw, selected_cust_type, selected_cust_name)
    render_month_tab(df_filtered, cur_cfg, t, lang)

@st.fragment(run_every="15s")
def render_inventory_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time=datetime.time(0, 0), end_time=datetime.time(23, 59)):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_inv = fetch_inventory_data(cache_version=cur_v)
    render_inventory_dashboard(df_inv, selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time, end_time)

@st.fragment(run_every="20s")
def render_summary_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time=datetime.time(0, 0), end_time=datetime.time(23, 59)):
    render_pipeline_summary(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time, end_time)

@st.fragment(run_every="30s")
def render_planning_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang):
    render_headcount_planning(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)

@st.fragment(run_every="15s")
def render_handover_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time=datetime.time(0, 0), end_time=datetime.time(23, 59)):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_hnd = fetch_handover_data(cache_version=cur_v)
    render_handover_dashboard(df_hnd, selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time, end_time)

# ==========================================
# MAIN APPLICATION CONTROLLER
# ==========================================
def main():
    # Kích hoạt luồng đồng bộ ngầm nếu chưa khởi chạy
    ensure_background_sync_started()

    if "current_lang" not in st.session_state:
        st.session_state.current_lang = "VN"

    lang = st.session_state.current_lang
    t = TRANSLATIONS[lang]
    today_vn = datetime.datetime.now(VN_TIMEZONE).date()

    # ==========================================
    # SIDEBAR: LOOKER STUDIO NAV PILLS (KHÔNG ICON) & FILTERS
    # ==========================================
    with st.sidebar:
        logo_b64 = get_logo_base64()
        logo_html = f'<img src="data:image/png;base64,{logo_b64}" class="sidebar-logo-img" alt="JDL"/>' if logo_b64 else ''

        st.markdown(f"""
        <div class="sidebar-brand-container">
            <div class="sidebar-brand-top">
                {logo_html}
                <div class="sidebar-brand-text">
                    <div class="brand-title">{t['brand_title']}</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Dropdown chọn kho cùng hàng với text KHO VẬN HÀNH (Single-container flexbox)
        warehouse_options = ["SGN-SM-1-VN"]
        if "selected_warehouse" not in st.session_state:
            st.session_state.selected_warehouse = "SGN-SM-1-VN"

        selected_wh = st.selectbox(
            label=t["lbl_select_warehouse"],
            options=warehouse_options,
            index=0,
            key="warehouse_selector",
            label_visibility="visible"
        )

        st.markdown(f'<div class="sidebar-section-title">{t["stages_header"]}</div>', unsafe_allow_html=True)

        if "selected_stage_idx" not in st.session_state:
            st.session_state.selected_stage_idx = 0

        # Looker Studio Style Animated Navigation Buttons (Native Streamlit)
        for idx, key in enumerate(STAGE_KEYS):
            stage_name = STAGE_CONFIGS[key][f"name_{lang.lower()}"].upper()
            is_active = (st.session_state.selected_stage_idx == idx)
            btn_kind = "primary" if is_active else "secondary"
            btn_prefix = "›" if not is_active else "▶"
            
            if st.button(f"{btn_prefix}  {stage_name}", key=f"nav_btn_{key}_{lang}", use_container_width=True, type=btn_kind):
                if st.session_state.selected_stage_idx != idx:
                    st.session_state.selected_stage_idx = idx
                    st.rerun()

        selected_key = STAGE_KEYS[st.session_state.selected_stage_idx]
        cur_cfg = STAGE_CONFIGS[selected_key]

        st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

        # 5. THÔNG TIN TRẠNG THÁI ĐỒNG BỘ TINH GỌN (SYNC STATUS LIVE FRAGMENT)
        render_sidebar_sync_status_fragment(t)

        # Footer
        st.markdown(f"""
        <div class="author-footer">
            {t['developed_by']}
        </div>
        """, unsafe_allow_html=True)


    # ==========================================
    # TẢI DỮ LIỆU CỦA KHÂU HIỆN TẠI
    # ==========================================
    df_raw = pd.DataFrame()
    df_inventory = pd.DataFrame()

    sync_st_main = get_global_sync_state()
    cur_v_main = sync_st_main.get("data_version", 0.0)

    if cur_cfg.get("type") == "inventory":
        with st.spinner("..."):
            try:
                df_inventory = fetch_inventory_data(cache_version=cur_v_main)
            except Exception as ex:
                st.error(str(ex))
                return
    elif cur_cfg.get("type") == "handover":
        with st.spinner("..."):
            try:
                df_raw = fetch_handover_data(cache_version=cur_v_main)
            except Exception as ex:
                st.error(str(ex))
                return
    elif cur_cfg.get("type") not in ["summary", "backlog", "planning"]:
        with st.spinner("..."):
            try:
                df_raw = fetch_stage_data(selected_key, cache_version=cur_v_main)
            except Exception as ex:
                st.error(str(ex))
                return
    elif cur_cfg.get("type") in ["summary", "planning"]:
        try:
            df_raw = fetch_stage_data("ĐÓNG GÓI", cache_version=cur_v_main)
        except Exception:
            df_raw = pd.DataFrame()

    # ==========================================
    # TIÊU ĐỀ KHÂU VẬN HÀNH & BỘ ĐIỀU KHIỂN (KHÁCH HÀNG + KHOẢNG THỜI GIAN)
    # ==========================================
    cur_stage_name = cur_cfg[f'name_{lang.lower()}'].upper()
    cur_stage_title = cur_cfg[f'title_{lang.lower()}']

    # Khởi tạo ngày và khung giờ mặc định nếu chưa có (chuẩn WMS: hôm nay từ 00:00 đến 23:59)
    if "wms_d_from" not in st.session_state:
        st.session_state.wms_d_from = today_vn
    if "wms_d_to" not in st.session_state:
        st.session_state.wms_d_to = today_vn
    if "wms_t_from" not in st.session_state:
        st.session_state.wms_t_from = datetime.time(0, 0)
    if "wms_t_to" not in st.session_state:
        st.session_state.wms_t_to = datetime.time(23, 59)
    if "date_range_val" not in st.session_state:
        st.session_state.date_range_val = (st.session_state.wms_d_from, st.session_state.wms_d_to)
    if "time_from_val" not in st.session_state:
        st.session_state.time_from_val = st.session_state.wms_t_from
    if "time_to_val" not in st.session_state:
        st.session_state.time_to_val = st.session_state.wms_t_to

    # 1. Tiêu đề khâu vận hành nằm phía trên
    st.markdown(f'<div style="font-size: 22px; font-weight: 800; color: #ffffff; letter-spacing: 0.5px; margin: 4px 0 14px 0;">{cur_stage_title}</div>', unsafe_allow_html=True)

    if cur_cfg.get("type") in ["planning"]:
        selected_cust_type = t["all_option"]
        selected_cust_name = t["all_option"]
        start_date = st.session_state.wms_d_from
        end_date = st.session_state.wms_d_to
        start_time, end_time = datetime.time(0, 0), datetime.time(23, 59)
        df_filtered = df_raw
    else:
        # 2. Bộ lọc khách hàng và Hộp chọn thời gian tích hợp chuẩn iWMS (Date & Time Range Picker)
        col_filter_type, col_filter_name, col_datetime_range = st.columns([1.0, 1.4, 2.6])
        
        with col_filter_type:
            st.markdown("<div style='margin-top: 4px;'>", unsafe_allow_html=True)
            cust_type_options = [t["all_option"], "B2C", "B2B"]
            selected_cust_type = st.selectbox(
                t['filter_cust_type'],
                options=cust_type_options,
                index=0
            )
            st.markdown("</div>", unsafe_allow_html=True)

        filter_source_df = df_inventory if cur_cfg.get("type") == "inventory" else df_raw
        if not filter_source_df.empty:
            if selected_cust_type == "B2B":
                available_owners = [t["all_option"]] + [g for g in B2B_GROUPS if g in filter_source_df['owner_name'].unique()]
                if len(available_owners) == 1:
                    available_owners = [t["all_option"]] + B2B_GROUPS
            elif selected_cust_type == "B2C":
                b2c_names = sorted([n for n in filter_source_df[filter_source_df['customer_type'] == 'B2C']['owner_name'].unique() if n])
                available_owners = [t["all_option"]] + b2c_names
            else:
                all_names = sorted([n for n in filter_source_df['owner_name'].unique() if n])
                available_owners = [t["all_option"]] + all_names
        else:
            available_owners = [t["all_option"]] + B2B_GROUPS if selected_cust_type == "B2B" else [t["all_option"]]

        with col_filter_name:
            st.markdown("<div style='margin-top: 4px;'>", unsafe_allow_html=True)
            selected_cust_name = st.selectbox(
                t['filter_cust_name'],
                options=available_owners,
                index=0
            )
            st.markdown("</div>", unsafe_allow_html=True)

        # Lấy giá trị ngày & giờ hiện tại
        start_date = st.session_state.wms_d_from
        end_date = st.session_state.wms_d_to
        start_time = st.session_state.wms_t_from
        end_time = st.session_state.wms_t_to

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        start_sec_str = "00"
        end_sec_str = "59" if end_time.minute == 59 else "00"
        dt_btn_label = f"🕒  {start_date.strftime('%Y-%m-%d')} {start_time.strftime('%H:%M')}:{start_sec_str}   -   {end_date.strftime('%Y-%m-%d')} {end_time.strftime('%H:%M')}:{end_sec_str}  ▾"

        with col_datetime_range:
            st.markdown(f"<div style='margin-top: 4px; font-size: 14px; font-weight: 500; color: #f1f5f9; margin-bottom: 4px;'>{t['date_range_header']}</div>", unsafe_allow_html=True)
            with st.popover(dt_btn_label, use_container_width=True):
                # Header popup mô phỏng chuẩn iWMS JoyLogistics
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; background: #0b1329; padding: 10px 14px; border-radius: 6px; border: 1px solid #334155; margin-bottom: 14px; font-family: monospace; font-size: 13.5px; font-weight: 600;">
                    <div style="display: flex; gap: 6px; align-items: center;">
                        <span style="background: #1e293b; padding: 3px 8px; border-radius: 4px; border: 1px solid #475569; color: #38bdf8;">{start_date.strftime('%Y-%m-%d')}</span>
                        <span style="background: #1e293b; padding: 3px 8px; border-radius: 4px; border: 1px solid #475569; color: #38bdf8;">{start_time.strftime('%H:%M')}:{start_sec_str}</span>
                    </div>
                    <span style="color: #64748b; font-size: 15px; font-weight: bold;">➔</span>
                    <div style="display: flex; gap: 6px; align-items: center;">
                        <span style="background: #1e293b; padding: 3px 8px; border-radius: 4px; border: 1px solid #475569; color: #38bdf8;">{end_date.strftime('%Y-%m-%d')}</span>
                        <span style="background: #1e293b; padding: 3px 8px; border-radius: 4px; border: 1px solid #475569; color: #38bdf8;">{end_time.strftime('%H:%M')}:{end_sec_str}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                dt_ver = st.session_state.get('wms_dt_ver', 0)
                col_p_from, col_p_to = st.columns(2)
                with col_p_from:
                    st.markdown(f"<div style='font-size: 12px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;'>{t['lbl_from_header']}</div>", unsafe_allow_html=True)
                    pop_d_start = st.date_input(
                        "Ngày bắt đầu",
                        value=st.session_state.wms_d_from,
                        max_value=today_vn + timedelta(days=1),
                        key=f"wms_pop_d_start_{dt_ver}",
                        label_visibility="collapsed"
                    )
                    pop_t_start = st.time_input(
                        "Giờ bắt đầu",
                        value=st.session_state.wms_t_from,
                        key=f"wms_pop_t_start_{dt_ver}",
                        step=1800,
                        label_visibility="collapsed"
                    )
                with col_p_to:
                    st.markdown(f"<div style='font-size: 12px; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 4px;'>{t['lbl_to_header']}</div>", unsafe_allow_html=True)
                    pop_d_end = st.date_input(
                        "Ngày kết thúc",
                        value=st.session_state.wms_d_to,
                        max_value=today_vn + timedelta(days=1),
                        key=f"wms_pop_d_end_{dt_ver}",
                        label_visibility="collapsed"
                    )
                    pop_t_end = st.time_input(
                        "Giờ kết thúc",
                        value=st.session_state.wms_t_to,
                        key=f"wms_pop_t_end_{dt_ver}",
                        step=1800,
                        label_visibility="collapsed"
                    )

                def apply_preset(d_from, d_to, t_from, t_to):
                    if d_from > d_to:
                        d_from, d_to = d_to, d_from
                    st.session_state.wms_d_from = d_from
                    st.session_state.wms_d_to = d_to
                    st.session_state.wms_t_from = t_from
                    st.session_state.wms_t_to = t_to
                    st.session_state.date_range_val = (d_from, d_to)
                    st.session_state.time_from_val = t_from
                    st.session_state.time_to_val = t_to
                    st.session_state.wms_dt_ver = st.session_state.get('wms_dt_ver', 0) + 1
                    st.rerun()

                # Nút chọn nhanh (Quick Presets)
                st.markdown("<div style='height: 1px; background: #334155; margin: 12px 0 8px 0;'></div>", unsafe_allow_html=True)
                st.markdown(f"<div style='font-size: 11px; font-weight: 600; color: #64748b; letter-spacing: 0.5px; margin-bottom: 6px;'>{t['lbl_quick_presets']}</div>", unsafe_allow_html=True)
                
                pr1, pr2, pr3, pr4, pr5 = st.columns(5)
                with pr1:
                    if st.button("Hôm nay", use_container_width=True, key="pop_pre_today"):
                        apply_preset(today_vn, today_vn, datetime.time(0, 0), datetime.time(23, 59))
                with pr2:
                    if st.button("Hôm qua", use_container_width=True, key="pop_pre_yesterday"):
                        yest = today_vn - timedelta(days=1)
                        apply_preset(yest, yest, datetime.time(0, 0), datetime.time(23, 59))
                with pr3:
                    if st.button("Ca ngày (8-17h)", use_container_width=True, key="pop_pre_day"):
                        apply_preset(st.session_state.wms_d_from, st.session_state.wms_d_to, datetime.time(8, 0), datetime.time(17, 0))
                with pr4:
                    if st.button("Ca đêm (22-06h)", use_container_width=True, key="pop_pre_night"):
                        apply_preset(today_vn - timedelta(days=1), today_vn, datetime.time(22, 0), datetime.time(6, 0))
                with pr5:
                    if st.button("7 ngày", use_container_width=True, key="pop_pre_7d"):
                        apply_preset(today_vn - timedelta(days=6), today_vn, datetime.time(0, 0), datetime.time(23, 59))

                st.markdown("<div style='height: 1px; background: #334155; margin: 12px 0 8px 0;'></div>", unsafe_allow_html=True)
                b_clear, b_ok = st.columns(2)
                with b_clear:
                    if st.button("Clear / Mặc định", use_container_width=True, key="pop_action_clear"):
                        apply_preset(today_vn, today_vn, datetime.time(0, 0), datetime.time(23, 59))
                with b_ok:
                    if st.button("OK / Xác nhận", type="primary", use_container_width=True, key="pop_action_ok"):
                        apply_preset(pop_d_start, pop_d_end, pop_t_start, pop_t_end)

                # Tự động cập nhật nếu thay đổi trực tiếp từ date/time input
                if (pop_d_start != st.session_state.wms_d_from or pop_d_end != st.session_state.wms_d_to or 
                    pop_t_start != st.session_state.wms_t_from or pop_t_end != st.session_state.wms_t_to):
                    apply_preset(pop_d_start, pop_d_end, pop_t_start, pop_t_end)

        start_date = st.session_state.wms_d_from
        end_date = st.session_state.wms_d_to
        start_time = st.session_state.wms_t_from
        end_time = st.session_state.wms_t_to

        df_filtered = filter_by_customer(df_raw, selected_cust_type, selected_cust_name)

    # ==========================================
    # ĐIỀU HƯỚNG HIỂN THỊ NỘI DUNG THEO KHÂU
    # ==========================================
    if cur_cfg.get("type") in ["planning", "backlog"]:
        render_planning_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)
    elif cur_cfg.get("type") == "summary":
        render_summary_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time, end_time)
    elif cur_cfg.get("type") == "inventory":
        render_inventory_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time, end_time)
    elif cur_cfg.get("type") == "handover":
        render_handover_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang, start_time, end_time)
    else:
        tab_day, tab_week, tab_month = st.tabs([
            t['tab_day'],
            t['tab_week'],
            t['tab_month']
        ])

        with tab_day:
            render_day_tab_fragment(selected_key, start_date, end_date, cur_cfg, selected_cust_type, selected_cust_name, t, lang, start_time, end_time)

        with tab_week:
            render_week_tab_fragment(selected_key, cur_cfg, selected_cust_type, selected_cust_name, t, lang)

        with tab_month:
            render_month_tab_fragment(selected_key, cur_cfg, selected_cust_type, selected_cust_name, t, lang)




if __name__ == "__main__":
    main()
