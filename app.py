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
            sync_state["is_running"] = True
            creds = get_sync_credentials()
            drive_svc = build_service("drive", "v3", credentials=creds, cache_discovery=False)
            sync_proj = getattr(creds, "project_id", None) or SYNC_PROJECT_ID
            if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
                sync_proj = st.secrets["gcp_service_account"].get("project_id", sync_proj)
            bq_client = bq_module.Client(credentials=creds, project=sync_proj)

            # Chạy Quick sync (is_full_sync=False, dry_run=False, folder=None)
            res = run_sync_cycle(drive_svc, bq_client, is_full=False, dry_run=False, folder=None)
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

        # Nghỉ đúng 300 giây (5 phút) trước lần quét kế tiếp
        time.sleep(300)

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
    },
    "ZH": {
        "app_title": "仓储管理运营监控看板 - 京东物流",
        "brand_title": "JingDong Logistics",
        "lbl_select_warehouse": "运营仓库：",
        "stages_header": "仓储作业环节",
        "date_range_header": "对账统计周期",
        "date_range_label": "选择统计区间（起始日期 - 截止日期）：",
        "btn_today": "今日",
        "btn_30d": "近30天",
        "btn_refresh": "刷新数据",
        "toast_refresh": "已从云端数据库同步最新数据！",
        "lang_header": "语言选择",
        "filter_cust_type": "客户类型：",
        "filter_cust_name": "客户名称：",
        "all_option": "全部",
        "status_connected": "数据库已连接",
        "status_sync": "同步频次：每2小时",
        "stage_prefix": "环节",
        "period_prefix": "统计区间",
        "tab_day": "按日分析（实时小时明细）",
        "tab_week": "按周分析（周环比分析）",
        "tab_month": "按月分析（月环比分析）",
        # Section titles (không icon)
        "sec_hourly": "按日及各小时作业件数深度分析",
        "sec_weekly": "周度作业量及周环比变动分析",
        "sec_monthly": "月度作业量及月环比变动分析",
        "sec_pipeline": "全仓各作业环节吞吐量综合总览",
        "sec_backlog": "待入库积压量及大促活动预测看板",
        # Metric labels
        "lbl_total_qty": "总作业件数（件）",
        "lbl_total_orders": "总订单数（单）",
        "lbl_customers_count": "作业客户数（家）",
        "lbl_avg_order": "单均件数（件 / 单）",
        "lbl_selected_period": "所选周期内累计",
        "lbl_orders_recorded": "已处理订单总量",
        "lbl_active_partners": "发生产能贡献的货主",
        "lbl_avg_size": "平均单张订单包裹规模",
        # Hourly
        "lbl_choose_date_hourly": "选择查看小时明细的具体日期：",
        "lbl_display_metric": "图表与明细指标维度：",
        "opt_qty": "作业件数（件）",
        "opt_orders": "作业订单数（单）",
        "lbl_dynamic_table": "各小时客户作业动态交叉表",
        "lbl_total_column": "合计",
        "lbl_customer_col": "客户名称",
        "lbl_chart_hourly": "全天24小时作业件数分布态势",
        "lbl_hour_axis": "作业时段",
        "btn_download_hourly": "导出小时报表 (.csv)",
        # Weekly
        "lbl_week_this": "本周作业总量",
        "lbl_week_prev": "上周作业总量",
        "lbl_week_growth": "周环比增长率",
        "lbl_week_orders": "本周订单总量",
        "lbl_week_diff": "环比差值",
        "lbl_week_table": "周度作业历史汇表明细",
        "col_week_start": "周起始日",
        "col_week_name": "统计周次",
        "col_week_orders": "总订单量（单）",
        "col_week_qty": "总作业件数（件）",
        "col_week_growth": "周环比增幅",
        "col_week_diff": "件数差额（件）",
        # Monthly
        "lbl_month_this": "当月作业总量",
        "lbl_month_prev": "上月作业总量",
        "lbl_month_growth": "月环比增长率",
        "lbl_month_orders": "当月订单总量",
        "lbl_month_diff": "环比差值",
        "lbl_month_table": "月度作业历史汇表明细",
        "col_month": "统计月份",
        "col_month_orders": "总订单量（单）",
        "col_month_qty": "总作业件数（件）",
        "col_month_growth": "月环比增幅",
        "col_month_diff": "件数差额（件）",
        # Headcount & Capacity Planning (Không icon)
        "sec_planning": "全仓人力定编规划与产能负荷测算",
        "plan_table_title": "周度人力定编规划看板（ISO标准周）",
        "plan_select_week": "选择排班周期（ISO周）：",
        "plan_fixed_header": "固定班组编制设定（周一至周六）",
        "plan_fixed_note": "说明：周日默认仅叉车（2人）、交接（2人）及2C作业；其余入库、理货及2B班组休息。",
        "plan_col_stage": "作业环节 / 班组",
        "task_receiving": "采购入库组 (Receiving)",
        "task_returns": "退货入库组 (Returns)",
        "task_inventory": "库内理货组 (Inventory)",
        "task_forklift": "叉车作业组 (Forklift)",
        "task_handover": "出库交接组 (Handover)",
        "task_pick_2c": "2C 拣货组 (2C Picking)",
        "task_pack_2c": "2C 打包组 (2C Packing)",
        "task_chint_envision": "B2B - 正泰与远景专线 (CHINT & ENVISION)",
        "task_kkv": "B2B - KKV 专线",
        "task_total_hc": "全仓作业到岗总人数",
        "sec_volume_comparison": "计划与实际作业吞吐产能对账",
        "lbl_vol_b2c": "2C 吞吐产能",
        "lbl_vol_b2b": "2B 吞吐产能",
        "lbl_fc_orders": "预计单量",
        "lbl_act_orders": "实际单量",
        "lbl_pct_orders": "单量达成率 (%)",
        "lbl_fc_pcs": "预计件数",
        "lbl_act_pcs": "实际件数",
        "lbl_pct_pcs": "件数达成率 (%)",
        "lbl_metric_col": "客户名称 / 指标",
        "lbl_client_col": "客户全称",
        "lbl_total_week": "全周合计",
        "lbl_total_b2c": "2C 业务总计",
        "lbl_total_b2b": "2B 业务总计",
        "lbl_filter_client": "筛选客户：",
        "plan_unit_people": "人",
        "plan_unit_orders": "单",
        "plan_unit_pcs": "件",
        "lbl_download_csv": "导出排班预测表格 (.csv)",
        "lbl_download_vol_csv": "导出产能对账表格 (.csv)",
        # Backlog
        "lbl_bl_qty": "待入库积压件数（件）",
        "lbl_bl_orders": "待接收订单数（单）",
        "lbl_bl_partners": "积压涉及货主（家）",
        "lbl_bl_event": "大促节点储备",
        "lbl_bl_chart": "积压件数排名前10之重点客户",
        "lbl_bl_event_title": "电商大促双日数营销日历",
        "col_event_date": "大促日期",
        "col_event_name": "营销节点名称",
        # Pipeline
        "col_stage_name": "仓储作业环节",
        "lbl_throughput_chart": "全流程各环节吞吐作业件数",
        "no_data": "所选周期内未检索到相关作业数据。",
        "msg_no_hourly_data": "该日期暂未产生相关作业数据。",
        # Tỉ trọng & Nhân viên
        "pie_share_orders": "按客户划分之订单量占比",
        "pie_share_qty": "按客户划分之作业件数占比",
        "sec_hourly_staff": "按员工分时段作业产量交叉报表",
        "col_worker": "员工姓名",
        "lbl_total_row": "总计",
        "sec_cust_share_tracking": "按客户追踪业务占比及环比增减",
        "col_share_current": "本期占比 (%)",
        "col_share_prev": "上期占比 (%)",
        "col_share_delta": "占比增减 (百分点)",
        "col_growth_rate": "环比增幅 (%)",
        "col_period_current": "本期",
        "col_period_prev": "上期",
        "col_period_delta": "环比差值 (+/-)",
        "lbl_chart_share_compare": "连续两期客户业务占比对比分析",
        "btn_download_staff_hourly": "导出员工小时明细报表 (.csv)",
        "btn_download_share_csv": "导出客户占比分析报表 (.csv)",
        "sync_auto_badge": "每5分钟自动后台同步",
        "sync_last_time": "最近同步时间：",
        "sync_running": "正在从云端Drive同步...",
        "sync_init": "正在执行首次后台同步...",
        "sync_new_files": "个新文件",
        "sync_error": "同步异常:",
        # Inventory tab translations
        "inv_title": "库存盘点与差异对账报表",
        "inv_metric_tasks": "盘点任务总数",
        "inv_metric_skus": "盘点品项数 (SKU)",
        "inv_metric_system_qty": "系统账面数量",
        "inv_metric_actual_qty": "实际盘点数量",
        "inv_metric_diff_qty": "盘点差异数量 (PCS)",
        "inv_metric_accuracy": "盘点准确率",
        "inv_sub_tasks": "已执行盘点任务次数",
        "inv_sub_skus": "涉及盘点SKU品项数",
        "inv_sub_sys": "WMS系统账面库存件数",
        "inv_sub_act": "现场实际清点件数",
        "inv_sub_diff": "系统与实盘数量差异",
        "inv_tab_daily": "按日盘点 (进度与差异)",
        "inv_tab_customer": "按客户对账 (客户明细)",
        "inv_tab_tasks": "盘点任务明细日志",
        "inv_chart_daily_title": "每日盘点件数与差异趋势分析",
        "inv_chart_customer_share": "各客户盘点业务量占比分析",
        "inv_chart_customer_diff": "盘点差异件数最高客户排行",
        "inv_col_date": "日期",
        "inv_col_dow": "星期",
        "inv_col_task_count": "任务数",
        "inv_col_sku_count": "SKU数",
        "inv_col_sys_qty": "系统数量 (PCS)",
        "inv_col_act_qty": "实际数量 (PCS)",
        "inv_col_diff_qty": "差异数量 (PCS)",
        "inv_col_accuracy": "准确率",
        "inv_col_status": "盘点状态",
        "inv_col_cust_name": "客户名称",
        "inv_col_cust_type": "客户类型",
        "inv_col_share_pct": "占比",
        "inv_col_task_no": "盘点任务编号",
        "inv_col_prod_name": "商品名称",
        "inv_col_location": "库位/货架",
        "inv_col_worker": "盘点人员",
        "inv_status_perfect": "完全吻合",
        "inv_status_diff": "存在差异",
        "inv_btn_download_daily": "导出每日盘点报表 (.csv)",
        "inv_btn_download_cust": "导出客户对账报表 (.csv)",
        "inv_btn_download_tasks": "导出任务明细报表 (.csv)",
        "developed_by": "由QC质量控制部开发",
        # Packing Backlog integration
        "card_backlog_label": "出库待打包积压 (BACKLOG)",
        "unit_order": "单",
        "unit_pcs_backlog": "件待打包",
        "sub_done_backlog_qty": "已打包 / 需处理总件数",
        "sub_done_backlog_orders": "已打包 / 需处理总单数",
        "sec_backlog_table": "各客户出库积压明细报表 (BACKLOG)",
        "col_customer_name": "客户名称",
        "col_customer_type": "客户类型",
        "col_backlog_orders": "积压订单数 (单)",
        "col_backlog_pcs": "积压件数 (PCS)",
        "col_backlog_share_orders": "单量占比 (%)",
        "col_backlog_share_pcs": "件数占比 (%)",
        "btn_download_backlog": "导出积压数据报表 (.csv)",
        "msg_no_backlog": "当前无积压待打包订单数据。",
        # Packing Customer Share Table
        "sec_packing_share_table": "各客户打包订单量与件数占比明细表",
        "col_packing_orders": "打包订单数",
        "col_packing_qty": "打包件数 (PCS)",
        "btn_download_packing_share": "导出打包占比明细 (.csv)"
    }
}

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
    },
    "ZH": {
        1: "周日",
        2: "周一",
        3: "周二",
        4: "周三",
        5: "周四",
        6: "周五",
        7: "周六"
    }
}

# Cấu hình 9 khâu vận hành (không icon)
STAGE_CONFIGS = {
    "TỔNG HỢP": {
        "name_vn": "TỔNG HỢP",
        "name_zh": "全流程总览",
        "title_vn": "TỔNG HỢP VẬN HÀNH TOÀN KHO",
        "title_zh": "全仓作业全流程吞吐总览",
        "desc_vn": "Bức tranh toàn cảnh luồng hàng hóa qua tất cả các mắt xích kho vận",
        "desc_zh": "全面监控从入库到出库各作业节点的流转效率与产能平衡",
        "type": "summary"
    },
    "NHẬP HÀNG": {
        "name_vn": "NHẬP HÀNG",
        "name_zh": "采购入库",
        "title_vn": "BÁO CÁO NHẬP HÀNG (MUA MỚI)",
        "title_zh": "采购入库作业监控报表",
        "desc_vn": "Theo dõi tiến độ nhận hàng nhà cung cấp và nhập kho lưu trữ",
        "desc_zh": "跟踪供应商送货交接与到货验收入库进度",
        "table": "fact_receiving",
        "where": "cargo_type = 'Purchase Inbound' AND receiving_time IS NOT NULL",
        "time_col": "receiving_time",
        "order_col": "order_no",
        "qty_col": "receiving_qty",
        "qty_label_vn": "Sản lượng nhập (Sản phẩm)",
        "qty_label_zh": "入库件数（件）",
        "order_label_vn": "Số đơn nhập",
        "order_label_zh": "入库单量（单）"
    },
    "HOÀN TRẢ": {
        "name_vn": "HOÀN TRẢ",
        "name_zh": "退货入库",
        "title_vn": "BÁO CÁO HOÀN TRẢ NHẬP KHO",
        "title_zh": "逆向退货入库监控报表",
        "desc_vn": "Theo dõi tiếp nhận và kiểm đếm các đơn hàng hoàn/trả về kho",
        "desc_zh": "监控逆向包裹接收、质检及返仓上架处理流转",
        "table": "fact_receiving",
        "where": "cargo_type = 'Return Inbound' AND receiving_time IS NOT NULL",
        "time_col": "receiving_time",
        "order_col": "order_no",
        "qty_col": "receiving_qty",
        "qty_label_vn": "Sản lượng hoàn (Sản phẩm)",
        "qty_label_zh": "退件件数（件）",
        "order_label_vn": "Số đơn hoàn",
        "order_label_zh": "退货单量（单）"
    },
    "TRONG KHO": {
        "name_vn": "TRONG KHO",
        "name_zh": "库内盘点",
        "title_vn": "BÁO CÁO KIỂM KÊ & ĐỐI SOÁT TỒN KHO",
        "title_zh": "库存盘点与差异对账报表",
        "desc_vn": "Theo dõi số lượng nhiệm vụ kiểm kê, số lượng SKU, sản lượng thực tế và đối soát chênh lệch tồn kho",
        "desc_zh": "监控盘点任务数、品项数(SKU)、实际盘点件数与系统差异对账",
        "type": "inventory",
        "table": "fact_inventory",
        "where": "inventory_time IS NOT NULL",
        "time_col": "inventory_time",
        "order_col": "inventory_check_no",
        "qty_col": "actual_qty",
        "qty_label_vn": "Sản lượng đếm thực tế (Sản phẩm)",
        "qty_label_zh": "实际盘点数量(件)",
        "order_label_vn": "Số nhiệm vụ kiểm kê",
        "order_label_zh": "盘点任务数"
    },
    "LÊN KỆ": {
        "name_vn": "LÊN KỆ",
        "name_zh": "上架作业",
        "title_vn": "BÁO CÁO LÊN KỆ",
        "title_zh": "物料库位上架作业报表",
        "desc_vn": "Theo dõi tiến độ cất hàng lên kệ từ khu vực đệm tiếp nhận",
        "desc_zh": "监控从暂存缓冲区至存储货位之上架作业时效与件数",
        "table": "fact_putaway",
        "where": "putaway_time IS NOT NULL",
        "time_col": "putaway_time",
        "order_col": "order_no",
        "qty_col": "putaway_qty",
        "qty_label_vn": "Sản lượng lên kệ (Sản phẩm)",
        "qty_label_zh": "上架件数（件）",
        "order_label_vn": "Số lệnh lên kệ",
        "order_label_zh": "上架任务数（单）"
    },
    "NHẶT HÀNG": {
        "name_vn": "NHẶT HÀNG",
        "name_zh": "拣货作业",
        "title_vn": "BÁO CÁO NHẶT HÀNG",
        "title_zh": "出库波次拣货作业报表",
        "desc_vn": "Theo dõi hiệu suất lấy hàng từ kệ chuẩn bị cho khâu đóng gói",
        "desc_zh": "监控拣货作业批次、拣选件数与出库集货履约效率",
        "table": "fact_picking",
        "where": "COALESCE(end_time, start_time) IS NOT NULL",
        "time_col": "COALESCE(end_time, start_time)",
        "order_col": "order_no",
        "qty_col": "COALESCE(actual_qty, expected_qty)",
        "qty_label_vn": "Sản lượng nhặt (Sản phẩm)",
        "qty_label_zh": "拣货件数（件）",
        "order_label_vn": "Số đơn nhặt",
        "order_label_zh": "拣货单量（单）"
    },
    "ĐÓNG GÓI": {
        "name_vn": "ĐÓNG GÓI",
        "name_zh": "复核打包",
        "title_vn": "BÁO CÁO ĐÓNG GÓI XUẤT KHO",
        "title_zh": "出库复核打包作业报表",
        "desc_vn": "Giám sát sản lượng đóng gói đơn hàng xuất kho theo thời gian thực",
        "desc_zh": "实时监控各打包工位包装复核产出与出库发运准备度",
        "table": "fact_packing",
        "where": "pack_time IS NOT NULL",
        "time_col": "pack_time",
        "order_col": "order_no",
        "qty_col": "packing_qty",
        "qty_label_vn": "Sản lượng đóng gói (Sản phẩm)",
        "qty_label_zh": "打包件数（件）",
        "order_label_vn": "Số đơn đóng gói",
        "order_label_zh": "打包单量（单）"
    },
    "BÀN GIAO": {
        "name_vn": "BÀN GIAO",
        "name_zh": "交接出库",
        "title_vn": "BÁO CÁO BÀN GIAO VẬN CHUYỂN",
        "title_zh": "快递承运商交接出库报表",
        "desc_vn": "Theo dõi bàn giao kiện hàng cho các đơn vị vận chuyển",
        "desc_zh": "监控发货包裹向各快递物流承运商交接封签时效",
        "table": "fact_handover",
        "where": "handover_time IS NOT NULL",
        "time_col": "handover_time",
        "order_col": "order_no",
        "qty_col": "1",
        "qty_label_vn": "Số kiện bàn giao (Kiện)",
        "qty_label_zh": "交接包裹数（件）",
        "order_label_vn": "Số đơn bàn giao",
        "order_label_zh": "交接订单数（单）"
    },
    "DỰ KIẾN": {
        "name_vn": "DỰ KIẾN",
        "name_zh": "排班预测",
        "title_vn": "KẾ HOẠCH ĐỊNH BIÊN NHÂN SỰ & NĂNG LỰC VẬN HÀNH",
        "title_zh": "人力排班与产能负荷测算看板",
        "desc_vn": "Mô phỏng nhu cầu nhân sự dựa trên sản lượng dự kiến và năng suất thực tế",
        "desc_zh": "基于作业预测件量与实际效率工时动态测算人力排班需求",
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


@st.cache_data(ttl=300, show_spinner=False)
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
        received_time
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_backlog`
    """
    try:
        df_backlog = client.query(sql_backlog).to_dataframe()
    except Exception as e:
        df_backlog = pd.DataFrame(columns=['order_no', 'owner_name', 'customer_type', 'piece_qty', 'received_time'])

    if not df_backlog.empty:
        df_backlog['piece_qty'] = pd.to_numeric(df_backlog['piece_qty'], errors='coerce').fillna(0.0)
        df_backlog['backlog_qty'] = df_backlog['piece_qty']
        df_backlog['backlog_orders'] = 1
        df_backlog['cargo_type'] = df_backlog['customer_type']
    else:
        df_backlog = pd.DataFrame(columns=['order_no', 'owner_name', 'customer_type', 'piece_qty', 'received_time', 'backlog_qty', 'backlog_orders', 'cargo_type'])

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
    if cust_type != "Tất cả" and cust_type != "全部":
        res = res[res['customer_type'] == cust_type]
    if cust_name != "Tất cả" and cust_name != "全部":
        res = res[res['owner_name'] == cust_name]
    return res


def filter_by_date_range(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """Lọc dữ liệu theo khoảng thời gian start và end."""
    if df.empty or 'op_date' not in df.columns:
        return df
    return df[(df['op_date'] >= start_date) & (df['op_date'] <= end_date)].copy()



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
def render_day_tab(df_filtered: pd.DataFrame, start_date: datetime.date, end_date: datetime.date, cfg: dict, stage_key: str, selected_cust_type: str, selected_cust_name: str, t: dict, lang: str):
    st.markdown(f'<div class="section-title">{t["sec_hourly"]}</div>', unsafe_allow_html=True)

    df_range = filter_by_date_range(df_filtered, start_date, end_date)

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
            df_cust_share = df_range.groupby(['owner_name', 'customer_type'], as_index=False).agg({
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
                'customer_type': '-',
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
                'customer_type': t['col_customer_type'],
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

    # 2. CHUYỂN ĐỔI CHỈ SỐ CHO BẢNG GIỜ (PCS / ORDERS)
    metric_choice = st.radio(
        t['lbl_display_metric'],
        options=[t["opt_qty"], t["opt_orders"]],
        horizontal=True,
        key=f"day_tab_metric_choice_{stage_key}"
    )

    val_col = 'total_qty' if metric_choice == t["opt_qty"] else 'total_orders'
    val_title = t["opt_qty"] if metric_choice == t["opt_qty"] else t["opt_orders"]

    # 3. BẢNG PHÂN BỔ SẢN LƯỢNG THEO KHUNG GIỜ (THEO KHÁCH HÀNG)
    fmt_d1 = start_date.strftime('%d/%m/%Y') if lang == "VN" else start_date.strftime('%Y-%m-%d')
    fmt_d2 = end_date.strftime('%d/%m/%Y') if lang == "VN" else end_date.strftime('%Y-%m-%d')
    date_range_label = fmt_d1 if start_date == end_date else f"{fmt_d1} — {fmt_d2}"

    st.markdown(f'<div class="chart-title-banner">{t["lbl_dynamic_table"]} — {date_range_label}</div>', unsafe_allow_html=True)

    pivot_cust = df_range.pivot_table(
        index='owner_name',
        columns='op_hour',
        values=val_col,
        aggfunc='sum',
        fill_value=0
    )
    # Chỉ giữ các khung giờ có phát sinh dữ liệu (sum > 0), ẩn các khung giờ không có số liệu
    raw_hour_cols = [c for c in pivot_cust.columns if isinstance(c, (int, float, np.integer)) or str(c).isdigit()]
    active_hours = sorted([int(h) for h in raw_hour_cols if pivot_cust[h].sum() > 0])

    if active_hours:
        pivot_cust = pivot_cust[active_hours]
        pivot_cust[t['lbl_total_column']] = pivot_cust.sum(axis=1)
        pivot_cust = pivot_cust.sort_values(t['lbl_total_column'], ascending=False)

        total_row = pivot_cust.sum(axis=0)
        total_row.name = t['lbl_total_row']
        pivot_cust_with_total = pd.concat([pivot_cust, pd.DataFrame([total_row])])

        rename_cols = {h: f"{h:02d}:00" for h in active_hours}
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
                file_name=f"hourly_customers_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
                key=f"btn_dl_hourly_cust_{stage_key}"
            )
    else:
        st.info(t["msg_no_hourly_data"])

    # 4. BẢNG SẢN LƯỢNG THEO TỪNG KHUNG GIỜ THEO NHÂN VIÊN (Áp dụng cho Nhặt hàng & Đóng gói)
    if cfg.get("table") in ["fact_picking", "fact_packing"]:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(f'<div class="chart-title-banner">{t["sec_hourly_staff"]} — {date_range_label}</div>', unsafe_allow_html=True)

        sync_st = get_global_sync_state()
        cur_v = sync_st.get("data_version", 0.0)
        with st.spinner("..."):
            df_workers_raw = fetch_hourly_worker_data(stage_key, start_date, end_date, cache_version=cur_v)

        df_workers = filter_by_customer(df_workers_raw, selected_cust_type, selected_cust_name)

        if not df_workers.empty:
            pivot_worker = df_workers.pivot_table(
                index='worker_name',
                columns='op_hour',
                values=val_col,
                aggfunc='sum',
                fill_value=0
            )
            raw_worker_hours = [c for c in pivot_worker.columns if isinstance(c, (int, float, np.integer)) or str(c).isdigit()]
            active_worker_hours = sorted([int(h) for h in raw_worker_hours if pivot_worker[h].sum() > 0])

            if active_worker_hours:
                pivot_worker = pivot_worker[active_worker_hours]
                pivot_worker[t['lbl_total_column']] = pivot_worker.sum(axis=1)
                pivot_worker = pivot_worker.sort_values(t['lbl_total_column'], ascending=False)

                worker_total_row = pivot_worker.sum(axis=0)
                worker_total_row.name = t['lbl_total_row']
                pivot_worker_with_total = pd.concat([pivot_worker, pd.DataFrame([worker_total_row])])

                rename_cols_worker = {h: f"{h:02d}:00" for h in active_worker_hours}
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
                        file_name=f"hourly_staff_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"btn_dl_hourly_staff_{stage_key}"
                    )
            else:
                st.info(t["msg_no_hourly_data"])
        else:
            st.info(t["msg_no_hourly_data"])

    # 5. BẢNG CHI TIẾT TỒN ĐỌNG (BACKLOG) THEO KHÁCH HÀNG (Áp dụng riêng cho ĐÓNG GÓI)
    if stage_key == "ĐÓNG GÓI":
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(f'<div class="chart-title-banner">{t["sec_backlog_table"]}</div>', unsafe_allow_html=True)

        if not df_bl_filtered.empty:
            df_bl_summary = df_bl_filtered.groupby(['owner_name', 'customer_type'], as_index=False).agg(
                backlog_orders=('order_no', 'nunique'),
                backlog_qty=('piece_qty', 'sum')
            ).sort_values(by='backlog_orders', ascending=False)

            tot_bl_tbl_orders = int(df_bl_summary['backlog_orders'].sum())
            tot_bl_tbl_qty = float(df_bl_summary['backlog_qty'].sum())

            df_bl_summary['share_orders'] = (df_bl_summary['backlog_orders'] / tot_bl_tbl_orders * 100) if tot_bl_tbl_orders > 0 else 0.0
            df_bl_summary['share_qty'] = (df_bl_summary['backlog_qty'] / tot_bl_tbl_qty * 100) if tot_bl_tbl_qty > 0 else 0.0

            total_row = pd.DataFrame([{
                'owner_name': t['lbl_total_row'],
                'customer_type': '-',
                'backlog_orders': tot_bl_tbl_orders,
                'backlog_qty': tot_bl_tbl_qty,
                'share_orders': 100.0,
                'share_qty': 100.0
            }])
            df_bl_display = pd.concat([df_bl_summary, total_row], ignore_index=True)

            df_bl_formatted = df_bl_display.copy()
            df_bl_formatted['backlog_orders'] = df_bl_formatted['backlog_orders'].apply(lambda x: f"{x:,.0f}")
            df_bl_formatted['backlog_qty'] = df_bl_formatted['backlog_qty'].apply(lambda x: f"{x:,.0f}")
            df_bl_formatted['share_orders'] = df_bl_formatted['share_orders'].apply(lambda x: f"{x:.1f}%")
            df_bl_formatted['share_qty'] = df_bl_formatted['share_qty'].apply(lambda x: f"{x:.1f}%")

            df_bl_formatted.columns = [
                t['col_customer_name'],
                t['col_customer_type'],
                t['col_backlog_orders'],
                t['col_backlog_pcs'],
                t['col_backlog_share_orders'],
                t['col_backlog_share_pcs']
            ]

            st.dataframe(df_bl_formatted, width="stretch", height=320, hide_index=True)

            col_bl_csv, _ = st.columns([1.5, 2.5])
            with col_bl_csv:
                csv_bl_bytes = df_bl_display.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label=t['btn_download_backlog'],
                    data=csv_bl_bytes,
                    file_name=f"packing_backlog_{datetime.date.today().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="btn_dl_packing_backlog"
                )
        else:
            st.info(t["msg_no_backlog"])

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

    prefix_w = "Tuần " if lang == "VN" else "第"
    suffix_w = "" if lang == "VN" else "周"
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
                format_func=lambda d: f"{'Tuần' if lang == 'VN' else '第'} {d.strftime('%W')} ({d.strftime('%d/%m/%Y') if lang == 'VN' else d.strftime('%Y-%m-%d')})",
                key="sb_week_share"
            )
        with col_w_metric:
            w_metric_choice = st.radio(
                t['lbl_display_metric'],
                options=[t["opt_qty"], t["opt_orders"]],
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
            t['filter_cust_type']: m_week['customer_type'],
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
            t['filter_cust_type']: "-",
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
                options=[t["opt_qty"], t["opt_orders"]],
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
            t['filter_cust_type']: m_month['customer_type'],
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
            t['filter_cust_type']: "-",
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
            df_backlog, df_events = fetch_backlog_data()
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
            tag = " [Tuần này]" if lang == "VN" else " [本周]"
        elif i < 0:
            tag = " [Tương lai]" if lang == "VN" else " [未来排班]"
        else:
            tag = ""
        lbl = f"Tuần {iso_week} ({m_start.strftime('%d/%m')} - {s_end.strftime('%d/%m/%Y')}){tag}" if lang == "VN" else f"第 {iso_week} 周 ({m_start.strftime('%m/%d')} - {s_end.strftime('%m/%d/%Y')}){tag}"
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

    dow_names = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"] if lang == "VN" else ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
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

        # Tính toán định biên Pick & Pack 2C
        uph_cfg = DOW_BENCHMARK_UPH.get(dow, {"pick_uph": 55.0, "pack_uph": 50.0})
        pick_hc = math.ceil(ord_for_hc / (uph_cfg["pick_uph"] * 8.0)) if ord_for_hc > 0 else 0
        pack_hc = math.ceil(ord_for_hc / (uph_cfg["pack_uph"] * 8.0)) if ord_for_hc > 0 else 0
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
            csv_rows.append({"Khâu vận hành" if lang=="VN" else "作业环节": stg["name"], **{date_cols[i]: stg["values"][i] for i in range(7)}})
        csv_rows.append({"Khâu vận hành" if lang=="VN" else "作业环节": t['task_total_hc'], **{date_cols[i]: daily_total_hcs[i] for i in range(7)}})
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
        vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": total_label, "Chỉ số" if lang=="VN" else "指标": t['lbl_fc_orders'], **{date_cols[i]: tot_fc_ords[i] for i in range(7)}, t['lbl_total_week']: grand_fc_o})
        vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": total_label, "Chỉ số" if lang=="VN" else "指标": t['lbl_act_orders'], **{date_cols[i]: (tot_act_ords[i] if tot_act_ords[i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (grand_act_o if grand_act_o is not None else "-")})
        vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": total_label, "Chỉ số" if lang=="VN" else "指标": t['lbl_fc_pcs'], **{date_cols[i]: tot_fc_pcss[i] for i in range(7)}, t['lbl_total_week']: grand_fc_p})
        vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": total_label, "Chỉ số" if lang=="VN" else "指标": t['lbl_act_pcs'], **{date_cols[i]: (tot_act_pcss[i] if tot_act_pcss[i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (grand_act_p if grand_act_p is not None else "-")})
        for c in display_clients:
            vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": c, "Chỉ số" if lang=="VN" else "指标": t['lbl_fc_orders'], **{date_cols[i]: clients_data[c]["fc_ords"][i] for i in range(7)}, t['lbl_total_week']: sum(clients_data[c]["fc_ords"])})
            vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": c, "Chỉ số" if lang=="VN" else "指标": t['lbl_act_orders'], **{date_cols[i]: (clients_data[c]["act_ords"][i] if clients_data[c]["act_ords"][i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (sum([v for v in clients_data[c]["act_ords"] if v is not None]) if any(v is not None for v in clients_data[c]["act_ords"]) else "-")})
            vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": c, "Chỉ số" if lang=="VN" else "指标": t['lbl_fc_pcs'], **{date_cols[i]: clients_data[c]["fc_pcs"][i] for i in range(7)}, t['lbl_total_week']: sum(clients_data[c]["fc_pcs"])})
            vol_export_rows.append({"Khách hàng" if lang=="VN" else "客户名称": c, "Chỉ số" if lang=="VN" else "指标": t['lbl_act_pcs'], **{date_cols[i]: (clients_data[c]["act_pcs"][i] if clients_data[c]["act_pcs"][i] is not None else "-") for i in range(7)}, t['lbl_total_week']: (sum([v for v in clients_data[c]["act_pcs"] if v is not None]) if any(v is not None for v in clients_data[c]["act_pcs"]) else "-")})
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
def render_pipeline_summary(cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str):
    st.markdown(f'<div class="section-title">{t["sec_pipeline"]}</div>', unsafe_allow_html=True)
    
    stages_to_fetch = ["NHẬP HÀNG", "HOÀN TRẢ", "LÊN KỆ", "NHẶT HÀNG", "ĐÓNG GÓI", "BÀN GIAO"]
    summary_data = []

    for s_name in stages_to_fetch:
        cfg_s = STAGE_CONFIGS[s_name]
        sync_st = get_global_sync_state()
        cur_v = sync_st.get("data_version", 0.0)
        try:
            df_s = fetch_stage_data(s_name, cache_version=cur_v)
            df_s_filtered = filter_by_customer(df_s, cust_type, cust_name)
            df_s_range = filter_by_date_range(df_s_filtered, start_date, end_date)
            qty = df_s_range['total_qty'].sum() if not df_s_range.empty else 0.0
            orders = df_s_range['total_orders'].sum() if not df_s_range.empty else 0
            summary_data.append({
                t['col_stage_name']: cfg_s[f'name_{lang.lower()}'].upper(),
                t['lbl_total_qty']: qty,
                t['lbl_total_orders']: orders
            })
        except Exception:
            summary_data.append({
                t['col_stage_name']: cfg_s[f'name_{lang.lower()}'].upper(),
                t['lbl_total_qty']: 0.0,
                t['lbl_total_orders']: 0
            })

    df_sum = pd.DataFrame(summary_data)

    st.markdown(f'<div class="chart-title-banner">{t["lbl_throughput_chart"]}</div>', unsafe_allow_html=True)
    fig_funnel = px.bar(
        df_sum,
        x=t['col_stage_name'],
        y=t['lbl_total_qty'],
        text=t['lbl_total_qty'],
        color=t['col_stage_name'],
        color_discrete_sequence=['#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899'],
        template="plotly_dark"
    )
    fig_funnel.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig_funnel.update_layout(
        title=None,
        paper_bgcolor="#1e293b",
        plot_bgcolor="#0f172a",
        margin=dict(l=30, r=30, t=10, b=40),
        showlegend=False
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

    display_sum = df_sum.copy()
    display_sum[t['lbl_total_qty']] = display_sum[t['lbl_total_qty']].apply(lambda x: f"{x:,.0f}")
    display_sum[t['lbl_total_orders']] = display_sum[t['lbl_total_orders']].apply(lambda x: f"{x:,}")
    st.dataframe(display_sum, width="stretch", hide_index=True)


def render_inventory_dashboard(df_inventory: pd.DataFrame, cust_type: str, cust_name: str, start_date: datetime.date, end_date: datetime.date, t: dict, lang: str):
    """
    Dashboard chuyên biệt cho khâu TRONG KHO (Kiểm kê & Đối soát tồn kho):
    - Đếm số nhiệm vụ kiểm kê (Task NO) mỗi ngày
    - Đếm số SKU kiểm kê mỗi ngày
    - Tổng sản lượng tồn hệ thống (system_qty) và thực tế (actual_qty)
    - Chênh lệch PCS giữa hệ thống và thực tế (difference_qty)
    - Phân tích chi tiết và đối soát theo từng khách hàng
    """
    if df_inventory.empty:
        st.info("Chưa có dữ liệu kiểm kê trong cơ sở dữ liệu." if lang == "VN" else "数据库中暂无盘点数据。")
        return

    df = df_inventory.copy()

    # 1. Lọc theo khoảng thời gian đối soát
    df = df[(df['inv_date'] >= start_date) & (df['inv_date'] <= end_date)]

    # 2. Lọc theo loại khách hàng
    if cust_type and cust_type != t["all_option"]:
        df = df[df['customer_type'] == cust_type]

    # 3. Lọc theo tên khách hàng cụ thể
    if cust_name and cust_name != t["all_option"]:
        df = df[df['owner_name'] == cust_name]

    if df.empty:
        st.warning("Không tìm thấy dữ liệu kiểm kê phù hợp với điều kiện lọc." if lang == "VN" else "所选筛选条件下未检索到盘点记录。")
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
                    'owner_name': 'Khác' if lang == 'VN' else '其他',
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
            "Tìm kiếm theo Mã phiếu hoặc Mã SKU:" if lang == "VN" else "按盘点单号或SKU品项搜索：",
            value="",
            placeholder="Nhập mã phiếu kiểm kê hoặc mã hàng..." if lang == "VN" else "输入盘点任务号或商品编码..."
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

@st.fragment(run_every="15s")
def render_day_tab_fragment(stage_key, start_date, end_date, cur_cfg, selected_cust_type, selected_cust_name, t, lang):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_raw = fetch_stage_data(stage_key, cache_version=cur_v)
    df_filtered = filter_by_customer(df_raw, selected_cust_type, selected_cust_name)
    render_day_tab(df_filtered, start_date, end_date, cur_cfg, stage_key, selected_cust_type, selected_cust_name, t, lang)

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
def render_inventory_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang):
    sync_st = get_global_sync_state()
    cur_v = sync_st.get("data_version", 0.0)
    df_inv = fetch_inventory_data(cache_version=cur_v)
    render_inventory_dashboard(df_inv, selected_cust_type, selected_cust_name, start_date, end_date, t, lang)

@st.fragment(run_every="20s")
def render_summary_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang):
    render_pipeline_summary(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)

@st.fragment(run_every="30s")
def render_planning_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang):
    render_headcount_planning(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)

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

        # 4. LANGUAGE SWITCHER VI - CN (CĂN GIỮA HOÀN TOÀN)
        st.markdown(f'<div class="lang-center-title">{t["lang_header"]}</div>', unsafe_allow_html=True)

        lang_options = ["VI", "CN"]
        cur_lang_default = "VI" if st.session_state.current_lang == "VN" else "CN"
        
        col_pad_l, col_center_pill, col_pad_r = st.columns([1, 2, 1])
        with col_center_pill:
            chosen_lang_pill = st.pills(
                label="Language Switcher",
                options=lang_options,
                default=cur_lang_default,
                selection_mode="single",
                label_visibility="collapsed"
            )

        new_lang = "VN" if chosen_lang_pill == "VI" else "ZH"
        if new_lang != st.session_state.current_lang:
            st.session_state.current_lang = new_lang
            st.rerun()

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

    # Khởi tạo ngày mặc định nếu chưa có (mặc định là hôm nay)
    if "date_range_val" not in st.session_state:
        st.session_state.date_range_val = (today_vn, today_vn)

    # 1. Tiêu đề khâu vận hành nằm phía trên
    st.markdown(f'<div style="font-size: 22px; font-weight: 800; color: #ffffff; letter-spacing: 0.5px; margin: 4px 0 14px 0;">{cur_stage_title}</div>', unsafe_allow_html=True)

    if cur_cfg.get("type") in ["planning"]:
        selected_cust_type = t["all_option"]
        selected_cust_name = t["all_option"]
        start_date, end_date = st.session_state.date_range_val
        df_filtered = df_raw
    else:
        # 2. Bộ lọc khách hàng và Khoảng thời gian đối soát nằm bên phải (tinh gọn, loại bỏ nút làm mới)
        col_filter_type, col_filter_name, col_date_range = st.columns([1.0, 1.5, 1.5])
        
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

        with col_date_range:
            st.markdown("<div style='margin-top: 4px;'>", unsafe_allow_html=True)
            date_input_res = st.date_input(
                t['date_range_header'],
                value=st.session_state.date_range_val,
                max_value=today_vn + timedelta(days=1),
                key="main_date_range_picker"
            )
            if isinstance(date_input_res, (list, tuple)):
                if len(date_input_res) == 2:
                    start_date, end_date = date_input_res
                elif len(date_input_res) == 1:
                    start_date = end_date = date_input_res[0]
                else:
                    start_date = end_date = today_vn
            else:
                start_date = end_date = date_input_res

            if start_date > end_date:
                start_date, end_date = end_date, start_date

            st.session_state.date_range_val = (start_date, end_date)
            st.markdown("</div>", unsafe_allow_html=True)

        df_filtered = filter_by_customer(df_raw, selected_cust_type, selected_cust_name)

    # ==========================================
    # ĐIỀU HƯỚNG HIỂN THỊ NỘI DUNG THEO KHÂU
    # ==========================================
    if cur_cfg.get("type") in ["planning", "backlog"]:
        render_planning_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)
    elif cur_cfg.get("type") == "summary":
        render_summary_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)
    elif cur_cfg.get("type") == "inventory":
        render_inventory_fragment(selected_cust_type, selected_cust_name, start_date, end_date, t, lang)
    else:
        tab_day, tab_week, tab_month = st.tabs([
            t['tab_day'],
            t['tab_week'],
            t['tab_month']
        ])

        with tab_day:
            render_day_tab_fragment(selected_key, start_date, end_date, cur_cfg, selected_cust_type, selected_cust_name, t, lang)

        with tab_week:
            render_week_tab_fragment(selected_key, cur_cfg, selected_cust_type, selected_cust_name, t, lang)

        with tab_month:
            render_month_tab_fragment(selected_key, cur_cfg, selected_cust_type, selected_cust_name, t, lang)




if __name__ == "__main__":
    main()
