"""
Module Zalo Notifier cho JDL Warehouse Dashboard
Hỗ trợ gửi tin nhắn cảnh báo đơn hàng cận hạn / trễ hạn SLA vào Nhóm Chat Zalo (Zalo Group)
Hỗ trợ 2 phương thức:
  1. Zalo Group Webhook URL (nhanh gọn qua webhook endpoint)
  2. Zalo OpenAPI v3.0 (qua Zalo OA access_token và group_id)
"""

import os
import re
import time
import json
import hashlib
import datetime
from datetime import timedelta, timezone
import requests
import pandas as pd

VN_TZ = timezone(timedelta(hours=7))

# Bộ nhớ tạm để chống spam (cooldown & deduplication)
_LAST_ALERT_STATE = {
    "signature": "",
    "timestamp": 0.0,
    "last_count": 0
}


def get_zalo_config() -> dict:
    """Đọc cấu hình Zalo từ Streamlit secrets hoặc biến môi trường."""
    cfg = {
        "bot_token": os.environ.get("ZALO_BOT_TOKEN", ""),
        "chat_id": os.environ.get("ZALO_CHAT_ID", ""),
        "webhook_url": os.environ.get("ZALO_WEBHOOK_URL", ""),
        "access_token": os.environ.get("ZALO_ACCESS_TOKEN", ""),
        "group_id": os.environ.get("ZALO_GROUP_ID", ""),
        "enabled": True,
        "urgent_hours": 1.0,      # Cận hạn <= 1 tiếng
        "cooldown_minutes": 30.0  # Không spam cùng 1 danh sách trong 30 phút
    }

    try:
        import streamlit as st
        if hasattr(st, "secrets") and "zalo" in st.secrets:
            z_sec = dict(st.secrets["zalo"])
            cfg["bot_token"] = z_sec.get("bot_token", cfg["bot_token"])
            cfg["chat_id"] = z_sec.get("chat_id", cfg["chat_id"])
            cfg["webhook_url"] = z_sec.get("webhook_url", cfg["webhook_url"])
            cfg["access_token"] = z_sec.get("access_token", cfg["access_token"])
            cfg["group_id"] = z_sec.get("group_id", cfg["group_id"])
            cfg["enabled"] = bool(z_sec.get("enabled", cfg["enabled"]))
            cfg["urgent_hours"] = float(z_sec.get("urgent_hours", cfg["urgent_hours"]))
            cfg["cooldown_minutes"] = float(z_sec.get("cooldown_minutes", cfg["cooldown_minutes"]))
    except Exception:
        pass

    # Đọc từ file secrets.toml cục bộ nếu chưa có cấu hình
    if not (cfg["bot_token"] or cfg["webhook_url"] or cfg["access_token"]):
        sec_file = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
        if os.path.exists(sec_file):
            try:
                import tomllib
                with open(sec_file, "rb") as f_sec:
                    t_sec = tomllib.load(f_sec)
                    if "zalo" in t_sec:
                        z_sec = t_sec["zalo"]
                        cfg["bot_token"] = z_sec.get("bot_token", cfg["bot_token"])
                        cfg["chat_id"] = z_sec.get("chat_id", cfg["chat_id"])
                        cfg["webhook_url"] = z_sec.get("webhook_url", cfg["webhook_url"])
                        cfg["access_token"] = z_sec.get("access_token", cfg["access_token"])
                        cfg["group_id"] = z_sec.get("group_id", cfg["group_id"])
                        cfg["enabled"] = bool(z_sec.get("enabled", cfg["enabled"]))
                        cfg["urgent_hours"] = float(z_sec.get("urgent_hours", cfg["urgent_hours"]))
                        cfg["cooldown_minutes"] = float(z_sec.get("cooldown_minutes", cfg["cooldown_minutes"]))
            except Exception:
                pass

    return cfg


def is_zalo_configured() -> bool:
    """Kiểm tra xem hệ thống đã được cấu hình Zalo hay chưa."""
    cfg = get_zalo_config()
    if not cfg.get("enabled"):
        return False
    return bool(
        (cfg.get("bot_token") and cfg.get("chat_id"))
        or cfg.get("webhook_url")
        or (cfg.get("access_token") and cfg.get("group_id"))
    )


def send_zalo_message(message_text: str, target_chat_id: str = None) -> tuple[bool, str]:
    """
    Gửi tin nhắn văn bản vào Nhóm Chat Zalo.
    Hỗ trợ 3 phương thức:
      1. Zalo Bot Platform API (bot_token + chat_id) - Phổ biến & chuẩn mới nhất
      2. Webhook URL
      3. Zalo OA OpenAPI (access_token + group_id)
    """
    cfg = get_zalo_config()
    if not cfg.get("enabled"):
        return False, "Tính năng thông báo Zalo đang bị tắt (enabled=False)."

    bot_token = cfg.get("bot_token", "").strip()
    chat_id = (target_chat_id or cfg.get("chat_id", "")).strip()
    webhook_url = cfg.get("webhook_url", "").strip()
    access_token = cfg.get("access_token", "").strip()
    group_id = cfg.get("group_id", "").strip()

    # 1. Ưu tiên: Zalo Bot Platform API (chuẩn bot-api.zaloplatforms.com)
    if bot_token and chat_id:
        url = f"https://bot-api.zaloplatforms.com/bot{bot_token}/sendMessage"
        headers = {"Content-Type": "application/json"}
        payload = {
            "chat_id": chat_id,
            "text": message_text
        }
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=15)
            data = res.json()
            if res.status_code == 200 and data.get("ok", True):
                return True, "Gửi tin nhắn vào Nhóm Zalo thành công qua Zalo Bot API!"
            else:
                err_desc = data.get("description") or data.get("message") or res.text[:200]
                return False, f"Zalo Bot API lỗi [{res.status_code}]: {err_desc}"
        except Exception as e:
            return False, f"Lỗi kết nối Zalo Bot Platform: {str(e)}"

    # 2. Gửi qua Webhook URL nếu được cấu hình
    if webhook_url:
        try:
            payload = {"text": message_text, "message": message_text}
            headers = {"Content-Type": "application/json"}
            res = requests.post(webhook_url, json=payload, headers=headers, timeout=12)
            if res.status_code in [200, 201, 204]:
                return True, "Gửi tin nhắn qua Webhook Zalo thành công!"
            else:
                return False, f"Lỗi Webhook HTTP {res.status_code}: {res.text[:200]}"
        except Exception as e:
            return False, f"Lỗi kết nối Webhook Zalo: {str(e)}"

    # 3. Gửi qua Zalo OpenAPI chính thức (v3.0/oa/group/message)
    if access_token and group_id:
        endpoint = "https://openapi.zalo.me/v3.0/oa/group/message"
        headers = {
            "Content-Type": "application/json",
            "access_token": access_token
        }
        payload = {
            "recipient": {
                "group_id": group_id
            },
            "message": {
                "text": message_text
            }
        }
        try:
            res = requests.post(endpoint, json=payload, headers=headers, timeout=15)
            data = res.json()
            if data.get("error") == 0 or res.status_code == 200:
                return True, "Gửi tin nhắn vào Nhóm Zalo thành công qua OpenAPI!"
            else:
                err_msg = data.get("message", "Unknown error")
                err_code = data.get("error", res.status_code)
                return False, f"Zalo API trả lỗi [{err_code}]: {err_msg}"
        except Exception as e:
            return False, f"Lỗi gọi Zalo OpenAPI: {str(e)}"

    return False, "Chưa cấu hình Zalo Bot Token + Chat ID, hoặc Webhook URL."


def format_sla_alert_message(df_urgent: pd.DataFrame, df_overdue: pd.DataFrame, now_vn: datetime.datetime, urgent_hours: float) -> str:
    """Tạo nội dung tin nhắn tiếng Việt định dạng tối giản, không emoji, chỉ giữ thông tin cần thiết."""
    tot_urgent = len(df_urgent)
    tot_overdue = len(df_overdue)
    tot_alert = tot_urgent + tot_overdue

    time_str = now_vn.strftime("%H:%M:%S %d/%m/%Y")
    
    lines = [
        "[CẢNH BÁO SLA ĐÓNG GÓI - KHO JDL]",
        f"Thời gian: {time_str}",
        f"Tổng đơn cần xử lý gấp: {tot_alert:,} đơn"
    ]

    # Đơn đã trễ hạn
    if tot_overdue > 0:
        lines.append("")
        lines.append(f"ĐÃ QUÁ HẠN: {tot_overdue:,} đơn")
        cust_overdue = df_overdue.groupby('owner_name').agg(
            cnt=('order_no', 'nunique'),
            times=('production_end_time', lambda s: ", ".join(sorted(set(str(x)[11:16] for x in s if pd.notnull(x)))[:3]))
        ).sort_values(by='cnt', ascending=False)
        for c_name, row in cust_overdue.head(5).iterrows():
            lines.append(f"- {c_name}: {row['cnt']:,} đơn (Hạn: {row['times']})")
        if len(cust_overdue) > 5:
            lines.append(f"- Và {len(cust_overdue) - 5} khách hàng khác...")

    # Đơn sắp quá hạn trong vòng <= urgent_hours tiếng
    if tot_urgent > 0:
        h_label = f"{urgent_hours:g} tiếng" if urgent_hours >= 1 else f"{int(urgent_hours*60)} phút"
        lines.append("")
        lines.append(f"SẮP QUÁ HẠN (<= {h_label}): {tot_urgent:,} đơn")
        cust_urgent = df_urgent.groupby('owner_name').agg(
            cnt=('order_no', 'nunique'),
            times=('production_end_time', lambda s: ", ".join(sorted(set(str(x)[11:16] for x in s if pd.notnull(x)))[:3]))
        ).sort_values(by='cnt', ascending=False)
        for c_name, row in cust_urgent.head(6).iterrows():
            lines.append(f"- {c_name}: {row['cnt']:,} đơn (Hạn: {row['times']})")
        if len(cust_urgent) > 6:
            lines.append(f"- Và {len(cust_urgent) - 6} khách hàng khác...")

    return "\n".join(lines)


def format_detailed_backlog_list(df_backlog: pd.DataFrame, now_vn: datetime.datetime = None, max_hours: float = 2.5, last_update_str: str = None) -> list[str]:
    """
    Tạo danh sách toàn bộ các đơn sắp trễ SLA (quá hạn hoặc sắp quá hạn <= max_hours tiếng).
    Phân tách rõ ràng giữa ĐÃ QUÁ HẠN và SẮP QUÁ HẠN:
      - Đơn đã trễ hạn luôn được đưa lên ĐẦU danh sách để ưu tiên xử lý gấp.
      - Sắp xếp theo: Khách hàng -> Thời gian còn lại.
    Trả về danh sách các tin nhắn (tự động chia nhỏ nếu vượt quá 1800 ký tự để gửi qua Zalo).
    """
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    time_str = now_vn.strftime('%H:%M:%S %d/%m/%Y')
    if last_update_str is None:
        last_update_str = get_last_data_update_time("BACKLOG")

    time_headers = [f"Thời gian báo cáo: {time_str}"]
    if last_update_str:
        time_headers.append(f"Dữ liệu cập nhật lúc: {last_update_str}")
    time_header_text = "\n".join(time_headers)

    if df_backlog is None or df_backlog.empty:
        return [f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\n{time_header_text}\nKhông có dữ liệu đơn Backlog tồn đọng."]

    df = df_backlog.copy()
    if 'prod_dt' not in df.columns:
        df['prod_dt'] = pd.to_datetime(df['production_end_time'], errors='coerce')

    df_today = df[df['prod_dt'].dt.date == now_vn.date()].copy()
    if df_today.empty:
        return [f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\n{time_header_text}\nKhông có đơn nào tồn đọng trong ngày hôm nay."]

    df_today['diff_sec'] = (df_today['prod_dt'] - now_vn).dt.total_seconds()
    # Lọc đơn rủi ro: quá hạn (diff_sec < 0) hoặc sắp quá hạn trong max_hours
    df_risk = df_today[df_today['diff_sec'] <= max_hours * 3600].copy()

    if df_risk.empty:
        next_min_sec = df_today['diff_sec'].min()
        next_h = next_min_sec / 3600
        return [
            f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\n"
            f"{time_header_text}\n"
            f"Hiện không có đơn nào quá hạn hoặc sắp quá hạn (trong {max_hours:g} tiếng tới).\n"
            f"Đơn có hạn gần nhất còn khoảng {next_h:.1f} tiếng nữa.\n"
            f"Tổng đơn tồn hôm nay: {len(df_today):,} đơn."
        ]

    # Tách bạch rõ ràng ĐÃ QUÁ HẠN vs SẮP QUÁ HẠN
    df_overdue = df_risk[df_risk['diff_sec'] < 0].copy()
    df_urgent  = df_risk[df_risk['diff_sec'] >= 0].copy()

    tot_overdue = len(df_overdue)
    tot_urgent  = len(df_urgent)
    total_orders = tot_overdue + tot_urgent

    # Tiêu đề tóm tắt rõ số lượng từng loại
    if tot_overdue > 0 and tot_urgent > 0:
        summary_line = f"Tổng đơn cần xử lý: {total_orders:,} đơn (Đã trễ: {tot_overdue:,} | Sắp trễ: {tot_urgent:,})"
    elif tot_overdue > 0:
        summary_line = f"Tổng đơn ĐÃ QUÁ HẠN: {tot_overdue:,} đơn"
    else:
        summary_line = f"Tổng đơn SẮP QUÁ HẠN: {tot_urgent:,} đơn"

    messages = []
    current_lines = [
        "[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]",
    ] + time_headers + [
        summary_line,
        ""
    ]

    def _append_section(sub_df: pd.DataFrame, section_title: str):
        nonlocal current_lines
        if sub_df.empty:
            return
        
        current_lines.append(section_title)
        sub_sorted = sub_df.sort_values(by=['owner_name', 'diff_sec'], ascending=[True, True])
        
        for owner, grp in sub_sorted.groupby('owner_name', sort=False):
            owner_header = f"Khách hàng: {owner} ({len(grp)} đơn)"
            lines_for_owner = [owner_header]

            for _, row in grp.iterrows():
                d_sec = row['diff_sec']
                if d_sec < 0:
                    over_sec = abs(d_sec)
                    h = int(over_sec // 3600)
                    m = int((over_sec % 3600) // 60)
                    t_rem = f"ĐÃ TRỄ {h}h{m:02d}p" if h > 0 else f"ĐÃ TRỄ {m} phút"
                else:
                    h = int(d_sec // 3600)
                    m = int((d_sec % 3600) // 60)
                    t_rem = f"còn {h}h{m:02d}p" if h > 0 else f"còn {m} phút"

                dt_str = str(row['production_end_time'])[11:16]
                lines_for_owner.append(f"- {row['order_no']}: {t_rem} (Hạn {dt_str})")

            lines_for_owner.append("")  # Dòng trống cách giữa các khách hàng

            # Kiểm tra độ dài tin nhắn (Zalo giới hạn ~2000 ký tự)
            test_text = "\n".join(current_lines + lines_for_owner)
            if len(test_text) > 1800:
                messages.append("\n".join(current_lines).strip())
                current_lines = [
                    "[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA (tiếp theo)]",
                    ""
                ] + lines_for_owner
            else:
                current_lines.extend(lines_for_owner)

    # 1. ĐƯA ĐƠN ĐÃ QUÁ HẠN LÊN ĐẦU TIÊN
    if tot_overdue > 0:
        _append_section(df_overdue, f"ĐÃ QUÁ HẠN ({tot_overdue:,} đơn)")

    # 2. TIẾP THEO LÀ ĐƠN SẮP QUÁ HẠN
    if tot_urgent > 0:
        h_label = f"{max_hours:g} tiếng" if max_hours >= 1 else f"{int(max_hours*60)} phút"
        _append_section(df_urgent, f"SẮP QUÁ HẠN (<= {h_label}) ({tot_urgent:,} đơn)")

    if current_lines:
        messages.append("\n".join(current_lines).strip())

    return messages


def handle_backlog_command(target_chat_id: str = None) -> tuple[bool, str]:
    """
    Xử lý lệnh /backlog: Lấy dữ liệu mới nhất từ BigQuery,
    tạo danh sách đơn sắp trễ (khách hàng -> time remaining) và gửi vào Zalo.
    """
    res = scan_and_alert_backlog_sla(force_send=True, notify_if_safe=True, target_chat_id=target_chat_id)
    if res.get("status") in ["sent", "no_today_orders", "safe"]:
        return True, res.get("message")
    return False, res.get("message")


def classify_packing_client(name: str) -> str:
    """Phân loại khách hàng B2B (KKV, Chint, Envision) vs B2C (các khách còn lại)."""
    u = str(name).upper()
    if any(k in u for k in ['KKV', 'JINHE', 'CHINT', 'ENVISION']):
        return 'B2B'
    return 'B2C'


def clean_packing_client_name(name: str) -> str:
    """Rút gọn tên thương hiệu khách hàng cho gọn gàng, dễ nhìn."""
    u = str(name).upper()
    if 'KKV' in u or 'JINHE' in u:
        return 'KKV'
    if 'CHINT' in u:
        return 'CHINT'
    if 'ENVISION' in u:
        return 'ENVISION'
    if 'CHICMAX' in u:
        return 'CHICMAX'
    if 'YAOYAO' in u:
        return 'YAOYAO'
    if 'VENTION' in u:
        return 'VENTIONVN'
    if 'MIRA' in u:
        return 'MIRA-LOTUS'
    if 'HADAY' in u:
        return 'HADAY'
    if 'POP MART' in u:
        return 'POP MART'
    if 'SHUNCAI' in u:
        return 'SHUNCAI'
    if 'CORESNAP' in u:
        return 'CORESNAP'
    if 'TOP ENTERPRISE' in u:
        return 'TOP ENTERPRISE'
    return str(name).strip()


def format_packing_progress_report(df_pack: pd.DataFrame, df_bl: pd.DataFrame, now_vn: datetime.datetime = None, last_update_str: str = None) -> str:
    """
    Tạo báo cáo tiến độ đóng gói theo Mẫu 2:
    - Chia 2 nhóm rõ ràng: B2B (KKV, Chint, Envision) và B2C (còn lại).
    - Khách nào sản lượng = 0 thì không hiện.
    - Không gom 'khách khác', chi tiết từng khách có sản lượng > 0.
    """
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    time_str = now_vn.strftime('%H:%M %d/%m/%Y')
    if last_update_str is None:
        last_update_str = get_last_data_update_time("PACKING")

    time_headers = [f"Thời gian báo cáo: {time_str}"]
    if last_update_str:
        time_headers.append(f"Dữ liệu cập nhật lúc: {last_update_str}")
    time_header_text = "\n".join(time_headers)

    bl_orders = 0
    if df_bl is not None and not df_bl.empty:
        if 'order_no' in df_bl.columns:
            if len(df_bl) == 1 and pd.api.types.is_numeric_dtype(df_bl['order_no']):
                bl_orders = int(df_bl['order_no'].iloc[0])
            else:
                bl_orders = int(df_bl['order_no'].nunique())
        elif 'total_orders' in df_bl.columns:
            bl_orders = int(df_bl['total_orders'].iloc[0])
        else:
            bl_orders = len(df_bl)

    if df_pack is None or df_pack.empty:
        return (
            f"[TIẾN ĐỘ ĐÓNG GÓI - KHO JDL]\n"
            f"{time_header_text}\n\n"
            f"TỔNG QUAN\n"
            f"- Đã đóng gói: 0 đơn (0 pcs)\n"
            f"- Tồn chờ đóng (Backlog): {bl_orders:,} đơn\n"
            f"- Tỷ lệ hoàn thành: 0.0%\n\n"
            f"Chưa có sản lượng đóng gói nào được ghi nhận hôm nay."
        )

    df = df_pack.copy()
    df['orders'] = pd.to_numeric(df['orders'], errors='coerce').fillna(0)
    df['pcs'] = pd.to_numeric(df['pcs'], errors='coerce').fillna(0)
    df = df[df['orders'] > 0]  # Bỏ khách có sản lượng = 0

    df['type'] = df['owner_name'].apply(classify_packing_client)
    df['display_name'] = df['owner_name'].apply(clean_packing_client_name)

    # Gom theo type và display_name
    df_agg = df.groupby(['type', 'display_name'], as_index=False)[['orders', 'pcs']].sum()

    tot_packed_orders = int(df_agg['orders'].sum())
    tot_packed_pcs = int(df_agg['pcs'].sum())
    tot_workload = tot_packed_orders + bl_orders
    pct_done = (tot_packed_orders / tot_workload * 100) if tot_workload > 0 else 0.0

    df_b2b = df_agg[df_agg['type'] == 'B2B'].sort_values(by='orders', ascending=False)
    df_b2c = df_agg[df_agg['type'] == 'B2C'].sort_values(by='orders', ascending=False)

    b2b_orders = int(df_b2b['orders'].sum())
    b2b_pcs = int(df_b2b['pcs'].sum())
    b2c_orders = int(df_b2c['orders'].sum())
    b2c_pcs = int(df_b2c['pcs'].sum())

    lines = [
        "[TIẾN ĐỘ ĐÓNG GÓI - KHO JDL]",
    ] + time_headers + [
        "",
        "TỔNG QUAN",
        f"- Đã đóng gói: {tot_packed_orders:,} đơn ({tot_packed_pcs:,} pcs)",
        f"- Tồn chờ đóng (Backlog): {bl_orders:,} đơn",
        f"- Tỷ lệ hoàn thành: {pct_done:.1f}%",
        "",
        f"KHÁCH HÀNG B2B (Đã đóng: {b2b_orders:,} đơn - {b2b_pcs:,} pcs)"
    ]

    if df_b2b.empty or b2b_orders == 0:
        lines.append("- Chưa có sản lượng")
    else:
        for _, r in df_b2b.iterrows():
            lines.append(f"- {r['display_name']}: {int(r['orders']):,} đơn ({int(r['pcs']):,} pcs)")

    lines.append("")
    lines.append(f"KHÁCH HÀNG B2C (Đã đóng: {b2c_orders:,} đơn - {b2c_pcs:,} pcs)")

    if df_b2c.empty or b2c_orders == 0:
        lines.append("- Chưa có sản lượng")
    else:
        for _, r in df_b2c.iterrows():
            lines.append(f"- {r['display_name']}: {int(r['orders']):,} đơn ({int(r['pcs']):,} pcs)")

    # Nếu có thông tin pack_time thì bổ sung dự báo Clear Backlog ở cuối
    if 'pack_time' in df_pack.columns and not df_pack.empty and bl_orders > 0:
        try:
            min_t = df_pack['pack_time'].min()
            max_t = df_pack['pack_time'].max()
            elapsed_hr = max((max_t - min_t).total_seconds() / 3600.0, 0.5)
            shift_avg_speed = max(int(tot_packed_orders / elapsed_hr), 1)

            t_60m_ago = max_t - datetime.timedelta(minutes=60)
            recent_orders = df_pack[df_pack['pack_time'] >= t_60m_ago]['order_no'].nunique() if 'order_no' in df_pack.columns else 0
            recent_speed = max(recent_orders, 1)

            def fmt_dur(hours):
                m = int(hours * 60)
                h = m // 60
                rem_m = m % 60
                return f"{h}h{rem_m:02d}p" if h > 0 else f"{rem_m} phút"

            dur_recent = fmt_dur(bl_orders / recent_speed)
            etc_recent = (now_vn + datetime.timedelta(hours=bl_orders / recent_speed)).strftime('%H:%M')

            lines.append("")
            lines.append("DỰ KIẾN CLEAR BACKLOG (ETC)")
            lines.append(f"- Tốc độ hiện tại: {recent_speed:,} đơn/giờ (TB ca: {shift_avg_speed:,} đơn/giờ)")
            lines.append(f"- Cần thêm: ~{dur_recent} để xử lý {bl_orders:,} đơn tồn")
            lines.append(f"- Dự kiến hoàn thành: ~{etc_recent} hôm nay")
        except Exception:
            pass

    return "\n".join(lines)


def format_etc_report(df_pack: pd.DataFrame, df_bl: pd.DataFrame, now_vn: datetime.datetime = None, last_update_str: str = None) -> str:
    """Tạo nội dung tin nhắn chuyên sâu dự báo thời gian clear backlog."""
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    time_str = now_vn.strftime('%H:%M %d/%m/%Y')
    if last_update_str is None:
        last_update_str = get_last_data_update_time("PACKING")

    time_headers = [f"Thời gian báo cáo: {time_str}"]
    if last_update_str:
        time_headers.append(f"Dữ liệu cập nhật lúc: {last_update_str}")
    time_header_text = "\n".join(time_headers)

    bl_orders = 0
    if df_bl is not None and not df_bl.empty:
        if 'order_no' in df_bl.columns:
            if len(df_bl) == 1 and pd.api.types.is_numeric_dtype(df_bl['order_no']):
                bl_orders = int(df_bl['order_no'].iloc[0])
            else:
                bl_orders = int(df_bl['order_no'].nunique())
        elif 'total_orders' in df_bl.columns:
            bl_orders = int(df_bl['total_orders'].iloc[0])
        else:
            bl_orders = len(df_bl)
    total_packed = int(df_pack['order_no'].nunique()) if (df_pack is not None and not df_pack.empty) else 0

    if bl_orders == 0:
        return f"[DỰ BÁO THỜI GIAN CLEAR BACKLOG - KHO JDL]\n{time_header_text}\n\nHiện tại kho đã CLEAR HẾT BACKLOG! Không còn đơn tồn đọng."

    if df_pack is None or df_pack.empty or total_packed == 0:
        return f"[DỰ BÁO THỜI GIAN CLEAR BACKLOG - KHO JDL]\n{time_header_text}\n\nChưa có sản lượng đóng gói nào hôm nay để tính tốc độ clear hàng."

    min_t = df_pack['pack_time'].min()
    max_t = df_pack['pack_time'].max()
    elapsed_hr = max((max_t - min_t).total_seconds() / 3600.0, 0.5)
    shift_avg_speed = max(int(total_packed / elapsed_hr), 1)

    t_60m_ago = max_t - datetime.timedelta(minutes=60)
    recent_orders = df_pack[df_pack['pack_time'] >= t_60m_ago]['order_no'].nunique()
    recent_speed = max(recent_orders, 1)

    def fmt_dur(hours):
        m = int(hours * 60)
        h = m // 60
        rem_m = m % 60
        return f"{h}h{rem_m:02d}p" if h > 0 else f"{rem_m} phút"

    dur_recent = fmt_dur(bl_orders / recent_speed)
    etc_recent = (now_vn + datetime.timedelta(hours=bl_orders / recent_speed)).strftime('%H:%M')

    dur_avg = fmt_dur(bl_orders / shift_avg_speed)
    etc_avg = (now_vn + datetime.timedelta(hours=bl_orders / shift_avg_speed)).strftime('%H:%M')

    s_min = min_t.strftime('%H:%M')
    s_max = max_t.strftime('%H:%M')

    lines = [
        "[DỰ BÁO THỜI GIAN CLEAR BACKLOG - KHO JDL]",
    ] + time_headers + [
        "",
        "TIẾN ĐỘ HIỆN TẠI",
        f"- Đã đóng gói: {total_packed:,} đơn (từ {s_min} đến {s_max})",
        f"- Tồn chờ đóng (Backlog): {bl_orders:,} đơn",
        "",
        "TỐC ĐỘ ĐÓNG GÓI",
        f"- 1 giờ gần nhất: {recent_speed:,} đơn/giờ",
        f"- Trung bình cả ca: {shift_avg_speed:,} đơn/giờ",
        "",
        "DỰ KIẾN HOÀN THÀNH (CLEAR TIME)",
        f"- Theo tốc độ gần nhất: cần ~{dur_recent} -> Xong lúc ~{etc_recent} hôm nay",
        f"- Theo tốc độ trung bình ca: cần ~{dur_avg} -> Xong lúc ~{etc_avg} hôm nay"
    ]
    return "\n".join(lines)


def get_bigquery_client():
    """Tạo BigQuery Client sử dụng bộ nạp chứng thực đồng nhất (hỗ trợ cả Env Var trên Cloud)."""
    from google.cloud import bigquery
    import sync_wms_local
    creds = sync_wms_local.get_credentials()
    return bigquery.Client(credentials=creds, project="jingdong-505914")


def get_last_data_update_time(folder_name: str, client = None) -> str | None:
    """Lấy thời điểm cập nhật dữ liệu gần nhất từ bảng sync_file_logs theo giờ VN (HH:MM dd/mm/yyyy)."""
    try:
        if client is None:
            client = get_bigquery_client()
        sql = f"""
        SELECT FORMAT_TIMESTAMP('%H:%M %d/%m/%Y', MAX(synced_at), 'Asia/Ho_Chi_Minh') as last_update
        FROM `jingdong-505914.wms_data.sync_file_logs`
        WHERE folder_name = '{folder_name}'
        """
        df = client.query(sql).to_dataframe()
        if not df.empty and pd.notna(df['last_update'].iloc[0]) and df['last_update'].iloc[0]:
            return str(df['last_update'].iloc[0])
    except Exception:
        pass
    return None


def handle_etc_command(target_chat_id: str = None) -> tuple[bool, str]:
    """Xử lý lệnh etc / clear: Truy vấn BigQuery và gửi dự báo thời gian clear backlog vào Zalo."""
    cfg = get_zalo_config()
    chat_id = target_chat_id or cfg.get("chat_id")
    if not chat_id:
        return False, "Chưa xác định được chat_id nhóm Zalo."

    try:
        client = get_bigquery_client()

        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
        today = now_vn.date()

        sql_pack = f"""
        SELECT pack_time, order_no
        FROM `jingdong-505914.wms_data.fact_packing`
        WHERE DATE(pack_time) = '{today}'
        """
        df_pack = client.query(sql_pack).to_dataframe()

        sql_bl = """
        SELECT order_no
        FROM `jingdong-505914.wms_data.fact_backlog`
        """
        df_bl = client.query(sql_bl).to_dataframe()

    except Exception as e:
        err_msg = f"[LỖI] Không thể đọc dữ liệu để dự báo ETC: {e}"
        send_zalo_message(err_msg)
        return False, err_msg

    report_text = format_etc_report(df_pack, df_bl, now_vn)

    if cfg.get("bot_token") and chat_id:
        url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
        res = requests.post(url, json={"chat_id": chat_id, "text": report_text}, headers={"Content-Type": "application/json"}, timeout=15)
        if res.status_code == 200:
            return True, "Đã gửi dự báo Clear Time vào Zalo thành công."
        return False, f"Lỗi Zalo API [{res.status_code}]: {res.text}"
    else:
        return send_zalo_message(report_text)


_LAST_PACKING_ALERT_STATE = {
    "signature": "",
    "timestamp": 0.0,
    "last_orders": 0
}


def scan_and_alert_packing_progress(force_send: bool = False, cooldown_minutes: float = 30.0) -> dict:
    """
    Tự động quét và gửi báo cáo tiến độ đóng gói vào Nhóm Zalo khi có file PACKING mới nạp.
    - Có cơ chế chống spam (cooldown_minutes) nếu sản lượng không thay đổi.
    """
    global _LAST_PACKING_ALERT_STATE

    cfg = get_zalo_config()
    if not cfg.get("enabled"):
        return {"status": "disabled", "message": "Tính năng thông báo Zalo đang tắt."}

    chat_id = cfg.get("chat_id")
    if not chat_id:
        return {"status": "not_configured", "message": "Chưa cấu hình chat_id nhóm Zalo."}

    try:
        client = get_bigquery_client()

        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
        today = now_vn.date()

        sql_pack = f"""
        SELECT 
            owner_name,
            COUNT(DISTINCT order_no) as orders,
            SUM(SAFE_CAST(packing_qty AS INT64)) as pcs
        FROM `jingdong-505914.wms_data.fact_packing`
        WHERE DATE(pack_time) = '{today}'
        GROUP BY owner_name
        HAVING orders > 0
        ORDER BY orders DESC
        """
        df_pack = client.query(sql_pack).to_dataframe()

        sql_bl = """
        SELECT COUNT(DISTINCT order_no) as order_no
        FROM `jingdong-505914.wms_data.fact_backlog`
        """
        df_bl = client.query(sql_bl).to_dataframe()

        if df_pack.empty:
            return {"status": "empty", "message": "Chưa có sản lượng đóng gói nào hôm nay."}

        tot_orders = int(df_pack['orders'].sum())
        tot_pcs = int(df_pack['pcs'].sum())

        sig = f"{today}_{tot_orders}_{tot_pcs}"
        now_ts = time.time()
        cooldown_sec = cooldown_minutes * 60

        if not force_send:
            if sig == _LAST_PACKING_ALERT_STATE["signature"]:
                if (now_ts - _LAST_PACKING_ALERT_STATE["timestamp"]) < cooldown_sec:
                    return {
                        "status": "skipped_cooldown",
                        "message": f"Sản lượng đóng gói ({tot_orders:,} đơn) đã gửi gần đây, tạm hoãn gửi lặp lại."
                    }

        report_text = format_packing_progress_report(df_pack, df_bl, now_vn)

        ok, err = False, ""
        if cfg.get("bot_token") and chat_id:
            url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
            res = requests.post(url, json={"chat_id": chat_id, "text": report_text}, headers={"Content-Type": "application/json"}, timeout=15)
            if res.status_code == 200:
                ok = True
            else:
                err = f"Lỗi Zalo API [{res.status_code}]: {res.text}"
        else:
            ok, err = send_zalo_message(report_text)

        if ok:
            _LAST_PACKING_ALERT_STATE["signature"] = sig
            _LAST_PACKING_ALERT_STATE["timestamp"] = now_ts
            _LAST_PACKING_ALERT_STATE["last_orders"] = tot_orders
            return {
                "status": "sent",
                "message": f"Đã gửi báo cáo tiến độ đóng gói ({tot_orders:,} đơn) vào nhóm Zalo thành công!",
                "total_orders": tot_orders
            }
        else:
            return {"status": "error", "message": f"Không thể gửi tin Zalo: {err}"}

    except Exception as e:
        return {"status": "error", "message": f"Lỗi xử lý báo cáo packing: {e}"}


def handle_packing_command(target_chat_id: str = None) -> tuple[bool, str]:
    """
    Xử lý khi người dùng nhắn lệnh packing: Ép buộc gửi báo cáo mới nhất ngay lập tức.
    """
    res = scan_and_alert_packing_progress(force_send=True)
    if res.get("status") == "sent":
        return True, res.get("message")
    return False, res.get("message")


def format_order_progress_report(df_order: pd.DataFrame, now_vn: datetime.datetime = None, target_date_str: str = None, last_update_str: str = None) -> str:
    """
    Tạo báo cáo số lượng đơn hàng khách đổ xuống hệ thống (ORDER):
    - Báo tổng đơn nhận.
    - Phân chia 2 nhóm rõ ràng: B2B (KKV, Chint, Envision...) và B2C (các khách còn lại).
    - Khách nào đơn = 0 thì không hiện.
    - Sắp xếp giảm dần theo số đơn.
    """
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    time_str = now_vn.strftime('%H:%M %d/%m/%Y')
    if last_update_str is None:
        last_update_str = get_last_data_update_time("ORDER")

    time_headers = [f"Thời gian báo cáo: {time_str}"]
    if last_update_str:
        time_headers.append(f"Dữ liệu cập nhật lúc: {last_update_str}")
    time_header_text = "\n".join(time_headers)

    if df_order is None or df_order.empty:
        date_display = target_date_str or now_vn.strftime('%d/%m/%Y')
        return (
            f"[TỔNG HỢP ĐƠN HÀNG HỆ THỐNG (ORDER) - KHO JDL]\n"
            f"{time_header_text}\n\n"
            f"Chưa có dữ liệu đơn hàng nào được ghi nhận cho ngày {date_display}."
        )

    df = df_order.copy()
    df['total_orders'] = pd.to_numeric(df['total_orders'], errors='coerce').fillna(0).astype(int)
    df = df[df['total_orders'] > 0]

    if df.empty:
        return (
            f"[TỔNG HỢP ĐƠN HÀNG HỆ THỐNG (ORDER) - KHO JDL]\n"
            f"{time_header_text}\n\n"
            f"Tổng số đơn nhận hôm nay: 0 đơn."
        )

    df['type'] = df['owner_name'].apply(classify_packing_client)
    df['display_name'] = df['owner_name'].apply(clean_packing_client_name)

    # Gom nhóm theo type và display_name
    df_agg = df.groupby(['type', 'display_name'], as_index=False)['total_orders'].sum()

    tot_orders = int(df_agg['total_orders'].sum())

    df_b2b = df_agg[df_agg['type'] == 'B2B'].sort_values(by='total_orders', ascending=False)
    df_b2c = df_agg[df_agg['type'] == 'B2C'].sort_values(by='total_orders', ascending=False)

    b2b_orders = int(df_b2b['total_orders'].sum())
    b2c_orders = int(df_b2c['total_orders'].sum())

    lines = [
        "[TỔNG HỢP ĐƠN HÀNG HỆ THỐNG (ORDER) - KHO JDL]",
    ] + time_headers + [
        "",
        "TỔNG QUAN",
        f"- Tổng đơn nhận từ khách: {tot_orders:,} đơn",
        "",
        f"KHÁCH HÀNG B2B (Đã nhận: {b2b_orders:,} đơn)"
    ]

    if df_b2b.empty or b2b_orders == 0:
        lines.append("- Chưa có đơn nhận")
    else:
        for _, r in df_b2b.iterrows():
            lines.append(f"- {r['display_name']}: {int(r['total_orders']):,} đơn")

    lines.append("")
    lines.append(f"KHÁCH HÀNG B2C (Đã nhận: {b2c_orders:,} đơn)")

    if df_b2c.empty or b2c_orders == 0:
        lines.append("- Chưa có đơn nhận")
    else:
        for _, r in df_b2c.iterrows():
            lines.append(f"- {r['display_name']}: {int(r['total_orders']):,} đơn")

    return "\n".join(lines)


def handle_order_command(target_chat_id: str = None) -> tuple[bool, str]:
    """
    Xử lý khi người dùng nhắn lệnh 'order':
    Truy vấn BigQuery fact_order, tính tổng đơn nhận và phân loại B2B / B2C theo từng khách hàng.
    """
    cfg = get_zalo_config()
    chat_id = target_chat_id or cfg.get("chat_id")
    if not chat_id:
        return False, "Chưa xác định được chat_id nhóm Zalo."

    try:
        client = get_bigquery_client()
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
        today_str = now_vn.strftime('%Y-%m-%d')

        sql_order = f"""
        SELECT 
            owner_name,
            customer_type,
            SUM(total_orders) as total_orders
        FROM `jingdong-505914.wms_data.fact_order`
        WHERE DATE(order_date) = '{today_str}'
        GROUP BY owner_name, customer_type
        HAVING total_orders > 0
        ORDER BY total_orders DESC
        """
        df_order = client.query(sql_order).to_dataframe()

        target_date_str = None
        if df_order.empty:
            sql_latest = """
            SELECT MAX(DATE(order_date)) as max_date
            FROM `jingdong-505914.wms_data.fact_order`
            """
            df_latest = client.query(sql_latest).to_dataframe()
            if not df_latest.empty and df_latest['max_date'].iloc[0] is not None:
                latest_date = str(df_latest['max_date'].iloc[0])
                sql_order_latest = f"""
                SELECT 
                    owner_name,
                    customer_type,
                    SUM(total_orders) as total_orders
                FROM `jingdong-505914.wms_data.fact_order`
                WHERE DATE(order_date) = '{latest_date}'
                GROUP BY owner_name, customer_type
                HAVING total_orders > 0
                ORDER BY total_orders DESC
                """
                df_order = client.query(sql_order_latest).to_dataframe()
                target_date_str = latest_date

        report_text = format_order_progress_report(df_order, now_vn=now_vn, target_date_str=target_date_str)
        if target_date_str and target_date_str != today_str:
            report_text += f"\n\n*(Lưu ý: Hôm nay ({today_str}) chưa có file Order mới, đang hiển thị dữ liệu ngày gần nhất {target_date_str})*"

    except Exception as e:
        err_msg = f"[LỖI] Không thể đọc dữ liệu fact_order: {e}"
        send_zalo_message(err_msg, target_chat_id=chat_id)
        return False, err_msg

    if cfg.get("bot_token") and chat_id:
        url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
        res = requests.post(url, json={"chat_id": chat_id, "text": report_text}, headers={"Content-Type": "application/json"}, timeout=15)
        if res.status_code == 200:
            return True, "Đã gửi báo cáo Order vào Zalo thành công."
        return False, f"Lỗi Zalo API [{res.status_code}]: {res.text}"
    else:
        return send_zalo_message(report_text, target_chat_id=chat_id)


def scan_and_alert_backlog_sla(
    df_backlog: pd.DataFrame = None, 
    force_send: bool = False, 
    custom_urgent_hours: float = 2.5, 
    notify_if_safe: bool = True,
    target_chat_id: str = None
) -> dict:
    """
    Quét dữ liệu backlog và gửi báo cáo chi tiết đơn sắp trễ SLA vào nhóm Zalo.
    Sử dụng định dạng chi tiết giống như khi gõ lệnh 'backlog' (nhóm theo Khách hàng -> Đơn hàng -> Thời gian còn lại).
    - custom_urgent_hours: mốc giờ cảnh báo (mặc định 2.5 tiếng).
    - force_send: bỏ qua cơ chế chống spam (dùng khi vừa nạp file mới hoặc gõ lệnh).
    - notify_if_safe: nếu True và không có đơn trễ (0 đơn rủi ro), vẫn gửi tin xác nhận an toàn vào nhóm.
    """
    global _LAST_ALERT_STATE

    if df_backlog is None:
        try:
            client = get_bigquery_client()
            df_backlog = client.query("SELECT * FROM `jingdong-505914.wms_data.fact_backlog`").to_dataframe()
        except Exception as e:
            return {"status": "error", "message": f"Không thể đọc BigQuery fact_backlog: {e}"}

    if df_backlog is None or df_backlog.empty:
        return {"status": "empty", "message": "Dữ liệu Backlog trống."}

    cfg = get_zalo_config()
    chat_id = target_chat_id or cfg.get("chat_id")
    if not chat_id:
        return {"status": "not_configured", "message": "Chưa cấu hình chat_id nhóm Zalo."}

    urgent_hours = custom_urgent_hours if custom_urgent_hours is not None else cfg.get("urgent_hours", 2.5)
    cooldown_sec = cfg.get("cooldown_minutes", 30.0) * 60

    now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
    today_vn = now_vn.date()

    # Chuẩn hóa cột prod_dt
    df = df_backlog.copy()
    if 'prod_dt' not in df.columns:
        if 'production_end_time' in df.columns:
            df['prod_dt'] = pd.to_datetime(df['production_end_time'], errors='coerce')
        else:
            return {"status": "no_prod_time", "message": "Không tìm thấy cột Production End Time."}

    # Lọc đơn có hạn hôm nay
    df_today = df[df['prod_dt'].dt.date == today_vn].copy()
    if df_today.empty:
        if notify_if_safe:
            last_sync = get_last_data_update_time("BACKLOG")
            sync_line = f"\nDữ liệu cập nhật lúc: {last_sync}" if last_sync else ""
            msg = f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\nThời gian báo cáo: {now_vn.strftime('%H:%M:%S %d/%m/%Y')}{sync_line}\nKhông có đơn nào tồn đọng trong ngày hôm nay ({today_vn})."
            send_zalo_message(msg, target_chat_id=chat_id)
        return {"status": "no_today_orders", "message": f"Không có đơn nào có hạn hôm nay ({today_vn})."}

    df_today['diff_sec'] = (df_today['prod_dt'] - now_vn).dt.total_seconds()
    df_risk = df_today[df_today['diff_sec'] <= urgent_hours * 3600].copy()
    total_risk = len(df_risk)

    if total_risk == 0 and not notify_if_safe:
        return {
            "status": "safe",
            "message": f"Tất cả các đơn hôm nay đều an toàn (> {urgent_hours:g}h nữa mới đến hạn).",
            "count": 0
        }

    # Tạo chữ ký dữ liệu để chống spam tin trùng lặp
    all_risk_ids = sorted(df_risk['order_no'].astype(str).tolist())
    sig_str = f"{today_vn}_{total_risk}_" + "_".join(all_risk_ids[:50])
    current_sig = hashlib.md5(sig_str.encode('utf-8')).hexdigest()
    now_ts = time.time()

    if not force_send:
        if current_sig == _LAST_ALERT_STATE["signature"]:
            if (now_ts - _LAST_ALERT_STATE["timestamp"]) < cooldown_sec:
                return {
                    "status": "skipped_cooldown",
                    "message": f"Danh sách đơn rủi ro ({total_risk} đơn) đã được gửi gần đây, tạm hoãn gửi lặp lại.",
                    "count": total_risk
                }

    # Định dạng tin nhắn chi tiết (giống hệt khi chat /backlog)
    messages = format_detailed_backlog_list(df, now_vn=now_vn, max_hours=urgent_hours)

    all_ok = True
    last_err = ""
    for msg in messages:
        if cfg.get("bot_token") and chat_id:
            url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
            res = requests.post(url, json={"chat_id": chat_id, "text": msg}, headers={"Content-Type": "application/json"}, timeout=15)
            if res.status_code != 200:
                all_ok = False
                last_err = f"Lỗi Zalo API [{res.status_code}]: {res.text}"
        else:
            ok, err = send_zalo_message(msg, target_chat_id=chat_id)
            if not ok:
                all_ok = False
                last_err = err
        time.sleep(0.5)

    if all_ok:
        _LAST_ALERT_STATE["signature"] = current_sig
        _LAST_ALERT_STATE["timestamp"] = now_ts
        _LAST_ALERT_STATE["last_count"] = total_risk
        return {
            "status": "sent",
            "message": f"Đã gửi báo cáo chi tiết Backlog ({total_risk} đơn sắp trễ) vào nhóm Zalo thành công!",
            "count": total_risk
        }
    else:
        return {
            "status": "error",
            "message": f"Không thể gửi tin Zalo: {last_err}",
            "count": total_risk
        }


def parse_productivity_query(raw_text: str, now_vn: datetime.datetime = None) -> dict | None:
    """
    Nhận diện và trích xuất tham số từ câu hỏi tự nhiên tiếng Việt về sản lượng nhân sự
    (Pick / Pack / Put Away / Receiving) theo ngày và khung giờ bất kỳ.
    Trả về dict tham số nếu khớp ý định hỏi sản lượng nhân sự / theo khung giờ, ngược lại trả về None.
    """
    if not raw_text:
        return None
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    # Xóa mention @Bot ... để tránh nhiễu
    clean = re.sub(r'@\S+', ' ', raw_text).strip()
    clean = re.sub(r'\bjingdong\s*logistics\b', ' ', clean, flags=re.IGNORECASE).strip()
    text_lower = re.sub(r'\s+', ' ', clean.lower()).strip()

    # Bỏ qua các lệnh đơn giản quen thuộc
    if text_lower in ["packing", "/packing", "dong goi", "backlog", "/backlog", "order", "/order", "etc", "clear", "ping", "test"]:
        return None

    # Nếu câu hỏi là hẹn giờ nhắc nhở thì không chặn
    if any(k in text_lower for k in ["nhắc", "kêu mình", "báo thức", "đánh thức"]):
        return None

    # 1. Xác định các khâu được nhắc tới
    has_pick = bool(re.search(r'\bpick(?:ing)?\b|lấy hàng|lay hang|nhặt hàng|nhat hang', text_lower))
    has_pack = bool(re.search(r'\bpack(?:ing)?\b|đóng gói|dong goi', text_lower))
    has_putaway = bool(re.search(r'put\s*away|lên kệ|len ke', text_lower))
    has_recv = bool(re.search(r'receiv(?:ing)?|nghiệm thu|nghiem thu|nhập hàng|nhap hang', text_lower))
    has_any_stage = has_pick or has_pack or has_putaway or has_recv

    # 2. Từ khóa chỉ báo hỏi về nhân sự / bảng sản lượng / hiệu suất
    worker_prod_kws = [
        "nhân sự", "nhan su", "nhân viên", "nhan vien", "nhân công", "nhan cong",
        "sản lượng", "san luong", "năng suất", "nang suat", "hiệu suất", "hieu suat",
        "bảng sản lượng", "bang san luong", "từng người", "tung nguoi", "mỗi người", "moi nguoi",
        "ai pick", "ai pack", "ai đóng", "ai lấy", "top pick", "top pack", "top nhân", "top nhan",
        "picker", "packer", "theo nhân", "theo nhan", "của nhân", "cua nhan"
    ]
    has_worker_prod = any(k in text_lower for k in worker_prod_kws)

    # 3. Trích xuất KHUNG GIỜ trước (để tránh chuỗi '17:00-17:30' gây nhiễu regex ngày '00-17')
    start_time = datetime.time(0, 0, 0)
    end_time = datetime.time(23, 59, 59)
    has_time_range = False
    time_label = "Cả ngày"
    date_text = text_lower

    m_range = re.search(
        r'(?:t[ừu]\s*)?(\d{1,2})\s*(?:h|g|gi[ờo]|:)\s*(\d{1,2})?\s*(?:p|ph[úu]t)?\s*'
        r'(?:đ[ếe]n|den|t[ớo]i|toi|-|–|~)\s*'
        r'(\d{1,2})\s*(?:h|g|gi[ờo]|:)\s*(\d{1,2})?\s*(?:p|ph[úu]t)?',
        text_lower
    )
    if m_range:
        sh, sm = int(m_range.group(1)), int(m_range.group(2) or 0)
        eh, em = int(m_range.group(3)), int(m_range.group(4) or 0)
        if 0 <= sh <= 23 and 0 <= sm <= 59 and 0 <= eh <= 23 and 0 <= em <= 59:
            start_time = datetime.time(sh, sm, 0)
            end_time = datetime.time(eh, em, 0)
            has_time_range = True
            time_label = f"{sh:02d}:{sm:02d} - {eh:02d}:{em:02d}"
            date_text = text_lower[:m_range.start()] + " " + text_lower[m_range.end():]
    else:
        m_single_h = re.search(r'(?:l[úu]c|khung|gi[ờo]|trong\s+khung)\s*(\d{1,2})\s*(?:h|g|gi[ờo])(?:\s*(\d{1,2}))?', text_lower)
        if m_single_h:
            sh = int(m_single_h.group(1))
            if 0 <= sh <= 23:
                start_time = datetime.time(sh, 0, 0)
                end_time = datetime.time(sh, 59, 59)
                has_time_range = True
                time_label = f"{sh:02d}:00 - {sh:02d}:59"
                date_text = text_lower[:m_single_h.start()] + " " + text_lower[m_single_h.end():]
        elif any(k in text_lower for k in ["buổi sáng", "buoi sang", "sáng nay", "sang nay", "sáng qua", "sang qua", "ca sáng", "ca sang"]):
            start_time = datetime.time(7, 0, 0)
            end_time = datetime.time(12, 0, 0)
            has_time_range = True
            time_label = "07:00 - 12:00 (Sáng)"
        elif any(k in text_lower for k in ["buổi chiều", "buoi chieu", "chiều nay", "chieu nay", "chiều qua", "chieu qua", "ca chiều", "ca chieu"]):
            start_time = datetime.time(12, 0, 0)
            end_time = datetime.time(18, 0, 0)
            has_time_range = True
            time_label = "12:00 - 18:00 (Chiều)"
        elif any(k in text_lower for k in ["buổi tối", "buoi toi", "tối nay", "toi nay", "tối qua", "toi qua", "ca tối", "ca toi"]):
            start_time = datetime.time(18, 0, 0)
            end_time = datetime.time(23, 59, 59)
            has_time_range = True
            time_label = "18:00 - 23:59 (Tối)"

    # 4. Trích xuất NGÀY từ date_text (đã loại bỏ chuỗi giờ)
    today = now_vn.date()
    target_date = today
    has_explicit_date = False

    m_iso = re.search(r'\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b', date_text)
    if m_iso:
        try:
            target_date = datetime.date(int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3)))
            has_explicit_date = True
        except ValueError:
            pass

    if not has_explicit_date:
        m_dmy = re.search(r'(?<!:)\b(\d{1,2})[-/](\d{1,2})(?:[-/](20\d{2}|\d{2}))?\b(?!:)', date_text)
        if m_dmy:
            d, mo = int(m_dmy.group(1)), int(m_dmy.group(2))
            yr_raw = m_dmy.group(3)
            yr = (2000 + int(yr_raw)) if (yr_raw and len(yr_raw) == 2) else (int(yr_raw) if yr_raw else today.year)
            try:
                cand = datetime.date(yr, mo, d)
                if not yr_raw and (cand - today).days > 7:
                    cand = datetime.date(yr - 1, mo, d)
                target_date = cand
                has_explicit_date = True
            except ValueError:
                pass

    if not has_explicit_date:
        m_vn_dm = re.search(r'ng[àa]y\s*(\d{1,2})\s*th[áa]ng\s*(\d{1,2})(?:\s*n[ăa]m\s*(20\d{2}))?', date_text)
        if m_vn_dm:
            d, mo = int(m_vn_dm.group(1)), int(m_vn_dm.group(2))
            yr = int(m_vn_dm.group(3)) if m_vn_dm.group(3) else today.year
            try:
                cand = datetime.date(yr, mo, d)
                if not m_vn_dm.group(3) and (cand - today).days > 7:
                    cand = datetime.date(yr - 1, mo, d)
                target_date = cand
                has_explicit_date = True
            except ValueError:
                pass

    if not has_explicit_date:
        if any(k in date_text for k in ["hôm kia", "hom kia"]):
            target_date = today - datetime.timedelta(days=2)
            has_explicit_date = True
        elif any(k in date_text for k in ["hôm qua", "hom qua", "ngày qua", "ngay qua", "chiều qua", "chieu qua", "sáng qua", "sang qua", "tối qua", "toi qua", "hôm trước", "hom truoc"]):
            target_date = today - datetime.timedelta(days=1)
            has_explicit_date = True
        else:
            m_vn_d = re.search(r'ng[àa]y\s*(\d{1,2})\b', date_text)
            if m_vn_d:
                d = int(m_vn_d.group(1))
                try:
                    target_date = datetime.date(today.year, today.month, d)
                    has_explicit_date = True
                except ValueError:
                    pass

    # 5. Điều kiện kích hoạt để không bắt nhầm tin nhắn thông thường hay lệnh 'packing' mặc định
    is_past_date = has_explicit_date and (target_date != today)
    should_trigger = (
        (has_worker_prod and (has_any_stage or has_time_range or has_explicit_date))
        or (has_pick and has_pack)
        or (has_pick and (has_time_range or has_explicit_date or has_worker_prod))
        or (has_pack and (has_time_range or is_past_date or has_worker_prod))
        or ((has_putaway or has_recv) and has_worker_prod)
    )
    if not should_trigger:
        return None

    stages = []
    if has_pick:
        stages.append("PICK")
    if has_pack:
        stages.append("PACK")
    if has_putaway:
        stages.append("PUTAWAY")
    if has_recv:
        stages.append("RECEIVING")
    if not stages:
        stages = ["PICK", "PACK"]

    # Trích xuất Top N nếu có (VD: "top 5", "top 10")
    top_n = None
    m_top = re.search(r'\btop\s*(\d{1,2})\b', text_lower)
    if m_top:
        top_n = max(1, int(m_top.group(1)))

    start_dt = datetime.datetime.combine(target_date, start_time)
    end_dt = datetime.datetime.combine(target_date, end_time)

    return {
        "stages": stages,
        "target_date": target_date,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "time_label": time_label,
        "has_time_range": has_time_range,
        "top_n": top_n,
    }


def _split_zalo_lines(lines: list[str], header_prefix: str, max_chars: int = 1800) -> list[str]:
    """Tự động chia danh sách dòng thành các tin nhắn Zalo <= max_chars ký tự."""
    messages = []
    cur = []
    cur_len = 0
    for ln in lines:
        add_len = len(ln) + 1
        if cur and (cur_len + add_len > max_chars):
            messages.append("\n".join(cur))
            cur = [f"{header_prefix} (tiếp)", ln]
            cur_len = len(cur[0]) + add_len
        else:
            cur.append(ln)
            cur_len += add_len
    if cur:
        messages.append("\n".join(cur))
    return messages


def query_worker_productivity_report(params: dict) -> list[str]:
    """
    Truy vấn BigQuery thống kê sản lượng theo từng nhân sự cho các khâu được yêu cầu
    (PICK / PACK / PUTAWAY / RECEIVING) đúng ngày & khung giờ.
    """
    client = get_bigquery_client()
    stages = params["stages"]
    start_str = params["start_dt"].strftime("%Y-%m-%d %H:%M:%S")
    end_str = params["end_dt"].strftime("%Y-%m-%d %H:%M:%S")
    date_display = params["target_date"].strftime("%d/%m/%Y")
    time_label = params["time_label"]
    top_n = params.get("top_n")

    stage_title = " & ".join(stages)
    header = f"[SẢN LƯỢNG NHÂN SỰ {stage_title} - KHO JDL]"
    lines = [
        header,
        f"Thời gian: {time_label} ngày {date_display}",
    ]
    if top_n:
        lines.append(f"(Hiển thị Top {top_n} nhân sự mỗi khâu)")

    sec_idx = 0

    # 1. PICKING
    if "PICK" in stages:
        sec_idx += 1
        sql_pick = f"""
        SELECT
            TRIM(REGEXP_REPLACE(
                COALESCE(NULLIF(TRIM(worker_name), ''), NULLIF(TRIM(worker_email), ''), NULLIF(TRIM(worker_id), ''), 'Không rõ'),
                r'\\s+', ' '
            )) AS worker,
            COUNT(DISTINCT order_no) AS orders,
            SUM(SAFE_CAST(actual_qty AS INT64)) AS pcs,
            COUNT(*) AS tasks
        FROM `jingdong-505914.wms_data.fact_picking`
        WHERE COALESCE(end_time, start_time) >= DATETIME '{start_str}'
          AND COALESCE(end_time, start_time) <= DATETIME '{end_str}'
        GROUP BY worker
        ORDER BY pcs DESC, orders DESC
        """
        sql_pick_tot = f"""
        SELECT
            COUNT(DISTINCT order_no) AS orders,
            SUM(SAFE_CAST(actual_qty AS INT64)) AS pcs,
            COUNT(*) AS tasks
        FROM `jingdong-505914.wms_data.fact_picking`
        WHERE COALESCE(end_time, start_time) >= DATETIME '{start_str}'
          AND COALESCE(end_time, start_time) <= DATETIME '{end_str}'
        """
        df_p = client.query(sql_pick).to_dataframe()
        df_pt = client.query(sql_pick_tot).to_dataframe()

        lines.append("")
        if df_p.empty or int(df_pt['tasks'].iloc[0] or 0) == 0:
            lines.append(f"{sec_idx}. NHÂN SỰ PICKING: Chưa có dữ liệu trong khung giờ này.")
        else:
            tot_o = int(df_pt['orders'].iloc[0] or 0)
            tot_pcs = int(df_pt['pcs'].iloc[0] or 0)
            lines.append(f"{sec_idx}. NHÂN SỰ PICKING ({len(df_p)} người | Tổng: {tot_o:,} đơn - {tot_pcs:,} pcs)")
            df_show = df_p.head(top_n) if top_n else df_p
            for i, (_, r) in enumerate(df_show.iterrows(), 1):
                lines.append(f"  {i}. {r['worker']}: {int(r['orders']):,} đơn ({int(r['pcs'] or 0):,} pcs)")

    # 2. PACKING (tự động map tên Tiếng Việt từ fact_picking qua email nếu có)
    if "PACK" in stages:
        sec_idx += 1
        sql_pack = f"""
        WITH staff_map AS (
            SELECT
                LOWER(TRIM(worker_email)) AS key_email,
                ANY_VALUE(TRIM(REGEXP_REPLACE(worker_name, r'\\s+', ' '))) AS vn_name
            FROM `jingdong-505914.wms_data.fact_picking`
            WHERE worker_name IS NOT NULL AND TRIM(worker_name) != ''
              AND worker_email IS NOT NULL AND TRIM(worker_email) != ''
            GROUP BY key_email
        ),
        pack_agg AS (
            SELECT
                TRIM(worker_name) AS raw_worker,
                COUNT(DISTINCT order_no) AS orders,
                SUM(SAFE_CAST(packing_qty AS INT64)) AS pcs
            FROM `jingdong-505914.wms_data.fact_packing`
            WHERE pack_time >= DATETIME '{start_str}'
              AND pack_time <= DATETIME '{end_str}'
            GROUP BY raw_worker
        )
        SELECT
            COALESCE(m.vn_name, REPLACE(p.raw_worker, '+', ' '), 'Không rõ') AS worker,
            p.orders,
            p.pcs
        FROM pack_agg p
        LEFT JOIN staff_map m ON LOWER(p.raw_worker) = m.key_email
        ORDER BY p.orders DESC, p.pcs DESC
        """
        sql_pack_tot = f"""
        SELECT
            COUNT(DISTINCT order_no) AS orders,
            SUM(SAFE_CAST(packing_qty AS INT64)) AS pcs
        FROM `jingdong-505914.wms_data.fact_packing`
        WHERE pack_time >= DATETIME '{start_str}'
          AND pack_time <= DATETIME '{end_str}'
        """
        df_k = client.query(sql_pack).to_dataframe()
        df_kt = client.query(sql_pack_tot).to_dataframe()

        lines.append("")
        if df_k.empty or int(df_kt['orders'].iloc[0] or 0) == 0:
            lines.append(f"{sec_idx}. NHÂN SỰ PACKING: Chưa có dữ liệu trong khung giờ này.")
        else:
            tot_o = int(df_kt['orders'].iloc[0] or 0)
            tot_pcs = int(df_kt['pcs'].iloc[0] or 0)
            lines.append(f"{sec_idx}. NHÂN SỰ PACKING ({len(df_k)} người | Tổng: {tot_o:,} đơn - {tot_pcs:,} pcs)")
            df_show = df_k.head(top_n) if top_n else df_k
            for i, (_, r) in enumerate(df_show.iterrows(), 1):
                lines.append(f"  {i}. {r['worker']}: {int(r['orders']):,} đơn ({int(r['pcs'] or 0):,} pcs)")

    # 3. PUTAWAY (nếu có hỏi)
    if "PUTAWAY" in stages:
        sec_idx += 1
        sql_pa = f"""
        SELECT
            COALESCE(NULLIF(TRIM(worker_id), ''), 'Không rõ') AS worker,
            COUNT(DISTINCT order_no) AS orders,
            SUM(SAFE_CAST(putaway_qty AS INT64)) AS pcs
        FROM `jingdong-505914.wms_data.fact_putaway`
        WHERE putaway_time >= DATETIME '{start_str}'
          AND putaway_time <= DATETIME '{end_str}'
        GROUP BY worker
        ORDER BY pcs DESC, orders DESC
        """
        df_pa = client.query(sql_pa).to_dataframe()
        lines.append("")
        if df_pa.empty:
            lines.append(f"{sec_idx}. NHÂN SỰ PUT AWAY: Chưa có dữ liệu trong khung giờ này.")
        else:
            lines.append(f"{sec_idx}. NHÂN SỰ PUT AWAY ({len(df_pa)} người | Tổng: {int(df_pa['pcs'].sum() or 0):,} pcs)")
            df_show = df_pa.head(top_n) if top_n else df_pa
            for i, (_, r) in enumerate(df_show.iterrows(), 1):
                lines.append(f"  {i}. {r['worker']}: {int(r['orders']):,} đơn ({int(r['pcs'] or 0):,} pcs)")

    # 4. RECEIVING (nếu có hỏi)
    if "RECEIVING" in stages:
        sec_idx += 1
        sql_rv = f"""
        SELECT
            COALESCE(NULLIF(TRIM(worker_id), ''), 'Không rõ') AS worker,
            COUNT(DISTINCT order_no) AS orders,
            SUM(SAFE_CAST(receiving_qty AS INT64)) AS pcs
        FROM `jingdong-505914.wms_data.fact_receiving`
        WHERE receiving_time >= DATETIME '{start_str}'
          AND receiving_time <= DATETIME '{end_str}'
        GROUP BY worker
        ORDER BY pcs DESC, orders DESC
        """
        df_rv = client.query(sql_rv).to_dataframe()
        lines.append("")
        if df_rv.empty:
            lines.append(f"{sec_idx}. NHÂN SỰ RECEIVING: Chưa có dữ liệu trong khung giờ này.")
        else:
            lines.append(f"{sec_idx}. NHÂN SỰ RECEIVING ({len(df_rv)} người | Tổng: {int(df_rv['pcs'].sum() or 0):,} pcs)")
            df_show = df_rv.head(top_n) if top_n else df_rv
            for i, (_, r) in enumerate(df_show.iterrows(), 1):
                lines.append(f"  {i}. {r['worker']}: {int(r['orders']):,} đơn ({int(r['pcs'] or 0):,} pcs)")

    return _split_zalo_lines(lines, header_prefix=header, max_chars=1800)


def handle_productivity_query(raw_text: str, target_chat_id: str = None, dry_run: bool = False) -> tuple[bool, str | list[str]]:
    """
    Xử lý câu hỏi tự nhiên về sản lượng nhân sự theo ngày/giờ và gửi kết quả vào Zalo.
    Nếu dry_run=True thì chỉ trả về danh sách tin nhắn (KHÔNG gửi Zalo).
    """
    params = parse_productivity_query(raw_text)
    if not params:
        return False, "Không nhận diện được yêu cầu tra cứu sản lượng nhân sự."

    try:
        messages = query_worker_productivity_report(params)
    except Exception as e:
        return False, f"Lỗi truy vấn BigQuery sản lượng nhân sự: {e}"

    if dry_run:
        return True, messages

    cfg = get_zalo_config()
    chat_id = target_chat_id or cfg.get("chat_id")
    if not chat_id:
        return False, "Chưa xác định được chat_id nhóm Zalo."

    all_ok = True
    last_err = ""
    for msg in messages:
        if cfg.get("bot_token") and chat_id:
            url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
            res = requests.post(url, json={"chat_id": chat_id, "text": msg}, headers={"Content-Type": "application/json"}, timeout=15)
            if res.status_code != 200:
                all_ok = False
                last_err = f"Lỗi Zalo API [{res.status_code}]: {res.text}"
        else:
            ok, err = send_zalo_message(msg, target_chat_id=chat_id)
            if not ok:
                all_ok = False
                last_err = err
        time.sleep(0.4)

    if all_ok:
        return True, f"Đã gửi bảng sản lượng nhân sự ({len(messages)} tin) vào Zalo thành công."
    return False, f"Không thể gửi tin Zalo: {last_err}"


# ==============================================================================
# CẢNH BÁO QUÁ HẠN SLA KHO (3-IN-1: NGHIỆM THU / LÊN KỆ / CHÊNH LỆCH)
# Tự động gửi vào Zalo khi phát hiện dữ liệu mới tải lên thư mục CHECK QUÁ HẠN
# ==============================================================================

def _add_sla_work_hours(start_dt: datetime.datetime, hours: int, exclude_sat: bool = True) -> datetime.datetime:
    """
    Cộng giờ SLA theo ngày làm việc:
      - exclude_sat=True: không tính Thứ 7 (5) và Chủ Nhật (6).
      - exclude_sat=False: chỉ không tính Chủ Nhật (6), Thứ 7 vẫn tính bình thường.
      - Nếu thời điểm bắt đầu rơi vào ngày nghỉ, đẩy sang 00:00:00 ngày làm việc kế tiếp.
    """
    excluded = {5, 6} if exclude_sat else {6}
    cur = start_dt
    if cur.weekday() in excluded:
        cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
        while cur.weekday() in excluded:
            cur += timedelta(days=1)

    rem_sec = int(hours * 3600)
    while rem_sec > 0:
        if cur.weekday() in excluded:
            cur = (cur + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            continue
        next_midnight = (cur + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        avail_sec = int((next_midnight - cur).total_seconds())
        if rem_sec <= avail_sec:
            return cur + timedelta(seconds=rem_sec)
        rem_sec -= avail_sec
        cur = next_midnight
    return cur


def _fmt_sla_rem(rem_sec: float) -> str:
    """Định dạng thời gian còn lại hoặc đã trễ gọn gàng (TRỄ XhYp / còn XhYp)."""
    if rem_sec < 0:
        s = abs(rem_sec)
        h = int(s // 3600)
        m = int((s % 3600) // 60)
        return f"TRỄ {h}h{m:02d}p" if h > 0 else f"TRỄ {m}p"
    h = int(rem_sec // 3600)
    m = int((rem_sec % 3600) // 60)
    return f"còn {h}h{m:02d}p" if h > 0 else f"còn {m}p"


def _to_naive_dt(val):
    """Chuyển đổi an toàn Timestamp/str/datetime sang datetime naive (hoặc None)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    ts = pd.to_datetime(val, errors="coerce")
    if pd.isna(ts):
        return None
    dt = ts.to_pydatetime()
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def format_warehouse_sla_report(
    df_rb: pd.DataFrame,
    df_ul: pd.DataFrame,
    df_pa: pd.DataFrame,
    df_dc: pd.DataFrame,
    now_vn: datetime.datetime = None,
    urgent_hours: float = 2.0,
) -> tuple[list[str], dict]:
    """
    Tổng hợp 1 tin nhắn cảnh báo SLA duy nhất cho 3 khâu:
      1. Chờ nghiệm thu (Receiving):
         - PI: từ unload_time (fallback start_receiving_time). Gói hàng/Package = 24h; Cont/Xe tải/YaoYao/khác = 48h. Trừ T7 & CN.
         - RS: từ start_receiving_time (nếu có). 48h. Trừ T7 & CN.
         - Loại bỏ các đơn đã có mặt trong Put Away Backlog.
      2. Chờ lên kệ (Put Away):
         - Loại ReturnRack; gom theo order_no lấy MIN(COALESCE(start_receiving_time, container_completion_time)).
         - PI = 24h; RS = 48h. Trừ T7 & CN.
      3. Chờ xử lý chênh lệch (Diff Center):
         - Từ create_time. 48h. Chỉ trừ CN (T7 vẫn tính).
    Quy tắc hiển thị:
      - Chỉ lọc các đơn/mã có hạn trong ngày hôm nay hoặc đã quá hạn (deadline.date() <= today_vn).
      - Chỉ liệt kê chi tiết từng mã đơn cho đơn ĐÃ QUÁ HẠN hoặc SẮP QUÁ HẠN <= 2h.
      - Đơn còn hạn hôm nay (> 2h): gom gọn theo Tên khách (số lượng).
      - Không ghi chữ '(An toàn)'.
    """
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
    today_vn = now_vn.date()

    # Chuẩn bị tập đơn Putaway để loại trừ khỏi Receiving Backlog
    pa_order_set = set()
    if df_pa is not None and not df_pa.empty and "order_no" in df_pa.columns:
        pa_order_set = set(
            df_pa["order_no"].dropna().astype(str).str.strip().str.upper()
        )

    # Chuẩn bị bản đồ Unloading (order_no -> earliest unload_time, arrival_type)
    ul_map = {}
    if df_ul is not None and not df_ul.empty and "order_no" in df_ul.columns:
        ul_tmp = df_ul.copy()
        ul_tmp["_ord"] = ul_tmp["order_no"].astype(str).str.strip().str.upper()
        ul_tmp["_ut"] = pd.to_datetime(ul_tmp.get("unload_time"), errors="coerce")
        ul_tmp = ul_tmp.sort_values(by="_ut", ascending=True, na_position="last")
        for _, r in ul_tmp.iterrows():
            ord_k = r["_ord"]
            if not ord_k or ord_k in ul_map:
                continue
            ul_map[ord_k] = {
                "unload_time": _to_naive_dt(r.get("unload_time")),
                "arrival_type": str(r.get("arrival_type") or "").strip(),
            }

    # --- 1. CHỜ NGHIỆM THU (RECEIVING) ---
    recv_items = []
    if df_rb is not None and not df_rb.empty and "order_no" in df_rb.columns:
        rb_tmp = df_rb.copy()
        rb_tmp["_ord"] = rb_tmp["order_no"].astype(str).str.strip()
        rb_tmp["_ord_u"] = rb_tmp["_ord"].str.upper()
        rb_tmp = rb_tmp[rb_tmp["_ord_u"].str.startswith(("PI", "RS"))]
        rb_tmp = rb_tmp[~rb_tmp["_ord_u"].isin(pa_order_set)]
        rb_tmp["_srt"] = pd.to_datetime(rb_tmp.get("start_receiving_time"), errors="coerce")
        rb_tmp = rb_tmp.sort_values(by="_srt", ascending=True, na_position="last")
        rb_tmp = rb_tmp.drop_duplicates(subset=["_ord_u"], keep="first")

        for _, r in rb_tmp.iterrows():
            ord_no = r["_ord"]
            ord_u = r["_ord_u"]
            owner_clean = clean_packing_client_name(r.get("owner_name") or "Không rõ")
            srt = _to_naive_dt(r.get("start_receiving_time"))

            if ord_u.startswith("PI"):
                ul_info = ul_map.get(ord_u, {})
                start_dt = ul_info.get("unload_time") or srt
                if start_dt is None:
                    continue
                arr_low = ul_info.get("arrival_type", "").lower()
                is_pkg = arr_low in ("package", "bao hàng", "gói hàng", "goi hang")
                sla_h = 24 if is_pkg else 48
            else:
                # RS (Hàng trả): 48h từ lúc quét con hàng đầu tiên
                if srt is None:
                    continue
                start_dt = srt
                sla_h = 48

            deadline = _add_sla_work_hours(start_dt, sla_h, exclude_sat=True)
            if deadline.date() <= today_vn:
                rem_sec = (deadline - now_vn).total_seconds()
                recv_items.append({
                    "id": ord_no,
                    "client": owner_clean,
                    "start_dt": start_dt,
                    "deadline": deadline,
                    "rem_sec": rem_sec,
                    "extra": "",
                })

    # --- 2. CHỜ LÊN KỆ (PUT AWAY) ---
    pa_items = []
    if df_pa is not None and not df_pa.empty and "order_no" in df_pa.columns:
        pa_tmp = df_pa.copy()
        pa_tmp["_ord"] = pa_tmp["order_no"].astype(str).str.strip()
        pa_tmp["_ord_u"] = pa_tmp["_ord"].str.upper()
        pa_tmp = pa_tmp[pa_tmp["_ord_u"].str.startswith(("PI", "RS"))]
        if "target_zone" in pa_tmp.columns:
            tz_low = pa_tmp["target_zone"].fillna("").astype(str).str.strip().str.lower()
            pa_tmp = pa_tmp[~tz_low.str.contains("returnrack", na=False)]

        srt_ser = pd.to_datetime(pa_tmp.get("start_receiving_time"), errors="coerce")
        cct_ser = pd.to_datetime(pa_tmp.get("container_completion_time"), errors="coerce")
        pa_tmp["_eff_start"] = srt_ser.combine_first(cct_ser)
        pa_tmp = pa_tmp[pa_tmp["_eff_start"].notna()]

        if not pa_tmp.empty:
            pa_grp = (
                pa_tmp.sort_values("_eff_start", ascending=True)
                .groupby("_ord_u", as_index=False)
                .first()
            )
            for _, r in pa_grp.iterrows():
                ord_no = r["_ord"]
                ord_u = r["_ord_u"]
                owner_clean = clean_packing_client_name(r.get("owner_name") or "Không rõ")
                start_dt = _to_naive_dt(r["_eff_start"])
                if start_dt is None:
                    continue
                sla_h = 24 if ord_u.startswith("PI") else 48
                deadline = _add_sla_work_hours(start_dt, sla_h, exclude_sat=True)
                if deadline.date() <= today_vn:
                    rem_sec = (deadline - now_vn).total_seconds()
                    pa_items.append({
                        "id": ord_no,
                        "client": owner_clean,
                        "start_dt": start_dt,
                        "deadline": deadline,
                        "rem_sec": rem_sec,
                        "extra": "",
                    })

    # --- 3. CHỜ XỬ LÝ CHÊNH LỆCH (DIFF CENTER) ---
    dc_items = []
    if df_dc is not None and not df_dc.empty and "create_time" in df_dc.columns:
        for _, r in df_dc.iterrows():
            start_dt = _to_naive_dt(r.get("create_time"))
            if start_dt is None:
                continue
            deadline = _add_sla_work_hours(start_dt, 48, exclude_sat=False)
            if deadline.date() <= today_vn:
                rem_sec = (deadline - now_vn).total_seconds()
                owner_clean = clean_packing_client_name(r.get("owner_name") or "Không rõ")
                item_id = str(r.get("goods_no") or r.get("task_no") or "N/A").strip()
                cell = str(r.get("cell") or "").strip()
                dtype_raw = str(r.get("difference_type") or "").strip()
                dtype_low = dtype_raw.lower()
                if "less" in dtype_low or "short" in dtype_low or "thiếu" in dtype_low or "loss" in dtype_low:
                    dtype_vn = "Thiếu"
                elif "more" in dtype_low or "over" in dtype_low or "dư" in dtype_low or "surplus" in dtype_low:
                    dtype_vn = "Dư"
                else:
                    dtype_vn = dtype_raw
                qty_val = r.get("untreated_qty")
                if qty_val is None or (isinstance(qty_val, float) and pd.isna(qty_val)):
                    qty_val = r.get("reported_qty")
                try:
                    qty_str = f"{int(float(qty_val))}" if qty_val is not None and not pd.isna(qty_val) else ""
                except Exception:
                    qty_str = ""
                parts = []
                if cell:
                    parts.append(cell)
                if dtype_vn or qty_str:
                    parts.append(f"{dtype_vn} {qty_str}".strip())
                extra_info = f" ({' | '.join(parts)})" if parts else ""
                dc_items.append({
                    "id": item_id,
                    "client": owner_clean,
                    "start_dt": start_dt,
                    "deadline": deadline,
                    "rem_sec": rem_sec,
                    "extra": extra_info,
                })

    # Tổng hợp thống kê
    all_buckets = [
        ("1. CHỜ NGHIỆM THU", "đơn", recv_items),
        ("2. CHỜ LÊN KỆ", "đơn", pa_items),
        ("3. CHỜ XỬ LÝ CHÊNH LỆCH", "mã", dc_items),
    ]

    urgent_sec = urgent_hours * 3600.0
    total_today = sum(len(b[2]) for b in all_buckets)
    total_overdue = sum(sum(1 for it in b[2] if it["rem_sec"] < 0) for b in all_buckets)
    total_urgent = sum(sum(1 for it in b[2] if 0 <= it["rem_sec"] <= urgent_sec) for b in all_buckets)

    header = "[CẢNH BÁO QUÁ HẠN SLA KHO - KHO JDL]"
    lines = [
        header,
        f"Cập nhật: {now_vn.strftime('%H:%M %d/%m/%Y')}",
    ]

    if total_today == 0:
        lines.append("")
        lines.append("Hiện không có đơn/mã nào đến hạn trong hôm nay hoặc quá hạn:")
        lines.append("- Chờ nghiệm thu: 0 đơn")
        lines.append("- Chờ lên kệ: 0 đơn")
        lines.append("- Chờ xử lý chênh lệch: 0 mã")
    else:
        summary_parts = [f"Hạn hôm nay: {total_today:,}"]
        if total_overdue > 0:
            summary_parts.append(f"Quá hạn: {total_overdue:,}")
        if total_urgent > 0:
            summary_parts.append(f"Sắp quá hạn <=2h: {total_urgent:,}")
        lines.append("Tổng quan: " + " | ".join(summary_parts))

        for title, unit, items in all_buckets:
            lines.append("")
            if not items:
                lines.append(f"{title}: Không có {unit} đến hạn hôm nay.")
                continue

            overdue = sorted([it for it in items if it["rem_sec"] < 0], key=lambda x: (x["client"], x["rem_sec"]))
            urgent = sorted([it for it in items if 0 <= it["rem_sec"] <= urgent_sec], key=lambda x: (x["client"], x["rem_sec"]))
            later = [it for it in items if it["rem_sec"] > urgent_sec]

            hdr_stats = [f"{len(items):,} {unit}"]
            if overdue:
                hdr_stats.append(f"Trễ: {len(overdue):,}")
            if urgent:
                hdr_stats.append(f"<=2h: {len(urgent):,}")
            lines.append(f"{title} ({' | '.join(hdr_stats)})")

            def _append_detail_group(sub_items: list, group_label: str):
                lines.append(f"• {group_label} ({len(sub_items):,} {unit}):")
                by_client = {}
                for it in sub_items:
                    by_client.setdefault(it["client"], []).append(it)
                for cli, cli_items in by_client.items():
                    lines.append(f"  - {cli} ({len(cli_items)}):")
                    for it in cli_items:
                        s_str = it["start_dt"].strftime("%H:%M %d/%m")
                        d_str = it["deadline"].strftime("%H:%M %d/%m")
                        r_str = _fmt_sla_rem(it["rem_sec"])
                        lines.append(f"    + {it['id']}{it['extra']} | Nhận: {s_str} | Hạn: {d_str} ({r_str})")

            if overdue:
                _append_detail_group(overdue, "ĐÃ QUÁ HẠN")
            if urgent:
                _append_detail_group(urgent, f"SẮP QUÁ HẠN <= {urgent_hours:g}H")
            if later:
                cnt_map = {}
                for it in later:
                    cnt_map[it["client"]] = cnt_map.get(it["client"], 0) + 1
                sorted_cnt = sorted(cnt_map.items(), key=lambda x: (-x[1], x[0]))
                compact = ", ".join(f"{k} ({v})" for k, v in sorted_cnt)
                lines.append(f"• Hạn hôm nay (> {urgent_hours:g}h) ({len(later):,} {unit}): {compact}")

    stats = {
        "total_today": total_today,
        "total_overdue": total_overdue,
        "total_urgent": total_urgent,
        "recv_today": len(recv_items),
        "pa_today": len(pa_items),
        "dc_today": len(dc_items),
    }
    messages = _split_zalo_lines(lines, header_prefix=header, max_chars=1800)
    return messages, stats


def scan_and_alert_warehouse_sla(target_chat_id: str = None, dry_run: bool = False) -> dict:
    """
    Truy vấn 4 bảng SLA trên BigQuery (receiving_backlog, unloading_result, putaway_backlog, diff_center),
    tạo 1 báo cáo tổng hợp duy nhất và tự động gửi vào Nhóm Chat Zalo.
    """
    cfg = get_zalo_config()
    if not dry_run and not cfg.get("enabled"):
        return {"status": "disabled", "message": "Tính năng thông báo Zalo đang tắt."}

    chat_id = target_chat_id or cfg.get("chat_id")
    if not dry_run and not chat_id:
        return {"status": "not_configured", "message": "Chưa cấu hình chat_id nhóm Zalo."}

    try:
        client = get_bigquery_client()
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

        df_rb = client.query("""
            SELECT order_no, owner_name, start_receiving_time, receive_date, to_be_received_qty, status
            FROM `jingdong-505914.wms_data.fact_receiving_backlog`
        """).to_dataframe()

        df_ul = client.query("""
            SELECT order_no, arrival_type, unload_time, owner_name
            FROM `jingdong-505914.wms_data.fact_unloading_result`
        """).to_dataframe()

        df_pa = client.query("""
            SELECT order_no, owner_name, start_receiving_time, container_completion_time, target_zone
            FROM `jingdong-505914.wms_data.fact_putaway_backlog`
        """).to_dataframe()

        df_dc = client.query("""
            SELECT goods_no, task_no, owner_name, cell, difference_type, reported_qty, untreated_qty, create_time
            FROM `jingdong-505914.wms_data.fact_diff_center`
        """).to_dataframe()
    except Exception as e:
        return {"status": "error", "message": f"Lỗi truy vấn BigQuery SLA kho: {e}"}

    messages, stats = format_warehouse_sla_report(df_rb, df_ul, df_pa, df_dc, now_vn=now_vn)

    if dry_run:
        return {
            "status": "dry_run",
            "messages": messages,
            "stats": stats,
            "message": f"[DRY RUN] Đã tạo {len(messages)} tin nhắn cảnh báo SLA kho.",
        }

    all_ok = True
    last_err = ""
    for msg in messages:
        if cfg.get("bot_token") and chat_id:
            url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
            res = requests.post(
                url,
                json={"chat_id": chat_id, "text": msg},
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if res.status_code != 200:
                all_ok = False
                last_err = f"Lỗi Zalo API [{res.status_code}]: {res.text}"
        else:
            ok, err = send_zalo_message(msg, target_chat_id=chat_id)
            if not ok:
                all_ok = False
                last_err = err
        time.sleep(0.4)

    if all_ok:
        return {
            "status": "sent",
            "stats": stats,
            "message": f"Đã gửi cảnh báo SLA kho ({len(messages)} tin | Trễ: {stats['total_overdue']}, <=2h: {stats['total_urgent']}, Hạn hôm nay: {stats['total_today']}) vào Zalo.",
        }
    return {"status": "error", "message": f"Không thể gửi tin Zalo SLA kho: {last_err}"}


# ==============================================================================
# TRA CỨU GIỜ CÔNG & OT NHÂN SỰ THEO LỆNH (@bot OT ...)
# Quy tắc:
#   - Định danh: user_code (cột A), ngày: attendance_date (cột B),
#     nhóm: attendance_group (cột M), lỗi: exception_type (cột N),
#     giờ công: final_actual_hours (cột AG).
#   - OT = max(final_actual_hours - 8.0, 0.0) mỗi người/ngày.
#   - 3 lỗi nghiêm trọng hiển thị chi tiết user_code | attendance_group:
#     1) Vắng ngày không có lịch (Absence on Non-scheduled Day)
#     2) Có mặt ngày không có lịch (Attendance on Non-scheduled Day)
#     3) Có mặt ngày nghỉ (Attendance on Rest Day)
# ==============================================================================

def _classify_serious_ot_exception(exc_type: str) -> str | None:
    """Phân loại 3 lỗi chấm công nghiêm trọng cần hiển thị chi tiết user_code - group."""
    low = str(exc_type or "").strip().lower()
    if not low or low == "normal":
        return None
    if ("absence" in low and ("non-scheduled" in low or "non scheduled" in low or "unscheduled" in low)) or "vắng ngày không có lịch" in low:
        return "Vắng ngày không có lịch"
    if ("attendance" in low and ("non-scheduled" in low or "non scheduled" in low or "unscheduled" in low)) or "có mặt ngày không có lịch" in low:
        return "Có mặt ngày không có lịch"
    if ("attendance" in low and ("rest day" in low or "day off" in low or "off day" in low)) or "có mặt ngày nghỉ" in low:
        return "Có mặt ngày nghỉ"
    return None


def _parse_vn_date_filter(text_lower: str, now_vn: datetime.datetime = None) -> dict:
    """
    Bộ trích xuất mốc ngày / khoảng ngày / nhiều ngày rời rạc / tháng / tuần bằng tiếng Việt:
      - 'từ ngày 1/11 đến 17/11', '1/11 - 17/11', '16-17 tháng 11'
      - '16 17 tháng 11', 'ngày 16 và 17 tháng 11', '15/11, 17/11'
      - 'tháng 11', 'tháng này', 'tháng trước', 'tuần này', 'tuần trước'
      - 'hôm nay', 'hôm qua', 'hôm kia', 'ngày 17/11', 'ngày 17 tháng 11', 'ngày 17'
      - Không ghi ngày -> is_latest=True (mặc định ngày mới nhất có dữ liệu).
    """
    import calendar

    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
    today = now_vn.date()

    def _norm_year(y_str):
        if not y_str:
            return today.year
        yv = int(y_str)
        return (2000 + yv) if yv < 100 else yv

    # 1. Khoảng ngày: "từ (ngày) D1/M1(/Y1) đến (ngày) D2/M2(/Y2)" hoặc "D1/M1 - D2/M2"
    m_range = re.search(
        r'(?:từ|tu)?\s*(?:ng[àa]y\s*)?(\d{1,2})[/.-](\d{1,2})(?:[/.-](\d{2,4}))?\s*'
        r'(?:đến|den|tới|toi|->|–|-|~)\s*'
        r'(?:ng[àa]y\s*)?(\d{1,2})[/.-](\d{1,2})(?:[/.-](\d{2,4}))?',
        text_lower,
    )
    if m_range:
        d1, m1, y1, d2, m2, y2 = m_range.groups()
        try:
            yy2 = _norm_year(y2)
            yy1 = _norm_year(y1) if y1 else yy2
            sd = datetime.date(yy1, int(m1), int(d1))
            ed = datetime.date(yy2, int(m2), int(d2))
            if sd > ed:
                sd, ed = ed, sd
            lbl = f"{sd.strftime('%d/%m/%Y')} - {ed.strftime('%d/%m/%Y')}"
            return {"start_date": sd, "end_date": ed, "dates": None, "label": lbl, "is_latest": False}
        except ValueError:
            pass

    # Helper format danh sách ngày rời rạc gọn gàng (VD: [16/11, 17/11] -> "16/11, 17/11/2026")
    def _fmt_dates_label(date_list: list) -> str:
        if len(date_list) == 1:
            return date_list[0].strftime("%d/%m/%Y")
        years = {d.year for d in date_list}
        if len(years) == 1:
            return ", ".join(d.strftime("%d/%m") for d in date_list) + f"/{date_list[0].year}"
        return ", ".join(d.strftime("%d/%m/%Y") for d in date_list)

    # 2. Khoảng ngày cùng tháng (có hoặc không có "từ"):
    #    VD: "từ ngày 1 đến ngày 17 tháng 11", "16-17 tháng 11", "1 đến 17 tháng 11"
    m_range_m = re.search(
        r'(?:từ|tu)?\s*(?:ng[àa]y\s*)?(\d{1,2})\s*'
        r'(?:đến|den|tới|toi|->|–|-|~)\s*'
        r'(?:ng[àa]y\s*)?(\d{1,2})\s*'
        r'th[áa]ng\s*(\d{1,2})(?:\s*(?:/|-|n[ăa]m\s*)(\d{2,4}))?',
        text_lower,
    )
    if m_range_m:
        d1, d2, mm, yy = m_range_m.groups()
        try:
            y_val = _norm_year(yy)
            sd = datetime.date(y_val, int(mm), int(d1))
            ed = datetime.date(y_val, int(mm), int(d2))
            if sd > ed:
                sd, ed = ed, sd
            lbl = f"{sd.strftime('%d/%m/%Y')} - {ed.strftime('%d/%m/%Y')}"
            return {"start_date": sd, "end_date": ed, "dates": None, "label": lbl, "is_latest": False}
        except ValueError:
            pass

    # 2b. Nhiều ngày cụ thể dạng D/M: "16/11 và 17/11", "15/11, 16/11, 17/11", "16/11 17/11"
    all_dm = re.findall(r'(\d{1,2})[/.-](\d{1,2})(?:[/.-](\d{2,4}))?', text_lower)
    if len(all_dm) >= 2:
        parsed_list = []
        for d_s, m_s, y_s in all_dm:
            try:
                parsed_list.append(datetime.date(_norm_year(y_s), int(m_s), int(d_s)))
            except ValueError:
                pass
        uniq_dates = sorted(set(parsed_list))
        if uniq_dates:
            return {
                "start_date": uniq_dates[0],
                "end_date": uniq_dates[-1],
                "dates": uniq_dates,
                "label": _fmt_dates_label(uniq_dates),
                "is_latest": False,
            }

    # 2c. Một hoặc nhiều ngày cụ thể đứng trước "tháng M":
    #     VD: "giờ công 16 17 tháng 11", "ngày 16 và 17 tháng 11", "15, 16, 17 tháng 11", "ngày 17 tháng 11"
    m_days_in_month = re.search(
        r'(?:ng[àa]y\s*)?(\d{1,2}(?:(?:\s*,\s*|\s+v[àa]\s+|\s*&\s*|\s+)(?:ng[àa]y\s*)?\d{1,2})*)\s*'
        r'th[áa]ng\s*(\d{1,2})(?:\s*(?:/|-|n[ăa]m\s*)(\d{2,4}))?',
        text_lower,
    )
    if m_days_in_month:
        days_raw, mm_s, yy_s = m_days_in_month.groups()
        mm = int(mm_s)
        yy = _norm_year(yy_s)
        if 1 <= mm <= 12:
            parsed_list = []
            for d_tok in re.findall(r'\d{1,2}', days_raw):
                try:
                    parsed_list.append(datetime.date(yy, mm, int(d_tok)))
                except ValueError:
                    pass
            uniq_dates = sorted(set(parsed_list))
            if uniq_dates:
                return {
                    "start_date": uniq_dates[0],
                    "end_date": uniq_dates[-1],
                    "dates": uniq_dates,
                    "label": _fmt_dates_label(uniq_dates),
                    "is_latest": False,
                }

    # 3. Cả tháng: "tháng 11", "tháng 11/2026", "tháng này", "tháng trước"
    m_month = re.search(r'th[áa]ng\s*(\d{1,2})(?:\s*(?:/|-|n[ăa]m\s*)(\d{2,4}))?', text_lower)
    if m_month:
        mm = int(m_month.group(1))
        yy = _norm_year(m_month.group(2))
        if 1 <= mm <= 12:
            last_d = calendar.monthrange(yy, mm)[1]
            sd = datetime.date(yy, mm, 1)
            ed = datetime.date(yy, mm, last_d)
            return {"start_date": sd, "end_date": ed, "dates": None, "label": f"Tháng {mm:02d}/{yy}", "is_latest": False}

    if "tháng này" in text_lower or "thang nay" in text_lower:
        last_d = calendar.monthrange(today.year, today.month)[1]
        sd = datetime.date(today.year, today.month, 1)
        ed = datetime.date(today.year, today.month, last_d)
        return {"start_date": sd, "end_date": ed, "dates": None, "label": f"Tháng {today.month:02d}/{today.year}", "is_latest": False}

    if "tháng trước" in text_lower or "thang truoc" in text_lower:
        first_this = today.replace(day=1)
        prev_last = first_this - timedelta(days=1)
        sd = prev_last.replace(day=1)
        ed = prev_last
        return {"start_date": sd, "end_date": ed, "dates": None, "label": f"Tháng {sd.month:02d}/{sd.year}", "is_latest": False}

    # 4. Tuần này / tuần trước
    if "tuần này" in text_lower or "tuan nay" in text_lower:
        sd = today - timedelta(days=today.weekday())
        ed = today
        return {"start_date": sd, "end_date": ed, "dates": None, "label": f"Tuần này ({sd.strftime('%d/%m')} - {ed.strftime('%d/%m/%Y')})", "is_latest": False}

    if "tuần trước" in text_lower or "tuan truoc" in text_lower:
        this_mon = today - timedelta(days=today.weekday())
        sd = this_mon - timedelta(days=7)
        ed = this_mon - timedelta(days=1)
        return {"start_date": sd, "end_date": ed, "dates": None, "label": f"Tuần trước ({sd.strftime('%d/%m')} - {ed.strftime('%d/%m/%Y')})", "is_latest": False}

    # 5. Ngày đơn lẻ hoặc nhiều ngày trong tháng hiện tại (không ghi tháng)
    if "hôm nay" in text_lower or "hom nay" in text_lower:
        return {"start_date": today, "end_date": today, "dates": None, "label": today.strftime("%d/%m/%Y"), "is_latest": False}
    if "hôm qua" in text_lower or "hom qua" in text_lower:
        d = today - timedelta(days=1)
        return {"start_date": d, "end_date": d, "dates": None, "label": d.strftime("%d/%m/%Y"), "is_latest": False}
    if "hôm kia" in text_lower or "hom kia" in text_lower:
        d = today - timedelta(days=2)
        return {"start_date": d, "end_date": d, "dates": None, "label": d.strftime("%d/%m/%Y"), "is_latest": False}

    if len(all_dm) == 1:
        d, m_val, y_val = all_dm[0]
        try:
            dt = datetime.date(_norm_year(y_val), int(m_val), int(d))
            return {"start_date": dt, "end_date": dt, "dates": None, "label": dt.strftime("%d/%m/%Y"), "is_latest": False}
        except ValueError:
            pass

    m_day_only = re.search(
        r'ng[àa]y\s*(\d{1,2}(?:(?:\s*,\s*|\s+v[àa]\s+|\s*&\s*|\s+)(?:ng[àa]y\s*)?\d{1,2})*)\b',
        text_lower,
    )
    if m_day_only:
        parsed_list = []
        for d_tok in re.findall(r'\d{1,2}', m_day_only.group(1)):
            try:
                parsed_list.append(datetime.date(today.year, today.month, int(d_tok)))
            except ValueError:
                pass
        uniq_dates = sorted(set(parsed_list))
        if uniq_dates:
            return {
                "start_date": uniq_dates[0],
                "end_date": uniq_dates[-1],
                "dates": uniq_dates if len(uniq_dates) > 1 else None,
                "label": _fmt_dates_label(uniq_dates),
                "is_latest": False,
            }

    # 6. Không ghi ngày cụ thể -> tự động lấy ngày mới nhất có dữ liệu
    return {"start_date": None, "end_date": None, "dates": None, "label": "Ngày mới nhất", "is_latest": True}


def parse_ot_query(raw_text: str, now_vn: datetime.datetime = None) -> dict | None:
    """
    Nhận diện câu lệnh/câu hỏi tự nhiên về OT & giờ công nhân sự, ví dụ:
      - '@bot OT ngày 17/11'
      - '@bot OT từ ngày 1/11 đến 17/11'
      - '@bot OT tháng 11'
      - 'giờ công 16 17 tháng 11'
    Trả về dict {start_date, end_date, dates, label, is_latest} hoặc None nếu không phải lệnh OT.
    """
    if not raw_text or not raw_text.strip():
        return None

    cleaned = re.sub(r'@\S+', ' ', raw_text).strip()
    text_lower = re.sub(r'\s+', ' ', cleaned.lower()).strip()
    if not text_lower:
        return None

    has_ot_kw = bool(
        re.search(r'(?:^|\s)/?ot(?:\s|$|[,.:;?!])', text_lower)
        or any(k in text_lower for k in ["tăng ca", "tang ca", "giờ công", "gio cong"])
    )
    if not has_ot_kw:
        return None

    return _parse_vn_date_filter(text_lower, now_vn=now_vn)


def format_ot_report(df: pd.DataFrame, period_label: str, is_single_day: bool = True) -> list[str]:
    """
    Tạo báo cáo giờ công & OT gửi Zalo theo đúng quy tắc:
      - Dùng user_code (cột A), attendance_date (cột B), attendance_group (cột M),
        exception_type (cột N), final_actual_hours (cột AG).
      - Giờ chuẩn = 8.0h/ngày -> OT = max(final_actual_hours - 8.0, 0.0).
      - Hiển thị chi tiết user_code | group cho đúng 3 lỗi nghiêm trọng.
    """
    header = "[BÁO CÁO GIỜ CÔNG & OT - KHO JDL]"
    if df is None or df.empty:
        return [f"{header}\nNgày: {period_label}\n\nChưa có dữ liệu giờ công cho khoảng thời gian này."]

    work = df.copy()
    work["user_code"] = work["user_code"].fillna("").astype(str).str.strip()
    work["attendance_group"] = (
        work["attendance_group"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace({"": "Không rõ nhóm", "nan": "Không rõ nhóm", "None": "Không rõ nhóm"})
    )
    work["exception_type"] = work["exception_type"].fillna("").astype(str).str.strip()
    work["final_actual_hours"] = pd.to_numeric(work["final_actual_hours"], errors="coerce").fillna(0.0)
    work["ot_hours"] = (work["final_actual_hours"] - 8.0).clip(lower=0.0)
    work["regular_hours"] = work["final_actual_hours"].clip(lower=0.0, upper=8.0)
    work["_dt"] = pd.to_datetime(work["attendance_date"], errors="coerce")

    n_days = int(work["_dt"].dt.date.nunique())
    single_view = is_single_day and (n_days <= 1)

    total_records = len(work)
    worked_df = work[work["final_actual_hours"] > 0]
    ot_df = work[work["ot_hours"] > 0]

    total_hours = float(work["final_actual_hours"].sum())
    total_regular = float(work["regular_hours"].sum())
    total_ot = float(work["ot_hours"].sum())
    ot_vs_regular_pct = (total_ot / total_regular * 100.0) if total_regular > 0 else 0.0

    period_line = f"Ngày: {period_label}" if single_view else f"Ngày: {period_label} ({n_days} ngày có dữ liệu)"
    lines = [
        header,
        period_line,
        "",
        "1. TỔNG QUAN",
    ]
    if single_view:
        lines.append(f"- Nhân sự đi làm: {len(worked_df):,} người (có OT: {len(ot_df):,})")
    else:
        uniq_users = int(worked_df["user_code"].nunique())
        lines.append(f"- Lượt đi làm: {len(worked_df):,} lượt ({uniq_users:,} người) | Lượt có OT: {len(ot_df):,}")
    lines.append(f"- Tổng giờ công: {total_hours:,.2f}h")
    lines.append(f"- Tổng giờ OT: {total_ot:,.2f}h")
    lines.append(f"- Tỷ lệ OT: {ot_vs_regular_pct:.1f}%")

    # 2. Tổng hợp theo Attendance Group (cột M): tên group: số giờ OT - số người OT - tỉ trọng % tổng OT toàn kho
    lines.append("")
    lines.append("2. CHI TIẾT THEO ATTENDANCE GROUP")
    if ot_df.empty or total_ot <= 0:
        lines.append("- Không có nhóm nào phát sinh OT.")
    else:
        grp_df = (
            ot_df.groupby("attendance_group", as_index=False)
            .agg(
                ot_users=("user_code", "nunique"),
                total_ot=("ot_hours", "sum"),
            )
            .sort_values(by=["total_ot", "ot_users"], ascending=[False, False])
        )
        for _, r in grp_df.iterrows():
            grp_share = (r["total_ot"] / total_ot * 100.0) if total_ot > 0 else 0.0
            lines.append(
                f"- {r['attendance_group']}: {r['total_ot']:.2f}h - {int(r['ot_users'])} người - {grp_share:.1f}%"
            )

    # 3. 3 Lỗi nghiêm trọng (cột N) -> hiển thị chi tiết user_code | group
    work["serious_cat"] = work["exception_type"].apply(_classify_serious_ot_exception)
    ser_df = work[work["serious_cat"].notna()].copy()

    lines.append("")
    if ser_df.empty:
        lines.append("3. LỖI NGHIÊM TRỌNG: Không có (0 lượt).")
    else:
        lines.append(f"3. LỖI NGHIÊM TRỌNG ({len(ser_df):,} lượt)")
        cat_order = [
            "Vắng ngày không có lịch",
            "Có mặt ngày không có lịch",
            "Có mặt ngày nghỉ",
        ]
        for cat in cat_order:
            sub = ser_df[ser_df["serious_cat"] == cat].sort_values(
                by=["_dt", "attendance_group", "user_code"], ascending=[True, True, True]
            )
            if sub.empty:
                continue
            lines.append(f"• {cat} ({len(sub):,}):")
            for _, r in sub.iterrows():
                if single_view:
                    lines.append(f"  - {r['user_code']} | {r['attendance_group']}")
                else:
                    d_str = r["_dt"].strftime("%d/%m") if pd.notna(r["_dt"]) else ""
                    lines.append(f"  - {d_str} | {r['user_code']} | {r['attendance_group']}")

    return _split_zalo_lines(lines, header_prefix=header, max_chars=1800)


def query_ot_report(params: dict) -> list[str]:
    """Truy vấn BigQuery `fact_work_hours` theo khoảng ngày hoặc danh sách ngày rời rạc và trả về tin nhắn Zalo."""
    client = get_bigquery_client()

    start_date = params.get("start_date")
    end_date = params.get("end_date")
    dates_list = params.get("dates")
    label = params.get("label") or ""

    if params.get("is_latest") or (start_date is None and not dates_list):
        df_max = client.query(
            "SELECT MAX(attendance_date) AS max_d FROM `jingdong-505914.wms_data.fact_work_hours`"
        ).to_dataframe()
        if df_max.empty or pd.isna(df_max["max_d"].iloc[0]):
            return ["[BÁO CÁO GIỜ CÔNG & OT - KHO JDL]\n\nChưa có dữ liệu giờ công nào trong hệ thống."]
        latest_d = pd.to_datetime(df_max["max_d"].iloc[0]).date()
        start_date = latest_d
        end_date = latest_d
        dates_list = None
        label = latest_d.strftime("%d/%m/%Y")

    if dates_list:
        in_clause = ", ".join(f"DATE '{d.strftime('%Y-%m-%d')}'" for d in dates_list)
        where_sql = f"attendance_date IN ({in_clause})"
        is_single = (len(dates_list) == 1)
    else:
        where_sql = (
            f"attendance_date >= DATE '{start_date.strftime('%Y-%m-%d')}' "
            f"AND attendance_date <= DATE '{end_date.strftime('%Y-%m-%d')}'"
        )
        is_single = (start_date == end_date)

    sql = f"""
    SELECT
        user_code,
        attendance_date,
        attendance_group,
        exception_type,
        final_actual_hours,
        ot_hours
    FROM `jingdong-505914.wms_data.fact_work_hours`
    WHERE {where_sql}
    """
    df = client.query(sql).to_dataframe()
    return format_ot_report(df, period_label=label, is_single_day=is_single)


def handle_ot_query(raw_text: str, target_chat_id: str = None, dry_run: bool = False) -> tuple[bool, str | list[str]]:
    """Xử lý câu lệnh tra cứu OT & giờ công nhân sự từ Zalo Bot."""
    params = parse_ot_query(raw_text)
    if not params:
        return False, "Không nhận diện được yêu cầu tra cứu OT."

    try:
        messages = query_ot_report(params)
    except Exception as e:
        return False, f"Lỗi truy vấn BigQuery giờ công/OT: {e}"

    if dry_run:
        return True, messages

    cfg = get_zalo_config()
    chat_id = target_chat_id or cfg.get("chat_id")
    if not chat_id:
        return False, "Chưa xác định được chat_id nhóm Zalo."

    all_ok = True
    last_err = ""
    for msg in messages:
        if cfg.get("bot_token") and chat_id:
            url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
            res = requests.post(
                url,
                json={"chat_id": chat_id, "text": msg},
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if res.status_code != 200:
                all_ok = False
                last_err = f"Lỗi Zalo API [{res.status_code}]: {res.text}"
        else:
            ok, err = send_zalo_message(msg, target_chat_id=chat_id)
            if not ok:
                all_ok = False
                last_err = err
        time.sleep(0.4)

    if all_ok:
        return True, f"Đã gửi báo cáo giờ công & OT ({len(messages)} tin) vào Zalo thành công."
    return False, f"Không thể gửi tin Zalo báo cáo OT: {last_err}"


# ==============================================================================
# TRA CỨU VẬT TƯ ĐÓNG GÓI - POP MART (ĐỀ XUẤT VS THỰC TẾ)
# Lệnh: "@bot vật tư pop ngày 17/11", "@bot vật tư pop từ 1/11 đến 17/11",
#       "@bot vật tư pop 16 17 tháng 11", "@bot vật tư pop tháng 11",...
# ==============================================================================

def parse_pop_material_query(raw_text: str, now_vn: datetime.datetime = None) -> dict | None:
    """
    Nhận diện câu lệnh tra cứu vật tư đóng gói khách hàng Pop Mart (1 ngày hoặc nhiều ngày).
    """
    if not raw_text or not raw_text.strip():
        return None

    cleaned = re.sub(r'@\S+', ' ', raw_text).strip()
    text_lower = re.sub(r'\s+', ' ', cleaned.lower()).strip()
    if not text_lower:
        return None

    has_mat_kw = any(k in text_lower for k in [
        "vật tư", "vat tu", "vattu", "consumable", "thùng", "thung", "hộp đóng gói", "hop dong goi", "carton"
    ])
    has_pop_kw = bool(
        re.search(r'(?:^|\s)pop(?:\s|$|[,.:;?!])', text_lower)
        or "popmart" in text_lower
        or "pop mart" in text_lower
    )
    if not (has_mat_kw and has_pop_kw):
        return None

    return _parse_vn_date_filter(text_lower, now_vn=now_vn)


def _is_pop_material_matched(rec_code: str, rec_name: str, act_code: str, act_name: str) -> bool:
    """
    Kiểm tra đơn có đóng đúng vật tư đề xuất không:
      - Khớp mã vật tư (rec_code == act_code, không phân biệt hoa thường), HOẶC
      - Trường hợp đặc biệt SIOP: WMS đề xuất rec_code='SIOP' (rec_name trống) và thực tế quét act_code='HC000498' / act_name='SIOP', HOẶC
      - Khớp tên vật tư (rec_name == act_name khi cả hai không rỗng).
    """
    rc = (rec_code or "").strip().upper()
    rn = (rec_name or "").strip().upper()
    ac = (act_code or "").strip().upper()
    an = (act_name or "").strip().upper()

    if not rc or not ac:
        return False
    if rc == ac:
        return True
    if rc == "SIOP" and (an == "SIOP" or ac == "SIOP"):
        return True
    if rn and an and rn == an:
        return True
    return False


def _fmt_mat_label(code: str, name: str, empty_fallback: str) -> str:
    c = (code or "").strip()
    n = (name or "").strip()
    if not c and not n:
        return empty_fallback
    if c and n and c.upper() != n.upper():
        return f"{c} | {n}"
    return c or n


def _fmt_mat_pair_side(code: str, name: str, empty_fallback: str) -> str:
    c = (code or "").strip()
    n = (name or "").strip()
    if not c and not n:
        return empty_fallback
    if c and n and c.upper() != n.upper():
        return f"{c} ({n})"
    return c or n


def format_pop_material_report(df: pd.DataFrame, period_label: str, is_single_day: bool = True) -> list[str]:
    """
    Tạo báo cáo vật tư đóng gói Pop Mart (theo đơn hàng duy nhất `order_no`), khớp 4 bảng mẫu:
      1. Tổng quan: Tổng đơn, Đơn có đề xuất vật tư, Đóng sai vật tư đề xuất, Đóng đúng vật tư đề xuất.
      2. Nhân sự đóng sai vật tư đề xuất (Staff ID | Order Qty | PCS Qty).
      3. Vật tư đóng gói thực tế (Consumables Code | Qty | % tổng đơn).
      4. Chi tiết cặp lệch vật tư (Đề xuất -> Thực tế | Order Qty | % tổng đơn đóng sai).
    """
    header = "[BÁO CÁO VẬT TƯ ĐÓNG GÓI - POP MART]"
    if df is None or df.empty:
        return [f"{header}\nNgày: {period_label}\n\nChưa có dữ liệu đóng gói Pop Mart cho khoảng thời gian này."]

    work = df.copy()
    work["order_no"] = work["order_no"].fillna("").astype(str).str.strip()
    work = work[work["order_no"] != ""].copy()
    if work.empty:
        return [f"{header}\nNgày: {period_label}\n\nChưa có dữ liệu đóng gói Pop Mart cho khoảng thời gian này."]

    for c in [
        "worker_name",
        "recommended_material_code",
        "recommended_material_name",
        "packing_material_code",
        "packing_material_name",
    ]:
        if c not in work.columns:
            work[c] = ""
        work[c] = (
            work[c]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "<NA>": ""})
        )

    if "packing_qty" in work.columns:
        work["packing_qty"] = pd.to_numeric(work["packing_qty"], errors="coerce").fillna(0).astype(int)
    else:
        work["packing_qty"] = 0

    work["_dt"] = pd.to_datetime(work.get("pack_time"), errors="coerce")
    n_days = int(work["_dt"].dt.date.nunique()) if work["_dt"].notna().any() else 1
    single_view = is_single_day and (n_days <= 1)

    # Cộng tổng PCS theo từng order_no trước khi gom về 1 dòng / đơn
    pcs_by_order = work.groupby("order_no")["packing_qty"].sum()

    work["_score"] = (
        (work["recommended_material_code"] != "").astype(int) * 2
        + (work["packing_material_code"] != "").astype(int)
    )
    ord_df = (
        work.sort_values(by=["_score", "_dt"], ascending=[False, False])
        .drop_duplicates(subset=["order_no"], keep="first")
        .copy()
    )
    ord_df["pcs"] = ord_df["order_no"].map(pcs_by_order).fillna(0).astype(int)

    total_orders = len(ord_df)
    ord_df["has_rec"] = ord_df["recommended_material_code"] != ""
    ord_df["is_matched"] = ord_df.apply(
        lambda r: _is_pop_material_matched(
            r["recommended_material_code"],
            r["recommended_material_name"],
            r["packing_material_code"],
            r["packing_material_name"],
        ),
        axis=1,
    )

    rec_df = ord_df[ord_df["has_rec"]].copy()
    rec_orders = len(rec_df)
    match_orders = int(rec_df["is_matched"].sum()) if rec_orders > 0 else 0
    inc_df = rec_df[~rec_df["is_matched"]].copy()
    mismatch_orders = len(inc_df)
    no_rec_orders = total_orders - rec_orders

    rec_rate = (rec_orders / total_orders * 100.0) if total_orders > 0 else 0.0
    match_rate = (match_orders / rec_orders * 100.0) if rec_orders > 0 else 0.0
    mismatch_rate = (mismatch_orders / rec_orders * 100.0) if rec_orders > 0 else 0.0

    date_line = f"Ngày: {period_label}" if single_view else f"Ngày: {period_label} ({n_days} ngày có dữ liệu)"
    lines = [
        header,
        date_line,
        "",
        "1. TỔNG QUAN",
        f"- Tổng đơn đã đóng gói: {total_orders:,} đơn",
        f"- Đơn có đề xuất vật tư: {rec_orders:,} đơn ({rec_rate:.1f}%)",
        f"- Đóng sai vật tư đề xuất: {mismatch_orders:,} đơn ({mismatch_rate:.1f}%)",
        f"- Đóng đúng vật tư đề xuất: {match_orders:,} đơn ({match_rate:.1f}%)",
    ]
    if no_rec_orders > 0:
        lines.append(f"- Chưa có đề xuất vật tư: {no_rec_orders:,} đơn ({no_rec_orders / total_orders * 100.0:.1f}%)")

    # 2. Vật tư đóng gói thực tế (Consumables Code | Qty | %)
    lines.append("")
    lines.append(f"2. VẬT TƯ ĐÓNG GÓI THỰC TẾ ({total_orders:,} đơn)")
    ord_df["act_label"] = ord_df.apply(
        lambda r: _fmt_mat_label(r["packing_material_code"], r["packing_material_name"], "Không quét vật tư"),
        axis=1,
    )
    act_grp = (
        ord_df.groupby("act_label", as_index=False)["order_no"]
        .count()
        .rename(columns={"order_no": "orders"})
        .sort_values(by=["orders", "act_label"], ascending=[False, True])
    )
    for _, r in act_grp.iterrows():
        pct = (r["orders"] / total_orders * 100.0) if total_orders > 0 else 0.0
        lines.append(f"- {r['act_label']}: {int(r['orders']):,} đơn ({pct:.2f}%)")

    # 3. Chi tiết lệch vật tư: Đề xuất -> Thực tế (Order Qty | % trên tổng đơn đóng sai)
    lines.append("")
    lines.append(f"3. CHI TIẾT LỆCH VẬT TƯ: ĐỀ XUẤT -> THỰC TẾ ({mismatch_orders:,} đơn)")
    if inc_df.empty:
        lines.append("- Không phát sinh lệch vật tư.")
    else:
        inc_df["rec_side"] = inc_df.apply(
            lambda r: _fmt_mat_pair_side(r["recommended_material_code"], r["recommended_material_name"], "N/A"),
            axis=1,
        )
        inc_df["act_side"] = inc_df.apply(
            lambda r: _fmt_mat_pair_side(r["packing_material_code"], r["packing_material_name"], "Không quét"),
            axis=1,
        )
        pair_grp = (
            inc_df.groupby(["rec_side", "act_side"], as_index=False)["order_no"]
            .count()
            .rename(columns={"order_no": "orders"})
            .sort_values(by=["orders", "rec_side", "act_side"], ascending=[False, True, True])
        )
        max_pairs = 35
        shown = pair_grp.head(max_pairs)
        for _, r in shown.iterrows():
            pct = (r["orders"] / mismatch_orders * 100.0) if mismatch_orders > 0 else 0.0
            lines.append(f"- {r['rec_side']} -> {r['act_side']}: {int(r['orders']):,} đơn ({pct:.2f}%)")
        if len(pair_grp) > max_pairs:
            tail = pair_grp.iloc[max_pairs:]
            tail_ord = int(tail["orders"].sum())
            tail_pct = (tail_ord / mismatch_orders * 100.0) if mismatch_orders > 0 else 0.0
            lines.append(f"- (+ {len(tail)} cặp lệch nhỏ khác): {tail_ord:,} đơn ({tail_pct:.2f}%)")

    return _split_zalo_lines(lines, header_prefix=header, max_chars=1800)


def query_pop_material_report(params: dict) -> list[str]:
    """Truy vấn BigQuery `fact_packing` cho khách hàng Pop Mart theo ngày / khoảng ngày / nhiều ngày."""
    client = get_bigquery_client()

    start_date = params.get("start_date")
    end_date = params.get("end_date")
    dates_list = params.get("dates")
    label = params.get("label") or ""

    if params.get("is_latest") or (start_date is None and not dates_list):
        df_max = client.query("""
            SELECT MAX(DATE(pack_time)) AS max_d
            FROM `jingdong-505914.wms_data.fact_packing`
            WHERE UPPER(owner_name) LIKE '%POP%'
        """).to_dataframe()
        if df_max.empty or pd.isna(df_max["max_d"].iloc[0]):
            return ["[BÁO CÁO VẬT TƯ ĐÓNG GÓI - POP MART]\n\nChưa có dữ liệu đóng gói Pop Mart trong hệ thống."]
        latest_d = pd.to_datetime(df_max["max_d"].iloc[0]).date()
        start_date = latest_d
        end_date = latest_d
        dates_list = None
        label = latest_d.strftime("%d/%m/%Y")

    if dates_list:
        in_clause = ", ".join(f"DATE '{d.strftime('%Y-%m-%d')}'" for d in dates_list)
        where_sql = f"DATE(pack_time) IN ({in_clause})"
        is_single = (len(dates_list) == 1)
    else:
        where_sql = (
            f"DATE(pack_time) >= DATE '{start_date.strftime('%Y-%m-%d')}' "
            f"AND DATE(pack_time) <= DATE '{end_date.strftime('%Y-%m-%d')}'"
        )
        is_single = (start_date == end_date)

    sql = f"""
    SELECT
        order_no,
        pack_time,
        worker_name,
        SAFE_CAST(packing_qty AS INT64) AS packing_qty,
        recommended_material_code,
        recommended_material_name,
        packing_material_code,
        packing_material_name
    FROM `jingdong-505914.wms_data.fact_packing`
    WHERE UPPER(owner_name) LIKE '%POP%'
      AND {where_sql}
    """
    df = client.query(sql).to_dataframe()
    return format_pop_material_report(df, period_label=label, is_single_day=is_single)


def handle_pop_material_query(raw_text: str, target_chat_id: str = None, dry_run: bool = False) -> tuple[bool, str | list[str]]:
    """Xử lý câu lệnh tra cứu vật tư đóng gói Pop Mart từ Zalo Bot."""
    params = parse_pop_material_query(raw_text)
    if not params:
        return False, "Không nhận diện được yêu cầu tra cứu vật tư Pop Mart."

    try:
        messages = query_pop_material_report(params)
    except Exception as e:
        return False, f"Lỗi truy vấn BigQuery vật tư Pop Mart: {e}"

    if dry_run:
        return True, messages

    cfg = get_zalo_config()
    chat_id = target_chat_id or cfg.get("chat_id")
    if not chat_id:
        return False, "Chưa xác định được chat_id nhóm Zalo."

    all_ok = True
    last_err = ""
    for msg in messages:
        if cfg.get("bot_token") and chat_id:
            url = f"https://bot-api.zaloplatforms.com/bot{cfg['bot_token']}/sendMessage"
            res = requests.post(
                url,
                json={"chat_id": chat_id, "text": msg},
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if res.status_code != 200:
                all_ok = False
                last_err = f"Lỗi Zalo API [{res.status_code}]: {res.text}"
        else:
            ok, err = send_zalo_message(msg, target_chat_id=chat_id)
            if not ok:
                all_ok = False
                last_err = err
        time.sleep(0.4)

    if all_ok:
        return True, f"Đã gửi báo cáo vật tư Pop Mart ({len(messages)} tin) vào Zalo thành công."
    return False, f"Không thể gửi tin Zalo vật tư Pop Mart: {last_err}"
