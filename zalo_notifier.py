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


def format_detailed_backlog_list(df_backlog: pd.DataFrame, now_vn: datetime.datetime = None, max_hours: float = 2.5) -> list[str]:
    """
    Tạo danh sách toàn bộ các đơn sắp trễ SLA (quá hạn hoặc sắp quá hạn <= max_hours tiếng).
    Phân tách rõ ràng giữa ĐÃ QUÁ HẠN và SẮP QUÁ HẠN:
      - Đơn đã trễ hạn luôn được đưa lên ĐẦU danh sách để ưu tiên xử lý gấp.
      - Sắp xếp theo: Khách hàng -> Thời gian còn lại.
    Trả về danh sách các tin nhắn (tự động chia nhỏ nếu vượt quá 1800 ký tự để gửi qua Zalo).
    """
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    if df_backlog is None or df_backlog.empty:
        return [f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\nThời gian: {now_vn.strftime('%H:%M:%S %d/%m/%Y')}\nKhông có dữ liệu đơn Backlog tồn đọng."]

    df = df_backlog.copy()
    if 'prod_dt' not in df.columns:
        df['prod_dt'] = pd.to_datetime(df['production_end_time'], errors='coerce')

    df_today = df[df['prod_dt'].dt.date == now_vn.date()].copy()
    if df_today.empty:
        return [f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\nThời gian: {now_vn.strftime('%H:%M:%S %d/%m/%Y')}\nKhông có đơn nào tồn đọng trong ngày hôm nay."]

    df_today['diff_sec'] = (df_today['prod_dt'] - now_vn).dt.total_seconds()
    # Lọc đơn rủi ro: quá hạn (diff_sec < 0) hoặc sắp quá hạn trong max_hours
    df_risk = df_today[df_today['diff_sec'] <= max_hours * 3600].copy()

    if df_risk.empty:
        next_min_sec = df_today['diff_sec'].min()
        next_h = next_min_sec / 3600
        return [
            f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\n"
            f"Thời gian: {now_vn.strftime('%H:%M:%S %d/%m/%Y')}\n"
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
        f"Thời gian: {now_vn.strftime('%H:%M:%S %d/%m/%Y')}",
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
    if 'TOP ENTERPRISE' in u:
        return 'TOP ENTERPRISE'
    return str(name).strip()


def format_packing_progress_report(df_pack: pd.DataFrame, df_bl: pd.DataFrame, now_vn: datetime.datetime = None) -> str:
    """
    Tạo báo cáo tiến độ đóng gói theo Mẫu 2:
    - Chia 2 nhóm rõ ràng: B2B (KKV, Chint, Envision) và B2C (còn lại).
    - Khách nào sản lượng = 0 thì không hiện.
    - Không gom 'khách khác', chi tiết từng khách có sản lượng > 0.
    """
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    bl_orders = int(df_bl['order_no'].nunique()) if (df_bl is not None and not df_bl.empty) else 0

    if df_pack is None or df_pack.empty:
        return (
            f"[TIẾN ĐỘ ĐÓNG GÓI - KHO JDL]\n"
            f"Thời gian: {now_vn.strftime('%H:%M %d/%m/%Y')}\n\n"
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

    time_str = now_vn.strftime('%H:%M %d/%m/%Y')
    lines = [
        "[TIẾN ĐỘ ĐÓNG GÓI - KHO JDL]",
        f"Thời gian: {time_str}",
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


def format_etc_report(df_pack: pd.DataFrame, df_bl: pd.DataFrame, now_vn: datetime.datetime = None) -> str:
    """Tạo nội dung tin nhắn chuyên sâu dự báo thời gian clear backlog."""
    if now_vn is None:
        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)

    time_str = now_vn.strftime('%H:%M %d/%m/%Y')
    bl_orders = int(df_bl['order_no'].nunique()) if (df_bl is not None and not df_bl.empty) else 0
    total_packed = int(df_pack['order_no'].nunique()) if (df_pack is not None and not df_pack.empty) else 0

    if bl_orders == 0:
        return f"[DỰ BÁO THỜI GIAN CLEAR BACKLOG - KHO JDL]\nThời gian: {time_str}\n\nHiện tại kho đã CLEAR HẾT BACKLOG! Không còn đơn tồn đọng."

    if df_pack is None or df_pack.empty or total_packed == 0:
        return f"[DỰ BÁO THỜI GIAN CLEAR BACKLOG - KHO JDL]\nThời gian: {time_str}\n\nChưa có sản lượng đóng gói nào hôm nay để tính tốc độ clear hàng."

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
        f"Thời gian: {time_str}",
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
            msg = f"[DANH SÁCH ĐƠN HÀNG SẮP QUÁ HẠN SLA]\nThời gian: {now_vn.strftime('%H:%M:%S %d/%m/%Y')}\nKhông có đơn nào tồn đọng trong ngày hôm nay ({today_vn})."
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
