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
    return bool(cfg.get("webhook_url") or (cfg.get("access_token") and cfg.get("group_id")))


def send_zalo_message(message_text: str) -> tuple[bool, str]:
    """
    Gửi tin nhắn văn bản vào Nhóm Chat Zalo.
    Trả về (True, "OK") nếu thành công, hoặc (False, "Lý do lỗi").
    """
    cfg = get_zalo_config()
    if not cfg.get("enabled"):
        return False, "Tính năng thông báo Zalo đang bị tắt (enabled=False)."

    webhook_url = cfg.get("webhook_url", "").strip()
    access_token = cfg.get("access_token", "").strip()
    group_id = cfg.get("group_id", "").strip()

    if not webhook_url and not (access_token and group_id):
        return False, "Chưa cấu hình Zalo Webhook URL hoặc (Access Token + Group ID)."

    # 1. Ưu tiên gửi qua Webhook URL nếu được cấu hình
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

    # 2. Gửi qua Zalo OpenAPI chính thức (v3.0/oa/group/message)
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

    return False, "Cấu hình Zalo không hợp lệ."


def format_sla_alert_message(df_urgent: pd.DataFrame, df_overdue: pd.DataFrame, now_vn: datetime.datetime, urgent_hours: float) -> str:
    """Tạo nội dung tin nhắn tiếng Việt định dạng chuẩn cảnh báo vận hành kho."""
    tot_urgent = len(df_urgent)
    tot_overdue = len(df_overdue)
    tot_alert = tot_urgent + tot_overdue

    time_str = now_vn.strftime("%H:%M:%S ngày %d/%m/%Y")
    
    lines = [
        "🚨 [CẢNH BÁO SLA ĐÓNG GÓI - KHO JDL] 🚨",
        f"⏰ Quét lúc: {time_str}",
        f"📊 TỔNG ĐƠN CẦN XỬ LÝ GẤP: {tot_alert:,} đơn",
        "------------------------------------"
    ]

    # Đơn đã trễ hạn
    if tot_overdue > 0:
        lines.append(f"🔴 ĐÃ QUÁ HẠN (OVERDUE): {tot_overdue:,} đơn")
        cust_overdue = df_overdue.groupby('owner_name').agg(
            cnt=('order_no', 'nunique'),
            times=('production_end_time', lambda s: ", ".join(sorted(set(str(x)[11:16] for x in s if pd.notnull(x)))[:3]))
        ).sort_values(by='cnt', ascending=False)
        for c_name, row in cust_overdue.head(5).iterrows():
            lines.append(f"  • {c_name}: {row['cnt']:,} đơn (Hạn: {row['times']})")
        if len(cust_overdue) > 5:
            lines.append(f"  • Và {len(cust_overdue) - 5} khách hàng khác...")
        lines.append("------------------------------------")
    else:
        lines.append("✅ ĐÃ QUÁ HẠN: 0 đơn")

    # Đơn cận hạn trong vòng <= urgent_hours tiếng
    if tot_urgent > 0:
        h_label = f"{urgent_hours:g} tiếng" if urgent_hours >= 1 else f"{int(urgent_hours*60)} phút"
        lines.append(f"🟡 CẬN HẠN TRONG VÒNG ≤ {h_label.upper()}: {tot_urgent:,} đơn")
        cust_urgent = df_urgent.groupby('owner_name').agg(
            cnt=('order_no', 'nunique'),
            times=('production_end_time', lambda s: ", ".join(sorted(set(str(x)[11:16] for x in s if pd.notnull(x)))[:3]))
        ).sort_values(by='cnt', ascending=False)
        for c_name, row in cust_urgent.head(6).iterrows():
            lines.append(f"  • {c_name}: {row['cnt']:,} đơn (Hạn: {row['times']})")
        if len(cust_urgent) > 6:
            lines.append(f"  • Và {len(cust_urgent) - 6} khách hàng khác...")
        lines.append("------------------------------------")
    else:
        lines.append("✅ CẬN HẠN: 0 đơn")

    lines.append("👉 Đề nghị Tổ Đóng Gói ưu tiên xuất & đóng các đơn hàng trên để đảm bảo SLA!")
    lines.append("🔗 Xem chi tiết tại Dashboard JDL.")

    return "\n".join(lines)


def scan_and_alert_backlog_sla(df_backlog: pd.DataFrame, force_send: bool = False, custom_urgent_hours: float = None) -> dict:
    """
    Quét dữ liệu backlog và gửi cảnh báo vào nhóm Zalo nếu có đơn cận hạn hoặc đã trễ.
    - custom_urgent_hours: số giờ cận hạn (mặc định đọc từ config, thường là 1.0 tiếng).
    - force_send: bỏ qua cơ chế chống spam (dùng khi người dùng bấm nút test/bắn tin thủ công).
    """
    global _LAST_ALERT_STATE

    if df_backlog is None or df_backlog.empty:
        return {"status": "empty", "message": "Dữ liệu Backlog trống."}

    cfg = get_zalo_config()
    urgent_hours = custom_urgent_hours if custom_urgent_hours is not None else cfg.get("urgent_hours", 1.0)
    cooldown_sec = cfg.get("cooldown_minutes", 30.0) * 60

    # Chuẩn bị thời gian
    now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
    today_vn = now_vn.date()
    urgent_time = now_vn + timedelta(hours=urgent_hours)

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
        return {"status": "no_today_orders", "message": f"Không có đơn nào có hạn hôm nay ({today_vn})."}

    # Phân loại: Đã trễ vs Cận hạn
    df_overdue = df_today[df_today['prod_dt'] < now_vn]
    df_urgent = df_today[(df_today['prod_dt'] >= now_vn) & (df_today['prod_dt'] <= urgent_time)]

    total_risk = len(df_overdue) + len(df_urgent)
    if total_risk == 0:
        return {
            "status": "safe",
            "message": f"Tất cả các đơn hôm nay đều an toàn (> {urgent_hours:g}h nữa mới đến hạn).",
            "count": 0
        }

    # Tạo chữ ký dữ liệu để chống spam tin trùng lặp
    all_risk_ids = sorted(df_overdue['order_no'].astype(str).tolist() + df_urgent['order_no'].astype(str).tolist())
    sig_str = f"{today_vn}_{len(df_overdue)}_{len(df_urgent)}_" + "_".join(all_risk_ids[:50])
    current_sig = hashlib.md5(sig_str.encode('utf-8')).hexdigest()

    now_ts = time.time()
    # Kiểm tra chống spam nếu không ép buộc gửi
    if not force_send:
        if current_sig == _LAST_ALERT_STATE["signature"]:
            if (now_ts - _LAST_ALERT_STATE["timestamp"]) < cooldown_sec:
                return {
                    "status": "skipped_cooldown",
                    "message": f"Danh sách đơn rủi ro ({total_risk} đơn) đã được gửi gần đây, tạm hoãn gửi lặp lại.",
                    "count": total_risk
                }

    # Soạn tin nhắn
    message_text = format_sla_alert_message(df_urgent, df_overdue, now_vn, urgent_hours)

    # Nếu chưa cấu hình Zalo
    if not is_zalo_configured():
        return {
            "status": "not_configured",
            "message": "Chưa cấu hình Zalo Bot (Webhook URL hoặc Token). Nội dung tin nhắn đã sẵn sàng.",
            "count": total_risk,
            "preview_message": message_text
        }

    # Thực hiện gửi tin
    success, msg = send_zalo_message(message_text)
    if success:
        _LAST_ALERT_STATE["signature"] = current_sig
        _LAST_ALERT_STATE["timestamp"] = now_ts
        _LAST_ALERT_STATE["last_count"] = total_risk
        return {
            "status": "sent",
            "message": f"Đã gửi cảnh báo {total_risk} đơn vào nhóm Zalo thành công!",
            "count": total_risk,
            "preview_message": message_text
        }
    else:
        return {
            "status": "error",
            "message": f"Không thể gửi tin Zalo: {msg}",
            "count": total_risk,
            "preview_message": message_text
        }
