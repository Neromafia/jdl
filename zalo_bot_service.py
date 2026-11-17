"""
Zalo Bot Background Service for JDL Dashboard
Lắng nghe các tin nhắn/lệnh từ Nhóm Zalo hoặc chat 1-1 (qua polling getUpdates)
Hỗ trợ lệnh:
  - /backlog hoặc backlog: Quét và gửi danh sách chi tiết các đơn sắp quá hạn SLA
  - /ping hoặc test: Kiểm tra trạng thái hoạt động của Bot
"""

import time
import json
import os
import sys
import re
import socket
import requests
import datetime
import threading
import zalo_notifier

VN_TZ = zalo_notifier.VN_TZ

_LOCK_SOCKET = None

def get_gemini_api_key() -> str:
    """Đọc Gemini API Key từ secrets.toml hoặc biến môi trường."""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "gemini" in st.secrets:
            return st.secrets["gemini"].get("api_key", "").strip()
    except Exception:
        pass

    sec_file = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
    if os.path.exists(sec_file):
        try:
            with open(sec_file, "r", encoding="utf-8") as f:
                content = f.read()
                m = re.search(r'\[gemini\]\s*[\r\n]+(?:[^\n]*\n)*?api_key\s*=\s*["\']([^"\']+)["\']', content)
                if m:
                    return m.group(1).strip()
        except Exception:
            pass

    return os.environ.get("GEMINI_API_KEY", "").strip()


def call_gemini_ai(prompt: str, user_name: str = "") -> str:
    """Gọi Gemini API (bản Free) để phản hồi các cuộc trò chuyện thông thường."""
    api_key = get_gemini_api_key()
    if not api_key:
        return (
            f"Chào {user_name or 'bạn'}! Tôi là Bot Trợ lý Kho Vận JDL (JingDong Logistics).\n\n"
            f"Hiện tại tôi hỗ trợ các lệnh tra cứu số liệu kho tự động:\n"
            f"- backlog : Xem danh sách đơn sắp quá hạn SLA\n"
            f"- packing : Xem tiến độ đóng gói theo nhóm khách hàng\n"
            f"- etc / clear : Dự báo thời gian hoàn thành (Clear Time)\n\n"
            f"👉 Để kích hoạt tính năng trò chuyện AI tự do với Gemini, vui lòng bổ sung gemini.api_key vào file cấu hình."
        )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
    sys_instruction = (
        "Bạn là Trợ lý AI Kho Vận JDL (JingDong Logistics). "
        "Hãy trả lời ngắn gọn, lịch sự, đúng trọng tâm và chuyên nghiệp bằng tiếng Việt (tối đa 2-3 câu hoặc 1 đoạn ngắn). "
        "Không dùng icon hoặc ký tự trang trí rườm rà. "
        "Nếu người dùng muốn xem số liệu kho thời gian thực, hãy hướng dẫn họ gõ: 'backlog', 'packing', hoặc 'etc'."
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": sys_instruction}]},
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 500
        }
    }
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()

        # Fallback thử gemini-1.5-flash
        url_fb = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
        r_fb = requests.post(url_fb, json=payload, headers=headers, timeout=12)
        if r_fb.status_code == 200:
            data = r_fb.json()
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()

        return "Xin lỗi, hiện tại tôi chưa thể kết nối đến máy chủ AI. Bạn hãy thử lại sau ít phút nhé."
    except Exception as e:
        return f"Xin lỗi, đã xảy ra lỗi khi kết nối trợ lý AI: {e}"

def acquire_singleton_lock(port: int = 54321) -> bool:
    """Đảm bảo chỉ có DUY NHẤT 1 tiến trình Bot Zalo chạy trên máy để không bị xung đột getUpdates."""
    global _LOCK_SOCKET
    try:
        _LOCK_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _LOCK_SOCKET.bind(("127.0.0.1", port))
        return True
    except socket.error:
        return False


def start_bot_listener(stop_event: threading.Event = None):
    if not acquire_singleton_lock():
        print("⚠️ [Zalo Bot Service] Đã có một tiến trình Bot khác đang chạy ngầm trên máy. Không chạy trùng lặp.", flush=True)
        return

    cfg = zalo_notifier.get_zalo_config()
    bot_token = cfg.get("bot_token")
    if not bot_token:
        print("❌ [Zalo Bot] Chưa cấu hình bot_token trong secrets.toml hoặc biến môi trường.", flush=True)
        return

    url_updates = f"https://bot-api.zaloplatforms.com/bot{bot_token}/getUpdates"
    url_send = f"https://bot-api.zaloplatforms.com/bot{bot_token}/sendMessage"

    print("🚀 [Zalo Bot Service] Đang khởi động trình lắng nghe lệnh Zalo...", flush=True)
    print(f"⏰ Thời gian bắt đầu: {datetime.datetime.now(VN_TZ).strftime('%H:%M:%S %d/%m/%Y')}", flush=True)
    print("👉 Hỗ trợ lệnh: backlog, packing, etc / clear, ping (kiểm tra bot)\n", flush=True)

    last_offset = 0

    while True:
        if stop_event and stop_event.is_set():
            print("🛑 [Zalo Bot Service] Nhận tín hiệu dừng.")
            break

        try:
            payload = {"timeout": 15}
            if last_offset > 0:
                payload["offset"] = last_offset

            res = requests.post(url_updates, json=payload, headers={"Content-Type": "application/json"}, timeout=20)
            
            if res.status_code == 200:
                data = res.json()
                # 408 là long-polling timeout bình thường khi không có tin mới
                if data.get("error_code") == 408:
                    continue

                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    if "result" in data and isinstance(data["result"], list):
                        items = data["result"]
                    elif "result" in data and isinstance(data["result"], dict):
                        items = [data["result"]]
                    elif "update_id" in data or "message" in data:
                        items = [data]

                for item in items:
                    up_id = item.get("update_id")
                    if up_id:
                        last_offset = up_id + 1

                    msg = item.get("message") or item.get("event") or item
                    chat = msg.get("chat", {})
                    chat_id = str(chat.get("id") or msg.get("chat_id") or "")
                    sender = msg.get("from", {}).get("display_name") or msg.get("from", {}).get("first_name", "User")
                    raw_text = msg.get("text", "").strip()

                    if not raw_text or not chat_id:
                        continue

                    text_lower = raw_text.lower()
                    print(f"📩 [{sender}] @ chat {chat_id}: '{raw_text}'", flush=True)

                    is_group = chat_id.startswith("zgr-")
                    is_mentioned = any(x in text_lower for x in ["@bot", "@jingdong", "@jdl"])
                    is_reply_to_bot = bool(msg.get("reply_to_message"))
                    has_command = any(c in text_lower for c in ["backlog", "packing", "dong goi", "etc", "clear", "ping", "test"])

                    # Trong nhóm chat: chỉ xử lý nếu được tag tên, được reply tin bot, hoặc chứa lệnh trực tiếp
                    if is_group and not (is_mentioned or is_reply_to_bot or has_command):
                        continue

                    # 1. Lệnh backlog
                    if "backlog" in text_lower:
                        print(f"  ⚡ Xử lý lệnh backlog cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_backlog_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả backlog: {ok} - {result_info}", flush=True)

                    # 2. Lệnh packing
                    elif "packing" in text_lower or "dong goi" in text_lower:
                        print(f"  ⚡ Xử lý lệnh packing cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_packing_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả packing: {ok} - {result_info}", flush=True)

                    # 3. Lệnh etc hoặc clear (Dự báo thời gian clear backlog)
                    elif "etc" in text_lower or "clear" in text_lower:
                        print(f"  ⚡ Xử lý lệnh etc/clear cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_etc_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả etc/clear: {ok} - {result_info}", flush=True)

                    # 4. Lệnh ping hoặc test
                    elif "ping" in text_lower or "test" in text_lower:
                        ping_reply = (
                            f"Bot JingDongLogistics đang hoạt động bình thường!\n"
                            f"Thời gian: {datetime.datetime.now(VN_TZ).strftime('%H:%M %d/%m/%Y')}\n\n"
                            f"Các câu lệnh khả dụng:\n"
                            f"- backlog : Xem danh sách đơn hàng sắp quá hạn SLA (Khách hàng -> Time remaining)\n"
                            f"- packing : Xem tiến độ đóng gói hôm nay (phân loại B2B vs B2C)\n"
                            f"- etc / clear : Dự báo thời gian hoàn thành (Clear Time) dựa trên tốc độ đóng gói thực tế"
                        )
                        requests.post(url_send, json={"chat_id": chat_id, "text": ping_reply}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 5. Các câu hỏi trò chuyện thông thường khác -> Chuyển sang Gemini AI
                    else:
                        clean_prompt = re.sub(r'@\S+', '', raw_text).strip()
                        if clean_prompt:
                            print(f"  🤖 Gửi câu hỏi cho Gemini AI: '{clean_prompt}'...", flush=True)
                            ai_reply = call_gemini_ai(clean_prompt, user_name=sender)
                            requests.post(url_send, json={"chat_id": chat_id, "text": ai_reply}, headers={"Content-Type": "application/json"}, timeout=15)

            else:
                print(f"⚠️ [Zalo Bot] HTTP {res.status_code}: {res.text[:150]}")
                time.sleep(3)

        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            print(f"⚠️ [Zalo Bot] Exception: {e}")
            time.sleep(2)


if __name__ == "__main__":
    start_bot_listener()
