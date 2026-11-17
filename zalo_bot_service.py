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
import requests
import datetime
import threading
import zalo_notifier

VN_TZ = zalo_notifier.VN_TZ

def start_bot_listener(stop_event: threading.Event = None):
    cfg = zalo_notifier.get_zalo_config()
    bot_token = cfg.get("bot_token")
    if not bot_token:
        print("❌ [Zalo Bot] Chưa cấu hình bot_token trong secrets.toml hoặc biến môi trường.")
        return

    url_updates = f"https://bot-api.zaloplatforms.com/bot{bot_token}/getUpdates"
    url_send = f"https://bot-api.zaloplatforms.com/bot{bot_token}/sendMessage"

    print("🚀 [Zalo Bot Service] Đang khởi động trình lắng nghe lệnh Zalo...", flush=True)
    print(f"⏰ Thời gian bắt đầu: {datetime.datetime.now(VN_TZ).strftime('%H:%M:%S %d/%m/%Y')}", flush=True)
    print("👉 Hỗ trợ lệnh: /backlog (xem đơn sắp trễ), /ping (kiểm tra bot)\n", flush=True)

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

                    # 1. Lệnh /backlog
                    if "/backlog" in text_lower or text_lower.endswith("backlog") or "backlog" in text_lower:
                        print(f"  ⚡ Xử lý lệnh /backlog cho chat {chat_id}...", flush=True)
                        requests.post(
                            url_send,
                            json={"chat_id": chat_id, "text": "⏳ Đang quét dữ liệu Backlog WMS mới nhất, vui lòng chờ trong giây lát..."},
                            headers={"Content-Type": "application/json"},
                            timeout=10
                        )
                        ok, result_info = zalo_notifier.handle_backlog_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả /backlog: {ok} - {result_info}", flush=True)

                    # 2. Lệnh /packing
                    elif "/packing" in text_lower or text_lower.endswith("packing"):
                        print(f"  ⚡ Xử lý lệnh /packing cho chat {chat_id}...", flush=True)
                        requests.post(
                            url_send,
                            json={"chat_id": chat_id, "text": "⏳ Đang tổng hợp báo cáo tiến độ Đóng Gói hôm nay..."},
                            headers={"Content-Type": "application/json"},
                            timeout=10
                        )
                        ok, result_info = zalo_notifier.handle_packing_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả /packing: {ok} - {result_info}", flush=True)

                    # 3. Lệnh /ping hoặc test
                    elif text_lower in ["/ping", "ping", "test", "/test"] or "test" in text_lower:
                        ping_reply = (
                            f"Bot JingDongLogistics đang hoạt động bình thường!\n"
                            f"Thời gian: {datetime.datetime.now(VN_TZ).strftime('%H:%M %d/%m/%Y')}\n\n"
                            f"Các câu lệnh khả dụng:\n"
                            f"- /backlog : Xem danh sách đơn hàng sắp quá hạn SLA (Khách hàng -> Time remaining)\n"
                            f"- /packing : Xem tiến độ đóng gói hôm nay (phân loại B2B vs B2C)"
                        )
                        requests.post(url_send, json={"chat_id": chat_id, "text": ping_reply}, headers={"Content-Type": "application/json"}, timeout=10)

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
