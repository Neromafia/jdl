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

_WAREHOUSE_CONTEXT_CACHE = {
    "text": "",
    "timestamp": 0.0
}

def get_warehouse_realtime_context(ttl_sec: float = 60.0) -> str:
    """Lấy bản tóm tắt số liệu kho bãi thực tế (Backlog, SLA, Packing) kèm cơ chế cache 60s."""
    global _WAREHOUSE_CONTEXT_CACHE
    now_ts = time.time()
    if _WAREHOUSE_CONTEXT_CACHE["text"] and (now_ts - _WAREHOUSE_CONTEXT_CACHE["timestamp"]) < ttl_sec:
        return _WAREHOUSE_CONTEXT_CACHE["text"]

    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        import pandas as pd

        import sync_wms_local
        creds = sync_wms_local.get_credentials()
        client = bigquery.Client(credentials=creds, project="jingdong-505914")

        now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
        today_str = now_vn.strftime('%Y-%m-%d')

        # 1. Backlog summary
        sql_bl = """
        SELECT owner_name, COUNT(DISTINCT order_no) as orders
        FROM `jingdong-505914.wms_data.fact_backlog`
        GROUP BY owner_name
        ORDER BY orders DESC
        """
        df_bl = client.query(sql_bl).to_dataframe()

        # 2. SLA risk orders
        sql_risk = """
        SELECT order_no, owner_name, production_end_time
        FROM `jingdong-505914.wms_data.fact_backlog`
        WHERE production_end_time IS NOT NULL
        """
        df_risk_raw = client.query(sql_risk).to_dataframe()
        df_risk_raw['prod_dt'] = pd.to_datetime(df_risk_raw['production_end_time'], errors='coerce')
        df_risk_today = df_risk_raw[df_risk_raw['prod_dt'].dt.date == now_vn.date()].copy()
        df_risk_today['diff_sec'] = (df_risk_today['prod_dt'] - now_vn).dt.total_seconds()
        df_urgent = df_risk_today[df_risk_today['diff_sec'] <= 2.5 * 3600].copy()

        # 3. Packing summary today
        sql_pk = f"""
        SELECT owner_name, COUNT(DISTINCT order_no) as orders, SUM(SAFE_CAST(packing_qty AS INT64)) as pcs
        FROM `jingdong-505914.wms_data.fact_packing`
        WHERE DATE(pack_time) = '{today_str}'
        GROUP BY owner_name
        ORDER BY orders DESC
        """
        df_pk = client.query(sql_pk).to_dataframe()

        # 4. Order intake summary today
        sql_ord = f"""
        SELECT owner_name, customer_type, SUM(total_orders) as total_orders
        FROM `jingdong-505914.wms_data.fact_order`
        WHERE DATE(order_date) = '{today_str}'
        GROUP BY owner_name, customer_type
        ORDER BY total_orders DESC
        """
        df_ord = client.query(sql_ord).to_dataframe()

        tot_ord_orders = int(df_ord['total_orders'].sum()) if not df_ord.empty else 0
        tot_bl_orders = int(df_bl['orders'].sum()) if not df_bl.empty else 0
        tot_pk_orders = int(df_pk['orders'].sum()) if not df_pk.empty else 0
        tot_pk_pcs = int(df_pk['pcs'].sum()) if not df_pk.empty and 'pcs' in df_pk.columns and pd.notnull(df_pk['pcs'].sum()) else 0

        lines = [
            f"DỮ LIỆU THỜI GIAN THỰC KHO JDL (CẬP NHẬT LÚC {now_vn.strftime('%H:%M:%S %d/%m/%Y')}):",
            f"1. ĐƠN HÀNG KHÁCH ĐỔ XUỐNG HỆ THỐNG HÔM NAY (ORDER): Tổng cộng {tot_ord_orders:,} đơn."
        ]
        if not df_ord.empty:
            lines.append("Số lượng đơn nhận theo từng khách hàng:")
            for _, r in df_ord.iterrows():
                if pd.notnull(r['owner_name']):
                    lines.append(f"- {r['owner_name']} ({r.get('customer_type', 'B2C')}): {int(r['total_orders']):,} đơn")

        lines.extend([
            f"\n2. TỒN CHỜ ĐÓNG GÓI (BACKLOG): Tổng cộng {tot_bl_orders:,} đơn.",
            "Số lượng đơn tồn theo từng khách hàng:"
        ])
        for _, r in df_bl.iterrows():
            if pd.notnull(r['owner_name']):
                lines.append(f"- {r['owner_name']}: {int(r['orders']):,} đơn tồn")

        if not df_urgent.empty:
            lines.append("\nCác đơn hàng có nguy cơ trễ SLA (quá hạn hoặc cận hạn <= 2.5 tiếng):")
            for _, r in df_urgent.iterrows():
                d_sec = r['diff_sec']
                h = int(abs(d_sec) // 3600)
                m = int((abs(d_sec) % 3600) // 60)
                status = f"ĐÃ TRỄ {h}h{m:02d}p" if d_sec < 0 else f"còn {h}h{m:02d}p"
                dt_str = str(r['production_end_time'])[11:16]
                lines.append(f"- {r['owner_name']} | Đơn {r['order_no']} | Hạn: {dt_str} ({status})")

        lines.append(f"\n2. TIẾN ĐỘ ĐÓNG GÓI HÔM NAY (PACKING): Đã đóng {tot_pk_orders:,} đơn ({tot_pk_pcs:,} pcs).")
        lines.append("Sản lượng đã đóng hôm nay theo từng khách hàng:")
        for _, r in df_pk.iterrows():
            if pd.notnull(r['owner_name']):
                lines.append(f"- {r['owner_name']}: {int(r['orders']):,} đơn đã đóng")

        context_str = "\n".join(lines)
        _WAREHOUSE_CONTEXT_CACHE["text"] = context_str
        _WAREHOUSE_CONTEXT_CACHE["timestamp"] = now_ts
        return context_str

    except Exception as e:
        return f"(Không thể tải dữ liệu realtime kho: {e})"


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
    """Gọi Gemini AI kết hợp ngữ cảnh dữ liệu thực tế kho JDL để trả lời thông minh bất kỳ câu hỏi nào."""
    api_key = get_gemini_api_key()
    if not api_key:
        return (
            f"Chào {user_name or 'bạn'}! Tôi là Bot Trợ lý Kho Vận JDL (JingDong Logistics).\n\n"
            f"Hiện tại tôi hỗ trợ các lệnh tra cứu số liệu kho tự động:\n"
            f"- order : Xem tổng đơn nhận và phân loại B2B vs B2C\n"
            f"- backlog : Xem danh sách đơn sắp quá hạn SLA\n"
            f"- packing : Xem tiến độ đóng gói theo nhóm khách hàng\n"
            f"- etc / clear : Dự báo thời gian hoàn thành (Clear Time)\n\n"
            f"👉 Để kích hoạt tính năng trò chuyện AI tự do với Gemini, vui lòng bổ sung gemini.api_key vào file cấu hình."
        )

    # Lấy dữ liệu kho thời gian thực để cấp cho AI
    warehouse_ctx = get_warehouse_realtime_context()

    models_to_try = ["gemini-3.5-flash-lite", "gemini-3.8-flash", "gemini-flash-latest"]
    sys_instruction = (
        "Bạn là Trợ lý AI Kho Vận JDL (JingDong Logistics). "
        "Dưới đây là DỮ LIỆU THỜI GIAN THỰC ĐANG DIỄN RA TẠI KHO JDL HÔM NAY:\n"
        f"{warehouse_ctx}\n\n"
        "HƯỚNG DẪN TRẢ LỜI:\n"
        "1. Trả lời câu hỏi của người dùng một cách trực tiếp, tự nhiên, chính xác, ngắn gọn (1-3 câu), bằng tiếng Việt.\n"
        "2. TUYỆT ĐỐI KHÔNG bắt người dùng phải gõ lệnh 'backlog' hay 'packing' vì bạn ĐÃ CÓ TOÀN BỘ SỐ LIỆU THỰC TẾ Ở TRÊN.\n"
        "3. Nếu người dùng hỏi về khách hàng/đối tác cụ thể (ví dụ: Chint, Chicmax, Jinhe, Mira-Lotus, Pop Mart...): "
        "Hãy tra cứu đúng tên khách hàng trong dữ liệu trên và trả lời chính xác số đơn tồn, đơn đã đóng, hoặc các đơn sắp trễ SLA (nếu có).\n"
        "4. Nếu người dùng hỏi các câu hỏi chung (chào hỏi, quy trình, nhân sự...), hãy trả lời thân thiện, lịch thiệp và hữu ích."
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": sys_instruction}]},
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 400
        }
    }
    headers = {"Content-Type": "application/json"}

    for model in models_to_try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=12)
            if r.status_code == 200:
                data = r.json()
                candidates = data.get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    if parts:
                        return parts[0].get("text", "").strip()
        except Exception:
            continue

    return "Xin lỗi, hiện tại tôi chưa thể kết nối đến máy chủ AI. Bạn hãy thử lại sau ít phút nhé."

def try_schedule_reminder(raw_text: str, sender: str, chat_id: str, bot_token: str) -> bool:
    """Tự động nhận diện yêu cầu hẹn giờ/nhắc nhở (cho chính mình hoặc nhắn vào group nhắc người khác)."""
    text = raw_text
    clean = re.sub(r'@bot\w*', '', text, flags=re.IGNORECASE).strip()
    clean_lower = clean.lower()

    keywords = ["nhắc", "nhac", "kêu", "keu", "gọi", "goi", "báo thức", "bao thuc", "hẹn", "hen", "đánh thức", "danh thuc"]
    if not any(k in clean_lower for k in keywords):
        return False

    now_vn = datetime.datetime.now(VN_TZ).replace(tzinfo=None)
    delay_sec = None
    target_label = ""

    # Dạng 1: "sau X phút" / "sau X tiếng"
    m_after = re.search(r'sau\s*(\d+)\s*(phút|tiếng|h|p)', clean_lower)
    if m_after:
        val = int(m_after.group(1))
        unit = m_after.group(2)
        if unit in ["tiếng", "h"]:
            delay_sec = val * 3600
            target_label = f"sau {val} tiếng"
        else:
            delay_sec = val * 60
            target_label = f"sau {val} phút"

    # Dạng 2: "lúc 15h10", "15h10", "15:10"
    if delay_sec is None:
        m_time = re.search(r'(?:lúc|luc)?\s*(\d{1,2})[h:g](\d{1,2})?', clean_lower)
        if m_time:
            hour = int(m_time.group(1))
            minute = int(m_time.group(2)) if m_time.group(2) else 0
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                target_dt = now_vn.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if target_dt > now_vn:
                    delay_sec = (target_dt - now_vn).total_seconds()
                    target_label = f"lúc {hour:02d}h{minute:02d}"

    if not (delay_sec and 0 < delay_sec <= 43200):
        return False

    # 1. Phát hiện gửi vào nhóm
    send_to_group = bool(re.search(r'\b(?:vào|trong|len|lên)\s*(?:group|nhóm|nhom|grp)\b', clean_lower))

    # 2. Bóc tách người được nhắc và hành động cần làm
    stripped = re.sub(r'\b(?:nhắn|nhan|bắn|gui|gửi)\s*(?:vào|trong)?\s*(?:group|nhóm|nhom|grp)\b', '', clean, flags=re.IGNORECASE)
    stripped = re.sub(r'\b(?:vào|trong)\s*(?:group|nhóm|nhom|grp)\b', '', stripped, flags=re.IGNORECASE)
    stripped = re.sub(r'sau\s*\d+\s*(?:phút|tiếng|h|p)\s*(?:nữa|nua)?', '', stripped, flags=re.IGNORECASE)
    stripped = re.sub(r'(?:lúc|luc)?\s*\d{1,2}[h:g]\d{0,2}', '', stripped, flags=re.IGNORECASE)
    stripped = re.sub(r'\s+', ' ', stripped).strip()

    m_rem = re.search(r'(?:nhắc|nhac|kêu|keu|gọi|goi|hẹn|hen)\s+(.+)', stripped, flags=re.IGNORECASE)
    target_person = sender
    action = ""
    is_self = True

    if m_rem:
        rem_content = m_rem.group(1).strip()
        rem_content = re.sub(r'\s*(?:nhé|nha|giúp|giup|nhe|với|voi)$', '', rem_content, flags=re.IGNORECASE).strip()

        m_self = re.match(r'^(?:cho\s+)?(?:mình|minh|tôi|toi|tao|tớ|to|em)\b', rem_content, flags=re.IGNORECASE)
        if m_self:
            is_self = True
            action = rem_content[m_self.end():].strip()
        else:
            is_self = False
            m_target = re.match(r'^((?:bạn|ban|anh|chị|chi|em|nhân viên|nhan vien|@)\s*[^\s]+|[A-Za-zÀ-ỹ0-9_]+)\s*(.*)', rem_content, flags=re.IGNORECASE)
            if m_target:
                target_person = m_target.group(1).strip()
                action = m_target.group(2).strip()
            else:
                target_person = rem_content
                action = ""

    # Nếu người dùng nhắn riêng và nhắc người khác, mặc định gửi vào group chat của kho
    cfg = zalo_notifier.get_zalo_config()
    group_chat_id = cfg.get("chat_id", "")
    if not is_self:
        send_to_group = True

    dest_chat_id = group_chat_id if (send_to_group and group_chat_id) else chat_id
    url_send = f"https://bot-api.zaloplatforms.com/bot{bot_token}/sendMessage"

    # Xây dựng nội dung lời nhắc tự nhiên
    if is_self:
        alert_msg = f"⏰ [NHẮC NHỞ] {sender} ơi, đã đến giờ hẹn rồi nhé ({target_label})! Hết giờ giải lao rồi, quay lại tiếp tục ca làm việc thôi nào!"
        ack_msg = f"Dạ nhận lệnh! Đúng {target_label} mình sẽ nhắn nhắc {sender} nhé, chúc bạn giải lao vui vẻ!"
    else:
        # Nhắn tự nhiên, bình thường, không kèm 'từ quản lý'
        if action:
            alert_msg = f"⏰ [NHẮC NHỞ] {target_person} ơi, đã đến giờ {action} rồi nhé! Bạn kiểm tra và xử lý giúp mình nha."
        else:
            alert_msg = f"⏰ [NHẮC NHỞ] {target_person} ơi, đã đến giờ hẹn rồi nhé! Bạn kiểm tra lại công việc giúp mình nha."

        action_desc = f": '{action}'" if action else ""
        if dest_chat_id == group_chat_id and chat_id != group_chat_id:
            ack_msg = f"Dạ nhận lệnh! Đúng {target_label} em sẽ gửi tin nhắn vào nhóm kho để nhắc {target_person}{action_desc} nhé."
        else:
            ack_msg = f"Dạ nhận lệnh! Đúng {target_label} em sẽ nhắn nhắc {target_person}{action_desc} nhé."

    def _reminder_task():
        try:
            requests.post(url_send, json={"chat_id": dest_chat_id, "text": alert_msg}, headers={"Content-Type": "application/json"}, timeout=15)
        except Exception:
            pass

    t = threading.Timer(delay_sec, _reminder_task)
    t.daemon = True
    t.start()

    requests.post(url_send, json={"chat_id": chat_id, "text": ack_msg}, headers={"Content-Type": "application/json"}, timeout=15)
    return True


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
                    sender_id = str(msg.get("from", {}).get("id") or "")
                    msg_id = str(msg.get("message_id") or "")
                    raw_text = msg.get("text", "").strip()

                    if not raw_text or not chat_id:
                        continue

                    text_lower = raw_text.lower()
                    print(f"📩 [{sender}] @ chat {chat_id}: '{raw_text}'", flush=True)
                    print(f"   DEBUG RAW MSG: {json.dumps(msg, ensure_ascii=False)}", flush=True)

                    is_group = chat_id.startswith("zgr-")
                    is_mentioned = any(x in text_lower for x in ["@bot", "@jingdong", "@jdl"])
                    is_reply_to_bot = bool(msg.get("reply_to_message"))
                    ot_params = zalo_notifier.parse_ot_query(raw_text)
                    pop_mat_params = zalo_notifier.parse_pop_material_query(raw_text)
                    has_command = bool(ot_params) or bool(pop_mat_params) or any(c in text_lower for c in [
                        "order", "don hang", "backlog", "packing", "dong goi", "etc", "clear", "ping", "test",
                        "bảng sản lượng", "bang san luong", "sản lượng nhân sự", "san luong nhan su",
                        "sản lượng nhân viên", "san luong nhan vien"
                    ])

                    # Trong nhóm chat: chỉ xử lý nếu được tag tên, được reply tin bot, hoặc chứa lệnh trực tiếp
                    if is_group and not (is_mentioned or is_reply_to_bot or has_command):
                        continue

                    # 0a. Ưu tiên nhận diện câu lệnh tra cứu vật tư đóng gói Pop Mart (VD: @bot vật tư pop ngày 17/11)
                    if pop_mat_params:
                        print(f"  📦 Xử lý truy vấn vật tư Pop Mart ({pop_mat_params['label']})...", flush=True)
                        ok, result_info = zalo_notifier.handle_pop_material_query(raw_text, target_chat_id=chat_id)
                        print(f"  ✅ Kết quả vật tư Pop Mart: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể tra cứu vật tư Pop Mart: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 0b. Nhận diện câu lệnh tra cứu giờ công & OT (VD: @bot OT ngày 17/11, từ 1/11 đến 17/11, tháng 11)
                    elif ot_params:
                        print(f"  🕒 Xử lý truy vấn OT ({ot_params['label']})...", flush=True)
                        ok, result_info = zalo_notifier.handle_ot_query(raw_text, target_chat_id=chat_id)
                        print(f"  ✅ Kết quả OT: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể tra cứu giờ công/OT: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 0c. Nhận diện câu hỏi tự nhiên về sản lượng nhân sự (Pick / Pack / Put Away / Receiving) theo ngày & khung giờ
                    elif (prod_params := zalo_notifier.parse_productivity_query(raw_text)):
                        print(f"  📊 Xử lý truy vấn sản lượng nhân sự ({prod_params['stages']} | {prod_params['time_label']} {prod_params['target_date']})...", flush=True)
                        ok, result_info = zalo_notifier.handle_productivity_query(raw_text, target_chat_id=chat_id)
                        print(f"  ✅ Kết quả sản lượng nhân sự: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể tra cứu sản lượng nhân sự: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 1. Lệnh order (Đơn hàng khách đổ xuống hệ thống)
                    elif "order" in text_lower or "don hang" in text_lower:
                        print(f"  ⚡ Xử lý lệnh order cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_order_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả order: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể quét đơn order: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 2. Lệnh backlog
                    elif "backlog" in text_lower:
                        print(f"  ⚡ Xử lý lệnh backlog cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_backlog_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả backlog: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể quét backlog: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 3. Lệnh packing
                    elif "packing" in text_lower or "dong goi" in text_lower:
                        print(f"  ⚡ Xử lý lệnh packing cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_packing_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả packing: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể quét packing: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 4. Lệnh etc hoặc clear (Dự báo thời gian clear backlog)
                    elif "etc" in text_lower or "clear" in text_lower:
                        print(f"  ⚡ Xử lý lệnh etc/clear cho chat {chat_id}...", flush=True)
                        ok, result_info = zalo_notifier.handle_etc_command(target_chat_id=chat_id)
                        print(f"  ✅ Kết quả etc/clear: {ok} - {result_info}", flush=True)
                        if not ok:
                            requests.post(url_send, json={"chat_id": chat_id, "text": f"⚠️ Không thể tính ETC: {result_info}"}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 5. Lệnh ping hoặc test
                    elif "ping" in text_lower or "test" in text_lower:
                        ping_reply = (
                            f"Bot JingDongLogistics đang hoạt động bình thường!\n"
                            f"Thời gian: {datetime.datetime.now(VN_TZ).strftime('%H:%M %d/%m/%Y')}\n\n"
                            f"Các câu lệnh khả dụng:\n"
                            f"- order : Xem tổng đơn nhận khách đổ xuống hôm nay (phân loại B2B vs B2C)\n"
                            f"- backlog : Xem danh sách đơn hàng sắp quá hạn SLA (Khách hàng -> Time remaining)\n"
                            f"- packing : Xem tiến độ đóng gói hôm nay (phân loại B2B vs B2C)\n"
                            f"- etc / clear : Dự báo thời gian hoàn thành (Clear Time) dựa trên tốc độ đóng gói thực tế\n"
                            f"- OT : Tra cứu giờ công, OT (>8h) & lỗi nghiêm trọng (VD: '@bot OT ngày 17/11', '@bot OT từ 1/11 đến 17/11', '@bot OT tháng 11')\n"
                            f"- Vật tư Pop Mart : Tra cứu vật tư đề xuất vs thực tế (VD: '@bot vật tư pop ngày 17/11', '@bot vật tư pop 16 17 tháng 11', '@bot vật tư pop tháng 11')\n"
                            f"- Hỏi tự nhiên sản lượng nhân sự: VD 'cho tôi bảng sản lượng nhân sự pick và pack từ 17h đến 17h30 ngày 17/11'"
                        )
                        requests.post(url_send, json={"chat_id": chat_id, "text": ping_reply}, headers={"Content-Type": "application/json"}, timeout=10)

                    # 5. Kiểm tra yêu cầu hẹn giờ / nhắc nhở
                    elif try_schedule_reminder(raw_text, sender, chat_id, bot_token):
                        print(f"  ⏰ Đã lên lịch nhắc nhở cho [{sender}] tại chat {chat_id}", flush=True)

                    # 6. Các câu hỏi trò chuyện thông thường khác -> Chuyển sang Gemini AI
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
