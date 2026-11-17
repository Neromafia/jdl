# JDL Warehouse Management Dashboard & 24/7 Zalo Bot

Hệ thống Quản lý Vận hành Kho Vận JDL (JingDong Logistics) tích hợp đồng bộ tự động Google Drive → BigQuery và Trợ lý ảo Zalo Bot 24/7.

## 🚀 Các tính năng chính

1. **Dashboard Giám sát Vận hành (Streamlit)**:
   - Theo dõi toàn diện các luồng: Inbound (Unloading, Receiving, Put Away), Outbound (Picking, Packing, Handover), Inventory, và Backlog.
   - Trực quan hóa tiến độ hoàn thành, năng suất nhân viên và phân loại khách hàng (B2B vs B2C).

2. **Đồng bộ tự động Google Drive → BigQuery (`sync_wms_local.py`)**:
   - Quét định kỳ mỗi 60 giây các thư mục báo cáo WMS trên Google Drive.
   - Phân tích cú pháp ngày, kiểm tra trùng lặp qua MD5 hash và nạp dữ liệu sạch vào Google BigQuery.

3. **Trợ lý kho AI & Bot Zalo 24/7 (`zalo_bot_service.py` & `zalo_notifier.py`)**:
   - **Tự động cảnh báo**: Bắn thông báo vào Nhóm Chat Zalo khi có đơn hàng cận hạn hoặc quá hạn SLA (`<= 2.5 tiếng`), và tiến độ đóng gói theo ca.
   - **Tra cứu thời gian thực**: Hỗ trợ các lệnh:
     - `backlog`: Danh sách chi tiết các đơn hàng có nguy cơ quá hạn SLA.
     - `packing`: Thống kê sản lượng đóng gói hôm nay (phân loại B2B / B2C).
     - `etc` / `clear`: Dự báo thời gian hoàn tất đơn hàng theo tốc độ đóng gói thực tế.
     - `ping`: Kiểm tra trạng thái hoạt động của Bot.
   - **Trò chuyện thông minh (Google Gemini AI)**: Trả lời tự nhiên các câu hỏi của nhân viên về số liệu tồn kho realtime.
   - **Hẹn giờ giải lao / nhắc việc**: Tự động nhận diện yêu cầu nhắc nhở theo mốc giờ hoặc sau X phút.

## ☁️ Triển khai Cloud (Railway / Render / Docker)

- Đã cấu hình sẵn `Dockerfile` và `docker-compose.yml`.
- Hỗ trợ nạp credentials qua biến môi trường:
  - `GCP_SERVICE_ACCOUNT_JSON`
  - `ZALO_BOT_TOKEN`
  - `ZALO_CHAT_ID`
  - `GEMINI_API_KEY`
