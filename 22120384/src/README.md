# 📘 README – Hướng dẫn sử dụng

## 🔧 Lưu ý cho Task_2.1:

- Cần chạy file `getDatesAndCategories.py` trước để tạo được 2 file: `categories.txt` và `dates.txt`.
- Khi chạy file `MapReduce - 22120384.py`, cần đổi lại **đường dẫn** tới 2 file `categories.txt` và `dates.txt` trong hàm `mapper_init`.  
  *(Lưu ý: Trước đó sử dụng đường dẫn trong môi trường Ubuntu để chạy thử.)*

---

## 🔥 Cách chạy và Lưu ý cho Task_2.2 và Task_2.3:

- Thay đổi đường dẫn trong đoạn mã:
  ```python
  lines = sc.textFile("file:///D:/Intrduction_To_Big_Data/22120384/src/Task_2.2/asr.csv")
  ```
  sao cho **phù hợp với đường dẫn tới file trên máy**.

- Khi nhấn **Run All**, đôi lúc sẽ gặp lỗi do **Spark chưa được khởi tạo thành công** nhưng đã chạy sang câu lệnh khác.  
  👉 Trường hợp này, chỉ cần **chạy lại các đoạn script ở dưới phần khai báo Spark** là được.
