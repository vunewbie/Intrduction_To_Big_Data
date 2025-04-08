from datetime import datetime, timedelta

DATE_FORMAT = "%m-%d-%y"

input_file = "asr.csv"
dates_set = set()
categories_set = set()

with open(input_file, 'r', encoding='utf-8') as file:
    next(file)  # Bỏ dòng tiêu đề
    for line in file:
        parts = line.strip().split(',')

        date_str = parts[2].strip()
        category = parts[9].strip()

        date_obj = datetime.strptime(date_str, DATE_FORMAT)

        dates_set.add(date_obj)
        categories_set.add(category)

# Tìm min - max
min_date = min(dates_set)
max_date = max(dates_set) + timedelta(days=2)

# Sinh toàn bộ các ngày từ min đến max
with open("dates.txt", "w", encoding='utf-8') as f_dates:
    current = min_date
    while current <= max_date:
        f_dates.write(current.strftime(DATE_FORMAT) + "\n")
        current += timedelta(days=1)

# Ghi danh sách category
with open("categories.txt", "w", encoding='utf-8') as f_cate:
    for cate in sorted(categories_set):
        f_cate.write(cate + "\n")