from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import csv
from datetime import datetime, timedelta

class calculateRevenue(MRJob):
        OUTPUT_PROTOCOL = RawValueProtocol
        def mapper(self, _, line):
                if "index" in line:  # Không tính dòng đầu tiên
                        return

                columns = line.strip().split(",")
                date = columns[2].strip() # Thuộc tính Date
                category = columns[9].strip() # Thuộc tính Category
                amount = columns[15].strip() # Thuộc tính Amount
                if amount == "":
                        amount = 0

                # Với mỗi dòng, phát ra 3 cặp key – value
                # Mỗi key là tuple (window_date, category) ứng với cửa sổ trượt 3 ngày
                DATE_FORMAT_INPUT = "%m-%d-%y"
                DATE_FORMAT_OUTPUT = "%d/%m/%Y"

                date_obj = datetime.strptime(date, DATE_FORMAT_INPUT)

                for offset in range(3):
                        window_date = date_obj + timedelta(days=offset)
                        window_date_str = window_date.strftime(DATE_FORMAT_OUTPUT)
                        yield (window_date_str, category), float(amount)
            
        def reducer(self, keys, values):
                total_revenue = sum(values)
                date, category = keys
                # Xuất kết quả theo định dạng CSV (không có header, mỗi dòng: report_date,category,revenue)
                yield None, f"{date},{category},{total_revenue:.2f}"

if __name__ == "__main__":
    calculateRevenue.run()
