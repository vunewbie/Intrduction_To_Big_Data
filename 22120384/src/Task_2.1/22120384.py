from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
from datetime import datetime, timedelta

class calculateRevenue(MRJob):
        OUTPUT_PROTOCOL = RawValueProtocol
        DATE_FORMAT_INPUT = "%m-%d-%y"
        DATE_FORMAT_OUTPUT = "%d/%m/%Y"
        
        all_dates = set()
        all_categories = set()

        def mapper_init(self):
                with open("/home/hadoop/dates.txt", 'r') as f:
                        for line in f:
                                date_obj = datetime.strptime(line.strip(), self.DATE_FORMAT_INPUT)
                                self.all_dates.add(date_obj.strftime(self.DATE_FORMAT_OUTPUT))

                with open("/home/hadoop/categories.txt", 'r') as f:
                        for line in f:
                                self.all_categories.add(line.strip())

        def mapper(self, _, line):
                if "index" in line:  # Không tính dòng đầu tiên
                        return

                columns = line.strip().split(",")
                date = columns[2].strip() # Thuộc tính Date
                category = columns[9].strip() # Thuộc tính Category
                amount = columns[15].strip() # Thuộc tính Amount
                if amount == "":
                        amount = 0.0

                date_obj = datetime.strptime(date, self.DATE_FORMAT_INPUT)

                # Với mỗi dòng, phát ra 3 cặp key – value
                # Mỗi key là tuple (window_date, category) ứng với cửa sổ trượt 3 ngày
                for offset in range(3):
                        window_date = date_obj + timedelta(days=offset)
                        window_date_str = window_date.strftime(self.DATE_FORMAT_OUTPUT)
                        yield (window_date_str, category), float(amount)

        def mapper_final(self):
                for date in self.all_dates:
                        for category in self.all_categories:
                                yield (date, category), 0.0
            
        def reducer(self, keys, values):
                total_revenue = sum(values)
                date, category = keys
                yield None, (date, category, total_revenue)

        def reducer_final_sorting(self, _, records):
                sorted_records = sorted(records, key=lambda x: datetime.strptime(x[0], self.DATE_FORMAT_OUTPUT))
                for date, category, revenue in sorted_records:
                        yield None, f"{date},{category},{revenue:.2f}"

        def steps(self):
                return [
                        MRStep(
                        mapper_init=self.mapper_init,
                        mapper=self.mapper,
                        mapper_final=self.mapper_final,
                        reducer=self.reducer),

                        MRStep(reducer=self.reducer_final_sorting)]
        
if __name__ == "__main__":
    calculateRevenue.run()
