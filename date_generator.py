import sys
from datetime import datetime, timedelta

def date_range(start_date, end_date, n_parallel):
    total_days = (end_date - start_date).days
    delta_days = total_days // n_parallel
    for i in range(n_parallel):
        s_date = start_date + timedelta(days=i * delta_days)
        e_date = start_date + timedelta(days=(i + 1) * delta_days) if i < n_parallel - 1 else end_date
        yield s_date, e_date

if __name__ == "__main__":
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
    n_parallel = int(sys.argv[3])

    for s_date, e_date in date_range(start_date, end_date, n_parallel):
        print(s_date.strftime("%Y-%m-%d"), e_date.strftime("%Y-%m-%d"))
