# Serial computation
import pandas as pd
import time
from collections import defaultdict

# Load datasets
students_df = pd.read_csv("Students Dataset.csv")
fee_df = pd.read_csv("Fee Payment Dataset.csv")

# Convert DataFrame to list
fee_list = fee_df.to_dict("records")

def serial_computation():
    start_time = time.time()

    date_frequency = defaultdict(list)
    for record in fee_list:
        date_frequency[record["Date of Payment"]].append(record["Roll Number"])

    result = [
        {"Date": date, "Frequency": len(ids), "Student IDs": ids}
        for date, ids in date_frequency.items()
    ]

    end_time = time.time()
    print(f"Serial Computation Time: {end_time - start_time} seconds")
    return result

if __name__ == "__main__":
    print("Running Serial Computation...")
    serial_result = serial_computation()
    print("Serial Computation Completed.")