# Parallel computation optimized for larger datasets
import pandas as pd
import time
from collections import defaultdict
from multiprocessing import Pool, cpu_count

# Load datasets
students_df = pd.read_csv("Students Dataset.csv")
fee_df = pd.read_csv("Fee Payment Dataset.csv")

# Convert DataFrame to list
fee_list = fee_df.to_dict("records")

def process_chunk(chunk):
    local_frequency = defaultdict(list)
    for record in chunk:
        local_frequency[record["Date of Payment"]].append(record["Roll Number"])
    return local_frequency

def parallel_computation():
    start_time = time.time()

    # Split data into chunks
    num_chunks = cpu_count()
    chunk_size = len(fee_list) // num_chunks
    chunks = [fee_list[i:i + chunk_size] for i in range(0, len(fee_list), chunk_size)]

    # Use multiprocessing Pool
    with Pool(processes=num_chunks) as pool:
        results = pool.map(process_chunk, chunks)

    # Combine results
    combined_frequency = defaultdict(list)
    for local_frequency in results:
        for date, ids in local_frequency.items():
            combined_frequency[date].extend(ids)

    result = [
        {"Date": date, "Frequency": len(ids), "Student IDs": ids}
        for date, ids in combined_frequency.items()
    ]

    end_time = time.time()
    print(f"Parallel Computation Time: {end_time - start_time} seconds")
    return result

if __name__ == "__main__":
    print("Running Parallel Computation...")
    parallel_result = parallel_computation()
    print("Parallel Computation Completed.")