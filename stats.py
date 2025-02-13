import os
import re

# Define the keys for different latency metrics
CPU_LATENCY_KEYS = {
    "get_from_memtable_time",
    "seek_on_memtable_time",
    "block_checksum_time",
    "block_decompress_time",
    "block_read_cpu_time"
}

INDEX_IO_KEYS = {"read_index_block_nanos"}
FILTER_IO_KEYS = {"read_filter_block_nanos"}
DISK_IO_KEYS = {"get_from_table_nanos"}

def extract_last_perf_context(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    # Find all occurrences of Perf Context
    perf_matches = re.findall(r'RocksDB Perf Context:\s*\n(.*?)\nRocksDB IO Stats Context:', content, re.S)
    
    if len(perf_matches) < 2:
        return None  # Ensure at least two occurrences exist

    # Extract the second occurrence
    return parse_key_value_pairs(perf_matches[-1].strip())

def parse_key_value_pairs(text):
    """ Parses key-value pairs, converting only specified keys to integers. """
    result = {}
    for pair in text.split(','):
        if '=' in pair:
            key, value = pair.split('=', 1)
            key = key.strip()
            value = value.strip()

            # Check if the key belongs to any category before conversion
            if key in CPU_LATENCY_KEYS | INDEX_IO_KEYS | FILTER_IO_KEYS | DISK_IO_KEYS:
                numeric_value = re.search(r'\d+', value)  # Extract numeric part
                result[key] = int(numeric_value.group()) if numeric_value else 0
            else:
                result[key] = value  # Keep as string for other entries

    return result

def main():
    for file in os.listdir('.'):
        if re.match(r'output_b\d{2}\.txt$', file):
            perf_dict = extract_last_perf_context(file)
            if perf_dict:
                # Compute total latency for each category
                total_cpu_nanos = sum(perf_dict.get(key, 0) for key in CPU_LATENCY_KEYS)
                total_index_io_nanos = sum(perf_dict.get(key, 0) for key in INDEX_IO_KEYS)
                total_filter_io_nanos = sum(perf_dict.get(key, 0) for key in FILTER_IO_KEYS)
                total_disk_io_nanos = sum(perf_dict.get(key, 0) for key in DISK_IO_KEYS)

                # Convert nanoseconds to seconds
                total_cpu_seconds = total_cpu_nanos / 1e9
                total_index_io_seconds = total_index_io_nanos / 1e9
                total_filter_io_seconds = total_filter_io_nanos / 1e9
                total_disk_io_seconds = total_disk_io_nanos / 1e9

                # Print results
                print(f"File: {file}")
                print(f"  Total CPU Latency: {total_cpu_seconds:.6f} seconds")
                print(f"  Total Index I/O Latency: {total_index_io_seconds:.6f} seconds")
                print(f"  Total Filter I/O Latency: {total_filter_io_seconds:.6f} seconds")
                print(f"  Total Disk I/O Latency: {total_disk_io_seconds:.6f} seconds")

if __name__ == "__main__":
    main()
