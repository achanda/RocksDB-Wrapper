import os
import re
import pandas as pd
from datetime import datetime

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

def extract_histograms(file_path):
    """ Extract histogram names and statistics from the file """
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find all histogram sections in the file
    # Pattern looks for histogram name followed by stats until the dashed line
    histogram_sections = re.findall(r'(DB_[A-Z_]+)\n(Count.*?)\n-{5,}', content, re.DOTALL)
    
    # Process each histogram section
    histogram_data = {}
    for name, stats_text in histogram_sections:
        # Create a dictionary for this histogram
        hist_stats = {}
        
        # Extract count and average
        count_avg_match = re.search(r'Count: (\d+) Average: ([\d.]+)', stats_text)
        if count_avg_match:
            hist_stats['count'] = int(count_avg_match.group(1))
            hist_stats['average'] = float(count_avg_match.group(2))
        
        # Extract min, median, max
        min_med_max_match = re.search(r'Min: (\d+)\s+Median: ([\d.]+)\s+Max: (\d+)', stats_text)
        if min_med_max_match:
            hist_stats['min'] = int(min_med_max_match.group(1))
            hist_stats['median'] = float(min_med_max_match.group(2))
            hist_stats['max'] = int(min_med_max_match.group(3))
        
        # We're not extracting percentiles as requested
        
        # Store all stats for this histogram
        histogram_data[name] = hist_stats
    
    return histogram_data

def main():
    # Define the order for displaying histograms
    histogram_order = [
        "DB_GET_CORE_JOULES",
        "DB_GET_FILTER_CORE_JOULES",
        "DB_GET_INDEX_CORE_JOULES",
        "DB_GET_DISK_CORE_JOULES",
        "DB_GET_PACKAGE_JOULES",
        "DB_GET_FILTER_PACKAGE_JOULES",
        "DB_GET_INDEX_PACKAGE_JOULES",
        "DB_GET_DISK_PACKAGE_JOULES",
        "DB_GET_DRAM_JOULES",
        "DB_GET_FILTER_DRAM_JOULES",
        "DB_GET_INDEX_DRAM_JOULES",
        "DB_GET_DISK_DRAM_JOULES"
    ]
    
    # Create a list to store all data for Excel
    excel_data = []
    
    # Process each file
    for file in os.listdir('.'):
        # Match the new filename format: direct_io_(enabled|disabled)_cache_\d+MB_(bloom|ribbon)_b\d{2}(_level-?\d+)?\.txt
        if re.match(r'direct_io_(enabled|disabled)_cache_\d+MB_(bloom|ribbon)_b\d{2}(_level-?\d+)?\.txt$', file):
            # Extract perf context data
            perf_dict = extract_last_perf_context(file)
            
            # Extract histogram data
            histogram_data = extract_histograms(file)
            
            # Extract configuration details from filename
            direct_io = "enabled" if "direct_io_enabled" in file else "disabled"
            cache_size = re.search(r'cache_(\d+)MB', file).group(1)
            filter_type = "Bloom" if "bloom" in file else "Ribbon"
            bits = re.search(r'b(\d{2})', file).group(1)
            level_match = re.search(r'level(-?\d+)', file)
            level = level_match.group(1) if level_match else "N/A"
            
            # Create a row for the Excel file
            row_data = {
                'Filename': file,
                'Direct_IO': direct_io,
                'Cache_Size_MB': cache_size,
                'Filter_Type': filter_type,
                'Bits_Per_Key': bits,
                'Bloom_Before_Level': level
            }
            
            # Add histogram data
            if histogram_data:
                for name in histogram_order:
                    if name in histogram_data:
                        stats = histogram_data[name]
                        # Add average, min, median, max to the row
                        row_data[f"{name}_Count"] = stats.get('count', 'N/A')
                        row_data[f"{name}_Average"] = stats.get('average', 'N/A')
                        row_data[f"{name}_Min"] = stats.get('min', 'N/A')
                        row_data[f"{name}_Median"] = stats.get('median', 'N/A')
                        row_data[f"{name}_Max"] = stats.get('max', 'N/A')
            
            # Add perf context data if available
            if perf_dict:
                # Compute total latency for each category
                total_cpu_nanos = sum(perf_dict.get(key, 0) for key in CPU_LATENCY_KEYS)
                total_index_io_nanos = sum(perf_dict.get(key, 0) for key in INDEX_IO_KEYS)
                total_filter_io_nanos = sum(perf_dict.get(key, 0) for key in FILTER_IO_KEYS)
                total_disk_io_nanos = sum(perf_dict.get(key, 0) for key in DISK_IO_KEYS)

                # Convert nanoseconds to seconds
                row_data['Total_CPU_Latency_Seconds'] = total_cpu_nanos / 1e9
                row_data['Total_Index_IO_Latency_Seconds'] = total_index_io_nanos / 1e9
                row_data['Total_Filter_IO_Latency_Seconds'] = total_filter_io_nanos / 1e9
                row_data['Total_Disk_IO_Latency_Seconds'] = total_disk_io_nanos / 1e9
            
            # Add the row to our data
            excel_data.append(row_data)
            
            # Print file information to console
            print(f"\n{'='*50}")
            print(f"File: {file}")
            print(f"{'='*50}")
            print(f"Configuration: Direct I/O {direct_io}, Cache {cache_size}MB, {filter_type} filter, {bits} bits per key")
            if level != "N/A":
                print(f"Bloom Before Level: {level}")
            
            # Print histogram data
            if histogram_data:
                print("\nHISTOGRAM DATA:")
                print("-" * 50)
                
                # Print histograms in the specified order
                for name in histogram_order:
                    if name in histogram_data:
                        stats = histogram_data[name]
                        print(f"{name}:")
                        print(f"  Count: {stats.get('count', 'N/A')}")
                        print(f"  Average: {stats.get('average', 'N/A'):.4f}")
                        print(f"  Min: {stats.get('min', 'N/A')}, Median: {stats.get('median', 'N/A'):.4f}, Max: {stats.get('max', 'N/A')}")
                        print()
            
            # Print perf context data if available
            if perf_dict:
                print("\nPERF CONTEXT DATA:")
                print("-" * 30)
                print(f"Total CPU Latency: {row_data['Total_CPU_Latency_Seconds']:.6f} seconds")
                print(f"Total Filter I/O Latency: {row_data['Total_Filter_IO_Latency_Seconds']:.6f} seconds")
                print(f"Total Index I/O Latency: {row_data['Total_Index_IO_Latency_Seconds']:.6f} seconds")
                print(f"Total Disk I/O Latency: {row_data['Total_Disk_IO_Latency_Seconds']:.6f} seconds")
    
    # Create Excel file if we have data
    if excel_data:
        # Create a DataFrame
        df = pd.DataFrame(excel_data)
        
        # Generate a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_filename = f"rocksdb_stats_{timestamp}.xlsx"
        
        # Write to Excel
        df.to_excel(excel_filename, index=False)
        print(f"\nData written to {excel_filename}")

if __name__ == "__main__":
    main()
