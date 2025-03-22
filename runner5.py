import subprocess
import signal
import sys
import os

def main():
    # Ignore SIGHUP to persist after logout
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    base_command = [
        "./bin/working_version",
        "--cc=1",
        "--size_ratio=4",
        "--entries_per_page=32",
        "--entry_size=128",
        "--buffer_size_in_pages=128",
        "--stat=1",
        "--pin_l0_filter_and_index_blocks_in_cache=true",
        "--pin_top_level_index_and_filter=true",
        "--bloom_before_level=6",
        "--cache_index_and_filter_blocks=true",
        "--cache_index_and_filter_blocks_with_high_priority=true"
    ]

    # Define test configurations
    test_configs = [
        {
            "name": "direct_io_disabled_cache_100MB_ribbon",
            "direct_io": False,
            "block_cache_mb": 100,
            "filter_type": 2  # Ribbon filter
        },
        {
            "name": "direct_io_disabled_cache_100MB_bloom",
            "direct_io": False,
            "block_cache_mb": 100,
            "filter_type": 1  # Bloom filter
        },
        {
            "name": "direct_io_disabled_cache_0MB_ribbon",
            "direct_io": False,
            "block_cache_mb": 0,
            "filter_type": 2  # Ribbon filter
        },
        {
            "name": "direct_io_disabled_cache_0MB_bloom",
            "direct_io": False,
            "block_cache_mb": 0,
            "filter_type": 1  # Bloom filter
        },
        {
            "name": "direct_io_enabled_cache_100MB_ribbon",
            "direct_io": True,
            "block_cache_mb": 100,
            "filter_type": 2  # Ribbon filter
        },
        {
            "name": "direct_io_enabled_cache_100MB_bloom",
            "direct_io": True,
            "block_cache_mb": 100,
            "filter_type": 1  # Bloom filter
        },
        {
            "name": "direct_io_enabled_cache_0MB_ribbon",
            "direct_io": True,
            "block_cache_mb": 0,
            "filter_type": 2  # Ribbon filter
        },
        {
            "name": "direct_io_enabled_cache_0MB_bloom",
            "direct_io": True,
            "block_cache_mb": 0,
            "filter_type": 1  # Bloom filter
        }
    ]
    
    # Run tests for each configuration
    for config in test_configs:
        filter_type_name = "Ribbon" if config['filter_type'] == 2 else "Bloom"
        bits_per_key = 6
        
        print(f"\n=== Running tests with direct io={config['direct_io']}, block cache={config['block_cache_mb']}, {filter_type_name} with bpk={bits_per_key} ===\n", flush=True)
        
        filename = f"{config['name']}.txt"
        
        # Build command with current configuration
        command = base_command + [
            f"--bits_per_key={bits_per_key}",
            f"--filter_type={config['filter_type']}",
            f"--bb={config['block_cache_mb']}",
            f"--dio={1 if config['direct_io'] else 0}"
        ]
        
        print(f"Running with direct io={'enabled' if config['direct_io'] else 'disabled'}, block cache={config['block_cache_mb']}, {filter_type_name} with bpk={bits_per_key}...", flush=True)
        with open(filename, 'w') as f:
            try:
                subprocess.run(
                    command,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"Error occurred: {e}", file=sys.stderr)
        
        print(f"Completed direct io={'enabled' if config['direct_io'] else 'disabled'}, block cache={config['block_cache_mb']}, {filter_type_name} with bpk={bits_per_key}", flush=True)

if __name__ == "__main__":
    main()
