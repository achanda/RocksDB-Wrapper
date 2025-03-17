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
        "--stat=1"
    ]

    # Define test configurations
    test_configs = [
        {
            "name": "direct_io_enabled_cache_100MB",
            "direct_io": True,
            "block_cache_mb": 100
        },
        {
            "name": "direct_io_disabled_cache_0MB",
            "direct_io": False,
            "block_cache_mb": 0
        },
        {
            "name": "direct_io_disabled_cache_100MB",
            "direct_io": False,
            "block_cache_mb": 100
        }
    ]
    
    # Run tests for each configuration
    for config in test_configs:
        print(f"\n=== Running tests with {config['name']} ===\n", flush=True)
        
        # Ribbon filter tests (filter_type=2) with bits_per_key=6
        filter_type = 2  # Ribbon filter
        bits_per_key = 6
        
        for bloom_before_level in [-1, 0, 2, 4, 6]:
            filename = f"{config['name']}_ribbon_b{bits_per_key:02d}_level{bloom_before_level}.txt"
            
            # Build command with current configuration
            command = base_command + [
                f"--bits_per_key={bits_per_key}",
                f"--filter_type={filter_type}",
                f"--bloom_before_level={bloom_before_level}",
                f"--bb={config['block_cache_mb']}",
                f"--dio={1 if config['direct_io'] else 0}"
            ]
            
            print(f"Running with {config['name']}, filter_type=Ribbon, bits_per_key={bits_per_key}, bloom_before_level={bloom_before_level}...", flush=True)
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
            
            print(f"Completed {config['name']}, filter_type=Ribbon, bits_per_key={bits_per_key}, bloom_before_level={bloom_before_level}", flush=True)
        
        # Bloom filter test (filter_type=1) with bits_per_key=6
        filter_type = 1  # Bloom filter
        bits_per_key = 6
        
        filename = f"{config['name']}_bloom_b{bits_per_key:02d}.txt"
        
        # Build command with current configuration
        command = base_command + [
            f"--bits_per_key={bits_per_key}",
            f"--filter_type={filter_type}",
            f"--bb={config['block_cache_mb']}",
            f"--dio={1 if config['direct_io'] else 0}"
        ]
        
        print(f"Running with {config['name']}, filter_type=Bloom, bits_per_key={bits_per_key}...", flush=True)
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
        
        print(f"Completed {config['name']}, filter_type=Bloom, bits_per_key={bits_per_key}", flush=True)
    
if __name__ == "__main__":
    main()
