import subprocess
import signal
import sys

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

    filter_type=2
    for bloom_before_level in range(0, 6):
        for bits in [2,10]:
            filename = f"output_b{bits:02d}_{bloom_before_level}.txt"
            command = base_command + [f"--bits_per_key={bits}", f"--filter_type={filter_type}", f"--bloom_before_level={bloom_before_level}"]

            print(f"Running with filter_type={filter_type} bits_per_key={bits} bloom_before_level={bloom_before_level}...", flush=True)
            with open(filename, 'w') as f:
                try:
                    subprocess.run(
                    command,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    check=True
                    )
                except subprocess.CalledProcessError as e:
                    print(f"Error occurred for bits_per_key={bits}: {e}", file=sys.stderr)

            print(f"Completed filter_type={filter_type} bits_per_key={bits} bloom_before_level={bloom_before_level}", flush=True)
    
if __name__ == "__main__":
    main()
