#sudo EnergiBridge/target/release/energibridge -o results.csv --summary python3 project/python/main.py



import os
import subprocess

#files = ["python/main.py", "java/Main.java"]

def run_docker_python(index=0):
    host_data_dir = os.path.abspath("data")
    volume_spec = f"{host_data_dir}:/app/data"

    cmd = [
        "sudo",
        "../EnergiBridge/target/release/energibridge",
        "-o", f"results/python/{index}.csv",
        "--summary",
        "docker", "run", "--rm",
        "-v", volume_spec,
        "pl-ingestion-python:latest"
    ]

    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print("Command finished with exit code 0")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed (code {e.returncode})")
        if e.stdout:
            print("Output:\n", e.stdout)




if __name__ == "__main__":
    for i in range(20):
        run_docker_python(i)