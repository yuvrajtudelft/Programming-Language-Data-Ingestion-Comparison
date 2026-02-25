#sudo EnergiBridge/target/release/energibridge -o results.csv --summary python3 project/python/main.py



import os
import random
import subprocess

#files = ["python/main.py", "java/Main.java"]

def run_docker_python(index=0, language="python"):
    host_data_dir = os.path.abspath("data")
    volume_spec = f"{host_data_dir}:/app/data"

    cmd = [
        "sudo",
        "../EnergiBridge/target/release/energibridge",
        "-o", f"results/{language}/{index}.csv",
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
    os.makedirs("results/python", mode=0o750, exist_ok=True)
    os.makedirs("results/go", mode=0o750, exist_ok=True)

    queue = []

    for i in range(5):
        queue.append((i, "python"))
        queue.append((i, "go"))

    random.shuffle(queue)
    print(queue)

    for item in queue:
        print(item)
        run_docker_python(*item)