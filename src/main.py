import json
import fire
import subprocess
import os

from config import load_config

cfg = load_config()

ray_host = cfg["ray.cluster.host"]
ray_port = cfg["ray.cluster.port"]
ray_dashboard_host = cfg["ray.dashboard.host"]
ray_dashboard_port = cfg["ray.dashboard.port"]
address = f"{ray_host}:{ray_port}"


def get_files_to_exclude(exceptions):
    """
    Get the list of files to exclude from Ray job submission.
    Args:
        exceptions (list): List of file names that should not be excluded.
    Returns:
        list: List of files in the current directory excluding the exceptions.
    """
    return [file for file in os.listdir(".") if file not in exceptions]


class Cluster:
    @staticmethod
    def check_ray():
        cmd = f"ray status --address={address}"
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        try:
            process.communicate(timeout=5)
            if process.returncode == 0:
                return
        except subprocess.TimeoutExpired:
            process.kill()
        print("Error: Ray is not running. Please start the Ray cluster first.")
        exit(1)

    @staticmethod
    def run_cmd(cmd):
        stdout, stderr = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        ).communicate()
        return stdout.decode("utf-8"), stderr.decode("utf-8")

    @staticmethod
    def run_cmd_reel_time_output(cmd):
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )
        while True:
            output = process.stdout.readline()
            if output:
                print(output, end="")
            elif process.poll() is not None:
                break
        return process.returncode, process.stderr.read()

    def start(self):
        print("Starting Ray Cluster")
        cmd = f'ray start --head --port={ray_port} --dashboard-host "{ray_dashboard_host}" --dashboard-port={ray_dashboard_port}'
        std_out, std_err = self.run_cmd(cmd)
        print(std_out)
        print(std_err)

    def stop(self, force=False):
        print("Stopping Ray Cluster")
        self.check_ray()
        std_out, std_err = self.run_cmd(f"ray stop {'--force' if force else ''}")
        print(std_out)
        print(std_err)

    def status(self):
        print("Ray Cluster's status")
        self.check_ray()
        std_out, std_err = self.run_cmd(f"ray status --address={address}")
        print(std_out)
        print(std_err)

    def submit_job(self, job_file, nowait=False):
        self.check_ray()
        # Exclude files from being sent to the cluster
        runtime_env = {"excludes": get_files_to_exclude(cfg["ray.files_to_include"])}
        exclude_config = f"--runtime-env-json='{json.dumps(runtime_env)}'"
        # Determine if the job should be submitted with --no-wait
        nowait = "--no-wait" if nowait else ""
        # Submit the job
        ray_cmd = f"RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1 ray job submit --working-dir . --address={address} {nowait} {exclude_config}"
        return_code, std_err = self.run_cmd_reel_time_output(
            f"{ray_cmd} -- python {job_file}"
        )
        print(return_code)
        print(std_err)

    def cancel_job(self, job_id):
        self.check_ray()
        print("Cancelling job")
        std_out, std_err = self.run_cmd(f"ray job stop --address={address} {job_id}")
        print(std_out)
        print(std_err)


if __name__ == "__main__":
    fire.Fire(Cluster)
