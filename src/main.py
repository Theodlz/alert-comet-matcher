import fire
import subprocess
import os

from config import load_config

cfg = load_config()

ray_host = cfg["ray"]["cluster"]["host"]
ray_port = cfg["ray"]["cluster"]["port"]
ray_dashboard_host = cfg["ray"]["dashboard"]["host"]
ray_dashboard_port = cfg["ray"]["dashboard"]["port"]
address = f"{ray_host}:{ray_port}"


def exclude_gitignore_file_from_ray(files_to_not_exclude):
    """
    Exclude files based on .gitignore file.
    Args:
        files_to_not_exclude: List of files present in the .gitignore file that should not be excluded.
    Returns:
        A string with the excludes file formatted for Ray runtime environment.
    """
    excludes = []
    if os.path.exists(".gitignore"):
        with open(".gitignore", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and line not in files_to_not_exclude:
                    excludes.append(line)

    excludes_str = ", ".join(f'"{e}"' for e in excludes)
    return f"--runtime-env-json='{{\"excludes\": [{excludes_str}]}}'"


class Cluster:
    def run_cmd(self, cmd):
        stdout, stderr = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        ).communicate()
        return stdout.decode("utf-8"), stderr.decode("utf-8")

    def start(self):
        print("Starting Ray Cluster")
        cmd = f'ray start --head --port={ray_port} --dashboard-host "{ray_dashboard_host}" --dashboard-port={ray_dashboard_port}'
        std_out, std_err = self.run_cmd(cmd)
        print(std_out)
        print(std_err)

    def stop(self, force=False):
        print("Stopping Ray Cluster")
        std_out, std_err = self.run_cmd(f"ray stop {'--force' if force else ''}")
        print(std_out)
        print(std_err)

    def status(self):
        print("Ray Cluster's status")
        std_out, std_err = self.run_cmd(f"ray status --address={address}")
        print(std_out)
        print(std_err)

    def submit_job(self, job_file, nowait=False, exclude_gitignore_files=True):
        exclude_config = exclude_gitignore_file_from_ray(["config.yaml"]) if exclude_gitignore_files else ""
        nowait = "--no-wait" if nowait else ""
        ray_cmd = f"RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1 ray job submit --working-dir . --address={address} {nowait} {exclude_config}"
        std_out, std_err = self.run_cmd(f"{ray_cmd} -- python {job_file}")
        print(std_out)
        print(std_err)

    def cancel_job(self, job_id):
        print("Cancelling job")
        std_out, std_err = self.run_cmd(f"ray job stop --address={address} {job_id}")
        print(std_out)
        print(std_err)


if __name__ == "__main__":
    fire.Fire(Cluster)
