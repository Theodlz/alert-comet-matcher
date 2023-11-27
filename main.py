import fire
import subprocess
import os

from config import load_config

config = load_config()

ray_host = config["ray"]["cluster"]["host"]
ray_port = config["ray"]["cluster"]["port"]
ray_dashboard_port = config["ray"]["dashboard"]["port"]
address = f"{ray_host}:{ray_port}"


class Cluster:
    def run_cmd(self, cmd):
        stdout, stderr = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        ).communicate()
        return stdout.decode("utf-8"), stderr.decode("utf-8")

    def start(self):
        print("Starting Ray Cluster")
        std_out, std_err = self.run_cmd(
            f"ray start --head --port={ray_port} --dashboard-port={ray_dashboard_port}"
        )
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

    def submit_job(self, job_file, nowait=False):
        # check that the job file exists
        if not os.path.isfile(job_file):
            raise ValueError(f"Job file {job_file} does not exist")
        print("Submitting job")
        cmd = f"ray job submit --working-dir . --address={address}"
        if nowait:
            cmd += " --no-wait"
        cmd += f" -- python {job_file}"

        std_out, std_err = self.run_cmd(cmd)
        print(std_out)
        print(std_err)

    def cancel_job(self, job_id):
        print("Cancelling job")
        std_out, std_err = self.run_cmd(f"ray job stop --address={address} {job_id}")
        print(std_out)
        print(std_err)


if __name__ == "__main__":
    fire.Fire(Cluster)
