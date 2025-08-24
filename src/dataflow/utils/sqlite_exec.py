from __future__ import annotations
import subprocess, os, signal

def exec_python_code(code: str, env: dict, timeout: int = 300):
    proc = subprocess.Popen(
        ['python3', '-c', code],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True, env={**os.environ, **env})
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return stdout, stderr
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        return None, "Execution timeout"
