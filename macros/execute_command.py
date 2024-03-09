import subprocess

def execute_command(command):
    try:
        result = subprocess.run(
            command, 
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        return {'success': True, 'output': result.stdout}
    except subprocess.CalledProcessError as e:
        return {'success': False, 'output': e.output}