from flask import Flask, render_template, request, jsonify
import subprocess
import threading
import queue
import time
import os
import logging
import errno
import re
import pty
import select
import fcntl
import termios
import struct
import signal
import json

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("dlt-meta-app.log"),
                              logging.StreamHandler()])
logger = logging.getLogger(__name__)

app = Flask(__name__)
command_queues = {}
response_queues = {}


def run_command(command_id, command, input_queue, output_queue, SHELL_FLG=False):

    # Handle export commands
    if command.startswith('export'):
        try:
            var, value = command.split(' ', 1)[1].split('=', 1)
            os.environ[var] = value
            print(f"env var: {os.environ[var]}")
            output_queue.put(('output', f"Exported {var}={value}\n"))
            output_queue.put(('exit', 0))
        except Exception as e:
            output_queue.put(('error', str(e)))
            output_queue.put(('exit', 1))
        return
    # Handle cd commands
    if command.startswith('cd'):
        try:
            path = command.split(' ', 1)[1]
            os.chdir(path)
            output_queue.put(('output', f"Changed directory to {os.getcwd()}\n"))
            output_queue.put(('exit', 0))
        except Exception as e:
            output_queue.put(('error', str(e)))
            output_queue.put(('exit', 1))
        return

    # If command is a Python script, ensure unbuffered output
    if command.startswith('python'):
        command = command.replace('python', 'python -u', 1)
    # Create a pseudo-terminal
    master, slave = pty.openpty()
    # Set the terminal size
    term_size = struct.pack('HHHH', 24, 80, 0, 0)
    fcntl.ioctl(slave, termios.TIOCSWINSZ, term_size)
    # Start the process
    process = subprocess.Popen(
        ["bash", "-c", command],
        shell=SHELL_FLG,
        stdin=slave,
        stdout=slave,
        stderr=slave,
        preexec_fn=os.setsid,
        text=False,
        bufsize=0,
        env=os.environ.copy()
    )
    # Close the slave fd, as the child process has it
    os.close(slave)
    # Set master to non-blocking mode
    fl = fcntl.fcntl(master, fcntl.F_GETFL)
    fcntl.fcntl(master, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    # Variables for tracking state
    waiting_for_input = False
    buffer = ""

    # Create events for signaling
    stop_event = threading.Event()
    last_prompt = None
    last_prompt_time = 0

    def output_reader():
        nonlocal buffer, waiting_for_input, last_prompt, last_prompt_time
        while not stop_event.is_set() and process.poll() is None:
            try:
                # Check if there's data to read
                r, _, _ = select.select([master], [], [], 0.1)
                if master in r:
                    # Read data
                    data = os.read(master, 1024)
                    if not data:
                        break
                    # Decode the data
                    try:
                        text = data.decode('utf-8')
                    except UnicodeDecodeError:
                        text = data.decode('latin-1')
                    # Add to output queue
                    output_queue.put(('output', text))
                    # Add to buffer for prompt detection
                    buffer += text
                    # Check for prompts in the buffer
                    lines = buffer.splitlines(True)
                    buffer = ""
                    for line in lines:
                        if line.endswith('\n'):
                            buffer = ""
                        else:
                            buffer += line
                        # Check if line looks like a prompt
                        if (('?' in line or ':' in line
                             or line.strip().endswith('>')
                             or '[y/n]' in line
                             or 'input' in line.lower()
                             or 'select' in line.lower()
                             or 'choose' in line.lower()
                             or 'continue' in line.lower()
                             or 'press' in line.lower()) and not line.strip().endswith('\\')):
                            # output_queue.put(('prompt', line))
                            # waiting_for_input = True

                            # Deduplicate prompts
                            current_time = time.time()
                            if last_prompt != line or current_time - last_prompt_time > 1.0:
                                output_queue.put(('prompt', line))
                                waiting_for_input = True
                                last_prompt = line
                                last_prompt_time = current_time
            except (OSError, IOError) as e:
                if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:
                    output_queue.put(('error', f"Output error: {str(e)}"))
                    break
            except Exception as e:
                output_queue.put(('error', f"Output error: {str(e)}"))
                break
            time.sleep(0.01)
    # Start output reader thread
    output_thread = threading.Thread(target=output_reader)
    output_thread.daemon = True
    output_thread.start()
    # Main loop for handling input
    try:
        while process.poll() is None:
            try:
                # Check if we have user input to send
                if not input_queue.empty():
                    user_input = input_queue.get()
                    # Add newline and encode
                    input_data = (user_input + '\n').encode('utf-8')
                    # Write to the master fd
                    os.write(master, input_data)
                    # Add to output queue
                    output_queue.put(('input', user_input))
                    # Reset state
                    waiting_for_input = False
                    last_prompt = None
                # If we're waiting for input but none is available, sleep briefly
                elif waiting_for_input:
                    time.sleep(0.1)
                # Otherwise just wait a bit
                else:
                    time.sleep(0.1)
            except Exception as e:
                output_queue.put(('error', f"Input error: {str(e)}"))
                time.sleep(0.1)
    except KeyboardInterrupt:
        # Handle keyboard interrupt
        pass
    finally:
        # Signal the output thread to stop
        stop_event.set()
        # Try to terminate the process gracefully
        try:
            if process.poll() is None:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=1)
        except Exception:
            # Force kill if necessary
            try:
                if process.poll() is None:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except Exception:
                pass
        # Close the master fd
        try:
            os.close(master)
        except Exception:
            pass
        # Wait for output thread to finish
        output_thread.join(timeout=1)
        # Process has ended
        exit_code = process.poll() if process.poll() is not None else -1
        output_queue.put(('exit', exit_code))


@app.route('/')
def index():
    return render_template('landingPage.html')


@app.route('/start_command', methods=['POST'])
def start_command():
    try:
        print("here")
        data = request.json
        command = data.get('command')
    except Exception as e:
        error_msg = f"Failed to parse request: {str(e)}"
        logger.error(error_msg)
        return jsonify({'error': error_msg, 'status': 'failed'}), 400

    if command == 'setup':
        try:
            current_directory = os.getcwd()
            print(f"Current directory: {current_directory}")
        except FileNotFoundError:
            print("The current working directory is no longer accessible.")
            # Optionally, set a default directory
            os.chdir("/")  # Change to root directory
            current_directory = os.getcwd()

        command_id = None

        # Always use /app/python/source_code as base directory to avoid nested directories
        source_code_dir = "/app/python/source_code"
        dlt_meta_path = f"{source_code_dir}/dlt-meta"

        print("Start setting up dlt-meta environment (pulling latest code and creating fresh environment)...")
        print(f"Target installation directory: {dlt_meta_path}")

        # Use subprocess for synchronous execution with proper error handling
        try:
            # Create source_code directory if it doesn't exist
            print("Step 1: Creating source_code directory...")
            subprocess.run(f"mkdir -p {source_code_dir}", shell=True, check=True, capture_output=True, text=True)
            print("✓ Source code directory created")

            # Remove existing dlt-meta directory
            print("Step 2: Removing old dlt-meta installation...")
            subprocess.run(f"rm -rf {dlt_meta_path}", shell=True, check=True, capture_output=True, text=True)
            print("✓ Old installation removed")

            # Clone fresh copy - try feature branch first, fallback to main
            print("Step 3: Cloning dlt-meta from GitHub...")
            clone_result = subprocess.run(
                f"cd {source_code_dir} && git clone -b 'feature/layer-terminology-update' https://github.com/girishchandriah/dlt-meta.git 2>&1",
                shell=True, capture_output=True, text=True
            )

            if clone_result.returncode != 0:
                print(f"Feature branch not found, trying main branch...")
                clone_result = subprocess.run(
                    f"cd {source_code_dir} && git clone https://github.com/girishchandriah/dlt-meta.git 2>&1",
                    shell=True, check=True, capture_output=True, text=True
                )

            print(f"✓ Repository cloned successfully")
            print(f"Clone output: {clone_result.stdout}")

            # Verify clone was successful
            if not os.path.exists(f"{dlt_meta_path}/src"):
                raise Exception(f"Clone failed - {dlt_meta_path}/src directory not found")

            # Create virtual environment
            print("Step 4: Creating virtual environment...")
            subprocess.run(f"python3 -m venv {dlt_meta_path}/.venv", shell=True, check=True, capture_output=True, text=True)
            print("✓ Virtual environment created")

            # Install dependencies
            print("Step 5: Installing dependencies...")
            subprocess.run(f"{dlt_meta_path}/.venv/bin/pip install --upgrade pip", shell=True, check=True, capture_output=True, text=True)
            print("✓ Pip upgraded")

            subprocess.run(f"{dlt_meta_path}/.venv/bin/pip install databricks-sdk", shell=True, check=True, capture_output=True, text=True)
            print("✓ databricks-sdk installed")

            subprocess.run(f"{dlt_meta_path}/.venv/bin/pip install PyYAML", shell=True, check=True, capture_output=True, text=True)
            print("✓ PyYAML installed")

        except subprocess.CalledProcessError as e:
            error_msg = f"Setup failed at command: {e.cmd}\nError: {e.stderr}\nOutput: {e.stdout}"
            logger.error(error_msg)
            print(error_msg)
            return jsonify({'command_id': None, 'error': error_msg, 'status': 'failed'})
        except Exception as e:
            error_msg = f"Setup failed: {str(e)}"
            logger.error(error_msg)
            print(error_msg)
            return jsonify({'command_id': None, 'error': error_msg, 'status': 'failed'})

        # Update environment variables after successful setup
        os.environ['PYTHONPATH'] = dlt_meta_path
        os.environ['HOME'] = source_code_dir
        os.environ['VIRTUAL_ENV'] = f"{dlt_meta_path}/.venv"
        os.environ['PATH'] = f"{dlt_meta_path}/.venv/bin:{os.environ.get('PATH', '')}"

        print(f"✅ Completed setting up dlt-meta environment at: {dlt_meta_path}")
        print(f"PYTHONPATH set to: {os.environ['PYTHONPATH']}")
        print(f"HOME set to: {os.environ['HOME']}")

        # Verify installation
        if os.path.exists(f"{dlt_meta_path}/src") and os.path.exists(f"{dlt_meta_path}/.venv"):
            print(f"✓ Installation verified - src directory and venv found")
            # List key directories
            try:
                dirs = os.listdir(dlt_meta_path)
                print(f"✓ Directories in dlt-meta: {', '.join(dirs)}")
            except:
                pass
            return jsonify({'command_id': 'setup_complete', 'status': 'success', 'message': 'Setup completed successfully'})
        else:
            print(f"⚠ Warning: Installation may be incomplete")
            return jsonify({'command_id': 'setup_incomplete', 'status': 'warning', 'message': 'Installation may be incomplete - src or venv not found'})

    else:
        command_id = str(time.time())
        input_queue = queue.Queue()
        output_queue = queue.Queue()
        command_queues[command_id] = input_queue
        response_queues[command_id] = output_queue
        thread = threading.Thread(target=run_command, args=(command_id, command, input_queue, output_queue))
        thread.daemon = True
        thread.start()
        return jsonify({'command_id': command_id})


@app.route('/send_input', methods=['POST'])
def send_input():
    data = request.json
    command_id = data.get('command_id')
    user_input = data.get('input')
    if command_id in command_queues:
        command_queues[command_id].put(user_input)
        return jsonify({'status': 'success'})
    else:
        return jsonify({'status': 'error', 'message': 'Command not found'})


@app.route('/get_output', methods=['GET'])
def get_output():
    command_id = request.args.get('command_id')
    if command_id in response_queues:
        output_queue = response_queues[command_id]
        result = []
        while not output_queue.empty():
            output_type, content = output_queue.get()
            result.append({'type': output_type, 'content': content})
        return jsonify({'status': 'success', 'output': result})
    else:
        return jsonify({'status': 'error', 'message': 'Command not found'})


@app.route('/cleanup', methods=['POST'])
def cleanup():
    data = request.json
    command_id = data.get('command_id')
    if command_id in command_queues:
        del command_queues[command_id]
    if command_id in response_queues:
        del response_queues[command_id]
    return jsonify({'status': 'success'})


@app.route('/onboarding', methods=['POST'])
def handle_onboard_form():

    print(f"onboard details: {request.form}")

    # Get source directory - try multiple locations
    current_directory = os.environ.get('PYTHONPATH')

    if not current_directory:
        # Try common locations where dlt-meta might be installed
        possible_paths = [
            '/app/python/source_code',  # Databricks App: dlt-meta root
            os.getcwd(),  # Current working directory might be dlt-meta root
            '/app/python/dlt-meta',
            '/app/python/source_code/dlt-meta',
            os.path.join(os.getcwd(), 'dlt-meta'),
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ]

        print(f"PYTHONPATH not set. Searching for dlt-meta in: {possible_paths}")

        for path in possible_paths:
            cli_path = os.path.join(path, 'src', 'cli.py')
            print(f"Checking: {cli_path} - exists: {os.path.exists(cli_path)}")
            if os.path.exists(cli_path):
                current_directory = path
                break

        if not current_directory:
            return jsonify({
                'modal_content': None,
                'stdout': '',
                'stderr': f"Could not find dlt-meta installation. Checked: {possible_paths}. Current working directory: {os.getcwd()}",
                'returncode': 1
            })

    # Ensure no trailing slash
    current_directory = current_directory.rstrip('/')

    print(f"Using dlt-meta directory: {current_directory}")

    # Create JSON object from form data
    json_data = {
        "unity_catalog_enabled": "1" if request.form.get('unity_catalog_enabled') == "1" else "0",
        "unity_catalog_name": request.form.get('unity_catalog_name', ''),
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "onboarding_file_path": request.form.get('onboarding_file_path', 'demo/conf/onboarding.template'),
        "local_directory": request.form.get('local_directory', '/app/python/source_code/dlt-meta/demo/'),
        "dlt_meta_schema": request.form.get('dlt_meta_schema',
                                            'dlt_meta_dataflowspecs_4e6c360d3e5c4b5ca6687fec8ffe2e14'),
        "landing_schema": request.form.get('landing_schema', 'dltmeta_landing_9c1aa383b36a49198d3e99d25f7180a4'),
        "refinery_schema": request.form.get('refinery_schema', 'dltmeta_refinery_7b4e981029b843c799bf61a0a121b3ca'),
        "treasury_schema": request.form.get('treasury_schema', 'dltmeta_treasury_8d3e99d25f7180a4'),
        "dlt_meta_layer": request.form.get('dlt_meta_layer', '1'),
        "landing_table": request.form.get('landing_table', 'landing_dataflowspec'),
        "refinery_table": request.form.get('refinery_table', 'refinery_dataflowspec'),
        "treasury_table": request.form.get('treasury_table', 'treasury_dataflowspec'),
        "overwrite": "1" if request.form.get('overwrite') == "1" else "0",
        "version": request.form.get('version', 'v1'),
        "environment": request.form.get('environment', 'prod'),
        "author": request.form.get('author', 'app-40zbx9 meta-dlt'),
        "update_paths": "1" if request.form.get('update_paths') == "1" else "0",
        "command": "onboard_ui",
        "flags": {"log_level": "info"},
    }

    json_string = json.dumps(json_data)
    result = subprocess.run(f"python3 {current_directory}/src/cli.py '{json_string}'",
                            shell=True,
                            capture_output=True,
                            text=True
                            )
    return extract_command_output(result)


@app.route('/deploy', methods=['POST'])
def handle_deploy_form():
    try:
        # Create JSON object from form data
        print(f"deploy details: {request.form}")

        # Get source directory - try multiple locations
        current_directory = os.environ.get('PYTHONPATH')

        if not current_directory:
            # Try common locations where dlt-meta might be installed
            possible_paths = [
                '/app/python/source_code',  # Databricks App: dlt-meta root
                os.getcwd(),  # Current working directory might be dlt-meta root
                '/app/python/dlt-meta',
                '/app/python/source_code/dlt-meta',
                os.path.join(os.getcwd(), 'dlt-meta'),
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ]

            print(f"PYTHONPATH not set. Searching for dlt-meta in: {possible_paths}")

            for path in possible_paths:
                cli_path = os.path.join(path, 'src', 'cli.py')
                print(f"Checking: {cli_path} - exists: {os.path.exists(cli_path)}")
                if os.path.exists(cli_path):
                    current_directory = path
                    break

            if not current_directory:
                raise FileNotFoundError(
                    f"Could not find dlt-meta installation. Checked: {possible_paths}. "
                    f"Current working directory: {os.getcwd()}"
                )

        # Ensure no trailing slash
        current_directory = current_directory.rstrip('/')

        print(f"Using dlt-meta directory: {current_directory}")
        print(f"CLI path: {current_directory}/src/cli.py")
        print(f"CLI exists: {os.path.exists(current_directory + '/src/cli.py')}")

        json_data = {
            "uc_enabled": "1" if request.form.get('uc_enabled') == "1" else "0",
            "uc_catalog_name": request.form.get('uc_catalog_name', ''),
            "serverless": "1" if request.form.get('serverless') == "1" else "0",
            "layer": request.form.get('deploylayer', 'landing'),
            "pipeline_name": request.form.get('pipeline_name', 'dlt_meta_pipeline'),
            "dlt_target_schema": request.form.get("dlt_target_schema"),
            "command": "deploy_ui",
            "flags": {"log_level": "info"},
            "onboard_landing_group": request.form.get("onboard_landing_group"),
            "onboard_refinery_group": request.form.get("onboard_refinery_group"),
            "onboard_treasury_group": request.form.get("onboard_treasury_group"),
            "dlt_meta_landing_schema": request.form.get("spc_schema_name"),
            "dlt_meta_refinery_schema": request.form.get("spc_schema_name"),
            "dlt_meta_treasury_schema": request.form.get("spc_schema_name"),
            "dataflowspec_landing_table": request.form.get("landing_dataflowspec_table"),
            "dataflowspec_refinery_table": request.form.get("refinery_dataflowspec_table"),
            "dataflowspec_treasury_table": request.form.get("treasury_dataflowspec_table"),
        }

        json_string = json.dumps(json_data)
        result = subprocess.run(f"python3 {current_directory}/src/cli.py '{json_string}'",
                                shell=True,
                                capture_output=True,
                                text=True
                                )
        return extract_command_output(result)
    except Exception as e:
        print(f"Error in handle_deploy_form: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'modal_content': None,
            'stdout': '',
            'stderr': f"Application error: {str(e)}",
            'returncode': 1
        })


@app.route('/rundemo', methods=['POST'])
def run_demo():
    code_to_run = request.json.get('demo_name', '')
    print(f"processing demo for :{request.json}")

    # Get source directory - try multiple locations
    current_directory = os.environ.get('PYTHONPATH')

    if not current_directory:
        # Try common locations where dlt-meta might be installed
        possible_paths = [
            '/app/python/source_code',  # Databricks App: dlt-meta root
            os.getcwd(),  # Current working directory might be dlt-meta root
            '/app/python/dlt-meta',
            '/app/python/source_code/dlt-meta',
            os.path.join(os.getcwd(), 'dlt-meta'),
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ]

        for path in possible_paths:
            cli_path = os.path.join(path, 'src', 'cli.py')
            if os.path.exists(cli_path):
                current_directory = path
                break

        if not current_directory:
            return jsonify({
                'modal_content': None,
                'stdout': '',
                'stderr': f"Could not find dlt-meta installation. Checked: {possible_paths}. CWD: {os.getcwd()}",
                'returncode': 1
            })

    # Ensure no trailing slash
    current_directory = current_directory.rstrip('/')
    demo_dict = {"demo_cloudfiles": "demo/launch_af_cloudfiles_demo.py",
                 "demo_acf": "demo/launch_acfs_demo.py",
                 "demo_refineryfanout": "demo/launch_refinery_fanout_demo.py",
                 "demo_dias": "demo/launch_dais_demo.py",
                 "demo_dlt_sink": "demo/launch_dlt_sink_demo.py",
                 "demo_dabs": "demo/generate_dabs_resources.py"
                 }
    demo_file = demo_dict.get(code_to_run, None)
    uc_name = request.json.get('uc_name', '')

    if code_to_run == 'demo_dabs':

        # Step 1: Generate Databricks resources
        subprocess.run(f"python3 {current_directory}/{demo_file} --uc_catalog_name {uc_name} "
                       f"--source=cloudfiles --profile DEFAULT",
                       shell=True,
                       capture_output=True,
                       text=True
                       )

        # Step 2: Change working directory to demo/dabs for all next commands
        subprocess.run("databricks bundle validate --profile=DEFAULT", cwd=f"{current_directory}/demo/dabs",
                       shell=True,
                       capture_output=True,
                       text=True)

        # Step 4: Deploy the bundle
        subprocess.run("databricks bundle deploy --target dev --profile=DEFAULT",
                       cwd=f"{current_directory}/demo/dabs", shell=True,
                       capture_output=True,
                       text=True)

        # Step 5: Run 'onboard_people' task
        rs1 = subprocess.run("databricks bundle run onboard_people -t dev --profile=DEFAULT",
                             cwd=f"{current_directory}/demo/dabs", shell=True,
                             capture_output=True,
                             text=True)
        print(f"onboarding completed: {rs1.stdout}")
        # Step 6: Run 'execute_pipelines_people' task
        result = subprocess.run("databricks bundle run execute_pipelines_people -t dev --profile=DEFAULT",
                                cwd=f"{current_directory}/demo/dabs",
                                shell=True,
                                capture_output=True,
                                text=True
                                )
        print(f"execution of pipeline completed: {result.stdout}")
    else:
        result = subprocess.run(f"python3 {current_directory}/{demo_file} --uc_catalog_name {uc_name} "
                                f"--profile DEFAULT",
                                shell=True,
                                capture_output=True,
                                text=True
                                )
    return extract_command_output(result)


def extract_command_output(result):
    stdout = result.stdout
    job_id_match = re.search(r"job_id=(\d+) | pipeline=(\d+)", stdout)
    url_match = re.search(r"(https?://[^\s]+)", stdout)

    job_id = job_id_match.group(1) or job_id_match.group(2) if job_id_match else None
    job_url = url_match.group(1) if url_match else None

    if job_url:
        modal_html = {'title': 'Job Created Successfully',
                      'job_id': job_id,
                      'job_url': job_url
                      }
    else:
        modal_html = None
    # Return the response as JSON
    return jsonify({
        'modal_content': modal_html,
        'stdout': result.stdout,
        'stderr': result.stderr,
        'returncode': result.returncode
    })


if __name__ == '__main__':
    app.run(debug=True)
