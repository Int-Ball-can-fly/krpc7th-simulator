# Kibo-RPC 7th Simulator

This workspace is used to develop programs for Kibo-RPC 7th and test them with the Web simulator or the local simulator.

For environment setup, API specifications, and detailed programming instructions, see the [Programming Manual](docs/ProgrammingManual.md).

## Before You Start

- Work in Linux or Ubuntu on WSL2.
- Docker is required when using the local simulator.
- The main directory for participant code is `users/catkin_ws/src/krpc_user/`.
- Do not modify provided files such as `docker/`, `rootfs/`, `rviz/`, `krpc_api_server`, or `krpc_ib2_msgs` unless necessary.

## Basic Usage

### 1. Edit Your Program

Edit the sample program in `users/catkin_ws/src/krpc_user/`.
Do not change the package name, `CMakeLists.txt`, `package.xml`, or launch file names.

### 2. Create a Submission Package

From the project root directory, run:

```bash
cd users
bash ./packing.sh my_submission
```

This creates `users/my_submission.tar.gz`. Upload this file to the Web simulator to run your program.

The Web simulator is available at:

- [Kibo-RPC Login Page](https://jaxa.krpc.jp/user-auth)

### 3. Use the Local Simulator

Build the Docker image the first time you use the local simulator or when the image is updated.

```bash
bash ./build.sh
```

Then, start the simulator in another terminal:

```bash
bash ./simulation.sh
```

While the simulator is running, open <http://localhost> in a browser to view it.
Your program is built and started automatically by `simulation.sh`.

To stop the simulation, press `Ctrl+C` in the terminal where it is running.

### 4. Check Results and Logs

Execution results are saved in `share/logs/`. The latest results are available in `share/logs/latest/`.

## Local Web Manager (Multiple Simulations)

The local Web Manager lets you upload a package created by `users/packing.sh`, watch the simulation, and run multiple isolated simulations. Each run has its own containers, user workspace, logs, and VNC session. By default, up to two simulations run at once; additional uploads wait in a queue.

Use Python 3.9 or newer. Install the Web Manager dependency in a virtual environment and start the service from the simulator project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r web/requirements.txt
python web/server.py
```

Build the simulator image first with `bash ./build.sh` if it is not already available locally.

Open <http://127.0.0.1:8080>. The manager listens on localhost only, so it can be exposed remotely through Tailscale Serve. Uploaded jobs, logs, and manager state are stored under `/mnt/data/simulator-manager/` by default. Set `SIM_MANAGER_DATA_DIR` to change this location.

Configuration is available through environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `SIM_MANAGER_HOST` | `127.0.0.1` | Web Manager listen address |
| `SIM_MANAGER_PORT` | `8080` | Web Manager HTTP port |
| `SIM_MAX_CONCURRENT` | `2` | Maximum simultaneous simulations |
| `SIM_UPLOAD_MAX_MB` | `512` | Maximum compressed package size |
| `SIM_ARCHIVE_MAX_MB` | `2048` | Maximum expanded package size |
| `SIM_RETENTION_DAYS` | `30` | Days to keep finished simulations, including their workspaces and artifacts; `0` disables automatic deletion |
| `SIM_CLEANUP_INTERVAL_HOURS` | `24` | Hours between cleanup runs; `0` disables the scheduled cleanup |
| `SIM_LOG_THRESHOLD_GB` | available space on the data filesystem | Maximum combined archived log/artifact storage before the manager pauses starting queued simulations; `0` disables the limit |

The manager keeps at most 200 task records. At startup and before accepting an upload, it removes the oldest finished tasks as needed, including their workspaces and artifacts. Queued and active tasks are preserved; if they leave no finished task available to remove, a new upload is rejected until a task is deleted or finishes. The task section shows the current record count.

Each simulation card has an **Export logs and ROS bag** link that downloads a ZIP of archived logs and `.bag` files from that run (the `latest/` duplicate is omitted). The selected task's runtime output is shown in the **工作日誌** section, which also provides a TXT download link. The manager keeps `manager.log` in the ZIP. Automatic cleanup runs at manager startup and then on the configured interval; it removes finished, failed, or stopped jobs past the retention period, including their uploaded workspace and artifacts. Running and queued jobs are kept.

By default, the manager measures available space on the filesystem containing its data directory and uses that as the log budget, so no fixed 20 GiB limit applies. Set `SIM_LOG_THRESHOLD_GB` to an explicit limit, or `0` to disable log-based pausing. When combined per-job `share/logs` data and manager logs reach the limit, queued simulations stay queued until space is available. The manager shows current usage in the page header. Finished tasks can be deleted as a whole from their task card; this removes that task's workspace, logs, and artifacts. Selecting a finished task shows its log ZIP download instead of noVNC. The score section shows each checkpoint's final Judge level in a grid and the Mission Remain Time (five-minute limit minus the Judge-recorded mission duration).

Task scores use `points × mg`, where each passed checkpoint contributes 10 points for Lv 3, 8 for Lv 2, and 5 for Lv 1, and `mg = 1 + (time remaining / 300)`. Tasks with any failed checkpoint are highlighted red; tasks without all checkpoints passed at Lv 3 are highlighted yellow. The task card and score panel show the calculated score.

## Replay

To replay a rosbag created by a simulation, run:

```bash
bash ./replay.sh
```

You can specify the playback speed and start time as arguments:

```bash
bash ./replay.sh <playback_speed> <start_time>
```

## More Information

The Programming Manual explains the following topics in detail:

- WSL2, Ubuntu, and Docker setup
- Submitting programs to the Web simulator and checking results
- Detailed instructions for using the local simulator
- Sample program structure and editing procedures
- ROS APIs, movement, camera, and checkpoint processing
- Log design, error handling, and AR marker recognition

For details, see [docs/ProgrammingManual.md](docs/ProgrammingManual.md).

