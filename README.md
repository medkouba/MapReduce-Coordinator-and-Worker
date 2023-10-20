# MapReduce Coordinator and Worker

This project is an implementation of a simple MapReduce framework with a Coordinator and Worker components. MapReduce is a programming model and an associated implementation for processing and generating large datasets that can be parallelized across a distributed cluster of computers.

## Table of Contents

- [Introduction](#introduction)
- [Coordinator](#coordinator)
- [Worker](#worker)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project consists of two main components:

- **Coordinator**: The Coordinator is responsible for managing the distribution of tasks to the Workers. It reads input data from a file, divides it into smaller blobs, and assigns them to Workers for processing. It then collects the results from Workers and generates a final output file.

- **Worker**: The Worker is responsible for performing the Map and Reduce tasks. It receives data blobs from the Coordinator, processes them, and returns the results. Workers can be run in parallel, and the results are aggregated by the Coordinator.

The project demonstrates a simple MapReduce model by processing text data to perform word count and generate a word frequency histogram.

## Coordinator

The Coordinator component contains the following functionalities:

- Reading input data from a file.
- Dividing data into smaller blobs.
- Distributing Map and Reduce tasks to Workers.
- Collecting and aggregating results from Workers.
- Writing the final output to a CSV file.

## Worker

The Worker component contains the following functionalities:

- Registering with the Coordinator.
- Receiving Map and Reduce tasks from the Coordinator.
- Processing data and performing word count.
- Sending results back to the Coordinator.

## Usage

To run the MapReduce program, you need to execute the Coordinator and one or more Workers. Here's how to use the project:

1. Run the Coordinator:
   python coordinator.py -f <input_file> -o <output_file>
-f: Path to the input file.
-o: Path to the output file (default is 'output.csv').
Run the Worker(s):


python worker.py --id <worker_id> --hostname <coordinator_hostname> --port <coordinator_port>
--id: Worker's ID.
--hostname: Coordinator's hostname (default is 'localhost').
--port: Coordinator's port (default is 8765).
Follow the instructions to specify the number of workers.

The Coordinator will distribute tasks to Workers, and the Workers will process the data.

Once the job is completed, the Coordinator will generate the output file.

Dependencies
This project uses the following dependencies:

Python 3.x
No external libraries are required.

Contributing
Contributions to this project are welcome. You can contribute by:

Reporting issues or bugs.
Suggesting new features or improvements.
Providing pull requests to enhance the code.
Please follow the project's coding style and guidelines when contributing.

License
This project is licensed under the MIT License.

Feel free to use and modify this project according to your needs. Make sure to acknowledge the original source if you choose to distribute your modified version.

