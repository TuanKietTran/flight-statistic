# Flight Statistic

## Overview
The Flight Statistic project is designed to gather, analyze, and visualize flight data. The primary goal is to help users gain insights into various metrics related to flight operations, such as delays, efficiency, and other critical parameters.

## Features
- Data extraction from multiple flight information sources.
- Data transformation and aggregation for analysis.
- Visualization tools for presenting flight statistics in an understandable format.

## Prerequisites
- **Docker**: Ensure Docker is installed to use the provided Docker setup.
- **Python 3.11+**: The project is built using Python, so make sure a compatible version is installed.

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/TuanKietTran/flight-statistic.git
   cd flight-statistic
   ```

2. **Setup Using Docker**
   - Make sure Docker is installed and running.
   - Use the provided `docker-compose.yaml` to build and start the services:
     ```bash
     docker-compose up --build
     ```

3. **Setup Without Docker**
   - Create a virtual environment and activate it:
     ```bash
     python -m venv venv
     source venv/bin/activate  # On Windows use `venv\Scripts\activate`
     ```
   - Install dependencies:
     ```bash
     pip install -r requirements.txt
     ```

4. **Run the Application**
   - Once dependencies are installed, you can run the main application script:
     ```bash
     python src/main.py
     ```

## Project Structure
- `data/`: Contains datasets used for analysis.
- `src/`: Main source code including data extraction and processing logic.
- `tasks/`: Automation scripts for scheduled data tasks.
- `test/`: Contains unit tests to ensure the quality of the codebase.

## Develop
After setting up, restore the package by running:
```bash
pip install -e .
```
Workflows for automation can be found in the `tasks` folder.

## Contributing
Feel free to fork this repository and submit pull requests. For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License.

