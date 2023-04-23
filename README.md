# Wargaming Test Assignment

## Installation

1. Clone the repository:

```sh
git clone https://github.com/sviperm/wargaming-test-assignment.git
```

2. Install the required dependencies:

```sh
pip install -r requirements.txt
```

3. Create `.env` file as `example.env` provided

4. (Optional) Install `gloud` locally and save you credential key if you read from GCP

5. (Optional) Create PostgreSQL database with docker using

```sh
docker-compose up
```

## Usage

The main entry point of the ETL pipeline is the `main.py` script. You can run the script with the following command:

```sh
python main.py
```

### Command-line Arguments

The script accepts the following command-line arguments:

- `--input`: The input file path to process. Default is `'gs://wg_test_assignment/events.json'`.
- `--batch_size`: The batch size for writing results to PostgreSQL. Default is `1000`.

For example, to process a different input file and use a batch size of 500, run:

```
python main.py --input path/to/events.json --batch_size 500
```

## Contributing

If you want to contribute to this project, please feel free to create a pull request or open an issue.
