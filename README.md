# stake-operations

Script to list all token operations

## Setup

Install the dependencies with

```sh
npm i
```

## Instructions to run

A subgraph API KEY is required. The script can be optionally run from a known block; otherwise, it will run from 0.

```sh
node index.js --key=<KEY> # Optionally, add --from=<block_number>
```

The `.csv` file will be created in the same folder where this runs. Note that each run will create a new csv file.
