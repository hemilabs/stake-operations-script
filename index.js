import hemilabs from "@hemilabs/token-list" with { type: "json" };
import fetch from "fetch-plus-plus";
import pDoWhilst from "p-do-whilst";
import pMinDelay from "p-min-delay";
import { checksumAddress as toChecksum, isAddressEqual } from "viem";
import { hemi, hemiSepolia } from "viem/chains";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import fs from "fs";
import path from "path";

import { getConfig } from "./config.js";

const argv = yargs(hideBin(process.argv)).parse();

const from = argv.from || 0;
const chainType = argv.chain || "mainnet";
const chain = chainType === "mainnet" ? hemi : hemiSepolia;

if (!argv.key || typeof argv.key !== "string") {
  throw new Error("Missing subgraph api key");
}
const apiKey = argv.key;

const config = getConfig(argv.key);

// As awful as it is, graph-node doesn't support cursor pagination (which is the
// recommended way to paginate in graphQL). It also doesn't support a way to natively get
// the amount of Entities saved in the subgraph. Max entities returned per query are 100

const getSubgraphUrl = function (chainName) {
  const subgraphId = config.stake[chainName];
  return `${config.apiUrl}/${apiKey}/subgraphs/id/${subgraphId}`;
};

const request = ({ schema, url }) =>
  // delay 3 seconds the request to avoid overloading the api
  pMinDelay(
    fetch(url, {
      body: JSON.stringify(schema),
      headers: {
        "Content-Type": "application/json",
        Origin: config.origin,
      },
      method: "POST",
    }),
    3000,
  );

const shouldQueryMore = ({ operations }) => operations.length === 100;

const mapInfo = (operations) =>
  operations.map(function (operation) {
    const {
      amount,
      blockNumber,
      blockTimestamp,
      depositor, // only for deposits
      token,
      transactionHash,
      withdrawer, // only for withdrawals
    } = operation;
    const newOperation = {
      account: toChecksum(depositor || withdrawer),
      amount: BigInt(amount),
      blockNumber: BigInt(blockNumber),
      blockTimestamp: new Date(blockTimestamp * 1000).toISOString(),
      tokenAddress: toChecksum(token),
      transactionHash: toChecksum(transactionHash),
      type: depositor ? "deposit" : "withdrawal",
    };

    const { decimals, symbol } =
      hemilabs.tokens.find(
        (t) =>
          isAddressEqual(t.address, newOperation.tokenAddress) &&
          t.chainId === chain.id,
      ) ?? {};

    newOperation.tokenDecimals = decimals;
    newOperation.tokenSymbol = symbol;

    return newOperation;
  });

// filter operations by transactionHash, removing all those whose transactionHash field
// is already present on previousOperations
const removeDuplicates = (previousOperations, operations) =>
  operations.filter(
    (operation) =>
      !previousOperations.some(
        (prevOp) => prevOp.transactionHash === operation.transactionHash,
      ),
  );

const csvFilePath = path.resolve(
  `stake_operations_${new Date().toISOString()}.csv`,
);

// Add headers to the CSV file
const headers = [
  "account",
  "amount",
  "blockNumber",
  "blockTimestamp",
  "tokenAddress",
  "tokenDecimals",
  "tokenSymbol",
  "transactionHash",
  "type",
];
const createCsvFile = () =>
  fs.writeFileSync(csvFilePath, headers.join(",") + "\n", "utf8");

// Utility function to write operations to a CSV file
const writeToCsv = function (operations) {
  const csvLines = operations.map((operation) =>
    headers.map((header) => operation[header]?.toString() ?? " ").join(","),
  );
  if (!operations.length) {
    return;
  }

  const { blockNumber, type } = operations.at(-1);
  console.log(
    "Writing %s %s operations up to block %s",
    operations.length,
    type,
    blockNumber,
  );

  fs.appendFileSync(csvFilePath, csvLines.join("\n") + "\n", "utf8");
};

const requestSubgraph = async function ({ accessor, query, url }) {
  await pDoWhilst(
    async function ({ fromBlock, operations: previousOperations, skip }) {
      const operations = await request({
        schema: {
          query,
          variables: { fromBlock, skip },
        },
        url,
      })
        .then(function (response) {
          if (response.errors) {
            console.error(response.errors);
            throw new Error("Failed to fetch", {
              cause: response.errors.join(", "),
            });
          }
          return accessor(response);
        })
        .then(mapInfo);

      // remove no duplicates, that may appear from updating the "fromBlock". However
      // we should return the original query elements to prevent from skipping options when querying
      // in the next iteration of the loop
      const noDuplicates = removeDuplicates(previousOperations, operations);

      // Write operations to CSV before the next iteration
      writeToCsv(noDuplicates);

      const calculateSkip = function () {
        // it turns out that GraphQL does not allow skip larger than 5000
        // if we hit that limit, we better use a different "fromBlock"
        if (skip <= 4900) {
          return { skip: skip + 100 };
        }
        // if we hit the limit, we need to increase the fromBlock. So grab the last one known as valid
        // and reset the "skip" to 0
        return {
          fromBlock: operations.at(-1).blockNumber.toString(),
          skip: 0,
        };
      };

      return { fromBlock, operations, ...calculateSkip() };
    },
    shouldQueryMore,
    { fromBlock: from, operations: [], skip: 0 },
  );
};

const readStake = async function (url) {
  const query = `query Stake ($fromBlock: BigInt, $skip: Int!) {
      deposits(first: 100, orderBy: blockNumber, orderDirection: asc, skip: $skip, where: { blockNumber_gte: $fromBlock }) {
        amount
        blockNumber,
        blockTimestamp
        depositor
        token
        transactionHash
      }
    }`;

  await requestSubgraph({
    accessor: (response) => response.data.deposits,
    query,
    url,
  });
};

const readUnstake = async function (url) {
  const query = `query Unstake ($fromBlock: BigInt, $skip: Int!) {
      withdraws(first: 100, orderBy: blockNumber, orderDirection: asc, skip: $skip, where: { blockNumber_gte: $fromBlock }) {
        amount
        blockNumber
        blockTimestamp
        withdrawer
        token
        transactionHash
      }
    }`;
  await requestSubgraph({
    accessor: (response) => response.data.withdraws,
    query,
    url,
  });
};

const readStakeOperations = async function () {
  console.info("Starting to collect stake information...");
  const url = getSubgraphUrl(chainType);

  // Call addCsvHeaders before starting the operations
  createCsvFile();

  return Promise.all([readStake(url), readUnstake(url)]);
};

// If it fails, let it blow up
// eslint-disable-next-line promise/catch-or-return
readStakeOperations().then(() =>
  console.info("All stake information saved to %s", csvFilePath),
);
