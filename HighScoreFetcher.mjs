import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand, ScanCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({ region: "us-east-1" });
const documentClient = DynamoDBDocumentClient.from(client);

const SCORE_TABLE_NAME_32 = "2048HighScores32";
const SCORE_TABLE_NAME_64 = "2048HighScores64";
const SCORE_TABLE_NAME_128 = "2048HighScores128";
const SCORE_TABLE_NAME_256 = "2048HighScores256";
const SCORE_TABLE_NAME_512 = "2048HighScores512";
const SCORE_TABLE_NAME_1024 = "2048HighScores1024";
const SCORE_TABLE_NAME_2048 = "2048HighScores2048";

export const handler = async (event) => {
  console.log(event.queryStringParameters);

  const id = event.queryStringParameters.id;
  const tableType = event.queryStringParameters.type;
  
  console.log("id is " + id);

  let SCORE_TABLE_NAME;

  switch (tableType) {
    case "32":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_32;
      break;
    case "64":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_64;
      break;
    case "128":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_128;
      break;
    case "256":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_256;
      break;
    case "512":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_512;
      break;
    case "1024":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_1024;
      break;
    case "2048":
      SCORE_TABLE_NAME = SCORE_TABLE_NAME_2048;
      break;
    default:
      return sendResponse(500, { op: "POST", status: "FORBIDDEN" });
  }

  // Send the player the top 100 entries and their own rank
  let minScore = 0;
  let allScoreData = await scanData(SCORE_TABLE_NAME, "score >= :minScore", { ":minScore": minScore }, "id, pName, score");

  allScoreData.Items.sort(sortByScore);

  let topScoreData = allScoreData.Items.slice(0, 10);

  console.log("Total list size: " + allScoreData.Items.length);
  console.log("Top scorers:", topScoreData);

  const topScoreDataWithoutIds = topScoreData.map(({ pName, score }) => ({ pName, score }));

  let rank = -1;
  let personalTopScore = -1;

  for (let i = 0; i < allScoreData.Items.length; i++) {
    if (i === 500) break; // No matching rank found within the first 500 entries

    if (allScoreData.Items[i].id === id) {
      rank = i + 1;
      personalTopScore = allScoreData.Items[i].score;
      console.log("Rank for player " + id + " is " + rank + " and their score is " + personalTopScore);
      break;
    }
  }

  return sendResponse(200, {
    op: "GET",
    status: "OK",
    topScores: topScoreDataWithoutIds,
    ownRank: rank,
    ownTopScore: personalTopScore
  });
};

function sortByScore(a, b) {
  return a.score < b.score ? -1 : 1;
}

// GENERAL DATA FETCH FUNCTION FOR A SINGLE ENTRY
async function getData(tableName, id, projectionExpression = null) {
  const command = new GetCommand({
    TableName: tableName,
    Key: {
      id: id,
    },
    ProjectionExpression: projectionExpression,
  });
  try {
    return documentClient.send(command);
  } catch (err) {
    console.log("Error getting data from " + tableName + ": " + err);
    throw err;
  }
}

// GENERAL SCAN FUNCTION FOR GETTING DATA
async function scanData(tableName, filterExpression, expressionAttributeValues, projectionExpression = "id") {
  const command = new ScanCommand({
    TableName: tableName,
    FilterExpression: filterExpression,
    ExpressionAttributeValues: expressionAttributeValues,
    ProjectionExpression: projectionExpression,
  });
  try {
    return documentClient.send(command);
  } catch (err) {
    console.log("Error getting data about scores: " + err);
    throw err;
  }
}

// GENERAL QUERY FUNCTION FOR GETTING DATA
async function queryData(tableName, keyConditionExpression, expressionAttributeValues, projectionExpression = "id") {
  const command = new QueryCommand({
    TableName: tableName,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: expressionAttributeValues,
    ProjectionExpression: projectionExpression,
    ReturnConsumedCapacity: "TOTAL",
  });
  try {
    return documentClient.send(command);
  } catch (err) {
    console.log("Error getting data about scores: " + err);
    throw err;
  }
}

// GENERAL DATA UPDATE FUNCTION FOR UPDATING A SINGLE ENTRY
async function updateData(tableName, id, updateExpression, attributeValues) {
  const command = new UpdateCommand({
    TableName: tableName,
    Key: {
      id: id,
    },
    UpdateExpression: updateExpression,
    ExpressionAttributeValues: attributeValues,
  });
  try {
    return documentClient.send(command);
  } catch (err) {
    console.log("Error updating data " + err);
    throw err;
  }
}

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    let j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

function getCurrentEpochTime() {
  const d = new Date();
  return Math.floor(d / 1000);
}

// Create a response
function sendResponse(statusCode, message) {
  return {
    statusCode: statusCode,
    body: JSON.stringify(message),
  };
}
