import { APIGatewayProxyEvent, Callback, Context } from "aws-lambda";
import { failure, success } from "./common/responses";

import {
  IIndex, IIndexDictionary, IIndexUpdate, IPutIndexRequest,
} from "./types";

import { AWSError, DynamoDB } from "aws-sdk";
import {
  DocumentClient as DocClient,
} from "aws-sdk/lib/dynamodb/document_client.d";
import { PromiseResult } from "aws-sdk/lib/request";
import { BatchWriteItemOutput } from "aws-sdk/clients/dynamodb";

export const TAGS_ID = "tags";
export const PEOPLE_ID = "people";
export const INDEX_KEYS_PROP = "indexKeys";


const dynamodb = new DynamoDB.DocumentClient({});


const defaultIndex: IIndex = {
  people: {},
  tags: {},
};

const getIndexTableName = (): string => {
  if (process.env.DYNAMODB_TABLE_INDEXES) {
    return process.env.DYNAMODB_TABLE_INDEXES;
  } else {
    throw new Error("No DYNAMODB_TABLE_INDEXES env variable set");
  }
};

export function getDynamoDbBatchGetItemParams(): DocClient.BatchGetItemInput {
  return {
    RequestItems: {
      [getIndexTableName()]: {
        Keys: [{
          id: TAGS_ID,
        },
        {
          id: PEOPLE_ID,
        }],
      },
    },
  };
}

export function getIndexRecords(
  ddbParams: DocClient.BatchGetItemInput,
): Promise<PromiseResult<DocClient.BatchGetItemOutput, AWSError>> {
  return dynamodb.batchGet(ddbParams).promise();
}

export function parseIndexesObject(ddbResponse: DocClient.BatchGetItemOutput): IIndex {
  const indexes: IIndex = {
    ...defaultIndex,
  };
  if (ddbResponse.Responses) {
    const tagsRecord = ddbResponse.Responses[getIndexTableName()]
      .find((response) => response.id === TAGS_ID);
    indexes.tags = tagsRecord && tagsRecord[INDEX_KEYS_PROP] ? tagsRecord[INDEX_KEYS_PROP] : indexes.tags;
    const peopleRecord = ddbResponse.Responses[getIndexTableName()]
      .find((response) => response.id === PEOPLE_ID);
    indexes.people = peopleRecord && peopleRecord[INDEX_KEYS_PROP] ? peopleRecord[INDEX_KEYS_PROP] : indexes.people;
  }
  return indexes;
}

export function updateCleanIndexes(indexObject: IIndex): Promise<PromiseResult<BatchWriteItemOutput, AWSError>> {
  const ddbParams: DocClient.BatchWriteItemInput = {
    RequestItems: {
      [getIndexTableName()]: [
        {
          PutRequest: {
            Item: {
              id: TAGS_ID,
              [INDEX_KEYS_PROP]: {
                ...indexObject.tags
              }
            }
          }
        },
        {
          PutRequest: {
            Item: {
              id: PEOPLE_ID,
              [INDEX_KEYS_PROP]: {
                ...indexObject.people
              }
            }
          }
        }
      ]
    }
  }
  return dynamodb.batchWrite(ddbParams).promise();
}

export function cleanZeroIndexes(indexObject: IIndex): Promise<IIndex> | IIndex {
  const cleanedIndex: IIndex = {
    ...defaultIndex,
  };
  let updateNeeded = false;
  Object.keys(indexObject.people).forEach((p) => {
    if (indexObject.people[p] <= 0) {
      updateNeeded = true;
    } else {
      cleanedIndex.people[p] = indexObject.people[p];
    }
  });

  Object.keys(indexObject.tags).forEach((t) => {
    if (indexObject.tags[t] <= 0) {
      updateNeeded = true;
    } else {
      cleanedIndex.tags[t] = indexObject.tags[t];
    }
  });
  return updateNeeded ?
    updateCleanIndexes(cleanedIndex)
      .then(() => cleanedIndex) :
    cleanedIndex;
}

// get each index (if no record then create one)
export async function getItem(event: APIGatewayProxyEvent, context: Context, callback: Callback): Promise<void> {
  // change to batch get items
  try {
    const ddbParams: DocClient.BatchGetItemInput = getDynamoDbBatchGetItemParams();
    const ddbResponse: DocClient.BatchGetItemOutput = await getIndexRecords(ddbParams);
    const indexesObject: IIndex = parseIndexesObject(ddbResponse);
    const cleanZeroIndexesObject: IIndex = await cleanZeroIndexes(indexesObject);
    return callback(null, success({
      ddbParams,
      ddbResponse,
      indexesObject,
      cleanZeroIndexesObject
    }));
  } catch (err) {
    console.error(err);
    return callback(null, failure(err));
  }
}

export function getDynamoDbUpdateItemParams(
  indexId: string,
  indexData: IIndexDictionary,
): DocClient.UpdateItemInput | null {
  const timestamp = new Date().getTime();
  const validKeys =  Object.keys(indexData).filter((k) => indexData[k] !== undefined);
  if (validKeys.length > 0) {
    /*
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
    "If an attribute name begins with a number or contains a space, a special character,
    or a reserved word, you must use an expression attribute name to replace that attribute's
    name in the expression."

    So because tags could be anything, we use idx
    */
    const ExpressionAttributeNames = validKeys.reduce((accum, key, idx) => ({
      ...accum,
      [`#${idx}`]: key,
      [`#indexKeysProp`]: INDEX_KEYS_PROP,
    }), {});

    const ExpressionAttributeValues = validKeys.reduce((accum, key, idx) => ({
      ...accum,
      [`:${idx}`]: indexData[key],
    }), {
      ":updatedAt": timestamp,
      ":zero": 0,
    });
    const updateKeyValues = validKeys.map((key, idx) => `#indexKeysProp.#${idx} = if_not_exists(#indexKeysProp.#${idx},:zero) + :${idx}`).join(", ");
    return {
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      Key: {
        id: indexId,
      },
      ReturnValues: "ALL_NEW",
      TableName: getIndexTableName(),
      UpdateExpression: `SET ${updateKeyValues}, updatedAt = :updatedAt`,
    };
  } else {
    return null;
  }
}

export function updateAndHandleEmptyMap(ddbParams: DocClient.UpdateItemInput): Promise<DocClient.UpdateItemOutput> {
  return dynamodb.update(ddbParams).promise()
  .catch((e) => {
    // this check may be a bit too brittle, but we want to only retry on this very specific error
    if (e.code === "ValidationException"
      && e.message === "The document path provided in the update expression is invalid for update"
    ) {
      const setMapParams: DocClient.UpdateItemInput = {
        ConditionExpression: "attribute_not_exists(#indexKeysProp)",
        ExpressionAttributeNames: {
          [`#indexKeysProp`]: INDEX_KEYS_PROP,
        },
        ExpressionAttributeValues: {
          ":emptyMap": {},
        },
        Key: ddbParams.Key,
        TableName: ddbParams.TableName,
        UpdateExpression: "SET #indexKeysProp = :emptyMap",
      };
      return dynamodb.update(setMapParams).promise()
        .then(() => {
          // now that the map has been created try the update again
          return dynamodb.update(ddbParams).promise();
        });
    } else {
      throw e;
    }
  });
}

export function updateIndexRecord(
  indexId: string,
  indexData: IIndexDictionary,
): Promise<DocClient.UpdateItemOutput> | undefined {
  const ddbParams: DocClient.UpdateItemInput | null = getDynamoDbUpdateItemParams(indexId, indexData);
  return ddbParams ? updateAndHandleEmptyMap(ddbParams) : undefined ;
}

export async function putItem(event: APIGatewayProxyEvent, context: Context, callback: Callback): Promise<void> {
  try {
    const requestBody: IPutIndexRequest = JSON.parse(event.body!) ;
    // update each index - change to batch write item
    const tagsUpdateResponse: DocClient.UpdateItemOutput | undefined =
      await updateIndexRecord(TAGS_ID, requestBody.indexUpdate.tags);
    const peopleUpdateResponse: DocClient.UpdateItemOutput | undefined =
      await updateIndexRecord(PEOPLE_ID, requestBody.indexUpdate.people);
    return callback(null, success({
      eventBody: event.body,
      requestBody,
      tagsUpdateResponse,
      peopleUpdateResponse
    }));
  } catch (err) {
    console.error(err, event.body);
    return callback(null, failure(err));
  }
}
