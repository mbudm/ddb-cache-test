import * as test from "tape";
import * as index from "./index";
import { IIndexDictionary, IIndex } from "./types";

import {
  DocumentClient as DocClient,
} from "aws-sdk/lib/dynamodb/document_client.d";

test("cleanZeroIndexes - removes any negative or zero values ", (t) => {
    const indexObject: IIndex = {
        people: {
            bob: 1,
            cynthia: 0,
        },
        tags: {
            yellow: -1,
            red: 0,
        }
    }

    const result = index.cleanZeroIndexes(indexObject);
    t.deepEqual(result.cleanedIndex.people, { bob: 1 }, "bob unaffected, cynthia removed");
    t.deepEqual(result.cleanedIndex.tags, { }, "both tags removed");
    t.end();
});