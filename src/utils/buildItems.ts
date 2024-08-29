import { QueryCommandOutput } from "@aws-sdk/lib-dynamodb";
import { CollectionName, Option } from "../BladeType";
import { encodeNext } from "./buildNext";

export default function buildItems<
  Opt extends Option,
  Collection extends string & CollectionName<Opt>
>(option: Opt, collection: Collection, result: QueryCommandOutput) {
  return {
    items: result.Items.map((ii) => option.schema[collection].getItem(ii)),
    next: encodeNext(result.LastEvaluatedKey),
  };
}
