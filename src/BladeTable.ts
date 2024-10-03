import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { BillingMode, ProvisionedThroughput } from "@aws-sdk/client-dynamodb";
import { Blade } from "./Blade";
import { BladeError } from "./BladeError";
import { BladeDocument } from "./BladeDocument";
import { BladeField, TypeKey } from "./BladeField";
import { BladeCollection } from "./BladeCollection";
import { BladeIndex, BladeIndexOption } from "./BladeIndex";
import { BladeResult, BladeView, KeyFilter } from "./BladeView";
import {
  BladeAttribute,
  BladeAttributeSchema,
  TypeFromBladeField,
} from "./BladeAttribute";
import { getFieldKind, MergeField, RecordOfBladeItem } from "./BladeUtility";

export interface BladeTableOption {
  keySchema: Record<string, BladeField>;
  attribute: Record<string, BladeAttribute<BladeAttributeSchema>>;
  index?: Record<string, BladeIndex<BladeIndexOption>>;
  billingMode?: BillingMode;
  provisionedThroughput?: ProvisionedThroughput;
}

export type IndexKeyFilter<Index extends BladeIndex<BladeIndexOption>> = {
  [Key in keyof Index["option"]["keySchema"]]?:
    | {
        condition: KeyFilter;
        value: TypeFromBladeField<Index["option"]["keySchema"][Key]>;
      }
    | BladeDocument<BladeAttribute<BladeAttributeSchema>>;
};

export class BladeTable<Option extends BladeTableOption> {
  public readonly name: string;
  public readonly option: Option;

  public namePrefix: string;
  public client: DynamoDBDocumentClient;

  constructor(name: string, option: Option) {
    this.name = name;
    this.option = option;

    if (!this.option.billingMode) {
      this.option.billingMode = "PROVISIONED";
    }

    if (
      this.option.billingMode === "PROVISIONED" &&
      !this.option.provisionedThroughput
    ) {
      this.option.provisionedThroughput = {
        ReadCapacityUnits: 20,
        WriteCapacityUnits: 15,
      };
    }

    const hashKey = getFieldKind(this.option.keySchema, "HashKey").at(0);
    if (!hashKey) throw new BladeError("NO_HASHKEY", "No HASH field is set");

    const typeField = getFieldKind(this.option.keySchema, "TypeKey").at(0);
    if (!typeField) {
      this.option.keySchema["_tk"] = TypeKey();
    }
  }

  open<T extends string & keyof Option["attribute"]>(type: T) {
    return new BladeCollection<Option["attribute"][T]>(
      new Blade<BladeTable<Option>>(this).open(type)
    );
  }

  query<T extends string & keyof Option["index"]>(index: T) {
    return {
      where: <Index extends Option["index"][T]>(key: IndexKeyFilter<Index>) => {
        const blade = new Blade<BladeTable<Option>>(this);
        for (const k in key) {
          if (key[k] instanceof BladeDocument) {
            blade.whereIndexKey(index, k, "=", key[k].getKey("HashKey"));
          } else {
            blade.whereIndexKey(index, k, key[k].condition, key[k].value);
          }
        }

        return new BladeView<
          MergeField<Index["option"]["attribute"]>,
          BladeResult<RecordOfBladeItem<Index["option"]["attribute"]>>
        >(blade, { count: 0, data: {} as any });
      },
    };
  }

  scan<T extends string & keyof Option["index"]>(index: T) {
    return new BladeView<
      MergeField<Option["index"][T]["option"]["attribute"]>,
      BladeResult<RecordOfBladeItem<Option["index"][T]["option"]["attribute"]>>
    >(new Blade<BladeTable<Option>>(this).setIndex(index), {
      count: 0,
      data: {} as any,
    });
  }
}
