import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { Blade } from "./Blade";
import { BladeView, KeyFilter } from "./BladeView";
import { BladeCollection } from "./BladeCollection";
import { BladeAttribute, BladeAttributeSchema } from "./BladeAttribute";
import { BladeField } from "./BladeField";
import { BladeIndex, BladeIndexOption } from "./BladeIndex";
import { BillingMode, ProvisionedThroughput } from "@aws-sdk/client-dynamodb";

export interface BladeTableOption {
  keySchema: Record<string, BladeField>;
  attribute: Record<string, BladeAttribute<BladeAttributeSchema>>;
  index?: Record<string, BladeIndex<BladeIndexOption>>;
  billingMode?: BillingMode;
  provisionedThroughput?: ProvisionedThroughput;
}

export class BladeTable<Option extends BladeTableOption> {
  public readonly name: string;
  public readonly option: Option;

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
  }

  open<T extends string & keyof Option["attribute"]>(type: T) {
    return new BladeCollection<Option["attribute"][T]>(
      new Blade<BladeTable<Option>>(this).open(type)
    );
  }

  query<T extends string & keyof Option["index"]>(index: T) {
    return {
      where: (key: Record<string, [KeyFilter, any]>) => {
        const blade = new Blade<BladeTable<Option>>(this);
        for (const k in key) {
          blade.whereIndexKey(index, k, key[k][0], key[k][1]);
        }

        return new BladeView(blade);
      },
    };
  }

  scan<T extends string & keyof Option["index"]>(index: T) {
    return new BladeView(new Blade<BladeTable<Option>>(this).setIndex(index));
  }
}
