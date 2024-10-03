import {
  OnDemandThroughput,
  Projection,
  ProvisionedThroughput,
} from "@aws-sdk/client-dynamodb";
import { BladeField } from "./BladeField";
import { BladeAttribute, BladeAttributeSchema } from "./BladeAttribute";

export type BladeIndexType = "LOCAL" | "GLOBAL";

export interface BladeIndexOption {
  keySchema: Record<string, BladeField>;
  attribute?: Record<string, BladeAttribute<BladeAttributeSchema>>;
  projection?: Projection;
  provisionedThroughput?: ProvisionedThroughput;
  onDemandThroughput?: OnDemandThroughput;
}

export class BladeIndex<Option extends BladeIndexOption> {
  public readonly type: BladeIndexType;
  public readonly option: Option;

  constructor(type: BladeIndexType, option: Option) {
    this.type = type;
    this.option = option;

    if (!this.option.projection) {
      this.option.projection = { ProjectionType: "ALL" };
    }
  }
}
