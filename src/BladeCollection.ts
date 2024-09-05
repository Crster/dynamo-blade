import { DynamoBladeCollectionType, DynamoBladeItemType, DynamoBladeOption } from "./DynamoBlade";

export interface BladeCollectionSchemaAttribute {
  type: DynamoBladeItemType & DynamoBladeCollectionType,
  itemType?: DynamoBladeItemType,
  
}

export type BladeCollectionSchemaAttributes = Record<string, DynamoBladeItemType>

export interface BladeCollectionSchema<
  Schema extends BladeCollectionSchemaAttributes
> {
  key: {
    hashKey: (ii: Schema) => any;
    sortKey?: (ii: Schema) => any;
  };
  attribute: Schema;
}

export default class BladeCollection<Schema extends BladeCollectionSchemaAttributes>{
  private readonly option: DynamoBladeOption;
  private readonly collection: string;
  private readonly schema: BladeCollectionSchema<Schema>;

  constructor(option: DynamoBladeOption, collection: string, schema: BladeCollectionSchema<Schema>) {
    this.option = option;
    this.collection = collection;
    this.schema = schema;
  }

  get(key: any) {
    return {} as Schema;
  }

  add() {}

  update() {}

  remove() {}
}
