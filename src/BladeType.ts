import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import BladeSchema from "./BladeSchema";

export type SchemaType =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | DateConstructor
  | BufferConstructor;

export interface SchemaDefinition {
  type: SchemaType | ArrayConstructor | SetConstructor | MapConstructor;
  itemType?: SchemaType;
  value?: (value: any) => any;
}

export type Option = {
  tableName: string;
  client: DynamoDBDocumentClient;
  primaryKey: {
    hashKey: [string, SchemaType];
    sortKey?: [string, SchemaType];
    separator?: string;
  };
  localIndex?: Record<string, BladeSchema<any, any>>;
  globalIndex?: Record<string, BladeSchema<any, any>>;
  schema: Record<string, BladeSchema<any, any>>;
};

export type CollectionName<Opt extends Option> = keyof Opt["schema"];

export type CollectionSchema<
  Opt extends Option,
  Collection extends CollectionName<Opt>
> = Opt["schema"][Collection]["attributes"];

export type CollectionSchemaKey<
  Opt extends Option,
  Collection extends CollectionName<Opt>
> = Record<Opt["schema"][Collection]["keyAttributes"][number], any>;

export type BladeItemType<T extends SchemaType> =
  T extends StringConstructor ? string : 
  T extends NumberConstructor ? number :
  T extends BooleanConstructor ? boolean :
  T extends DateConstructor ? Date :
  T extends BufferConstructor ? Buffer : never;

export type BladeItem<Schema extends BladeSchema<any, any>> = {
  [key in keyof Schema["attributes"]]: 
    Schema["attributes"][key] extends StringConstructor ? string : 
    Schema["attributes"][key] extends NumberConstructor ? number :
    Schema["attributes"][key] extends BooleanConstructor ? boolean :
    Schema["attributes"][key] extends DateConstructor ? Date :
    Schema["attributes"][key] extends BufferConstructor ? Buffer :
    Schema["attributes"][key] extends SchemaDefinition ?
    Schema["attributes"][key]["type"] extends StringConstructor ? string :
    Schema["attributes"][key]["type"] extends NumberConstructor ? number :
    Schema["attributes"][key]["type"] extends BooleanConstructor ? boolean :
    Schema["attributes"][key]["type"] extends DateConstructor ? Date :
    Schema["attributes"][key]["type"] extends BufferConstructor ? Buffer :
    Schema["attributes"][key]["type"] extends ArrayConstructor ? Array<BladeItemType<Schema["attributes"][key]["itemType"]>> :
    Schema["attributes"][key]["type"] extends SetConstructor ? Set<BladeItemType<Schema["attributes"][key]["itemType"]>> :
    Schema["attributes"][key]["type"] extends MapConstructor ? Map<string, BladeItemType<Schema["attributes"][key]["itemType"]>> :
    never : never;
};

export type BillingMode = "PROVISIONED" | "PAY_PER_REQUEST";

export type ValueFilter =
  | "="
  | "!="
  | ">"
  | ">="
  | "<"
  | "<="
  | "BETWEEN"
  | "BEGINS_WITH"
  | "IN";

export type DataFilter =
  | "ATTRIBUTE_EXISTS"
  | "ATTRIBUTE_NOT_EXISTS"
  | "ATTRIBUTE_TYPE"
  | "CONTAINS"
  | "SIZE"
  | "SIZE_GT"
  | "SIZE_LT";

export type QueryResult<T> = { items: Array<T>; next?: string };

export type RemoveField<T> = {
  [P in keyof T]?: boolean;
};

export type UpdateValue<T> =
  | Partial<T>
  | { $add: Partial<T> }
  | { $set: Partial<T> }
  | { $remove: RemoveField<T> }
  | { $delete: Partial<T> };

export type Condition = {
  field: string;
  condition: ValueFilter | DataFilter;
  value?: any;
};
