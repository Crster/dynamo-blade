import BladeSchema from "./BladeSchema";

export default interface BladeOption {
  tableName: string;
  schema: Record<string, BladeSchema<any>>;
}
