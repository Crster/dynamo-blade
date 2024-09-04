import BladeTable from "./BladeTable";
export default interface BladeOption {
  tableName: string;
  schema: Record<string, BladeTable<any, any>>;
}
