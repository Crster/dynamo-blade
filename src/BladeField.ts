import {
  ScalarType,
  BladeAttribute,
  BladeAttributeSchema,
  EventHandler,
} from "./BladeAttribute";

export type BladeFieldKind =
  | "PrimaryKey"
  | "HashKey"
  | "SortKey"
  | "TypeKey"
  | "OnCreate"
  | "OnModify"
  | "List"
  | "Set"
  | "Document"
  | "Scalar";

export interface BladeField {
  kind: BladeFieldKind;
  type: ScalarType | BladeAttribute<BladeAttributeSchema> | EventHandler;
  required?: boolean;
}

export function OptionalField<Type extends ScalarType>(type: Type) {
  return { kind: "Scalar", type, required: false } as const;
}

export function RequiredField<Type extends ScalarType>(type: Type) {
  return { kind: "Scalar", type, required: true } as const;
}

export function SetField<Type extends ScalarType, Required extends boolean>(
  itemType: Type,
  required?: Required
) {
  return { kind: "Set", type: itemType, required } as const;
}

export function ListField<
  Schema extends BladeAttribute<BladeAttributeSchema>,
  Required extends boolean
>(schema: Schema, required?: Required) {
  return { kind: "List", type: schema, required } as const;
}

export function DocumentField<
  Schema extends BladeAttribute<BladeAttributeSchema>,
  Required extends boolean
>(schema: Schema, required?: Required) {
  return { kind: "Document", type: schema, required } as const;
}

export function PrimaryKey<Type extends ScalarType>(type: Type) {
  return { kind: "PrimaryKey", type, required: true } as const;
}

export function HashKey<Type extends ScalarType>(type: Type) {
  return { kind: "HashKey", type, required: true } as const;
}

export function SortKey<Type extends ScalarType>(type: Type) {
  return { kind: "SortKey", type, required: false } as const;
}

export function TypeKey() {
  return { kind: "TypeKey", type: String, required: false } as const;
}

export function CreatedOn() {
  return {
    kind: "OnCreate",
    type: () => new Date().toISOString(),
    required: true,
  } as const;
}

export function ModifiedOn() {
  return {
    kind: "OnModify",
    type: () => new Date().toISOString(),
    required: true,
  } as const;
}

export function Override<Handler extends EventHandler>(handler: Handler) {
  return { kind: "OnModify", type: handler, required: false } as const;
}

export function Default<Handler extends EventHandler>(handler: Handler) {
  return { kind: "OnCreate", type: handler, required: false } as const;
}
