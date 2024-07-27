import { Condition } from "../BladeType";

export default function buildCondition(
  expressionAttributeName: Map<string, string>,
  expressionAttributeValues: Map<string, string>,
  conditions?: Array<Condition>
) {
  const conditionExpressions = [];

  if (conditions && Array.isArray(conditions) && conditions.length > 0) {
    for (let xx = 0; xx < conditions.length; xx++) {
      const condition = conditions[xx];

      switch (condition.condition) {
        case "=":
          conditionExpressions.push(`#conField${xx} = :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "!=":
          conditionExpressions.push(`#conField${xx} <> :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "<":
          conditionExpressions.push(`#conField${xx} < :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "<=":
          conditionExpressions.push(`#conField${xx} <= :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case ">":
          conditionExpressions.push(`#conField${xx} > :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case ">=":
          conditionExpressions.push(`#conField${xx} >= :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "BETWEEN":
          if (Array.isArray(condition.value) && condition.value.length === 2) {
            conditionExpressions.push(
              `#conField${xx} BETWEEN :conValueFrom${xx} AND :conValueTo${xx}`
            );
            expressionAttributeName.set(`#conField${xx}`, condition.field);
            expressionAttributeValues.set(
              `:conValueFrom${xx}`,
              condition.value.at(0)
            );
            expressionAttributeValues.set(
              `:conValueTo${xx}`,
              condition.value.at(1)
            );
          }
          break;
        case "IN":
          if (Array.isArray(condition.value)) {
            const values: Array<any> = [];
            condition.value.forEach((val, index) => {
              values.push(`:conValue${xx}${index}`);
              expressionAttributeValues.set(`:conValue${xx}${index}`, val);
            });

            conditionExpressions.push(
              `#conField${xx} IN (${condition.value.join(",")})`
            );
            expressionAttributeName.set(`#conField${xx}`, condition.field);
          }
          break;
        case "BEGINS_WITH":
          conditionExpressions.push(
            `begins_with(#conField${xx}, :conValue${xx})`
          );
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "ATTRIBUTE_EXISTS":
          conditionExpressions.push(`attribute_exists(#conField${xx})`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          break;
        case "ATTRIBUTE_NOT_EXISTS":
          conditionExpressions.push(`attribute_not_exists(#conField${xx})`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          break;
        case "ATTRIBUTE_TYPE":
          conditionExpressions.push(
            `attribute_type(#conField${xx}, :conValue${xx})`
          );
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "CONTAINS":
          conditionExpressions.push(`contains(#conField${xx}, :conValue${xx})`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "SIZE":
          conditionExpressions.push(`size(#conField${xx}) = :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "SIZE_GT":
          conditionExpressions.push(`size(#conField${xx}) < :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
        case "SIZE_LT":
          conditionExpressions.push(`size(#conField${xx}) > :conValue${xx}`);
          expressionAttributeName.set(`#conField${xx}`, condition.field);
          expressionAttributeValues.set(`:conValue${xx}`, condition.value);
          break;
      }
    }
  }

  if (conditionExpressions.length > 0) {
    return conditionExpressions.join(" AND ");
  }

  return undefined;
}
