export interface IResult<T> {
    item: Record<string, Record<string, T>>,
    next?: string
}