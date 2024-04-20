const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition, joinType } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);
    data = await join(data, joinTable, joinCondition, fields, table, joinType);
    const filteredData = data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    return select(filteredData, fields);
}

function select(data, fields) {
    return data.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

async function join(data, joinTable, joinCondition, fields, table, type="INNER") {
    if (!joinTable || !joinCondition) return data;
    const joinData = await readCSV(`${joinTable}.csv`);
    if(type==="RIGHT") return join(joinData, table, joinCondition, fields, joinTable, "LEFT");
    const result = [];
    const [leftField, rightField] = getJoinFields(joinCondition, table, joinTable);
    for (const mainRow of data) {
        const mainValue = mainRow[leftField];
        let atLeastOne = false;
        for (const joinRow of joinData) {
            const joinValue = joinRow[rightField];
            if (mainValue === joinValue) {
                result.push(buildRow(mainRow, joinRow, table, fields));
                atLeastOne = true;
            }
        }
        if(!atLeastOne && type === "LEFT") result.push(buildRow(mainRow, {}, table, fields));
    }
    return result;
}

function getJoinFields(joinCondition, table1, table2) {
    const [t1, v1] = joinCondition.left.split('.');
    const [t2, v2] = joinCondition.right.split('.');
    return table1 == t1 && table2 == t2 ? [v1, v2] : [v2, v1];
}

function buildRow(mainRow, joinRow, table, fields) {
    const row = {};

    for (const field of fields) {
        const [tableName, fieldName] = field.split('.');
        row[field] = (tableName === table ? mainRow[fieldName] : joinRow[fieldName]) || null;
    }

    return row;
}

module.exports = executeSELECTQuery;
