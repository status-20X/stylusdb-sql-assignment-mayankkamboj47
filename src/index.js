const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);
    data = await join(joinTable, joinCondition, data, table, fields);
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

async function join(joinTable, joinCondition, data, table, fields) {
    if (!joinTable || !joinCondition) return data;
    const joinData = await readCSV(`${joinTable}.csv`);
    const result = [];
    const [leftField, rightField] = getJoinFields(joinCondition);
    for (const mainRow of data) {
        const mainValue = mainRow[leftField];
        for (const joinRow of joinData) {
            const joinValue = joinRow[rightField];
            if (mainValue === joinValue) {
                result.push(buildRow(mainRow, joinRow, table, fields));
            }
        }
    }
    return result;
}

function getJoinFields(joinCondition) {
    const left = joinCondition.left.split('.')[1];
    const right = joinCondition.right.split('.')[1];
    return [left, right];
}

function buildRow(mainRow, joinRow, table, fields) {
    const row = {};

    for (const field of fields) {
        const [tableName, fieldName] = field.split('.');
        row[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
    }

    return row;
}

module.exports = executeSELECTQuery;
