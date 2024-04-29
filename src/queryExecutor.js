const { parseSelectQuery, parseInsertQuery, parseDeleteQuery } = require('./queryParser');
const {readCSV, writeCSV} = require('./csvReader');

async function executeSELECTQuery(query) {
    try {
        const { fields, table, whereClauses, joinTable, joinCondition, joinType, groupByFields, hasAggregateWithoutGroupBy, orderByFields, limit, isDistinct } = parseSelectQuery(query);
        let data = await readCSV(`${table}.csv`);
        data = await join(data, joinTable, joinCondition, fields, table, joinType);
        data = data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)));
        order(data, orderByFields);
        data = applyGroupBy(data, groupByFields, hasAggregateWithoutGroupBy, fields);
        data = select(data, fields)
        if (isDistinct) {
            data = [...new Map(data.map(item => [fields.map(field => item[field]).join('|'), item])).values()];
        }
        if (limit !== null && limit >= 0) return data.slice(0, limit);

        return data;
    } catch (e) {
        throw new Error('Error executing query: '+e.message)
    }
}

async function executeDELETEQuery(query) {
    const {whereClauses, table} = parseDeleteQuery(query);
    let data = await readCSV(table + '.csv');
    let filteredData = data.filter(row => !whereClauses.every(clause => evaluateCondition(row, clause)));
    writeCSV(table + '.csv', filteredData);
}

async function executeINSERTQuery(query) {
    let {columns, values, table} = parseInsertQuery(query);
    if (columns.length !== values.length) 
        throw new Error(`columns.length (${columns.length}) does not equal values.length (${values.length})`);
    let tableData = await readCSV(table + '.csv');
    if(!columns.every(c=>(c in tableData[0]))) 
        throw new Error(`Columns ${c} should be a subset of table columns ${Object.keys(tableData[0])}`)
    let row = Object.fromEntries(Object.keys(table[0]).map(n=>[n, null]));
    for(let i=0;i<columns.length;i++){
        row[columns[i]] = values[i];
    }
    tableData.push(row);
    await writeCSV(table + '.csv', tableData);
}

function select(data, fields) {
    let values = Object.values(data);
    values.sort((a, b) => b.summary.i - a.summary.i);
    return values.map(({ summary, rows }) =>
        fields.every(f => (f in summary)) ?
            [Object.fromEntries(fields.map(f => [f, summary[f]]))] :
            rows.map(r => Object.fromEntries(fields.map(f => [f, summary[f] || r[f]])))
    ).flat();
}

function order(data, orderByFields) {
    if (!orderByFields) return;
    data.sort((a, b) => {
        for (let { fieldName, order } of orderByFields) {
            if (a[fieldName] < b[fieldName]) return order === 'ASC' ? -1 : 1;
            if (a[fieldName] > b[fieldName]) return order === 'ASC' ? 1 : -1;
        }
        return 0;
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
        case 'LIKE':
            const regexPattern = '^' + value.replace(/\%/g, '.*') + '$';
            return new RegExp(regexPattern, 'i').test(row[field]);
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

async function join(data, joinTable, joinCondition, fields, table, type = "INNER") {
    if (!joinTable || !joinCondition) return data;
    const joinData = await readCSV(`${joinTable}.csv`);
    let colnames = Object.keys(joinData[0]).map(n => [joinTable, n]).concat(Object.keys(data[0]).map(n => [table, n]));
    if (type === "RIGHT") return join(joinData, table, joinCondition, fields, joinTable, "LEFT");
    const result = [];
    const [leftField, rightField] = getJoinFields(joinCondition, table, joinTable);
    for (const mainRow of data) {
        const mainValue = mainRow[leftField];
        let atLeastOne = false;
        for (const joinRow of joinData) {
            const joinValue = joinRow[rightField];
            if (mainValue === joinValue) {
                result.push(buildRow(mainRow, joinRow, table, colnames));
                atLeastOne = true;
            }
        }
        if (!atLeastOne && type === "LEFT") result.push(buildRow(mainRow, {}, table, colnames));
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

    for (const [table, field] of fields) {
        row[table + '.' + field] = mainRow[field] || joinRow[field] || null;
    }

    return row;
}

function applyGroupBy(data, groupByFields, hasAggregateWithoutGroupBy, fields) {
    // edge case, this will cause things to break if i is also a column name, as it is overshadowed by an index
    let i = 0;
    let fns = {
        MIN: colname => rows => Math.min(...rows.map(r => Number(r[colname]))),
        MAX: colname => rows => Math.max(...rows.map(r => Number(r[colname]))),
        SUM: colname => rows => rows.map(r => Number(r[colname])).reduce((a, b) => a + b),
        AVG: colname => rows => rows.map(r => Number(r[colname])).reduce((a, b) => a + b) / rows.length,
        COUNT: ____ => rows => rows.length
    }
    let groups = {};
    if (groupByFields === null || groupByFields.length === 0) groups = { 0: { summary: { i }, rows: data } }
    else for (let row of data) {
        let groupName = groupByFields.map(f => row[f]).join('\n');
        if (!(groupName in groups)) {
            let summary = Object.fromEntries(groupByFields.map(f => [f, row[f]]));
            summary[i] = i++;
            groups[groupName] = { summary, rows: [] };
        }
        groups[groupName].rows.push(row);
    }
    for (let field of fields) {
        let match;
        if ((match = field.match(/(MAX|MIN|AVG|SUM|COUNT)\((.+)\)/))) {
            let [, f, arg] = match;
            for (let groupName in groups) {
                let group = groups[groupName];
                group.summary[field] = fns[f](arg)(group.rows);
            }
        }
    }
    return groups;
}
module.exports = {executeSELECTQuery, executeINSERTQuery, executeDELETEQuery};
