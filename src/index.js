const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition, joinType, groupByFields, hasAggregateWithoutGroupBy} = parseQuery(query);
    let data = await readCSV(`${table}.csv`);
    data = await join(data, joinTable, joinCondition, fields, table, joinType);
    data = data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)));
    data = applyGroupBy(data, groupByFields, hasAggregateWithoutGroupBy, fields);
    return select(data, fields);
}

function select(data, fields) {
    return Object.values(data).map(({summary, rows}) =>
        fields.every(f=>(f in summary)) ? 
          [Object.fromEntries(fields.map(f=>[f, summary[f]]))] :
          rows.map(r => Object.fromEntries(fields.map(f=>[f, summary[f] || r[f]])))
    ).flat();
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
    let colnames = Object.keys(joinData[0]).map(n=>[joinTable, n]).concat(Object.keys(data[0]).map(n=>[table, n]));
    if(type==="RIGHT") return join(joinData, table, joinCondition, fields, joinTable, "LEFT");
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
        if(!atLeastOne && type === "LEFT") result.push(buildRow(mainRow, {}, table, colnames));
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
        row[table+'.'+field] = mainRow[field] || joinRow[field] || null;
    }

    return row;
}

function applyGroupBy(data, groupByFields, hasAggregateWithoutGroupBy, fields) {
    let fns = {
        MIN : colname => rows => Math.min(...rows.map(r=>Number(r[colname]))),
        MAX : colname => rows => Math.max(...rows.map(r=>Number(r[colname]))),
        SUM : colname => rows => rows.map(r=>Number(r[colname])).reduce((a,b)=>a+b),
        AVG : colname => rows => rows.map(r=>Number(r[colname])).reduce((a,b)=>a+b)/rows.length,
        COUNT : ____  => rows => rows.length
    }
    let groups = {};
    if(groupByFields === null || groupByFields.length === 0) groups = {0 : {summary : {}, rows : data}}
    else for(let row of data) {
        let groupName = groupByFields.map(f=>row[f]).join('\n');
        if(!(groupName in groups)) {
            let summary = Object.fromEntries(groupByFields.map(f=>[f, row[f]]));
            groups[groupName] = {summary, rows : []};
        }
        groups[groupName].rows.push(row);
    }
    for(let field of fields) {
        let match;
        if((match=field.match(/(MAX|MIN|AVG|SUM|COUNT)\((.+)\)/))) {
            let [, f, arg] = match;
            for (let groupName in groups) {
                let group = groups[groupName];
                group.summary[field] = fns[f](arg)(group.rows);
            }
        }
    }
    return groups;
}
module.exports = executeSELECTQuery;
