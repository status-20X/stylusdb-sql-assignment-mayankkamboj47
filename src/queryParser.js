function parseQuery(query) {
    // First, let's trim the query to remove any leading/trailing whitespaces
    query = query.trim();

    let {joinTable, joinCondition, joinType} = parseJoinClause(query);
    let {hasAggregateWithoutGroupBy, groupByFields} = parseGroupBy(query);
    // Initialize variables for different parts of the query

    // Split the query at the WHERE clause if it exists
    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // Everything before WHERE clause

    // Todo : Move this entirely to the new module system, where all kinds of patterns are matched
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim().split(/GROUP BY/)[0] : null;


    // Todo : Move this to a function too, and extract from the returned values
    const selectRegex = /^SELECT\s(.+?)\sFROM\s+(\S+)/i;
    const selectMatch = query.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }

    const [, fields, table] = selectMatch;

    // Parse the JOIN part if it exists

    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }

    return {
        fields: fields.split(',').map(field => field.trim()),
        table: table.trim(),
        whereClauses,
        joinTable,
        joinCondition,
        joinType,
        groupByFields,
        hasAggregateWithoutGroupBy
    };
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            let [, field, operator, value] = match;
            value = value.trim();
            if (value[0] === "'" || value[0]=== '"') value = value.slice(1, value.length-1);
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });

}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

function parseGroupBy(query) {
    const groupByMatch = query.match(/\sGROUP BY\s(.+)/i);
    const hasAggregateWithoutGroupBy = !Boolean(groupByMatch) && /((SUM|COUNT|AVG|MIN|MAX)\(.+)\)/.test(query);
    const groupByFields = groupByMatch ? groupByMatch[1].split(',').map(f=>f.trim()) : null;
    return {groupByFields, hasAggregateWithoutGroupBy};
}


module.exports.parseQuery = parseQuery;
module.exports.parseJoinClause = parseJoinClause;
