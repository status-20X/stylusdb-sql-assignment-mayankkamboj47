function parseSelectQuery(query) {
    try {
        // First, let's trim the query to remove any leading/trailing whitespaces
        query = query.trim();
        let isDistinct = false;
        if (query.toUpperCase().includes('SELECT DISTINCT')) {
            isDistinct = true;
            query = query.replace('SELECT DISTINCT', 'SELECT');
        }
        
        let { joinTable, joinCondition, joinType } = parseJoinClause(query);
        let { hasAggregateWithoutGroupBy, groupByFields } = parseGroupBy(query);
        let limit = parseLimit(query);
        // Initialize variables for different parts of the query
        const whereClause = parseWhereString(query);
        const orderByFields = parseOrderBy(query);
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
            hasAggregateWithoutGroupBy,
            orderByFields,
            limit,
            isDistinct
        };
    }
    catch (e) {
        throw new Error('Query parsing error: ' + e.message);
    }
}

function parseDeleteQuery(query) {
    query = query.trim();
    let whereString = parseWhereString(query);
    let whereClause;
    if(whereString) whereClause = parseWhereClause(whereString);
    let match;
    if(match=/DELETE FROM (\S+)/.exec(query)) {
        return {
            table : match[1].trim(),
            whereClauses : whereClause  || null
        }
    }
}
function parseInsertQuery(query) {
    let match;
    if(match=/INSERT INTO (.+) \((.+)\) VALUES \((.+)\)/.exec(query)) {
        return {
            table : match[1],
            columns:match[2].split(',').map(c=>c.trim()),
            values: match[3].split(',').map(v=>v.trim())
                    .map(v=> (v.charAt(0) === v.charAt(v.length -1) && v.charAt(0)==='"' || v.charAt(0)==="'") ?
                              v.slice(1, v.length - 1) : 
                              v)
        }
    }
}

function parseLimit(query) {
    const limitRegex = /\sLIMIT\s(\d+)/i;
    const limitMatch = query.match(limitRegex);

    let limit = null;
    if (limitMatch) {
        limit = parseInt(limitMatch[1]);
    }
    return limit;

}
function parseWhereString(query) {
    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // Everything before WHERE clause

    // Todo : Move this entirely to the new module system, where all kinds of patterns are matched
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim().split(/(GROUP BY|ORDER BY|LIMIT)/)[0] : null;
    return whereClause;
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)\s*?(=|!=|>|<|>=|<=|LIKE)\s*?(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            let [, field, operator, value] = match;
            value = value.trim();
            if (value[0] === "'" || value[0] === '"') value = value.slice(1, value.length - 1);
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });

}

function parseOrderBy(query) {
    const orderByRegex = /\sORDER BY\s(.+)/i;
    const orderByMatch = query.match(orderByRegex);

    let orderByFields = null;
    if (orderByMatch) {
        orderByFields = orderByMatch[1].split(',').map(field => {
            const [fieldName, order] = field.trim().split(/\s+/);
            return { fieldName, order: order ? order.toUpperCase() : 'ASC' };
        });
    }
    return orderByFields;
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
    query = query.split(/ORDER/)[0];
    const groupByMatch = query.match(/\sGROUP BY\s(.+)/i);
    const hasAggregateWithoutGroupBy = !Boolean(groupByMatch) && /((SUM|COUNT|AVG|MIN|MAX)\(.+)\)/.test(query);
    const groupByFields = groupByMatch ? groupByMatch[1].split(',').map(f => f.trim()) : null;
    return { groupByFields, hasAggregateWithoutGroupBy };
}


module.exports.parseSelectQuery = parseSelectQuery;
module.exports.parseInsertQuery = parseInsertQuery;
module.exports.parseDeleteQuery = parseDeleteQuery;
module.exports.parseJoinClause = parseJoinClause;
