const readline = require('readline');
const { executeSELECTQuery, executeINSERTQuery, executeDELETEQuery } = require('./queryExecutor');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

rl.setPrompt('SQL> ');
console.log('SQL Query Engine CLI. Enter your SQL commands, or type "exit" to quit.');

rl.prompt();

rl.on('line', async (line) => {
    if (line.toLowerCase() === 'exit') {
        rl.close();
        return;
    }

    try {
        line = line.trim()
        if (line.indexOf('INSERT') > -1)
          console.log(executeINSERTQuery(line));
        else if(line.indexOf('DELETE') > -1)
          console.log(executeDELETEQuery(line));
        else if(line.indexOf('SELECT') > -1)
          console.log(executeSELECTQuery(line))
        else
          console.log('I only understand SELECT, INSERT and DELETE queries. Can\'t understand ' + line);
        // Execute the query - do your own implementation
    }catch (error) {
        console.error('Error:', error.message);
    }

    rl.prompt();
}).on('close', () => {
    console.log('Exiting SQL CLI');
    process.exit(0);
});
