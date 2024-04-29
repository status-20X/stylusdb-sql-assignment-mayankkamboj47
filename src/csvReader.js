const fs = require('fs');
const csv = require('csv-parser');
const { Transform } = require('stream');

function readCSV(filePath) {
    const results = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                resolve(results);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

function writeCSV(filePath, data) {
  const transformer = new Transform({
    transform(chunk, encoding, callback) {
      this.push(chunk);
      callback();
    }
  });

  const writeStream = fs.createWriteStream(filePath);
  const csvStream = transformer.pipe(writeStream);

  return new Promise((resolve, reject) => {
    csvStream.on('error', (error) => {
      reject(error);
    });

    csvStream.on('finish', () => {
      resolve();
    });

    // Get the keys (column names) from the first object in the data array
    const keys = Object.keys(data[0]);
    // Write the header row with column names
    csvStream.write(`${keys.join(',')}\n`);

    data.forEach((row) => {
      const csvRow = keys.map(key => row[key]).join(',');
      csvStream.write(`${csvRow}\n`);
    });

    csvStream.end();
  });
}

module.exports = {readCSV, writeCSV};