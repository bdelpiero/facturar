import fs from "fs"
import path from "path"
import csv from "csv-parser"

// These could be set with args passed to the script
const csvFilePath = "input/books.csv"
const outputFolder = "output"
const maxBooksPerJson = 25

function writeJsonToFile(jsonData, outputFolder, subdirectory, fileIndex) {
  const outputFileName = `output_${fileIndex}.json`
  const outputDirPath = path.join(outputFolder, subdirectory)

  // Create output directory and subdirectories if they don't exist
  if (!fs.existsSync(outputDirPath)) {
    fs.mkdirSync(outputDirPath, { recursive: true })
    console.log(`Output directory ${outputDirPath} created successfully.`)
  }

  const outputPath = path.join(outputDirPath, outputFileName)

  try {
    fs.writeFileSync(outputPath, JSON.stringify(jsonData, null, 2))
    console.log(`File ${outputFileName} created successfully.`)
  } catch (error) {
    console.error(`Error creating file ${outputFileName}: ${error.message}`)
    process.exit(1)
  }
}

function parseCSVAndSplitToJson(csvFilePath, outputFolder, subdirectory) {
  const jsonData = []

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on("data", (row) => {
      jsonData.push(row)
    })
    .on("end", () => {
      const totalRecords = jsonData.length
      const totalJsonFiles = Math.ceil(totalRecords / maxBooksPerJson)

      // Calculate the number of books per json so that we have an even distribution of records between files
      const booksPerJson = Math.min(maxBooksPerJson, Math.ceil(totalRecords / totalJsonFiles))

      console.log(`Total records: ${totalRecords}`)
      console.log(`Books per JSON file: ${booksPerJson}`)
      console.log(`Total JSON files needed: ${totalJsonFiles}`)

      // Distribute data to JSON files
      let currentFileIndex = 1
      for (let index = 0; index < totalRecords; index++) {
        const isLastRecord = index === totalRecords - 1
        const shouldWriteToJson = index === currentFileIndex * booksPerJson - 1 || isLastRecord
        if (shouldWriteToJson) {
          writeJsonToFile(
            jsonData.slice((currentFileIndex - 1) * booksPerJson, currentFileIndex * booksPerJson),
            outputFolder,
            subdirectory,
            currentFileIndex
          )
          currentFileIndex++
        }
      }

      console.log("Conversion complete.")
    })
    .on("error", (error) => {
      console.error(`Error reading CSV file: ${error.message}`)
      process.exit(1)
    })
}

function main() {
  const subdirectory = process.argv[2]

  if (!subdirectory) {
    console.error("Error: Please provide a subdirectory name as the first argument.")
    process.exit(1)
  }

  parseCSVAndSplitToJson(csvFilePath, outputFolder, subdirectory)
}

main()
