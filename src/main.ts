import { Hono } from 'hono'
import { logger } from 'hono/logger'
import { prettyJSON } from 'hono/pretty-json'
import * as fs from 'fs'
import * as path from 'path'
import * as readline from 'readline'
import { DATA_DIR, PORT } from './config'

// Define interfaces
interface PrometheusTimeSeries {
  metricName: string
  labels: Map<string, string>
  value: number
  timestamp?: number
}

interface ValidationError {
  line: number
  error: string
  content: string
}

interface FileValidationSummary {
  filePath: string
  totalLines: number
  validLines: number
  invalidLines: number
  errors: ValidationError[]
  isValid: boolean
}

interface DirectoryValidationResults {
  directoryPath: string
  fileCount: number
  validFileCount: number
  invalidFileCount: number
  fileSummaries: FileValidationSummary[]
  overallStatus: 'valid' | 'invalid'
}

// Create Hono app
const app = new Hono()

// Middleware
app.use(logger())
app.use(prettyJSON())

// Validation functions
function validatePrometheusTimeSeries(line: string): {
  isValid: boolean
  data?: PrometheusTimeSeries
  error?: string
} {
  // Trim whitespace
  line = line.trim()

  // Basic format check
  if (!line || line.length === 0) {
    return { isValid: false, error: 'Empty line' }
  }

  try {
    // Split the line into metric and value parts
    // First, find the closing brace position to properly handle spaces in label values
    let metricEndPos = line.indexOf('}')
    if (metricEndPos === -1) {
      // No labels, find the first space after metric name
      metricEndPos = line.indexOf(' ')
      if (metricEndPos === -1) {
        return {
          isValid: false,
          error: 'Invalid format: missing space after metric name',
        }
      }
    } else {
      // Move past the closing brace
      metricEndPos++
    }

    const metricPart = line.substring(0, metricEndPos).trim()
    const valuePart = line.substring(metricEndPos).trim()

    // Split the value part into value and optional timestamp
    const valueParts = valuePart.split(' ')
    if (valueParts.length === 0 || valueParts.length > 2) {
      return { isValid: false, error: 'Invalid value format' }
    }

    // Parse value
    const value = parseFloat(valueParts[0])
    if (isNaN(value)) {
      return { isValid: false, error: 'Invalid numeric value' }
    }

    // Parse optional timestamp
    let timestamp: number | undefined = undefined
    if (valueParts.length === 2) {
      timestamp = parseInt(valueParts[1], 10)
      if (isNaN(timestamp)) {
        return { isValid: false, error: 'Invalid timestamp' }
      }
    }

    // Parse metric name and labels
    let metricName: string
    const labels = new Map<string, string>()

    // Check for labels
    const labelStartPos = metricPart.indexOf('{')
    if (labelStartPos === -1) {
      // No labels
      metricName = metricPart
    } else {
      // Extract metric name
      metricName = metricPart.substring(0, labelStartPos).trim()

      // Validate metric name
      if (!isValidMetricName(metricName)) {
        return { isValid: false, error: 'Invalid metric name format' }
      }

      // Extract labels
      const labelEndPos = metricPart.lastIndexOf('}')
      if (labelEndPos === -1) {
        return { isValid: false, error: 'Unclosed label brackets' }
      }

      const labelsStr = metricPart
        .substring(labelStartPos + 1, labelEndPos)
        .trim()
      if (labelsStr.length > 0) {
        // Split labels with respect to commas that are not within quotes
        const labelPairs = splitLabels(labelsStr)

        for (const labelPair of labelPairs) {
          const [key, value] = parseLabel(labelPair)
          if (!key || !value) {
            return {
              isValid: false,
              error: `Invalid label format: ${labelPair}`,
            }
          }

          // Validate label name
          if (!isValidLabelName(key)) {
            return { isValid: false, error: `Invalid label name: ${key}` }
          }

          labels.set(key, value)
        }
      }
    }

    return {
      isValid: true,
      data: {
        metricName,
        labels,
        value,
        timestamp,
      },
    }
  } catch (error) {
    return {
      isValid: false,
      error: `Error parsing line: ${error instanceof Error ? error.message : String(error)}`,
    }
  }
}

function isValidMetricName(name: string): boolean {
  const metricNameRegex = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/
  return metricNameRegex.test(name)
}

function isValidLabelName(name: string): boolean {
  const labelNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/
  return labelNameRegex.test(name)
}

function splitLabels(labelsStr: string): string[] {
  const result: string[] = []
  let current = ''
  let inQuotes = false
  let escaped = false

  for (let i = 0; i < labelsStr.length; i++) {
    const char = labelsStr[i]

    if (escaped) {
      current += char
      escaped = false
    } else if (char === '\\') {
      escaped = true
    } else if (char === '"') {
      inQuotes = !inQuotes
      current += char
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim())
      current = ''
    } else {
      current += char
    }
  }

  if (current.trim()) {
    result.push(current.trim())
  }

  return result
}

function parseLabel(labelStr: string): [string, string] {
  const equalsPos = labelStr.indexOf('=')
  if (equalsPos === -1) {
    return ['', '']
  }

  const name = labelStr.substring(0, equalsPos).trim()
  let value = labelStr.substring(equalsPos + 1).trim()

  // Check if value is quoted
  if (value.startsWith('"') && value.endsWith('"')) {
    value = value.substring(1, value.length - 1)
    // Unescape quotes
    value = value.replace(/\\"/g, '"')
  } else {
    return ['', '']
  }

  return [name, value]
}

async function validateFile(filePath: string): Promise<FileValidationSummary> {
  const summary: FileValidationSummary = {
    filePath,
    totalLines: 0,
    validLines: 0,
    invalidLines: 0,
    errors: [],
    isValid: true,
  }

  try {
    // Check if file exists
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`)
    }

    const fileStream = fs.createReadStream(filePath)
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    })

    let lineNumber = 0

    for await (const line of rl) {
      lineNumber++
      summary.totalLines++

      // Skip empty lines
      if (!line.trim()) {
        continue
      }

      const result = validatePrometheusTimeSeries(line)

      if (result.isValid) {
        summary.validLines++
      } else {
        summary.invalidLines++
        summary.errors.push({
          line: lineNumber,
          error: result.error || 'Unknown error',
          content: line,
        })
      }
    }

    summary.isValid = summary.invalidLines === 0
    return summary
  } catch (error) {
    console.error(`Error processing file ${filePath}:`, error)
    summary.errors.push({
      line: 0,
      error: `File processing error: ${error instanceof Error ? error.message : String(error)}`,
      content: '',
    })
    summary.isValid = false
    return summary
  }
}

function getFilesInDirectory(directoryPath: string): string[] {
  try {
    const entries = fs.readdirSync(directoryPath, { withFileTypes: true })
    return entries
      .filter((entry) => entry.isFile() && entry.name.endsWith('.txt'))
      .map((entry) => path.join(directoryPath, entry.name))
  } catch (error) {
    console.error(`Error reading directory ${directoryPath}:`, error)
    return []
  }
}

async function validateDirectory(
  directoryPath: string,
): Promise<DirectoryValidationResults> {
  const results: DirectoryValidationResults = {
    directoryPath,
    fileCount: 0,
    validFileCount: 0,
    invalidFileCount: 0,
    fileSummaries: [],
    overallStatus: 'valid',
  }

  try {
    // Check if directory exists
    if (!fs.existsSync(directoryPath)) {
      throw new Error(`Directory not found: ${directoryPath}`)
    }

    // Get all .txt files in the directory
    const files = getFilesInDirectory(directoryPath)
    results.fileCount = files.length

    console.log(`Found ${files.length} .txt files in ${directoryPath}`)

    // Validate each file
    for (const filePath of files) {
      console.log(`Validating file: ${filePath}`)
      const fileSummary = await validateFile(filePath)
      results.fileSummaries.push(fileSummary)

      if (fileSummary.isValid) {
        results.validFileCount++
      } else {
        results.invalidFileCount++
      }
    }

    results.overallStatus = results.invalidFileCount === 0 ? 'valid' : 'invalid'
    return results
  } catch (error) {
    console.error(`Error validating directory ${directoryPath}:`, error)
    results.overallStatus = 'invalid'
    return results
  }
}

// Create a serializable version of the validation results
function serializeValidationResults(results: DirectoryValidationResults): any {
  return {
    directoryPath: results.directoryPath,
    fileCount: results.fileCount,
    validFileCount: results.validFileCount,
    invalidFileCount: results.invalidFileCount,
    overallStatus: results.overallStatus,
    fileSummaries: results.fileSummaries.map((summary) => ({
      ...summary,
      labels: summary.errors.map((error) => ({
        line: error.line,
        error: error.error,
        content: error.content,
      })),
    })),
  }
}

// Validate a single line of Prometheus time series data
function validateSingleLine(line: string): any {
  const result = validatePrometheusTimeSeries(line)

  if (result.isValid && result.data) {
    // Convert Map to plain object for JSON serialization
    const labelsObj: Record<string, string> = {}
    result.data.labels.forEach((value, key) => {
      labelsObj[key] = value
    })

    return {
      isValid: true,
      data: {
        ...result.data,
        labels: labelsObj,
      },
    }
  } else {
    return {
      isValid: false,
      error: result.error,
    }
  }
}

// Routes
app.get('/', (c) => {
  return c.json({
    service: 'Prometheus Time Series Validator',
    version: '1.0.0',
    endpoints: [
      {
        path: '/metrics',
        method: 'GET',
        description:
          'Display all valid Prometheus metrics from the configured directory (for scraping)',
      },
      {
        path: '/metrics/validate',
        method: 'POST',
        description: 'Validate a single line of Prometheus metrics data',
      },
      {
        path: '/metrics/file/:filename',
        method: 'GET',
        description: 'Validate a specific file in the metrics directory',
      },
    ],
  })
})

// Display all valid metrics from all files, ignoring invalid metrics and duplicates
app.get('/metrics', async (c) => {
  try {
    // Get all .txt files in the configured directory
    const files = getFilesInDirectory(DATA_DIR)

    // Use Map to store metrics with their identifier as key to prevent duplicates
    // Map key will be "metricName{labels}" to ensure uniqueness
    const metricsMap = new Map<string, string>()

    // Process each file
    for (const filePath of files) {
      try {
        // Read file line by line
        const fileStream = fs.createReadStream(filePath)
        const rl = readline.createInterface({
          input: fileStream,
          crlfDelay: Infinity,
        })

        for await (const line of rl) {
          // Skip empty lines
          if (!line.trim()) {
            continue
          }

          // Validate the line
          const result = validatePrometheusTimeSeries(line)

          // If valid, add to collected metrics
          if (result.isValid && result.data) {
            // Extract the metric identifier (metric name + labels)
            const metricData = result.data

            // Convert labels Map to a sorted array of key-value pairs for consistent order
            const labelPairs: [string, string][] = []
            metricData.labels.forEach((value, key) => {
              labelPairs.push([key, value])
            })

            // Sort by label name for consistent order
            labelPairs.sort((a, b) => a[0].localeCompare(b[0]))

            // Build labels string in consistent order
            let labelsStr = ''
            if (labelPairs.length > 0) {
              const labelStrs = labelPairs.map(([k, v]) => `${k}="${v}"`)
              labelsStr = `{${labelStrs.join(',')}}`
            }

            // Create metric identifier
            const metricId = `${metricData.metricName}${labelsStr}`

            // Store in map (last occurrence wins in case of duplicates)
            metricsMap.set(metricId, line)
          }
        }
      } catch (error) {
        // Log error but continue processing other files
        console.error(`Error processing file ${filePath}:`, error)
      }
    }

    // Convert map values to array
    const uniqueMetrics = Array.from(metricsMap.values())

    // Return valid metrics as plain text (standard Prometheus format)
    return c.text(uniqueMetrics.join('\n'), 200, {
      'Content-Type': 'text/plain; version=0.0.4',
    })
  } catch (error) {
    console.error('Error processing metrics:', error)
    return c.text(
      `# Error: Failed to retrieve metrics: ${error instanceof Error ? error.message : String(error)}`,
      500,
    )
  }
})

// Validate a single line submitted in the request body
app.post('/metrics/validate', async (c) => {
  try {
    const body = await c.req.json()

    if (!body.line) {
      return c.json({ error: 'No metrics line provided in request body' }, 400)
    }

    const result = validateSingleLine(body.line)
    return c.json(result)
  } catch (error) {
    console.error('Error validating line:', error)
    return c.json(
      {
        error: `Failed to validate metrics line: ${error instanceof Error ? error.message : String(error)}`,
      },
      500,
    )
  }
})

// Validate a specific file in the metrics directory
app.get('/metrics/file/:filename', async (c) => {
  try {
    const filename = c.req.param('filename')

    // Sanitize filename to prevent directory traversal
    const sanitizedFilename = path.basename(filename)
    const filePath = path.join(DATA_DIR, sanitizedFilename)

    if (!fs.existsSync(filePath)) {
      return c.json({ error: `File not found: ${sanitizedFilename}` }, 404)
    }

    // Only allow .txt files
    if (!filePath.endsWith('.txt')) {
      return c.json({ error: 'Only .txt files are supported' }, 400)
    }

    const summary = await validateFile(filePath)
    return c.json({
      timestamp: new Date().toISOString(),
      ...summary,
    })
  } catch (error) {
    console.error('Error validating file:', error)
    return c.json(
      {
        error: `Failed to validate file: ${error instanceof Error ? error.message : String(error)}`,
      },
      500,
    )
  }
})

// Health check
app.get('/health', (c) => {
  return c.json({ status: 'ok', timestamp: new Date().toISOString() })
})

// Start the server
console.log(`Starting Prometheus Validator Service on port ${PORT}`)
console.log(`Using metrics directory: ${DATA_DIR}`)

export default {
  port: Number(PORT),
  fetch: app.fetch,
}
