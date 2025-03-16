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
        path: '/health',
        method: 'GET',
        description: 'Health check endpoint',
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
