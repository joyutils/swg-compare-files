const fs = require('node:fs/promises')

const LOCAL_FILES_PATH = './local.json'
const REMOTE_FILES_PATH = './remote.json'
const DIFF_PATH = './diff.json'

function sortFiles(files) {
  return files.slice().sort((a, b) => a.localeCompare(b, 'en', { numeric: true }))
}

const STORAGE_BAGS_QUERY = `
query GetStorageBucketBags($storageBucket: ID!, $limit: Int!, $offset: Int!) {
  storageBags(
    where: { storageBuckets_some: { id_eq: $storageBucket } }
    orderBy: createdAt_ASC
    limit: $limit
    offset: $offset
  ) {
    id
  }
}
`

const STORAGE_BAGS_OBJECTS_QUERY = `
query GetStorageBagsObjects($storageBags: [ID!]!, $limit: Int!, $offset: Int!) {
  storageBags(
    where: { id_in: $storageBags }
    orderBy: createdAt_ASC
    limit: $limit
    offset: $offset
  ) {
    objects {
      id
    }
  }
}
`

async function fetchPaginatedData(query, variables, pageSize) {
  let hasMoreData = true;
  let offset = 0;
  let data = []

  while (hasMoreData) {
    const response = await fetch('https://query.joystream.org/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: query,
        variables: { ...variables, limit: pageSize, offset: offset }
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      console.log(error)
      throw new Error(`Error fetching data: ${response.statusText}`)
    }

    const jsonResponse = await response.json()

    data = data.concat(jsonResponse.data.storageBags)

    hasMoreData = jsonResponse.data.storageBags.length === pageSize;
    offset += pageSize;
  }

  return data
}

async function getAllBucketObjects(bucketId) {
  console.log('Getting bags...')
  const bucketBags = await fetchPaginatedData(STORAGE_BAGS_QUERY, { storageBucket: bucketId }, 3000)
  console.log(`Found ${bucketBags.length} bags`)
  const bucketBagsIds = bucketBags.map(bag => bag.id)

  console.log('Getting objects...')
  let objects = []
  const BATCH_SIZE = 1000
  for (let i = 0; i < bucketBagsIds.length; i += BATCH_SIZE) {
    const bucketBagsWithObjects = await fetchPaginatedData(STORAGE_BAGS_OBJECTS_QUERY, { storageBags: bucketBagsIds.slice(i, i + BATCH_SIZE) }, BATCH_SIZE)
    const bucketObjects = bucketBagsWithObjects.map(bag => bag.objects).flat()
    objects = objects.concat(bucketObjects)
  }
  console.log(`Found ${objects.length} objects`)
  const objectsIds = objects.map(object => object.id)
  const sortedObjectsIds = sortFiles(objectsIds)
  await fs.writeFile(REMOTE_FILES_PATH, JSON.stringify(sortedObjectsIds))
}

async function getFilesList(path) {
  console.log('Getting files...')
  const files = await fs.readdir(path)
  console.log(`Found ${files.length} files`)
  const sortedFiles = sortFiles(files)
  await fs.writeFile(LOCAL_FILES_PATH, JSON.stringify(sortedFiles))
}

async function printDifferences() {
  const localFiles = JSON.parse(await fs.readFile(LOCAL_FILES_PATH))
  const remoteFiles = JSON.parse(await fs.readFile(REMOTE_FILES_PATH))

  const localFilesSet = new Set(localFiles)
  const remoteFilesSet = new Set(remoteFiles)

  const uniqueInLocal = new Set([...localFilesSet].filter(id => !remoteFilesSet.has(id)))
  const uniqueInRemote = new Set([...remoteFilesSet].filter(id => !localFilesSet.has(id)))

  uniqueInRemote.forEach(id => console.log(`Remote file ${id} is not present locally`))
  uniqueInLocal.forEach(id => console.log(`Unexpected local file ${id}`))

  console.log(`Found ${uniqueInRemote.size} missing objects`)
  console.log(`Found ${uniqueInLocal.size} unexpected local objects`)

  await fs.writeFile(DIFF_PATH, JSON.stringify({ unexpectedLocal: [...uniqueInLocal], missingObjects: [...uniqueInRemote] }))
}

const command = process.argv[2]
const arg = process.argv[3]

if (command === 'localFiles') {
  if (!arg) {
    console.log('Please provide a path')
    process.exit(1)
  }
  getFilesList(arg)
} else if (command === 'bucketObjects') {
  if (!arg) {
    console.log('Please provide a bucket id')
    process.exit(1)
  }
  getAllBucketObjects(arg)
} else if (command === 'diff') {
  printDifferences()
} else {
  console.log('Unknown command')
  process.exit(1)
}
