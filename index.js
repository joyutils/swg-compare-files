const fs = require('node:fs/promises')

const LOCAL_FILES_PATH = './local.json'
const REMOTE_FILES_PATH = './remote.json'
const getRemoteBagPath = (bagId) => `./remote-${bagId}.json`
const DIFF_PATH = './diff.json'
const getDiffBagPath = (bagId) => `./diff-${bagId}.json`

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
    id
    objects {
      id
      isAccepted
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

async function getAllBucketObjects(bucketId, bagId) {
  console.log('Getting bags...')
  const allBucketBags = await fetchPaginatedData(STORAGE_BAGS_QUERY, { storageBucket: bucketId }, 3000)
  const bucketBags = bagId != null ? allBucketBags.filter(bag => bag.id.includes(bagId)) : allBucketBags
  console.log(`Found ${bucketBags.length} bags`)
  const bucketBagsIds = bucketBags.map(bag => bag.id)

  console.log('Getting objects...')
  const bagObjectsMap = {}
  const BATCH_SIZE = 1000
  for (let i = 0; i < bucketBagsIds.length; i += BATCH_SIZE) {
    const bucketBagsWithObjects = await fetchPaginatedData(STORAGE_BAGS_OBJECTS_QUERY, { storageBags: bucketBagsIds.slice(i, i + BATCH_SIZE) }, BATCH_SIZE)
    bucketBagsWithObjects.forEach(bag => {
      const acceptedObjects = bag.objects.filter(object => object.isAccepted)
      if (acceptedObjects.length !== 0) {
        bagObjectsMap[bag.id] = sortFiles(acceptedObjects.map(object => object.id))
      }
    })
  }
  const totalObjectsCount = Object.values(bagObjectsMap).flat().length
  console.log(`Found ${totalObjectsCount} accepted objects`)
  await fs.writeFile(bagId != null ? getRemoteBagPath(bagId) : REMOTE_FILES_PATH, JSON.stringify(bagObjectsMap))
}

async function getLocalFiles(path) {
  console.log('Getting files...')
  const allFiles = await fs.readdir(path)
  const files = allFiles.filter(file => !isNaN(parseInt(file)))
  console.log(`Found ${files.length} files`)
  const sortedFiles = sortFiles(files)
  await fs.writeFile(LOCAL_FILES_PATH, JSON.stringify(sortedFiles))
}

async function getDifferences(bagId) {
  const localFiles = JSON.parse(await fs.readFile(LOCAL_FILES_PATH))
  const remoteFiles = JSON.parse(await fs.readFile(bagId != null ? getRemoteBagPath(bagId) : REMOTE_FILES_PATH))

  const localFilesSet = new Set(localFiles)
  const allRemoteFilesSet = new Set(Object.values(remoteFiles).flat())

  const unexpectedLocal = new Set([...localFilesSet].filter(id => !allRemoteFilesSet.has(id)))
  console.log(`Unexpected local files: ${JSON.stringify([...unexpectedLocal])}`)

  const missingObjectsPerBag = {}
  Object.entries(remoteFiles).forEach(([bagId, objects]) => {
    const missingObjects = objects.filter(id => !localFilesSet.has(id))
    if (missingObjects.length !== 0) {
      console.log(`Bag ${bagId} missing ${missingObjects.length} objects: ${JSON.stringify(missingObjects)}`)
      missingObjectsPerBag[bagId] = missingObjects
    }
  })

  const missingObjects = new Set(Object.values(missingObjectsPerBag).flat())

  console.log(`Missing ${missingObjects.size} objects`)
  console.log(`Found ${unexpectedLocal.size} unexpected local objects`)

  await fs.writeFile(bagId != null ? getDiffBagPath(bagId) : DIFF_PATH, JSON.stringify({ unexpectedLocal: [...unexpectedLocal], missingObjectsPerBag: missingObjectsPerBag }))
}

const command = process.argv[2]
const arg = process.argv[3]
const arg2 = process.argv[4]

if (command === 'localFiles') {
  if (!arg) {
    console.log('Please provide a path')
    process.exit(1)
  }
  getLocalFiles(arg)
} else if (command === 'bucketObjects') {
  if (!arg || isNaN(parseInt(arg))) {
    console.log('Please provide a bucket id')
    process.exit(1)
  }
  if (arg2 && isNaN(parseInt(arg2))) {
    console.log('If you want to get only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }
  getAllBucketObjects(arg, arg2)
} else if (command === 'diff') {
  if (arg && isNaN(parseInt(arg))) {
    console.log('If you want to diff only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }
  getDifferences(arg)
} else {
  console.log('Unknown command')
  process.exit(1)
}
