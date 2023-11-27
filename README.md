Get a list of all local objects

```sh
node index.js localFiles ./data
```

Then fetch a list of all objects in a bucket (can take few minutes):

```sh
node index.js bucketObjects 0

# for a single bag
# node index.js bucketObjects 0 1234 
```

Get the differences

```
node index.js diff

# for a single bag
# node index.js diff 1234
```
