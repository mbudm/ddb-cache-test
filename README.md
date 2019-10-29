
## update/add index
`sls invoke -f put --path payloads/tag-increment.json`
`sls invoke -f put --path payloads/tag-decrement.json`

## get 
`sls invoke -f get`

## reset the indexes table items
`aws dynamodb delete-item --table-name ddb-cache-test-dev-indexes --key '{"id":{"S":"tags"}}' --region us-east-1`
`aws dynamodb delete-item --table-name ddb-cache-test-dev-indexes --key '{"id":{"S":"people"}}' --region us-east-1`
