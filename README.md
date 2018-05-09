# lambda-athena-query
Grab data from athena!

- Async Query Function/API
- Sync Query Function/API
- Get Result Function/API
- CLI tool


- Make Go app... make executable wrappers...

### Extract Example
```
AWS_REGION=us-west-2 ./build/aq-darwin-amd64 extract -d athena-database -b aws-s3-bucket-name -q 'SELECT * FROM "athena-database"."tablename" limit 10;'
```

fe3cd0a3-b3aa-4e3a-9f84-4cba8e147748
