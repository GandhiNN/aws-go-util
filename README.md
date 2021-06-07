# aws-go-util
README

```
go run main.go -env=prd -tables=plm_gnpt.csv -svc=athena -output=plm_gnpt_athena_prd_validation_2021-05-17.csv
```

```
go run main.go -env=prd -tables=plm_gnpt.csv -svc=ddb -output=plm_gnpt_ddb_prd_validation_2021-05-17.csv --ddbPrefix=pipe-el-plmcsdm2
```