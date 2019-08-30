
```
cf create-service cleardb spark job-db
cf create-service scheduler-for-pcf standard accesslog-formatter-scheduler
cf cups accesslog-formatter -p credentials.json
```

```
./mvnw clean package -DskipTests=true
cf push
```


```
cf run-task accesslog-formatter "$(cf curl /v2/apps/$(cf app accesslog-formatter --guid) | jq -r .entity.detected_start_command) --spring.batch.job.enabled=true"
```


```
cf create-job accesslog-formatter accesslog-formatter-job "$(cf curl /v2/apps/$(cf app accesslog-formatter --guid) | jq -r .entity.detected_start_command) --spring.batch.job.enabled=true"
cf schedule-job accesslog-formatter-job "*/10 * ? * *"
```