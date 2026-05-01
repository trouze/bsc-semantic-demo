-- Run after image is pushed and compute pool is active
CREATE OR REPLACE SERVICE DEMO_BSC.AGENT_APP
  IN COMPUTE POOL DEMO_AGENT_POOL
  FROM SPECIFICATION $$
spec:
  containers:
    - name: agent
      image: /demo_bsc/agent_repo/agent_app:latest
      env:
        DBT_SL_HOST: ${DBT_SL_HOST}
        DBT_ENVIRONMENT_ID: ${DBT_ENVIRONMENT_ID}
      secrets:
        - snowflakeSecret: DEMO_BSC.DBT_CLOUD_TOKEN
          envVarName: DBT_CLOUD_TOKEN
      volumeMounts:
        - name: catalog-cache
          mountPath: /data
  endpoints:
    - name: streamlit
      port: 8501
      public: true
  volumes:
    - name: catalog-cache
      source: block
      size: 5Gi
$$
  QUERY_WAREHOUSE = DEMO_WH
  EXTERNAL_ACCESS_INTEGRATIONS = (DBT_CLOUD_EAI);
