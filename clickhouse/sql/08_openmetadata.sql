CREATE USER IF NOT EXISTS openmetadata_user
    IDENTIFIED WITH sha256_password BY 'openmetadata_password';
GRANT SELECT, SHOW ON system.* TO openmetadata_user;
GRANT SELECT, USAGE ON model_gold.* TO openmetadata_user;
GRANT SELECT, USAGE ON model_silver.* TO openmetadata_user;
GRANT SELECT, USAGE ON bronze.* TO openmetadata_user;
SHOW GRANTS FOR openmetadata_user;