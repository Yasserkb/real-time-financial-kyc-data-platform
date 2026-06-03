terraform {
  required_version = ">= 1.6.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.95"
    }
  }
}

resource "snowflake_database" "kyc_analytics" {
  name    = var.database_name
  comment = "Analytics database for the real-time financial KYC data platform."
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.kyc_analytics.name
  name     = "RAW"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.kyc_analytics.name
  name     = "MARTS"
}
