resource "google_sql_database_instance" "mysql" {
  name             = "cdc-mysql-poc"
  database_version = "MYSQL_8_0"
  region           = "asia-south1"

  settings {
    tier = "db-f1-micro"

    # Optional but recommended:
    activation_policy = "ALWAYS"
    backup_configuration {
      enabled = true
    }
  }
}
