resource "azurerm_storage_container" "temp" {
  count = var.temp_container == null ? 1 : 0

  name                  = "${var.prefix}-temp"
  storage_account_id    = var.temp_storage_account != null ? var.temp_storage_account.id : var.storage_account.id
  container_access_type = "private"
}
