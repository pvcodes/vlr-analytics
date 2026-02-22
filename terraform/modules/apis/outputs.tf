output "enabled_services" {
  description = "Set of enabled API service names"
  value       = { for k, v in google_project_service.gcp_services : k => v.id }
}
