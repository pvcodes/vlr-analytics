output "completion_topic_name" {
  value = google_pubsub_topic.completion_topic.name
}

output "completion_subscription_name" {
  value = google_pubsub_subscription.completion_subscription_with_dlq.name
}
