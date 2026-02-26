resource "google_project_service" "pubsub_api" {
  project = var.project_id
  service = "pubsub.googleapis.com"

  disable_on_destroy = false
}

resource "google_pubsub_topic" "completion_topic" {
  name = var.completion_topic_name
  depends_on = [
    google_project_service.pubsub_api
  ]
}

resource "google_pubsub_subscription" "completion_subscription" {
  name  = var.completion_subscription_name
  topic = google_pubsub_topic.completion_topic.id

  ack_deadline_seconds = 20

  expiration_policy {
    ttl = ""
  }
}

# Dead Letter Topic 
resource "google_pubsub_topic" "completion_dlq" {
  name = "${var.completion_topic_name}-dlq"
}

resource "google_pubsub_subscription" "completion_dlq_sub" {
  name  = "${var.completion_subscription_name}-dlq"
  topic = google_pubsub_topic.completion_dlq.id
}


resource "google_pubsub_subscription" "completion_subscription_with_dlq" {
  name  = "${var.completion_subscription_name}-with-dlq"
  topic = google_pubsub_topic.completion_topic.id

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.completion_dlq.id
    max_delivery_attempts = 5
  }

  ack_deadline_seconds = 20

  expiration_policy {
    ttl = ""
  }
}
resource "google_pubsub_topic_iam_member" "publisher_binding" {
  topic  = google_pubsub_topic.completion_topic.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${var.cloud_run_service_account}"
}
