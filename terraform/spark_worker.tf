resource "kubernetes_deployment" "spark_worker" {
  metadata {
    name = "spark-worker"
    labels = {
      app = "spark-worker"
    }
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "spark-worker"
      }
    }
    template {
      metadata {
        labels = {
          app = "spark-worker"
        }
      }
      spec {
        container {
          image = "bde2020/spark-worker:latest"
          name  = "spark-worker"
          ports {
            container_port = 8081
          }
          env {
            name  = "SPARK_MASTER"
            value = "spark://spark-master:7077"
          }
          env {
            name  = "MINIO_ENDPOINT"
            value = "http://minio:9000"
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "spark_worker" {
  metadata {
    name = "spark-worker"
  }
  spec {
    selector = {
      app = "spark-worker"
    }
    port {
      port        = 8081
      target_port = 8081
    }
    type = "NodePort"
  }
}
