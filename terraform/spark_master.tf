resource "kubernetes_deployment" "spark_master" {
  metadata {
    name = "spark-master"
    labels = {
      app = "spark-master"
    }
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "spark-master"
      }
    }
    template {
      metadata {
        labels = {
          app = "spark-master"
        }
      }
      spec {
        container {
          image = "bde2020/spark-master:latest"
          name  = "spark-master"
          ports {
            container_port = 7077
          }
          ports {
            container_port = 8080
          }
          env {
            name  = "INIT_DAEMON_STEP"
            value = "setup_spark"
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

resource "kubernetes_service" "spark_master" {
  metadata {
    name = "spark-master"
  }
  spec {
    selector = {
      app = "spark-master"
    }
    port {
      port        = 7077
      target_port = 7077
    }
    port {
      port        = 8080
      target_port = 8080
    }
    type = "NodePort"
  }
}
