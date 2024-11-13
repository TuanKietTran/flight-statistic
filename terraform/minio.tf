resource "kubernetes_deployment" "minio" {
  metadata {
    name = "minio"
    labels = {
      app = "minio"
    }
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "minio"
      }
    }
    template {
      metadata {
        labels = {
          app = "minio"
        }
      }
      spec {
        container {
          image = "minio/minio:latest"
          name  = "minio"
          ports {
            container_port = 9000
          }
          ports {
            container_port = 9001
          }
          env {
            name  = "MINIO_ROOT_USER"
            value = "minioadmin"
          }
          env {
            name  = "MINIO_ROOT_PASSWORD"
            value = "minioadmin"
          }
          args = ["server", "/data", "--console-address", ":9001"]
          volume_mount {
            name       = "minio-data"
            mount_path = "/data"
          }
        }
        volume {
          name = "minio-data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.minio_data.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "minio" {
  metadata {
    name = "minio"
  }
  spec {
    selector = {
      app = "minio"
    }
    port {
      port        = 9000
      target_port = 9000
    }
    port {
      port        = 9001
      target_port = 9001
    }
    type = "NodePort"
  }
}

resource "kubernetes_persistent_volume_claim" "minio_data" {
  metadata {
    name = "minio-data-pvc"
  }
  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = "10Gi"
      }
    }
  }
}
