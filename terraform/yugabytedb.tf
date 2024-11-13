resource "kubernetes_deployment" "yugabytedb" {
  metadata {
    name = "yugabytedb"
    labels = {
      app = "yugabytedb"
    }
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "yugabytedb"
      }
    }
    template {
      metadata {
        labels = {
          app = "yugabytedb"
        }
      }
      spec {
        container {
          image = "yugabytedb/yugabyte:latest"
          name  = "yugabytedb"
          ports {
            container_port = 5433
          }
          ports {
            container_port = 9042
          }
          ports {
            container_port = 7000
          }
          ports {
            container_port = 9000
          }
          ports {
            container_port = 15433
          }
          env {
            name  = "CLUSTER_NAME"
            value = "yugabytedb-cluster"
          }
          env {
            name  = "YB_MASTER_WEB_PORT"
            value = "7000"
          }
          env {
            name  = "YB_TSERVER_WEB_PORT"
            value = "9000"
          }
          args = ["bin/yugabyted", "start", "--daemon=false"]
        }
      }
    }
  }
}

resource "kubernetes_service" "yugabytedb" {
  metadata {
    name = "yugabytedb"
  }
  spec {
    selector = {
      app = "yugabytedb"
    }
    port {
      port        = 5433
      target_port = 5433
    }
    port {
      port        = 9042
      target_port = 9042
    }
    port {
      port        = 7000
      target_port = 7000
    }
    port {
      port        = 9000
      target_port = 9000
    }
    port {
      port        = 15433
      target_port = 15433
    }
    type = "NodePort"
  }
}