Write-Host "Mise à jour du fichier hosts..."

$hosts = "C:\Windows\System32\drivers\etc\hosts"

$entries = @(
"127.0.0.1 airflow.itceq.tn",
"127.0.0.1 minio.itceq.tn",
"127.0.0.1 grafana.itceq.tn",
"127.0.0.1 prometheus.itceq.tn",
"127.0.0.1 metabase.itceq.tn",
"127.0.0.1 pgadmin.itceq.tn",
"127.0.0.1 spark.itceq.tn",
"127.0.0.1 catalog.itceq.tn",
"127.0.0.1 marquez.itceq.tn",
"127.0.0.1 minioquest.itceq.tn"
)

foreach ($entry in $entries) {
    if (-not (Select-String -Path $hosts -Pattern $entry -Quiet)) {
        Add-Content -Path $hosts -Value "`n$entry"
        Write-Host "Ajout : $entry"
    }
    else {
        Write-Host "Existe déjà : $entry"
    }
}

Write-Host " Hosts mis à jour."
