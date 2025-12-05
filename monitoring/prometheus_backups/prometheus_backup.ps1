# Script de sauvegarde Prometheus
# Usage: .\prometheus_backup.ps1

# Configuration
$BACKUP_DIR = "prometheus_backups"
$DATE = Get-Date -Format "yyyyMMdd_HHmm"
$BACKUP_FILE = "prometheus_backup_$DATE.tar.gz"
$CONTAINER_NAME = "prometheus"
$RETENTION_DAYS = 30

# Créer le dossier de backup s'il n'existe pas
if (!(Test-Path $BACKUP_DIR)) {
    New-Item -ItemType Directory -Path $BACKUP_DIR -Force | Out-Null
    Write-Host "✓ Dossier de backup créé: $BACKUP_DIR" -ForegroundColor Green
}

Write-Host "`n=== Sauvegarde Prometheus ===" -ForegroundColor Cyan
Write-Host "Date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan

# Vérifier si le conteneur Prometheus est en cours d'exécution
$containerRunning = docker ps --filter "name=$CONTAINER_NAME" --format "{{.Names}}" | Select-String -Pattern "^$CONTAINER_NAME$"

if (!$containerRunning) {
    Write-Host "✗ Erreur: Le conteneur Prometheus n'est pas en cours d'exécution" -ForegroundColor Red
    exit 1
}

Write-Host "`n1. Création d'un snapshot..." -ForegroundColor Yellow

# Créer un snapshot via l'API Prometheus (si admin API est activé)
# Sinon, nous allons simplement copier les données
try {
    $response = Invoke-WebRequest -Uri "http://localhost:9090/api/v1/admin/tsdb/snapshot" -Method Post -ErrorAction SilentlyContinue
    if ($response.StatusCode -eq 200) {
        $snapshotName = ($response.Content | ConvertFrom-Json).data.name
        Write-Host "✓ Snapshot créé: $snapshotName" -ForegroundColor Green
        
        # Exporter le snapshot
        docker exec $CONTAINER_NAME tar czf /tmp/$BACKUP_FILE -C /prometheus/snapshots $snapshotName
        docker cp "${CONTAINER_NAME}:/tmp/$BACKUP_FILE" "$BACKUP_DIR\$BACKUP_FILE"
        docker exec $CONTAINER_NAME rm /tmp/$BACKUP_FILE
    }
} catch {
    Write-Host "⚠ API Admin non activée, backup direct du volume..." -ForegroundColor Yellow
    
    # Backup direct du volume (méthode alternative)
    Write-Host "2. Copie des données du volume..." -ForegroundColor Yellow
    docker run --rm -v data_platform_prometheus_data:/source -v ${PWD}/script_backups/prometheus_backups:/backup alpine tar czf /backup/$BACKUP_FILE -C /source .
}

if (Test-Path "$BACKUP_DIR\$BACKUP_FILE") {
    $fileSize = (Get-Item "$BACKUP_DIR\$BACKUP_FILE").Length / 1MB
    Write-Host "✓ Backup créé avec succès: $BACKUP_FILE ($([math]::Round($fileSize, 2)) MB)" -ForegroundColor Green
    
    # Nettoyer les anciens backups
    Write-Host "`n3. Nettoyage des anciens backups (>$RETENTION_DAYS jours)..." -ForegroundColor Yellow
    $oldBackups = Get-ChildItem -Path $BACKUP_DIR -Filter "prometheus_backup_*.tar.gz" | 
                  Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$RETENTION_DAYS) }
    
    if ($oldBackups) {
        foreach ($oldBackup in $oldBackups) {
            Remove-Item $oldBackup.FullName -Force
            Write-Host "  ✓ Supprimé: $($oldBackup.Name)" -ForegroundColor Gray
        }
    } else {
        Write-Host "  ✓ Aucun ancien backup à supprimer" -ForegroundColor Gray
    }
    
    # Liste des backups disponibles
    Write-Host "`n4. Backups disponibles:" -ForegroundColor Yellow
    Get-ChildItem -Path $BACKUP_DIR -Filter "prometheus_backup_*.tar.gz" | 
        Sort-Object LastWriteTime -Descending | 
        ForEach-Object {
            $size = [math]::Round($_.Length / 1MB, 2)
            Write-Host "  - $($_.Name) ($size MB) - $($_.LastWriteTime)" -ForegroundColor Gray
        }
    
    Write-Host "`n✓ Sauvegarde terminée avec succès!" -ForegroundColor Green
} else {
    Write-Host "✗ Erreur lors de la création du backup" -ForegroundColor Red
    exit 1
}
