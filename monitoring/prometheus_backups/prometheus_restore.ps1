# Script de restauration Prometheus
# Usage: .\prometheus_restore.ps1 <nom_fichier_backup>

param(
    [Parameter(Mandatory=$false)]
    [string]$BackupFile
)

$BACKUP_DIR = ".\script_backups\prometheus_backups"
$CONTAINER_NAME = "prometheus"

Write-Host "`n=== Restauration Prometheus ===" -ForegroundColor Cyan

# Si aucun fichier n'est spécifié, afficher la liste
if (!$BackupFile) {
    Write-Host "`nBackups disponibles:" -ForegroundColor Yellow
    $backups = Get-ChildItem -Path $BACKUP_DIR -Filter "prometheus_backup_*.tar.gz" | Sort-Object LastWriteTime -Descending
    
    if ($backups.Count -eq 0) {
        Write-Host "✗ Aucun backup trouvé dans $BACKUP_DIR" -ForegroundColor Red
        exit 1
    }
    
    for ($i = 0; $i -lt $backups.Count; $i++) {
        $size = [math]::Round($backups[$i].Length / 1MB, 2)
        Write-Host "  $($i + 1). $($backups[$i].Name) ($size MB) - $($backups[$i].LastWriteTime)" -ForegroundColor Gray
    }
    
    $selection = Read-Host "`nSélectionnez le numéro du backup à restaurer (ou Entrée pour annuler)"
    
    if ([string]::IsNullOrWhiteSpace($selection)) {
        Write-Host "Restauration annulée" -ForegroundColor Yellow
        exit 0
    }
    
    $index = [int]$selection - 1
    if ($index -ge 0 -and $index -lt $backups.Count) {
        $BackupFile = $backups[$index].Name
    } else {
        Write-Host "✗ Sélection invalide" -ForegroundColor Red
        exit 1
    }
}

$BACKUP_PATH = "$BACKUP_DIR\$BackupFile"

# Vérifier que le fichier existe
if (!(Test-Path $BACKUP_PATH)) {
    Write-Host "✗ Erreur: Le fichier $BackupFile n'existe pas" -ForegroundColor Red
    exit 1
}

Write-Host "`nFichier à restaurer: $BackupFile" -ForegroundColor Yellow

# Confirmation
$confirm = Read-Host "`n⚠ ATTENTION: Cette opération va écraser les données actuelles de Prometheus. Continuer? (oui/non)"
if ($confirm -ne "oui") {
    Write-Host "Restauration annulée" -ForegroundColor Yellow
    exit 0
}

# Arrêter le conteneur Prometheus
Write-Host "`n1. Arrêt du conteneur Prometheus..." -ForegroundColor Yellow
docker-compose stop prometheus
Write-Host "✓ Conteneur arrêté" -ForegroundColor Green

# Restaurer les données
Write-Host "`n2. Restauration des données..." -ForegroundColor Yellow
docker run --rm -v data_platform_prometheus_data:/target -v ${PWD}/script_backups/prometheus_backups:/backup alpine sh -c "rm -rf /target/* && tar xzf /backup/$BackupFile -C /target"

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Données restaurées" -ForegroundColor Green
} else {
    Write-Host "✗ Erreur lors de la restauration" -ForegroundColor Red
    exit 1
}

# Redémarrer le conteneur
Write-Host "`n3. Redémarrage du conteneur Prometheus..." -ForegroundColor Yellow
docker-compose start prometheus

# Attendre que Prometheus soit prêt
Write-Host "Attente du démarrage de Prometheus..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:9090/-/ready" -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            $ready = $true
            break
        }
    } catch {
        Start-Sleep -Seconds 2
    }
}

if ($ready) {
    Write-Host "✓ Prometheus est prêt" -ForegroundColor Green
    Write-Host "`n✓ Restauration terminée avec succès!" -ForegroundColor Green
    Write-Host "Prometheus est accessible sur http://localhost:9090" -ForegroundColor Cyan
} else {
    Write-Host "⚠ Prometheus a démarré mais n'est pas encore prêt. Vérifiez manuellement." -ForegroundColor Yellow
}
