# ===========================================
# Auto-relance du script en mode Administrateur
# ===========================================

# Vérifier si le script est lancé en admin
$IsAdmin = ([Security.Principal.WindowsPrincipal] `
    [Security.Principal.WindowsIdentity]::GetCurrent() `
    ).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)

Write-Host "Is Admin: $IsAdmin"

if (-not $IsAdmin) {
    Write-Host "Elevation en mode administrateur requise..."

    # Construire chemin du script entre guillemets
    $scriptPath = '"' + $PSCommandPath + '"'

    # Relancer le script en admin et MAINTENIR la fenêtre ouverte
    Start-Process powershell.exe `
        -Verb RunAs `
        -ArgumentList "-NoExit", "-ExecutionPolicy Bypass", "-File $scriptPath"

    exit
}

# ===========================================
# IMPORTANT : revenir dans le dossier du script
# ===========================================

$scriptFolder = Split-Path -Path $PSCommandPath
Set-Location -Path $scriptFolder
Write-Host "Working Directory: $scriptFolder"

# ===========================================
# Mise à jour du fichier hosts
# ===========================================

Write-Host "Mise a jour du fichier hosts..."
powershell -NoProfile -ExecutionPolicy Bypass -File "./update-hosts.ps1"

# ===========================================
# Démarrage de Docker
# ===========================================

Write-Host "Demarrage de la plateforme de donnees..."
docker compose up -d
