# run_brown.ps1
# Carrega variáveis do .env para o processo e roda o app (Docker-free)

param(
  [string]$EnvPath = "..\..\sinan.env"
)

if (!(Test-Path $EnvPath)) {
  Write-Error "Arquivo .env não encontrado: $EnvPath"
  exit 1
}

Get-Content $EnvPath | ForEach-Object {
  $line = $_.Trim()
  if ($line -eq "" -or $line.StartsWith("#")) { return }

  $parts = $line.Split("=", 2)
  if ($parts.Count -ne 2) { return }

  $key = $parts[0].Trim()
  $val = $parts[1].Trim()

  # remove aspas nas pontas, se existirem
  if (($val.StartsWith("'") -and $val.EndsWith("'")) -or ($val.StartsWith('"') -and $val.EndsWith('"'))) {
    $val = $val.Substring(1, $val.Length - 2)
  }

  [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
}

# (opcional) ativa venv se existir
if (Test-Path ".\.venv\Scripts\Activate.ps1") {
  . .\.venv\Scripts\Activate.ps1
}

python -m vertex.descriptive_dashboard
