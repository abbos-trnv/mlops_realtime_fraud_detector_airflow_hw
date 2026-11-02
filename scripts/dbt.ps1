param(
    [ValidateSet('setup', 'load', 'build', 'test', 'docs', 'serve', 'lint')]
    [string]$Action = 'build'
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
$dbtExe = Join-Path $projectRoot '.venv-dbt/Scripts/dbt.exe'
function Invoke-Checked {
    param([string]$Executable, [string[]]$Arguments)
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed: $Executable $Arguments" }
}
switch ($Action) {
    setup {
        Invoke-Checked 'py' @('-3.12', '-m', 'venv', '.venv-dbt')
        Invoke-Checked '.venv-dbt/Scripts/python.exe' @('-m', 'pip', 'install', '-r', 'requirements-dbt.txt')
        Set-Location dbt
        Invoke-Checked $dbtExe @('deps', '--profiles-dir', '.')
    }
    load {
        Invoke-Checked 'docker' @('compose', '-f', 'compose.dbt.yaml', 'up', '-d', '--wait')
        Invoke-Checked '.venv-dbt/Scripts/python.exe' @('scripts/load_dbt_data.py')
    }
    build {
        Set-Location dbt
        Invoke-Checked $dbtExe @('build', '--profiles-dir', '.')
    }
    test {
        Set-Location dbt
        Invoke-Checked $dbtExe @('test', '--profiles-dir', '.')
    }
    docs {
        Set-Location dbt
        Invoke-Checked $dbtExe @('docs', 'generate', '--profiles-dir', '.')
    }
    serve {
        Set-Location dbt
        Invoke-Checked $dbtExe @('docs', 'serve', '--profiles-dir', '.', '--host', '127.0.0.1', '--port', '8082')
    }
    lint {
        Set-Location dbt
        Invoke-Checked '../.venv-dbt/Scripts/sqlfluff.exe' @('lint', 'models', 'tests', '--config', '.sqlfluff')
    }
}
