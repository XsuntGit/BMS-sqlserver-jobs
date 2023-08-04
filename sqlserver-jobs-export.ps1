param(
    [Parameter(Mandatory)][string]$servers,
    [Parameter(Mandatory)][string]$basepath
)
Set-DbatoolsInsecureConnection -SessionOnly
Import-Module dbatools
foreach ($server in $servers.Split(',')) {
    $jobs = (Get-DbaAgentJob -SqlInstance $server | Select-Object Name).Name
    foreach ($job in $jobs) {
        if (-not (Test-Path($basepath + "\" + $server))) {
            New-Item -Path ($basepath + "\" + $server) -ItemType Directory
        }
        $path = $basepath + "\" + $server + "\" + ($job -replace '[^a-zA-Z0-9]',' ') + ".sql"
        Get-DbaAgentJob -SqlInstance $server -Job $job | Export-DbaScript -FilePath $path
    }
}
