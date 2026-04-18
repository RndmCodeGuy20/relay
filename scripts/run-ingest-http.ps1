param(
    [string]$FilePath = "requests/ingest.http",
    [int]$DelayMs = 0,
    [int]$TimeoutSec = 30,
    [switch]$StopOnFail,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-Template {
    param(
        [string]$Value,
        [hashtable]$Variables
    )

    if ([string]::IsNullOrEmpty($Value)) {
        return $Value
    }

    return [regex]::Replace($Value, "\{\{\s*([a-zA-Z0-9_]+)\s*\}\}", {
        param($m)
        $key = $m.Groups[1].Value
        if ($Variables.ContainsKey($key)) {
            return [string]$Variables[$key]
        }
        return $m.Value
    })
}

function Parse-Variables {
    param([string]$RawContent)

    $vars = @{}
    $matches = [regex]::Matches($RawContent, "(?m)^\s*@([A-Za-z0-9_]+)\s*=\s*(.+?)\s*$")
    foreach ($match in $matches) {
        $name = $match.Groups[1].Value
        $value = $match.Groups[2].Value.Trim()
        $vars[$name] = $value
    }

    return $vars
}

function Parse-Requests {
    param([string]$RawContent)

    $blocks = [regex]::Split($RawContent, "(?m)^###.*\r?\n")
    $requests = @()

    foreach ($block in $blocks) {
        $trimmed = $block.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }

        if ($trimmed.StartsWith("@")) {
            continue
        }

        $lines = $trimmed -split "\r?\n"

        $methodLineIndex = -1
        for ($i = 0; $i -lt $lines.Length; $i++) {
            $line = $lines[$i].Trim()
            if ([string]::IsNullOrWhiteSpace($line)) {
                continue
            }
            if ($line -match "^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+\S+") {
                $methodLineIndex = $i
                break
            }
        }

        if ($methodLineIndex -lt 0) {
            continue
        }

        $methodLineParts = $lines[$methodLineIndex].Trim() -split "\s+", 2
        $method = $methodLineParts[0]
        $url = $methodLineParts[1]

        $headers = @{}
        $bodyStartIndex = -1

        for ($i = $methodLineIndex + 1; $i -lt $lines.Length; $i++) {
            $line = $lines[$i]
            if ([string]::IsNullOrWhiteSpace($line)) {
                $bodyStartIndex = $i + 1
                break
            }

            if ($line -match "^\s*([A-Za-z0-9-]+)\s*:\s*(.+?)\s*$") {
                $headerName = $matches[1]
                $headerValue = $matches[2]
                $headers[$headerName] = $headerValue
            }
        }

        $body = ""
        if ($bodyStartIndex -ge 0 -and $bodyStartIndex -lt $lines.Length) {
            $body = ($lines[$bodyStartIndex..($lines.Length - 1)] -join "`n").Trim()
        }

        $requests += [PSCustomObject]@{
            Method  = $method
            URL     = $url
            Headers = $headers
            Body    = $body
        }
    }

    return $requests
}

if (-not (Test-Path $FilePath)) {
    throw "request file not found: $FilePath"
}

$rawContent = Get-Content -Raw -Path $FilePath
$vars = Parse-Variables -RawContent $rawContent
$requests = Parse-Requests -RawContent $rawContent

if ($requests.Count -eq 0) {
    throw "no requests found in: $FilePath"
}

$invokeWebRequestSupportsSkipHttpErrorCheck = (Get-Command Invoke-WebRequest).Parameters.ContainsKey("SkipHttpErrorCheck")

$passed = 0
$failed = 0

for ($i = 0; $i -lt $requests.Count; $i++) {
    $r = $requests[$i]
    $index = $i + 1

    $resolvedUrl = Resolve-Template -Value $r.URL -Variables $vars
    $resolvedBody = Resolve-Template -Value $r.Body -Variables $vars

    Write-Host "[$index/$($requests.Count)] $($r.Method) $resolvedUrl"

    if ($DryRun) {
        continue
    }

    $headers = @{}
    foreach ($k in $r.Headers.Keys) {
        $headers[$k] = Resolve-Template -Value ([string]$r.Headers[$k]) -Variables $vars
    }

    $params = @{
        Method    = $r.Method
        Uri       = $resolvedUrl
        Headers   = $headers
        TimeoutSec = $TimeoutSec
    }

    if (-not [string]::IsNullOrWhiteSpace($resolvedBody)) {
        $params["Body"] = $resolvedBody
    }

    if ($invokeWebRequestSupportsSkipHttpErrorCheck) {
        $params["SkipHttpErrorCheck"] = $true
    }

    try {
        $resp = Invoke-WebRequest @params
        $status = [int]$resp.StatusCode
        if ($status -ge 200 -and $status -lt 300) {
            $passed++
            Write-Host "  -> PASS ($status)"
        } else {
            $failed++
            Write-Host "  -> FAIL ($status)"
            if ($StopOnFail) {
                throw "request failed with status $status"
            }
        }
    }
    catch {
        $failed++
        Write-Host "  -> FAIL (exception: $($_.Exception.Message))"
        if ($StopOnFail) {
            throw
        }
    }

    if ($DelayMs -gt 0 -and $index -lt $requests.Count) {
        Start-Sleep -Milliseconds $DelayMs
    }
}

Write-Host ""
Write-Host "Completed: $($requests.Count) request(s), passed=$passed, failed=$failed"

if (-not $DryRun -and $failed -gt 0) {
    exit 1
}
