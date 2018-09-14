function Write-Log([String]$level, [String]$message) {
    $formatted = [string]::Format("[{0:u}] [{1, -5}] [{2, -7}] {3}", (Get-Date), $PID, $level, $message)
    if ($level.ToLower().StartsWith("error")) {
        Write-Error -Message $formatted -ErrorAction Continue
    }
    else {
        [Console]::Error.WriteLine($formatted)
    }
    # $formatted | Out-File -Append -filepath $this.path -Encoding ASCII
}
