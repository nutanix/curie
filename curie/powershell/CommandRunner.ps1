# Import all functions and classes
using module ./Vmm.psm1

Import-Module ./Vmm.psm1
Import-Module ./Logging.psm1


function Connect-VmmSession ($vmm_address, $username, $password) {
    # $str = "Username = " + $username
    # Write-Log "INFO"  $str
    # Write-Log "INFO" "Password = *********"
    # $str = "VMM Server = " + $vmm_address
    # Write-Log "INFO"  $str

    # create new PS remote session
    $session = $null
    # do the stuff
    $pwd = ConvertTo-SecureString -AsPlainText -Force -String $password
    $cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $username, $pwd
    # TODO (iztokp): Use session options when they become available on Linux.
    #$so = New-PSSessionOption –SkipCACheck –SkipCNCheck –SkipRevocationCheck
    #$session = New-PSSession -SessionOption $so -ComputerName $requestJson.host_address -Credential $cred -Authentication Negotiate
    $session = New-PSSession -ComputerName $vmm_address -Credential $cred -Authentication Negotiate
    Write-Log "INFO" "Session created successfully"

    try {
        $test = Invoke-Command -Session $session {}
        $test = $null
    } catch {}

    $sessionpid=Invoke-Command -Session $session -ScriptBlock {$PID} -ErrorAction Stop
    Write-Log "INFO" "Remote session PID is '$sessionpid'."

    $sc = {
        $vmmPwd = ConvertTo-SecureString -AsPlainText -Force -String $Using:password
        $vmmCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $Using:username, $vmmPwd
        $vmmServer = Get-SCVMMServer -ComputerName $Using:vmm_address -Credential $vmmCred
        $vmmServer
    }

    $vmmServer = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop
    If ( $vmmServer -ne $Null) {
        Write-Log "INFO" "Connected to VMM server " + $vmm_address
    }
    else {
        Write-Log "ERROR" "Could not connect to VMM server: " + $_.Exception.Message
        throw "Could not connect to VMM server: " + $_.Exception.Message
    }
    return $session
}

function NeedsRedaction($param_name)
{
    $lower = ([string]$param_name).ToLower()
    return ($lower -Like "*user*" -Or
            $lower -Like "*pass*" -Or
            $lower -Like "*login*")
}

function SanitizeParams($params)
{
    $redaction_string = "<REDACTED>"
    $sanitized = ""
    $index = 0
    while($index -lt $params.Length)
    {
        $param_name = $params[$index]
        $param_value = $params[$index + 1]

        # Special case for json_params. The below will hide sensitive info
        # in the first level of the hashtable, and will skip array types.
        if($param_name.ToLower() -Eq "-json_params")
        {
            $json_params = ConvertFrom-Json -AsHashtable $param_value
            if($json_params -is [hashtable])
            {
                $sanitized_json_params = @{}
                foreach ($json_param in $json_params.GetEnumerator())
                {
                    $value = $json_param.Value
                    if (NeedsRedaction $json_param.Name)
                    {
                        $value = $redaction_string
                    }
                    $sanitized_json_params[$json_param.Name] = $value
                }
                $param_value = ConvertTo-Json $sanitized_json_params -Compress
            }
        }
        elseif(NeedsRedaction $param_name)
        {
            $param_value = $redaction_string
        }

        $sanitized += " " + [string]$param_name + " " + [string]$param_value
        $index = $index + 2
    }
    return $sanitized
}

# Main

function ExecuteCommand($vmm_address, $username, $password, $function_name) {
    $exitCode = 0
    try
    {
        $session = Connect-VmmSession -vmm_address $vmm_address -username $username -password $password
        if ($session -eq $null)
        {
            throw "Connect-VmmSession failed."
        }
        Write-Log "INFO"  "Connection successful"

        # Check if PS session state is Open
        if ($session.State -ne 'Opened') {
            $sessState = $session.State.ToString()
            throw "Session is in state '$sessState'. Should be in state 'Opened'."
        }

        Write-Log "INFO" ("Start executing function '" + $function_name + "' -session <session>" + (SanitizeParams $Args))
        $val = &$function_name $session @Args

        $ret = ConvertTo-JSON -InputObject $val -Depth 5 -Compress
        "`n" + $ret  # JSON to stdout with leading newline character.
        Write-Log "INFO" ("Command success: " + $ret)
    } catch [HypervRestApiException] {
        $e = $PSItem.Exception
        $val = [HypervRestApiResponse]::new($e.Error)

        $ret = ConvertTo-JSON -InputObject $val -Depth 5 -Compress
        "`n" + $ret  # JSON to stdout with leading newline character.
        Write-Log "INFO" ("Command failure: " + $ret)
    } catch {
        $exitCode=1
        $ret = "{}"
        "`n" + $ret  # JSON to stdout with leading newline character.
        Write-Log "ERROR" ("Unhandled command failure: " + $_.Exception.Message)
    } finally {
        try
        {
            Invoke-Command -Session $session -ScriptBlock {Stop-Process -Id $PID | Out-Null} -ErrorAction Stop | Out-Null
        } catch { }
        Write-Log "INFO" "Disconnect successful"
    }
    exit $exitCode
}
