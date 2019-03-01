#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
# Author: bostjan.ferlic@nutanix.com
#

Import-Module $PSScriptRoot/Logging.psm1

class HypervRestApiMessageResource {
    [string]$reason
    [string]$message
    [object]$details
    HypervRestApiMessageResource($reason, $message, $details) {
        $this.reason = $reason
        $this.message = $message
        $this.details = $details
    }
}

# Class represents an error response body
class HypervRestApiError {
    [int]$code
    [System.Collections.ArrayList]$message_list
    HypervRestApiError($code, $reason, $message, $details) {
        $this.code = $code
        $this.message_list = @()
        $this.message_list.Add([HypervRestApiMessageResource]::new($reason, $message, $details))
    }
    AddMessage($reason, $message, $details) {
        $this.message_list.Add([HypervRestApiMessageResource]::new($reason, $message, $details))
    }
}

# Class represents a HTTP response with HTTP status code and response body
class HypervRestApiResponse {
    [int]$StatusCode
    [object]$Response
    HypervRestApiResponse([int]$statusCode, [object]$response) {
        $this.StatusCode = $statusCode
        $this.Response = $response
    }
    HypervRestApiResponse([HypervRestApiError]$errorResponse) {
        $this.StatusCode = $errorResponse.code
        $this.Response = $errorResponse.message_list
    }
}

class HypervRestApiException : System.Exception {
    [HypervRestApiError]$Error

    HypervRestApiException($code, $reason, $message, $details) : base($message) {
        $this.Error = [HypervRestApiError]::new($code,$reason, $message, $details)
    }
    AddMessage($reason, $message, $details) {
        $this.Error.AddMessage($reason, $message, $details)
    }
}


Function Set-VmmVMPowerState($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    [HypervRestApiResponse]$response = $null
    try {
        $sc = {
            $tasks = @{}
            foreach($param in $Using:params)
            {
                $vm_id = $param.vm_id
                $vm = Get-SCVirtualMachine -ID $vm_id
                switch($param.power_state) {
                    "off" {
                        if($vm.Status -eq "Running") {
                            $result = Stop-SCVirtualMachine -VM $vm -RunAsynchronously -jobvariable "XRayTurnOffVm"
                        } elseif($vm.Status -eq "Saved") {
                            $result = DiscardSavedState-VM -VM $vm -RunAsynchronously -jobvariable "XRayTurnOffVm"
                        } elseif($vm.Status -eq "CreationFailed") {
                            $result = Repair-SCVirtualMachine -VM $vm -Dismiss -Force -RunAsynchronously -jobvariable "XRayTurnOffVm"
                        } else {
                            $result = Stop-SCVirtualMachine -VM $vm -RunAsynchronously -Force -jobvariable "XRayTurnOffVm"
                        }
                        $result = $null
                        $tasks[$vm_id] = $XRayTurnOffVm.ID.Guid
                    }
                    "on" {
                        if($vm.Status -eq "Saved") {
                            $result = DiscardSavedState-VM -VM $vm -RunAsynchronously
                        }
                        $result = Start-SCVirtualMachine -VM $vm -RunAsynchronously -jobvariable "XRayTurnOnVm"
                        $result = $null
                        $tasks[$vm_id] = $XRayTurnOnVm.ID.Guid
                    }
                }
            }
            $tasks
        }
        $tasks = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $ret = @()
        foreach($task in $tasks.GetEnumerator())
        {
            $obj = @{}
            $obj['vm_id'] = $task.Name
            $obj['task_id'] = $task.Value
            $obj["task_type"] = "vmm"
            $ret += $obj
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error while setting VM power state. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while setting VM power state. " + $msg, $params)
    }

    return $response
}

# Returns list of tasks. Tasks requested must be of same type (e.g. vmm or ps).
Function Get-Task($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    [HypervRestApiResponse]$response = $null

    $ret = @() # Create an array

    try {
        if($params[0].task_type -eq 'vmm') {
            try {
                $sc = {
                    $ret = @()
                    foreach($task in $Using:params) {
                        $js = @{}
                        $js['task_id'] = $task.task_id
                        $js['task_type'] = $task.task_type

                        # Handle special case when task was not created
                        if ($task.task_id -eq [GUID]::Empty) {
                            $js['completed'] = $true
                            $js['progress'] = "0"
                            $js['state'] = "failed"
                            $js['status'] = ""
                            $js['error'] = "Task could not be created."
                            continue
                        }

                        $status = Get-SCJob -ID $task.task_id -ErrorAction Stop


                        $js['completed'] = $status.IsCompleted
                        $js['progress'] = $status.ProgressValue.ToString()

                        switch ($status.Status) {
                                Failed { $js['state'] = 'failed'; break }
                                Canceled { $js['state'] = 'stopped'; break }
                                Completed { $js['state'] = 'completed'; break }
                                SucceedWithInfo { $js['state'] = 'completed'; break } # Threat "Completed w/Info" tasks the same way as "successfuly" completed tasks
                                Default { $js['state'] = 'running'; break } # Running, ...
                        }
                        $js['status'] = $status.StatusString
                        if ($status.ErrorInfo.Code.ToString() -ne "Success") {
                            $errMsg = "VMM Job ID: " + $task.task_id + "`n"
                            $errMsg += $status.ErrorInfo | Out-String
                            $js['error'] = $errMsg
                        } else {
                            $js['error'] = ""
                        }

                        $ret += $js
                    }
                    ,$ret
                }

                $ret = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction stop
                if (-not $ret) {
                    throw "Invoke-Command returned null."
                }
            } catch {
                # This is unexpected so we throw an exception
                $msg = $_.Exception.Message
                Write-Log "ERROR"  "Cannot get job status. " + $msg
                throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Cannot get job status. " + $msg, $params)
            }
        } else {
            foreach ($task in $params) {
                $taskId = $task.task_id
                $taskType = $task.task_type

                $js = @{}
                $js['task_id'] = $taskId
                $js['task_type'] = $taskType

                # Handle special case when task was not created
                if ($taskType -eq [GUID]::Empty) {
                    $js['completed'] = $true
                    $js['progress'] = "0"
                    $js['state'] = "failed"
                    $js['status'] = ""
                    $js['error'] = "Task could not created."
                    continue
                }

                try {
                    # PS job
                    $job = Get-Job -InstanceId $taskId

                    if ($job -ne $null) {
                        # (iztokp): To get reals status we must check the child job.
                        # We assume here that there is only one child job. There can be more if job is started
                        # e.g. for multiple computers.
                        $childJob = $job.ChildJobs[0]
                        $js['error'] = ''
                        $js['status'] = ''

                        if ($childJob.State -eq 'Completed') {
                            $js['state'] = 'completed'
                            $js['completed'] = $true
                            $js['progress'] = 100
                        } elseif ($childJob.State -eq 'Failed') {
                            $js['state'] = 'failed'
                            $js['completed'] = $true
                            $js['progress'] = 100
                        } elseif ($childJob.State -eq 'Stopped') {
                            $js['state'] = 'stopped'
                            $js['completed'] = $true
                            $js['progress'] = 100
                        } else {
                            # Running, AtBreakpoint, Blocked, Disconnected, NotStarted, Stopping, Suspended, Suspending
                            $js['state'] = 'running'
                            $js['completed'] = $false

                            # check if we can get the progress
                            $pct = 0
                            $statusMsg = ''
                            try {
                                # get last progress status
                                $progress = $childJob.Progress[$childJob.Progress.Count-1]
                                if ($progress -ne $null) {
                                    if ($progress.PercentComplete -gt 0) {
                                        $pct = $progress.PercentComplete
                                    }
                                    $statusMsg = $progress.Activity
                                }
                            } finally {
                                $js['progress'] = $pct
                                $js['status'] = $statusMsg
                            }
                        }

                        # get error if possible
                        if ($childJob.Error.Count -gt 0) {
                            $js['state'] = 'failed'
                            $js['error'] = $childJob.Error | Out-String
                        }
                    }
                    else {
                        throw "Job with the specified ID '"+$taskId+"' does not exist!"
                    }
                    # PS job needs to be removed when completed
                    if ($js['completed']) {
                        $job | Remove-Job | Out-Null
                        Write-Log "INFO"  "Removed completed job " + $taskId
                    }

                } catch {
                    # This is unexpected so we throw an exception
                    $msg = $_.Exception.Message
                    Write-Log "ERROR"  "Cannot get job status. " + $msg
                    throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Cannot get job status. " + $msg, $params)
                }
                $ret += $js
            }
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error stopping tasks. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error stopping tasks. " + $msg, $params)
    }

    return $response
}

# Restarts tasks
Function Restart-Task($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    [HypervRestApiResponse]$response = $null

    $ret = @() # Create an array

    try {
        foreach ($task in $params) {
            $taskId = $task.task_id
            $taskType = $task.task_type

            $js = @{}
            if ($taskType -eq 'vmm') {
                $sc = {
                    $job = Get-SCJob -ID $using:taskId -ErrorAction Stop
                    $status = Restart-SCJob -Job $job
                    $job = $null
                    $status
                }
                try {
                    $result = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

                    $js['task_id'] = $result.ID
                    $js['task_type'] = $taskType
                    $js['completed'] = $result.IsCompleted
                    $js['progress'] = $result.ProgressValue.ToString()
                    $status = ''
                    switch ($result.Status) {
                        Failed { $status='failed' }
                        Completed { $status='completed' }
                        Canceled { $status='stopped' }
                        Default { $status='running' } # Running, ...
                    }
                    $js['state'] = $status
                    $js['status'] = $result.StatusString
                    $js['error'] = $result.ErrorInfo
                } catch {
                    # This is unexpected so we throw an exception
                    Write-Log "ERROR"   $_.Exception.Message
                    throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", $_.Exception.Message, $params)
                }
            }
            $ret += $js
        }

        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error restarting tasks. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error restarting tasks. " + $msg, $params)
    }

    return $response
}

# Stops tasks
Function Stop-Task($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    [HypervRestApiResponse]$response = $null

    $ret = @() # Create an array

    try {
        foreach ($task in $params) {
            $taskId = $task.task_id
            $taskType = $task.task_type

            $js = @{}
            if ($taskType -eq 'vmm') {
                $sc = {
                    $job = Get-SCJob -ID $using:taskId -ErrorAction Stop
                    $status = Stop-SCJob -Job $job
                    $job = $null
                    $status
                }
                try {
                    $result = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

                    $js['task_id'] = $taskId
                    $js['task_type'] = $taskType
                    $js['completed'] = $result.IsCompleted
                    $js['progress'] = $result.ProgressValue.ToString()
                    $status = ''
                    switch ($result.Status) {
                        Failed { $status='failed' }
                        Completed { $status='completed' }
                        Canceled { $status='stopped' }
                        Default { $status='running' } # Running, ...
                    }
                    $js['state'] = $status
                    $js['status'] = $result.StatusString
                    $js['error'] = $result.ErrorInfo
                } catch {
                    # This is unexpected so we throw an exception
                    Write-Log "ERROR"   $_.Exception.Message
                    throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", $_.Exception.Message, $params)
                }
            }
            else {
                try{
                    # PS job
                    $jb = Stop-Job -InstanceId $taskId -PassThru
                    $job = $jb.ChildJobs[0]
                    if ($job -ne $null) {
                        $js['task_id'] = $taskId
                        $js['task_type'] = $taskType
                        if ($job.JobStateInfo -eq 'Completed') {
                            $js['completed'] = $true
                            $js['progress'] = 100
                        } elseif ($job.JobStateInfo -eq 'Failed') {
                            $js['completed'] = $true
                            $js['progress'] = 100
                        } elseif ($job.JobStateInfo -eq 'Stopped') {
                            $js['completed'] = $true
                            $js['progress'] = 100
                        } else {
                            $js['completed'] = $false
                            $js['progress'] = 0
                        }
                        $status = ''
                        switch ($job.State) {
                            Failed { $status='failed' }
                            Completed { $status='completed' }
                            Stopped {$status='stopped'}
                            Default { $status='running' } # Running, AtBreakpoint, Blocked, Disconnected, NotStarted, Stopping, Suspended, Suspending
                        }
                        $js['state'] = $status
                        $js['status'] = $job.StatusMessage
                        $js['error'] = ''
                    }else{
                        Write-Log "WARNING"  "Job with the specified ID '"+$taskId+"' does not exist!"
                        throw "Job with the specified ID '"+$taskId+"' does not exist!"
                    }
                } catch{
                    # This is unexpected so we throw an exception
                    Write-Log "ERROR"   $_.Exception.Message
                    throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", $_.Exception.Message, $params)
                }
            }
            $ret += $js
        }

        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error stopping tasks. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error stopping tasks. " + $msg, $params)
    }

    return $response
}

# Transfers all golden disk images to the VMM library server target location. If the goldimage is comressed (.zip)
# the compressed file is uncompressed and then deleted on the target.
# Function checks if goldimage with the same filename already exists on the VMM library server share root path. If
# if does exist we do not copy the goldimage file. If compressed (.zip) goldimage is transfered then we check if
# the same file without .zip extension exists in the VMM library server share root path.
Function Install-VmmDiskImage($session, $json_params="{}", $overwriteFiles) {
    $params = ConvertFrom-Json $json_params
    [HypervRestApiResponse]$response = $null
    $ret = @()

    try {
        $goldimagesDiskList = $params.goldimage_disk_list
        $goldimagesTargetDir = $params.goldimage_target_dir
        $diskName = $params.disk_name

        $vmmLibraryServerShare = $params.vmm_library_server_share

        $libraryHost = $params.vmm_library_server
        $pass = $params.vmm_library_server_password
        $user = $params.vmm_library_server_user

        if (-not $params.transfer_type) {
            $sc = {
                param($user, $pass, $libHost, $libShare, $targetDir, $diskImageList, $overwriteFiles, $verbose)

                $VerbosePreference='Continue'

                $passSecure = ConvertTo-SecureString -AsPlainText -Force -String $pass
                $cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $user, $passSecure

                $s = New-PSSession -ComputerName $libHost -Credential $cred -Authentication Negotiate
                if ($verbose) {
                    Write-Verbose "Connected to VMM library server host '$libHost'." 4>&1
                }

                # Due to a strange error on Linux platform, we are ignoring the error from first 'Invoke-Command'
                # "error": "Windows Principal functionality is not supported on this platform.
                #   CategoryInfo          : NotSpecified: (:) [Invoke-Command], PlatformNotSupportedException
                #   FullyQualifiedErrorId : System.PlatformNotSupportedException,Microsoft.PowerShell.Commands.InvokeCommandCommand
                #   PSComputerName        : localhost\n \n",
                # TODO (iztokp): We should remove this 'Invoke-Command' after PS bug is fixed: https://github.com/PowerShell/PowerShell/issues/6435
                try {
                    $test = Invoke-Command -Session $s {}
                    $test = $null
                } catch {}

                $localDrive = $null

                try {
                    # Map first free drive on the VMM library server to the library share
                    $mapLibraryShare = {
                        param($path, $user, $pass)
                        # try to connect a few times
                        $freeDrive = ""
                        $ret = $null
                        $numOfRetries = 10
                        for ($i=0; $i -lt $numOfRetries; $i++) {
                            for($j=67;$j -lt 90; ++$j) {
                                $d = [char]$j
                                if (-not (Get-PSDrive($d)2>$Null)) {
                                    if (-not (Get-SmbMapping($d + ":")2>$Null)) {
                                        break
                                    }
                                }
                            }
                            $freeDrive = $d + ":"
                            try {
                                $mapping = New-SmbMapping -LocalPath $freeDrive -RemotePath $path -Persistent $true -UserName $user `
                                                          -Password $pass -ErrorAction Stop 4>&1
                                break
                            } catch {
                                if ($i -lt ($numOfRetries-1)) {
                                    Start-Sleep -Seconds 5
                                } else {
                                    #Write-Error "Could not map '$path' to '$freeDrive'."
                                    throw
                                }
                            }
                        }
                        if ($mapping) {
                            $ret = $mapping | Select-Object Status,LocalPath
                        }
                        $ret
                    }
                    $drive = Invoke-Command -Session $s -ScriptBlock $mapLibraryShare -ArgumentList $libShare, $user, $pass -ErrorAction Stop
                    if ($verbose) {
                        Write-Verbose $drive 4>&1
                    }
                    $localDrive = $drive.LocalPath

                    if ($drive.Status.ToString() -eq 'OK') {
                        $targetPath = $localDrive + "\" + $targetDir

                        $targetFiles=@()
                        $transferFiles=@()
                        $sourceFiles=@($diskImageList)

                        foreach ($src in $sourceFiles) {

                            $filename = $src
                            if($filename.indexOf("/") -ge 0)
                            {
                                $filename = $filename.Substring($filename.LastIndexOf("/") + 1)
                            }
                            # (bostjan): This is only needed if PS server is run on Windows.
                            if($filename.indexOf("\") -ge 0)
                            {
                                $filename = $filename.Substring($filename.LastIndexOf("\") + 1)
                            }
                            if ($overwriteFiles) {
                                Write-Info "Overwrite of all disk image files is forced." 4>&1
                                $transferFiles += $src
                                $targetFile = $targetPath + "\" + $filename
                                $targetFiles += $targetFile
                            }
                            else
                            {
                                $checkFileExistsScriptBlock = {
                                    param($targetDiskFile)
                                    $diskExists = Test-Path $targetDiskFile
                                    $diskExists
                                }
                                $checkFileScriptBlock = {
                                    param($targetDiskFile, $diskSize, $diskLastWrite)
                                    $diskExists = Test-Path $targetDiskFile
                                    $sameSize = $false
                                    if ($diskExists) {
                                        # Test size
                                        $file = Get-ChildItem $targetDiskFile
                                        $sameSize = $diskSize -eq $file.Length
                                    }
                                    $diskOk = $diskExists -and $sameSize
                                    $diskOk
                                }

                                # Check if disk image already exists in root or target folder
                                # TODO (iztokp): We should check if the files are the same, e.g. md5 hash (Get-FileHash <file> -Algorithm MD5)
                                # Since md5 check is slow we will compare at least size in bytes and last write time.
                                $disk = Get-Item -Path $src
                                if (-not $disk) {
                                    Write-Error "Error copying disk image '$src'. File does not exist." -ErrorAction Stop
                                    break
                                }
                                if ($disk.Extension -eq ".zip") {
                                    # If compressed image (.zip) we should check if goldimage without .zip extension
                                    # exists on the target
                                    $rootFileName = $filename.Substring(0,$filename.LastIndexOf("."))
                                    $rootFile = $localDrive + "\" + $rootFileName
                                } else {
                                    $rootFile = $localDrive + "\" + $fileName
                                }
                                $diskSize = $disk.Length
                                $diskLastWrite = $disk.LastWriteTimeUtc

                                $diskExists = Invoke-Command -Session $s -ScriptBlock $checkFileExistsScriptBlock -ArgumentList $rootFile `
                                                             -ErrorAction Stop
                                if ($verbose) {
                                    Write-Verbose $diskExists 4>&1
                                }

                                If(-not $diskExists)
                                {
                                    $targetFile = $targetPath + "\" + $filename
                                    $diskExists = Invoke-Command -Session $s -ScriptBlock $checkFileScriptBlock `
                                                  -ArgumentList $targetFile, $diskSize, $diskLastWrite -ErrorAction Stop
                                    If(-not $diskExists)
                                    {
                                        $str = "Adding disk image '" + $disk.Name + "' to transfer queue."
                                        if ($verbose) {
                                            Write-Verbose $str 4>&1
                                        }
                                        $transferFiles += $src
                                        $targetFiles += $targetFile
                                    }
                                }
                            }
                        }
                        # Create target path if it does not exist
                        $folderExists = Invoke-Command -Session $s { param($path); Test-Path $path -Verbose 4>&1 } -ArgumentList $targetPath
                        if ($verbose) {
                            Write-Verbose $folderExists 4>&1
                        }
                        if (-not $folderExists) {
                            $ic = Invoke-Command -Session $s { param($path); New-Item -ItemType Directory -Path $path -Force `
                                                 -ErrorAction Stop -Verbose 4>&1 } -ArgumentList $targetPath -ErrorAction Stop
                            if ($verbose) {
                                Write-Verbose $ic 4>&1
                            }
                            $ic = $null
                        }
                        if($transferFiles.Count -gt 0)
                        {
                            for ($i=0; $i -lt $transferFiles.Count; $i++)
                            {
                                try
                                {
                                    $transferFiles[$i] | Copy-Item -Force -Recurse -Destination $targetFiles[$i] -ToSession $s -ErrorAction Stop
                                    Write-Verbose "Copy file successfull: $transferFiles[$i]" 4>&1
                                } catch {
                                    $source = $transferFiles[$i]
                                    $target = $targetFiles[$i]
                                    $err = "Exception copying disk image '$source' to '$target'. " + $_.Exception.Message
                                    Write-Error $err -ErrorAction Stop
                                    break
                                }
                                $uncompress = {
                                    param($targetFile)
                                    $file = Get-Item $targetFile
                                    if ($file.Extension -eq ".zip") {
                                        # Silencing Expand-Archive progress bar output
                                        $oldProgressPreference=$ProgressPreference
                                        $ProgressPreference='SilentlyContinue'
                                        $file | Expand-Archive -Force -DestinationPath $file.Directory -ErrorAction Stop
                                        $ProgressPreference=$oldProgressPreference
                                        $file | Remove-Item -Force
                                    }
                                }
                                # if file is compressed (ends with .zip)
                                $cp = Invoke-Command -Session $s -ScriptBlock $uncompress -ArgumentList $targetFiles[$i] -ErrorAction Stop
                                if ($verbose) {
                                    Write-Verbose $cp 4>&1
                                }
                                $cp | Out-Null
                            }
                        }
                    } else {
                        throw "Copying disk image to VMM library share failed. Cannot map '$libShare'."
                    }
                } catch {
                    #rethrow
                    throw
                } finally {
                    # Need to make sure that drive is always unmapped and the session is closed.
                    $removeMappedDrive = {
                        param($drive)

                        Write-Verbose "Removing SMB mapping '$drive'." 4>&1
                        Remove-SMBMapping -Force -LocalPath $drive -UpdateProfile 2>$Null

                        $psDrive = $drive -replace ":" #remove unwanted colon from PSDrive name
                        If ( (Get-PSDrive -Name $psDrive) 2>$Null ) {
                            Write-Verbose "Running Remove-PsDrive -Name $psDrive -Force " 4>&1
                            Remove-PSDrive -Name $psDrive -Force
                        }
                    }

                    # Remove the mapped SMB share
                    if ($localDrive) {
                        try {
                            $rmDrive = Invoke-Command -Session $s -ScriptBlock $removeMappedDrive -ArgumentList $localDrive
                            if ($verbose) {
                                Write-Verbose $rmDrive 4>&1
                            }
                        } catch {} # skip any remove error we might hit
                    }

                    # destroy the session to library server
                    try {
                        $s | Remove-PSSession -Verbose 4>&1 | Out-Null
                    } catch {}
                    $s = $null
                }
            }
            $copyImage = Invoke-Command -Session $session -ScriptBlock $sc -ArgumentList $user, $pass, $libraryHost, $vmmLibraryServerShare, `
                           $goldimagesTargetDir, $goldimagesDiskList, $overwriteFiles, $true -ErrorAction Stop
            Write-Log "INFO" $copyImage
            $copyImage | Out-Null
        } elseif ($params.transfer_type -eq 'bits') { # this is the new copy

            $sc = {
                param($user, $pass, $libHost, $libShare, $targetDir, $diskImageList, $diskName, $verbose)

                $VerbosePreference='Continue'

                $passSecure = ConvertTo-SecureString -AsPlainText -Force -String $pass
                $cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $user, $passSecure

                $s = New-PSSession -ComputerName $libHost -Credential $cred -Authentication Negotiate
                if ($verbose) {
                    Write-Verbose "Connected to VMM library server host '$libHost'." 4>&1
                }

                # Due to a strange error on Linux platform, we are ignoring the error from first 'Invoke-Command'
                # "error": "Windows Principal functionality is not supported on this platform.
                #   CategoryInfo          : NotSpecified: (:) [Invoke-Command], PlatformNotSupportedException
                #   FullyQualifiedErrorId : System.PlatformNotSupportedException,Microsoft.PowerShell.Commands.InvokeCommandCommand
                #   PSComputerName        : localhost\n \n",
                # TODO (iztokp): We should remove this 'Invoke-Command' after PS bug is fixed: https://github.com/PowerShell/PowerShell/issues/6435
                try {
                    $test = Invoke-Command -Session $s {}
                    $test = $null
                } catch {}

                if ($verbose) {
                    $sessionpid=Invoke-Command -Session $s -ScriptBlock {$PID}
                    Write-Verbose "Remote VMM library server host session PID is '$sessionpid'." 4>&1
                }

                $localDrive = $null

                try {
                    # Map first free drive on the VMM library server to the library share
                    $mapLibraryShare = {
                        param($path, $user, $pass)
                        # try to connect a few times
                        $freeDrive = ""
                        $ret = $null
                        $numOfRetries = 10
                        for ($i=0; $i -lt $numOfRetries; $i++) {
                            for($j=67;$j -lt 90; ++$j) {
                                $d = [char]$j
                                if (-not (Get-PSDrive($d)2>$Null)) {
                                    if (-not (Get-SmbMapping($d + ":")2>$Null)) {
                                        break
                                    }
                                }
                            }
                            $freeDrive = $d + ":"
                            try {
                                $mapping = New-SmbMapping -LocalPath $freeDrive -RemotePath $path -Persistent $true -UserName $user `
                                                          -Password $pass -ErrorAction Stop -Verbose 4>&1
                                break
                            } catch {
                                if ($i -lt ($numOfRetries-1)) {
                                    Start-Sleep -Seconds 5
                                } else {
                                    #Write-Error "Could not map '$path' to '$freeDrive'."
                                    throw
                                }
                            }
                        }
                        if ($mapping) {
                            $ret = $mapping | Select-Object Status,LocalPath
                        }
                        $ret
                    }
                    $drive = Invoke-Command -Session $s -ScriptBlock $mapLibraryShare -ArgumentList $libShare, $user, $pass -ErrorAction Stop
                    if ($verbose) {
                        Write-Verbose $drive 4>&1
                    }
                    $localDrive = $drive.LocalPath

                    if ($drive.Status.ToString() -eq 'OK') {
                        $targetPath = $localDrive + "\" + $targetDir

                        # Create target path if it does not exist
                        $folderExists = Invoke-Command -Session $s { param($path); Test-Path $path -ErrorAction Stop } `
                                                       -ArgumentList $targetPath -ErrorAction Stop

                        if (-not $folderExists) {
                            $ic = Invoke-Command -Session $s { param($path); New-Item -ItemType Directory -Path $path -Force -ErrorAction Stop } `
                                                 -ArgumentList $targetPath -ErrorAction Stop
                            $ic = $null
                        }
                        $targetFiles=@()
                        $sourceFiles=@($diskImageList)
                        if ($sourceFiles.Count -gt 1) {
                            # we don't expect more than 1 goldimage currently so throw an exception here
                            throw "Unsupported number of goldimages to copy. Maximum 1 goldimage is allowed."
                        }
                        foreach ($src in $sourceFiles) {
                            $target = $targetPath + "\" + $diskName
                            $targetFiles += $target
                        }

                        # Copy all disk images to the VMM library server share
                        $transferFiles = {
                            param($sourceFiles, $targetFiles)

                            for ($i=0; $i -lt $sourceFiles.Count; $i++) {
                                try {
                                    # Copy/Transfer file
                                    [Net.ServicePointManager]::ServerCertificateValidationCallback = {$true}

                                    $srcFilename = $sourceFiles[$i].Substring($sourceFiles[$i].LastIndexOf("/") + 1)
                                    # Uncompress goldimage if in .zip format
                                    if ($srcFilename.Substring($srcFilename.LastIndexOf('.')+1) -eq "zip") {
                                        # if source file is '.zip' we will copy it to %temp%
                                        $tmp = New-TemporaryFile
                                        $tmpZipFilename = $tmp.FullName + ".zip"
                                        Rename-Item -Path $tmp.FullName -NewName $tmpZipFilename
                                        $tmp = Get-Item $tmpZipFilename
                                        (new-object System.Net.WebClient).DownloadFile($sourceFiles[$i], $tmp.FullName)

                                        # Silencing Expand-Archive progress bar output
                                        $oldProgressPreference=$ProgressPreference
                                        $ProgressPreference='SilentlyContinue'
                                        $targetDir = $targetFiles[$i].Substring(0,$targetFiles[$i].LastIndexOf('\') + 1)
                                        $tmp | Expand-Archive -Force -DestinationPath $targetDir -ErrorAction Stop
                                        $ProgressPreference=$oldProgressPreference
                                        # Remove the temporary .zip file
                                        $tmp | Remove-Item -Force -ErrorAction Stop
                                        # Rename the destination file
                                        $unzippedFilename = $targetDir + $srcFilename.Substring(0,$srcFilename.LastIndexOf('.zip'))
                                        Rename-Item $unzippedFilename $targetFiles[$i]
                                    } else {
                                        (new-object System.Net.WebClient).DownloadFile($sourceFiles[$i], $targetFiles[$i])
                                    }
                                } catch {
                                    $source = $sourceFiles[$i]
                                    $target = $targetFiles[$i]
                                    throw "Error copying disk image '$source' to '$target'. " + $_.Exception.Message
                                }
                            }
                        }
                        Invoke-Command -Session $s -ScriptBlock $transferFiles -ArgumentList $sourceFiles, $targetFiles `
                                       -ErrorAction Stop | Out-Null

                    } else {
                        throw "Copying disk image to VMM library share failed. Cannot map $libShare."
                    }
                } catch {
                    #rethrow
                    throw
                } finally {
                    # Need to make sure that drive is always unmapped and the session is closed.
                    $removeMappedDrive = {
                        param($drive)

                        Write-Verbose "Removing SMB mapping '$drive'." 4>&1
                        Remove-SMBMapping -Force -LocalPath $drive -UpdateProfile 2>$Null

                        $psDrive = $drive -replace ":" #remove unwanted colon from PSDrive name
                        If ( (Get-PSDrive -Name $psDrive) 2>$Null ) {
                            Write-Verbose "Running Remove-PsDrive -Name $psDrive -Force " 4>&1
                            Remove-PSDrive -Name $psDrive -Force
                        }
                        $drive
                    }

                    # Remove the mapped SMB share
                    if ($localDrive) {
                        try {
                            $rmDrive = Invoke-Command -Session $s -ScriptBlock $removeMappedDrive -ArgumentList $localDrive
                            if ($verbose) {
                                Write-Verbose $rmDrive 4>&1
                            }
                        } catch {} # skip any remove error we might hit
                    }

                    # destroy the session to library server
                    try {
                        $s | Remove-PSSession
                        Write-Verbose "Disconnected from VMM library server host '$libHost'." 4>&1
                    } catch {}
                    $s = $null
                }
                $ret = $true
                $ret
            }
            $copyImage = Invoke-Command -Session $session -ScriptBlock $sc -ArgumentList $user, $pass, $libraryHost, $vmmLibraryServerShare, `
                           $goldimagesTargetDir, $goldimagesDiskList, $diskName, $true -ErrorAction Stop
            Write-Log "INFO" $copyImage
            $copyImage | Out-Null
        }
        $response = [HypervRestApiResponse]::new(200, "Install-VmmDiskImage success")
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error copying disk image. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error copying disk image. " + $msg, $params)
    }

    return $response
}

 # Removes all files in Library server target path
Function Remove-VmmDiskImages($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null
    try {
        $targetDir = $params.target_dir
        $vmmLibraryServerShare = $params.vmm_library_server_share
        $libraryHost = $params.vmm_library_server
        $pass = $params.vmm_library_server_password
        $user = $params.vmm_library_server_user

        $sc = {
            param($user, $pass, $libHost, $libShare, $targetDir, $verbose)

            $VerbosePreference='Continue'

            $passSecure = ConvertTo-SecureString -AsPlainText -Force -String $pass
            $cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $user, $passSecure

            $s = New-PSSession -ComputerName $libHost -Credential $cred -Authentication Negotiate
            if ($verbose) {
                Write-Verbose "Connected to VMM library server host '$libHost'." 4>&1
            }

            # Due to a strange error on Linux platform, we are ignoring the error from first 'Invoke-Command'
            # "error": "Windows Principal functionality is not supported on this platform.
            #   CategoryInfo          : NotSpecified: (:) [Invoke-Command], PlatformNotSupportedException
            #   FullyQualifiedErrorId : System.PlatformNotSupportedException,Microsoft.PowerShell.Commands.InvokeCommandCommand
            #   PSComputerName        : localhost\n \n",
            # TODO (iztokp): We should remove this 'Invoke-Command' after PS bug is fixed: https://github.com/PowerShell/PowerShell/issues/6435
            try {
                $test = Invoke-Command -Session $s {}
                $test = $null
            } catch {}

            if ($verbose) {
                $sessionpid=Invoke-Command -Session $s -ScriptBlock {$PID}
                Write-Verbose "Remote VMM library server host session PID is '$sessionpid'." 4>&1
            }

            try {
                # All in one script block
                $sb = {
                    param($path, $user, $pass, $target)

                    $localDrive = $null

                    try {
                        # Map first free drive on the VMM library server to the library share
                        $freeDrive = ""
                        $numOfRetries = 10
                        for ($i=0; $i -lt $numOfRetries; $i++) {
                            for($j=67;$j -lt 90; ++$j) {
                                $d = [char]$j
                                if (-not (Get-PSDrive($d)2>$Null)) {
                                    if (-not (Get-SmbMapping($d + ":")2>$Null)) {
                                        break
                                    }
                                }
                            }
                            $freeDrive = $d + ":"
                            try {
                                $mapping = New-SmbMapping -LocalPath $freeDrive -RemotePath $path -Persistent $true -UserName $user `
                                                          -Password $pass -ErrorAction Stop
                                break
                            } catch {
                                if ($i -lt ($numOfRetries-1)) {
                                    Start-Sleep -Seconds 5
                                } else {
                                    #Write-Error "Could not map '$path' to '$freeDrive'."
                                    throw
                                }
                            }
                        }
                        if ($mapping) {
                            $drive = $mapping | Select-Object Status,LocalPath
                        } else {
                            throw "Could not map '$path'."
                        }
                        $localDrive = $drive.LocalPath

                        if ($drive.Status.ToString() -eq 'OK') {
                            $destination = $localDrive + "\" + $target

                            # Remove target path if it exists
                            if (Test-Path $destination) {
                                # Silence down any errors we might get removing files. See issue XRAY-1503.
                                Get-Item -Path $destination |  Get-ChildItem -Directory | `
                                    Remove-Item -Recurse -ErrorAction SilentlyContinue
                            }
                        } else {
                            throw "Removing disk image from VMM library share failed. Cannot map $path."
                        }
                    } catch {
                        #rethrow
                        throw
                    } finally {
                        # Remove the mapped SMB share
                        if ($localDrive) {
                            Write-Verbose "Removing SMB mapping '$localDrive'." 4>&1
                            try {
                                Remove-SMBMapping -Force -LocalPath $localDrive -UpdateProfile 2>$Null

                                $psDrive = $localDrive -replace ":" #remove unwanted colon from PSDrive name
                                If ( (Get-PSDrive -Name $psDrive) 2>$Null ) {
                                    Write-Verbose "Running Remove-PsDrive -Name $psDrive -Force " 4>&1
                                    Remove-PSDrive -Name $psDrive -Force 2>$Null
                                }
                            } catch {}
                        }
                    }
                    $localDrive
                }
                $rmDsk = Invoke-Command -Session $s -ScriptBlock $sb -ArgumentList $libShare, $user, $pass, $targetDir -ErrorAction Stop
                if ($verbose) {
                    Write-Verbose $rmDsk 4>&1
                }
                $rmDsk = $null
            } finally {
                # destroy the session to library server
                if ($s) {
                    $s | Remove-PSSession
                    Write-Verbose "Disconnected from VMM library server host '$libHost'." 4>&1
                }
                $s = $null
            }
        }
        $rmDisk = Invoke-Command -Session $session -ScriptBlock $sc -ArgumentList $user, $pass, $libraryHost, $vmmLibraryServerShare, $targetDir, $true `
                                 -ErrorAction Stop
        Write-Log "INFO" $rmDisk
        $rmDisk = $null
        $response = [HypervRestApiResponse]::new(200, "Remove-VmmDiskImages success")
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   ("Error removing disk image. " + $msg)
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error removing disk image. " + $msg, $params)
    }

    return $response
}


Function Update-Library($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $goldImageDiskPath = $params.goldimage_disk_path
        $sharePath = $goldImageDiskPath.Substring(0, $goldImageDiskPath.IndexOf("\", $goldImageDiskPath.IndexOf("\",2)+1))
        $fileName = $goldImageDiskPath.Substring($goldImageDiskPath.LastIndexOf("\") + 1)

        $shareFile = $sharePath + "\" + $fileName

        $sc = {
            $disks = Get-SCVirtualHardDisk
            # Check if goldimage can be found on VMM library server share root
            $virtualHardDisk = $disks | Where-Object Location -eq $Using:shareFile
            if ($virtualHardDisk -and
                ($virtualHardDisk.State.ToString() -eq 'Normal')) {
                return
            }

            $virtualHardDisk = $disks | Where-Object Location -eq $Using:goldimageDiskPath
            if (-not $virtualHardDisk -or
                ($virtualHardDisk.State.ToString() -ne 'Normal')) {
                $libShare = Get-SCLibraryShare | Where-Object Path -eq $Using:sharePath
                $jobRunning = $true
                # Wait while other refresh job is running
                while ($jobRunning) {
                    $jobRunning = $false
                    $jobs = Get-SCJob -Running
                    foreach ($job in  $jobs) {
                        # Check if a 'Refresh library share' job is in running state for the Library Share
                        if (($job.TargetObjectID.Guid -eq $libShare.ID.Guid) -and
                            ($job.Name -eq 'Refresh library share') -and
                            (-not $job.IsCompleted)) {
                            $jobRunning = $true
                            break
                        }
                        # Check if a 'Update library' job is in running state for the Library Server
                        if (($job.ResultName -eq $libShare.LibraryServer) -and
                            ($job.Name -eq 'Update Library') -and
                            (-not $job.IsCompleted)) {
                            $jobRunning = $true
                            break
                        }
                    }
                    if($jobRunning) {
                        Start-Sleep -Seconds 10
                    }
                }

                # Check again if goldimage disk is available
                $virtualHardDisk = Get-SCVirtualHardDisk | Where-Object Location -eq $Using:goldimageDiskPath

                # Start another refresh if goldimage is still not found, otherwise skip refresh
                if (-not $virtualHardDisk -or
                    ($virtualHardDisk.State.ToString() -ne 'Normal')) {
                    # Run library refresh
                    $goldimageFileName = $Using:goldImageDiskPath
                    $goldimageDiskDir =  $goldimageFileName.Substring(0, $goldimageFileName.LastIndexOf("\"))
                    $libShare = $libShare | Read-SCLibraryShare -Path $goldimageDiskDir
                }
            }
        }
        Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop
        $response = [HypervRestApiResponse]::new(200, "Update-Library success")
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error updating VMM library share. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error updating VMM library share. " + $msg, $params)
    }

    return $response
}

# Creates VM template using the speficied goldimage and VM properties (CPU, MEM, ...).
# If goldimage is found on the VMM library server root path then the one is used, otherwise
# the goldimage on the specified location is used.
Function Install-VmmVMTemplate ($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmName = $params.vm_name
        $goldimageDiskPath = $params.goldimage_disk_path
        $vmmNetworkName = $params.vmm_network_name
        $vCpus = $params.vcpus
        $ramMB = $params.ram_mb

        $sharePath = $goldImageDiskPath.Substring(0, $goldImageDiskPath.IndexOf("\", $goldImageDiskPath.IndexOf("\",2)+1))
        $fileName = $goldImageDiskPath.Substring($goldImageDiskPath.LastIndexOf("\") + 1)
        $shareFile = $sharePath + "\" + $fileName

        $sc = {

            $hardwareProfile = Get-SCHardwareProfile -ErrorAction Stop | Where-Object Name -eq "$Using:vmName"

            if($hardwareProfile -eq $null)
            {
                $jobguid1 = [guid]::NewGuid().ToString() 

                New-SCVirtualScsiAdapter -JobGroup $jobguid1  -AdapterID 7 -ShareVirtualScsiAdapter $false -ScsiControllerType DefaultTypeNoType `
                                         -ErrorAction Stop
                New-SCVirtualDVDDrive -JobGroup $jobguid1  -Bus 1 -LUN 0 -ErrorAction Stop
                $vmNetwork = Get-SCVMNetwork -Name $Using:vmmNetworkName
                New-SCVirtualNetworkAdapter -JobGroup $jobguid1 -MACAddressType Dynamic -Synthetic -EnableVMNetworkOptimization $false `
                                            -EnableMACAddressSpoofing $false -EnableGuestIPNetworkVirtualizationUpdates $false `
                                            -IPv4AddressType Dynamic -IPv6AddressType Dynamic -VMNetwork $vmNetwork -ErrorAction Stop
                $description = "cluster_name=" + $Using:cluster_name
                $hardwareProfile = New-SCHardwareProfile -Name "$Using:vmName" -Description $description -CPUCount $Using:vCpus `
                                                         -MemoryMB $Using:ramMB -DynamicMemoryEnabled $false -MemoryWeight 5000 `
                                                         -VirtualVideoAdapterEnabled $false -CPUExpectedUtilizationPercent 20 `
                                                         -DiskIops 0 -CPUMaximumPercent 100 -CPUReserve 0 -NumaIsolationRequired $false `
                                                         -NetworkUtilizationMbps 0 -CPURelativeWeight 100 -HighlyAvailable $true `
                                                         -DRProtectionRequired $false -CPULimitFunctionality $false `
                                                         -CPULimitForMigration $false -CheckpointType Production -Generation 1 `
                                                         -JobGroup $jobguid1 -ErrorAction Stop
            }
        }
        Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $sc = {

            $vmTemplate = Get-SCVMTemplate -Name "$Using:vmName" -ErrorAction Stop

            if($vmTemplate -eq $null)
            {
                $jobguid2 = [guid]::NewGuid().ToString()

                $disks = Get-SCVirtualHardDisk -ErrorAction Stop
                # Check if goldimage can be found on VMM library server share root
                $virtualHardDisk = $disks | Where-Object Location -eq $Using:shareFile
                if (-not $virtualHardDisk -or
                    ($virtualHardDisk.State.ToString() -ne 'Normal')) {
                    # Goldimage not found in root, therefore we take it from the specified location
                    $virtualHardDisk = $disks | Where-Object Location -eq $Using:goldimageDiskPath
                }

                New-SCVirtualDiskDrive -IDE -Bus 0 -LUN 0 -JobGroup $jobguid2 -CreateDiffDisk $false -VirtualHardDisk $virtualHardDisk `
                                       -VolumeType BootAndSystem -ErrorAction Stop
                $description = "cluster_name=" + $Using:cluster_name
                $vmTemplate = New-SCVMTemplate -Name "$Using:vmName" -Description $description -RunAsynchronously -Generation 1 `
                                               -HardwareProfile $hardwareProfile -JobGroup $jobguid2 -NoCustomization -ErrorAction Stop
            }
        }
        Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        #$ret = @()
        # TODO (bostjanf): Above scripts could be executed as PS jobs.
        #if($taskId)
        #{
        #    $task = @{}
        #    $task["task_id"] = $taskId.toString()
        #    $task["task_type"] = "vmm"
        #    $ret += $task
        #}
        #else {
        #    throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Cannot start VMM job to create new VM template.', $params)
        #}

        $response = [HypervRestApiResponse]::new(200, $null)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error creating VM template. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error creating VM template. " + $msg, $params)
    }

    return $response
}


Function New-SimpleVmmVM ($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmHostMap = $params.vm_info_maps

        $CreateVM = {
            foreach($vm in $Using:vmHostMap)
            {
                $vmName = $vm.vm_name
                $vmmNetworkName = $vm.vmm_network_name
                $vCpus = $vm.vcpus
                $ramMB = $vm.ram_mb
                $vmHostId = $vm.node_id
                $vmDatastorePath = $vm.vm_datastore_path

                # We need to check for object type returned by the Get-SCVMHost cmdlet since there seems to be a bug
                # where we occasionally get some other type instead of VMM host
                $i = 0
                $maxIterations = 10
                do {
                    $wrongObjType = $false
                    $vmHost = Get-SCVMHost -ID $vmHostId
                    $i = $i + 1
                    if ($vmHost.GetType().FullName -ne "Microsoft.SystemCenter.VirtualMachineManager.Host") {
                        $wrongObjType = $true
                        Start-Sleep -Seconds 5
                    }
                } while($wrongObjType -and $i -le $maxIterations)

                if ($wrongObjType) {
                    throw "Cannot get VMHost '$vmHostId'."
                }

                $jobguid1 = [guid]::NewGuid().ToString() 

                New-SCVirtualScsiAdapter -JobGroup $jobguid1  -AdapterID 7 -ShareVirtualScsiAdapter $false `
                                        -ScsiControllerType DefaultTypeNoType -ErrorAction Stop
                New-SCVirtualDVDDrive -JobGroup $jobguid1  -Bus 1 -LUN 0 -ErrorAction Stop
                $vmNetwork = Get-SCVMNetwork -Name $vmmNetworkName
                New-SCVirtualNetworkAdapter -JobGroup $jobguid1 -MACAddressType Dynamic -Synthetic `
                                            -EnableVMNetworkOptimization $false -EnableMACAddressSpoofing $false `
                                            -EnableGuestIPNetworkVirtualizationUpdates $false `
                                            -IPv4AddressType Dynamic -IPv6AddressType Dynamic -VMNetwork $vmNetwork `
                                            -ErrorAction Stop
                $description = "cluster_name=" + $Using:cluster_name
                $created = New-SCVirtualMachine -Name $vmName -Description $description -Path $vmDatastorePath `
                                                -CPUCount $vCpus -MemoryMB $ramMB -DynamicMemoryEnabled $false `
                                                -JobGroup $jobguid1 -VMHost $vmHost -HighlyAvailable $true `
                                                -Generation 1 -JobVariable XRayCreateVM -RunAsynchronously `
                                                -StartAction NeverAutoTurnOnVM -StopAction ShutdownGuestOS
                $XRayCreateVM.ID.Guid  # Return value.
            }
        }

        $jobs = Invoke-Command -Session $session -ScriptBlock $CreateVM -ErrorAction Stop

        $ret = @()
        if($jobs)
        {
            foreach($job in $jobs){
                $task = @{}
                $task["task_id"] = $job.ToString()
                $task["task_type"] = "vmm"
                $ret += $task
            }
        }
        else {
            Write-Log "ERROR"  "Cannot start PS task to create VM."
            throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Cannot start PS task to create VM.', $params)
        }

        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error creating VM. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error creating VM. " + $msg, $params)
    }
    return $response
}


# TODO - This function assumes that no other disks are currently attached to
# the VM.
Function Attach-DisksVM($session, $cluster_Name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmDiskMap = $params.vm_disk_maps

        $AttachDisks = {
            foreach ($vmInfo in $Using:vmDiskMap) {
                $vm_name = $vmInfo.vm_name
                $disk_paths = $vmInfo.disk_paths

                $vm = Get-SCVirtualMachine -Name $vm_name
                $index = 0
                $jobguid1 = [guid]::NewGuid().ToString()
                foreach($path in $disk_paths)
                {
                    $diskPath = Split-Path $path -Parent
                    $diskFileName = Split-Path $path -Leaf
                    if ($index -eq 0)
                    {
                        $created = New-SCVirtualDiskDrive -IDE -Bus 0 -LUN $index `
                                            -CreateDiffDisk $false -Path $diskPath -Filename $diskFileName -UseLocalVirtualHardDisk -BootVolume `
                                            -JobGroup $jobguid1
                    }
                    else
                    {
                        $created = New-SCVirtualDiskDrive -SCSI -Bus 0 -LUN $index `
                                            -Path $diskPath -Filename $diskFileName -UseLocalVirtualHardDisk `
                                            -JobGroup $jobguid1
                    }
                    $index++
                }
                $update_vm = Set-SCVirtualMachine -VM $vm -JobGroup $jobguid1 -JobVariable XRayAttachDisks -RunAsynchronously -ErrorAction Stop
                $XRayAttachDisks.ID.Guid  # Return value.
            }
        }

        $ret = @()
        $jobs = Invoke-Command -Session $session -ScriptBlock $AttachDisks -ErrorAction Stop

        $ret = @()
        if($jobs)
        {
            foreach($job in $jobs){
                $task = @{}
                $task["task_id"] = $job.ToString()
                $task["task_type"] = "vmm"
                $ret += $task
            }
        }
        else {
            Write-Log "ERROR"  "Cannot start PS task to attach disks to VM."
            throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Cannot start PS task to to attach disks to VM.', $params)
        }

        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error attaching disks to VM. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error attaching disks VM. " + $msg, $params)
    }
    return $response
}


Function Remove-ClusterVmmObjects($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        if($params.vmm_library_server_share)
        {
            $vmmLibraryServerShare = $params.vmm_library_server_share
            $targetDir = $params.target_dir
            $vmmLibraryServerPathFilter = $vmmLibraryServerShare + "\" + $targetDir + "\*"
        }
        else
        {
            $vmmLibraryServerPathFilter = $null
        }
        $vmDatastorePath  = $params.vm_datastore_path
        $vmNamePrefix = $params.vm_name_prefix

        $DeleteObjects = {
            $nameFilter =  $Using:vmNamePrefix + '*'
            $clusterFilter = "cluster_name=" + $Using:cluster_name
            $templates = Get-SCVMTemplate -ErrorAction Stop | Where-Object Name -Like $nameFilter | Where-Object Description -EQ $clusterFilter

            foreach($template in $templates)
            {
                Remove-SCVMTemplate -VMTemplate $template | Out-Null
            }

            $hardwareProfiles = Get-SCHardwareProfile | Where-Object Name -Like $nameFilter | Where-Object Description -EQ $clusterFilter

            foreach($hardwareProfile in $hardwareProfiles)
            {
                Remove-SCHardwareProfile -HardwareProfile $hardwareProfile | Out-Null
            }
            if($Using:vmmLibraryServerPathFilter)
            {
                Get-SCVirtualHardDisk |  Where-Object Location -like $Using:vmmLibraryServerPathFilter | Remove-SCVirtualHardDisk | Out-Null
                Get-SCApplicationPackage |  Where-Object SharePath -like $Using:vmmLibraryServerPathFilter | Remove-SCApplicationPackage | Out-Null
            }
            $diskFilter = $Using:vmDatastorePath + "\" + $nameFilter
            Get-SCVirtualHardDisk -all | Where-Object Location -like $diskFilter | Remove-SCVirtualHardDisk | Out-Null
        }
        Invoke-Command -Session $session -ScriptBlock $DeleteObjects -ErrorAction Stop
        $response = [HypervRestApiResponse]::new(200, "Remove-ClusterVmmObjects success")
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error removing VMM objects (templates, HW profiles, virtual disks). " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error removing VMM objects (templates, HW profiles, virtual disks). " + $msg, $params)
    }

    return $response
}

# Returns ScVMM server version
Function Get-VMMVersion($session, $json_params="{}") {
    $response = $null

    try {
        $sc = {
            $vmmServer.ProductVersion
        }
        $vmmServerVersion = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $response = [HypervRestApiResponse]::new(200, $vmmServerVersion)
    } catch {
        # rethrow exception
        Write-Log "ERROR"   "Could not get ScVMM server version. " + $_.Exception.Message
        throw [HypervRestApiException]::new(401, "BAD_REQUEST", "Could not get ScVMM server version. " + $_.Exception.Message, $null)
    }

    return $response
}

# Returns all HyperV clusters defined in SC VMM with their default storage locations and networks.
Function Get-VmmHypervCluster($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $clusterNameFilter = ".*"
        $clusterType = $null

        if ($params -ne $null)
        {
            if ($params.name -ne $null) {
                $clusterNameFilter = $params.name
            }
            if ($params.type -ne $null) {
                $clusterType = $params.type
            }
        }

        $sc = {
            $clusterList = @()
            $clusters = Get-SCVMHostCluster
            foreach ($cluster in $clusters) {
                if ($cluster.ClusterName -match $Using:clusterNameFilter -and
                    ($Using:clusterType -eq $null -or $cluster.VirtualizationPlatform.ToString() -eq $Using:clusterType)) {
                    $c = @{}
                    $c['name'] = $cluster.ClusterName
                    $c['type'] = $cluster.VirtualizationPlatform.ToString()
                    $storages = @()
                    $fileShares = $cluster.RegisteredStorageFileShares
                    if ($fileShares -ne $null) {
                        foreach ($fileShare in $fileShares) {
                            $storage = @{}
                            $storage['name'] = $fileShare.Name
                            $storage['path'] = $fileShare.SharePath
                            $storages += $storage
                        }
                    }
                    $sharedVolumes = $cluster.SharedVolumes
                    if ($sharedVolumes -ne $null) {
                        foreach ($sharedVolume in $sharedVolumes) {
                            $storage = @{}
                            $storage['name'] = $sharedVolume.Name
                            $storage['path'] = $sharedVolume.Name
                            $storages += $storage
                        }
                    }
                    $c['storage'] = $storages
                    $networks = @()
                    $net = $cluster | Get-SCClusterVirtualNetwork
                    if ($net -ne $null) {
                        $networks += $net.LogicalNetworks.Name
                    }
                    $c['networks'] = $networks
                    $clusterList += $c
                }
            }
            $clusters = $null
            ,$clusterList
        }

        $clusters = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $response = [HypervRestApiResponse]::new(200, $clusters)
    } catch {
        # rethrow exception
        Write-Log "ERROR"   "Could not list clusters. " + $_.Exception.Message
        throw [HypervRestApiException]::new(401, "BAD_REQUEST", "Could not list clusters. " + $_.Exception.Message, $null)
    }

    return $response
}

# Returns all VMM Library Shares on current VMM environment.
Function Get-VmmLibraryShares($session, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {

        $sc = {
            $sharesList = @()
            $shares = Get-SCLibraryShare
            foreach ($share in $shares) {
                $s = @{}
                $s['name'] = $share.Name
                $s['path'] = $share.Path
                $s['server'] = $share.LibraryServer.FullyQualifiedDomainName
                $sharesList += $s
            }
            $shares = $null
            ,$sharesList
        }

        $shares = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $response = [HypervRestApiResponse]::new(200, $shares)
    } catch {
        # rethrow exception
        Write-Log "ERROR"   "Could not list library shares. " + $_.Exception.Message
        throw [HypervRestApiException]::new(401, "BAD_REQUEST", "Could not list library shares. " + $_.Exception.Message, $null)
    }

    return $response
}

# Returns list of cluster nodes. If specific nodes are requested then
# node refresh is started in VMM othervise not. Nodes list returned is sorted by node FQDN.
Function Get-VmmHypervClusterNode($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null
    $sc = $null

    try {
        if ($params -ne $null) {
            $sc = {
                $hosts = Get-SCVMHostCluster -Name $Using:cluster_name |
                    Get-SCVMHost | Sort-Object -Property FullyQualifiedDomainName
                $cluster = Get-SCVMHostCluster -Name $Using:cluster_name
                $refreshRunning = $false
                $jobs = Get-SCJob -Running
                foreach ($job in  $jobs) {
                    # Check if a 'Refresh host cluster' job is in running state for the Node
                    if (($job.TargetObjectID.Guid -eq $cluster.ID.Guid) -and
                        ($job.Name -eq 'Refresh host cluster') -and
                        (-not $job.IsCompleted)) {
                        $refreshRunning = $true
                        break
                    }
                }
                # Start another refresh if not running, otherwise skip refresh
                if (-not $refreshRunning) {
                    $cl = Get-SCVMHostCluster -Name $Using:cluster_name |
                        Read-SCVMHostCluster -RunAsynchronously
                    $cl = $null
                }
                $hosts = $hosts |
                    Select-Object Name, ID, FullyQualifiedDomainName, ComputerState, HyperVVersion, ClusterNodeStatus, OverallState, @{Name="BMCPort";Expression={$_.physicalMachine.bmcport}}, @{Name="BMCAddress";Expression={$_.physicalMachine.bmcaddress}}
                # Resolve IPv4 only addresses of the FQDN
                foreach ($vmmHost in $hosts) {
                    $ips = [System.Net.DNS]::GetHostByName($vmmHost.FullyQualifiedDomainName).AddressList |
                        Where-Object { $_.AddressFamily -eq 'InterNetwork' } |
                        Select-Object -Expand IPAddressToString
                    $vmmHost | Add-Member -Name 'IPv4AddressList' -Type NoteProperty -Value $ips
                }
                $hosts
            }
        } else {
            $sc = {
                $hosts = Get-SCVMHostCluster -Name $Using:cluster_name |
                    Get-SCVMHost |
                        Select-Object Name, ID, FullyQualifiedDomainName, ComputerState, HyperVVersion, ClusterNodeStatus, OverallState, @{Name="BMCPort";Expression={$_.physicalMachine.bmcport}}, @{Name="BMCAddress";Expression={$_.physicalMachine.bmcaddress}} |
                        Sort-Object -Property FullyQualifiedDomainName
                # Resolve IPv4 only addresses of the FQDN
                foreach ($vmmHost in $hosts) {
                    $ips = [System.Net.DNS]::GetHostByName($vmmHost.FullyQualifiedDomainName).AddressList |
                        Where-Object { $_.AddressFamily -eq 'InterNetwork' } |
                        Select-Object -Expand IPAddressToString
                    $vmmHost | Add-Member -Name 'IPv4AddressList' -Type NoteProperty -Value $ips
                }
                $hosts
            }
        }
        $hosts = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $ret = @()
        foreach ($clusterHost in $hosts) {
            $h = @{}
            $h['name'] = $clusterHost.Name
            $h['id'] = $clusterHost.ID.Guid
            $h['fqdn'] = $clusterHost.FullyQualifiedDomainName
            if ($clusterHost.ComputerState.ToString() -eq "Responding" -and
                $clusterHost.ClusterNodeStatus.ToString() -eq "Stopped") {

                # This is a special case when 'ComputerState' is not updated appropriately
                $h['state'] = "NotResponding"
            } else {
                $h['state'] = $clusterHost.ComputerState.ToString()
            }

            $h['version'] =  $clusterHost.HyperVVersion.ToString()
            $h['overall_state'] =  $clusterHost.OverallState.ToString()
            if ($clusterHost.BMCPort -ne $null) {
                $h['bmc_port'] = $clusterHost.BMCPort.toString()
            } else {
                $h['bmc_port'] = ""
            }
            if ($clusterHost.BMCAddress -ne $null) {
                $h['bmc_address'] = $clusterHost.BMCAddress.toString()
            } else {
                $h['bmc_address'] = ""
            }
            # Add IPv4 addresses
            if ($clusterHost.IPv4AddressList.GetType().IsArray) {
                $h['ips'] = $clusterHost.IPv4AddressList
            } else {
                $h['ips'] = @($clusterHost.IPv4AddressList)
            }

            $ret += $h
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch {
        # rethrow exception
        Write-Log "ERROR"  "Could not list nodes. " + $_.Exception.Message
        throw [HypervRestApiException]::new(401, "BAD_REQUEST", "Could not list nodes. " + $_.Exception.Message, $params)
    }

    return $response
}

#Power Off Nodes
Function Set-VmmHypervClusterNodeShutdown($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null
    $objs = @()
    try {
        $node = $params[0]
        $sc = {
            $node = $Using:node
            $fqdn = $node.fqdn
            $pass = $node.password
            $username = $node.username
            $passwd = ConvertTo-SecureString $pass -AsPlainText -Force
            $creds = New-Object System.Management.Automation.PSCredential ($username, $passwd)
            Stop-Computer -ComputerName $fqdn -Credential $creds -Force
        }
        Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop
        $response = [HypervRestApiResponse]::new(200, "Set-VmmHypervClusterNodeShutdown success")
    } catch {
        # rethrow exception
        Write-Log "ERROR"  "Could not shutdown nodes. " + $_.Exception.Message
        throw [HypervRestApiException]::new(401, "BAD_REQUEST", "Could not shutdown nodes. " + $_.Exception.Message, $null)
    }

    return $response
}

Function Get-VmmVM($session, $cluster_name, $json_params="{}", $refresh=$false) {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $sc = {
            $cluster_name = $Using:cluster_name
            $params = $Using:params
            $refresh = $Using:refresh

            $ret = @()
            $machines = $null

            # Waiting for 'Refresh library server' job to complete due to VMM error:
            # https://support.microsoft.com/en-sg/help/2756886/error-801-missing-objecttype-and-vmmadminui-has-stopped-working-in-sys
            try {
                # We are sorting VMs by Name and Operating System to return VMs always in the same order since there may be duplicate objects.
                $machines = Get-SCVMHostCluster -Name $cluster_name | Get-SCVMHost | Get-SCVirtualMachine | Sort-Object -Property Name,OperatingSystem
            }
            catch {
                # if error try waiting for update library to complete
                $refreshJobRunning = $true
                $maxWait = 6
                $i = 0
                # Wait while other refresh job is running
                $jobs = Get-SCJob -Running
                while ($refreshJobRunning) {
                    $refreshJobRunning = $false
                    foreach ($job in $jobs) {
                        # Check if a 'Refresh library share' job is in running state for the Node
                        if ($job.Name -eq 'Refresh library share' -and
                            -not $job.IsCompleted) {
                            $refreshJobRunning = $true
                            break
                        }
                    }
                    Start-Sleep -Seconds 10
                    $i = $i + 1
                    if ($i -gt $maxWait) {
                        # exit from the wait loop if we waited for more than $maxWait intervals
                        break
                    }
                }
                # try listing VMs ince again
                $machines = Get-SCVMHostCluster -Name $cluster_name | Get-SCVMHost | Get-SCVirtualMachine
            }

            # Get list of running jobs
            $jobs = Get-SCJob -Running

            foreach ($machine in $machines)
            {
                $found = $true
                $vmId = $machine.ID.Guid
                $vmName = $machine.Name
                if($params -ne $null)
                {
                    $found = $false
                    foreach ($vm in $params) {
                        if (($vmId -eq $vm.vm_id) -or ($vmName -eq $vm.vm_name)) {
                            $found = $true
                            break
                        }
                    }
                }
                if ($found)
                {
                    # Try to get VM IPs
                    $ipaddr = $null
                    try {
                        $ipaddr = $machine | Get-SCVirtualNetworkAdapter -ErrorAction Stop | Select-Object -ExpandProperty IPv4Addresses
                    } catch {}

                    # Don't start VM refresh if 'refresh' query parameter is set not set to 'false'
                    # By explicitly setting "refresh" to "false" we skip VM refresh job.
                    if ($refresh) {
                        if ($ipaddr -eq $null  -and $machine.Status.toString() -eq 'Running' -and $found)
                        {
                            # Run refresh for the virtual machines (only if we sent a list of VMs that we wan't to query).
                            # This means that only the when we sent VM list in params and a running VM is found and is without IP we will
                            # run update. Update VM is a job that should not be called unnecessary.

                            $refreshRunning = $false
                            foreach ($job in $jobs) {
                                # Check if a 'Refresh virtual machine' job is in running state for the VM
                                if (($job.TargetObjectID.Guid -eq $vmId) -and
                                    ($job.Name -eq 'Refresh virtual machine') -and
                                    (-not $job.IsCompleted)) {
                                    $refreshRunning = $true
                                    break
                                }
                            }
                            # Start another refresh if not running, otherwise skip refresh
                            if (-not $refreshRunning) {
                                $machine | Read-SCVirtualMachine -RunAsynchronously | Out-Null
                            }
                        }
                    }

                    $obj = @{}
                    $obj['name'] = $machine.Name
                    $obj['id'] = $machine.ID.Guid
                    $obj['node_id'] = $machine.HostID
                    $obj['status'] = $machine.Status.ToString()

                    $ips = @()
                    if ($ipaddr -ne $null) {
                        foreach ($ip in $ipaddr) {
                            $ips += $ip.ToString()
                        }
                    }
                    $obj['ips'] = $ips

                    $diskpaths = @()
                    foreach($disk in $machine.VirtualHardDisks) {
                      $diskpaths += $disk.Location.ToString()
                    }
                    $obj['virtualharddiskpaths'] = $diskpaths
                    $obj['memory'] = $machine.Memory.ToString()
                    $obj['cpucount'] = $machine.CPUCount.ToString()
                    $ret += $obj
                }
            }
            $ret
        }
        $machines = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop
        $ret = @()
        foreach($machine in $machines)
        {
            $obj = @{}
            $obj['name'] = $machine['name']
            $obj['id'] = $machine['id']
            $obj['node_id'] = $machine['node_id']
            $obj['status'] = $machine['status']
            $obj['ips'] = $machine['ips']
            $obj['virtualharddiskpaths'] = $machine['virtualharddiskpaths']
            $obj['memory'] = $machine['memory']
            $obj['cpucount'] = $machine['cpucount']
            #$obj['raw'] = $machine['raw']
            $ret += $obj
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error trying to get VMs. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error trying to get VMs. " + $msg, $params)
    }

    return $response
}

Function Read-VmmVM($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $sc = {
            $cluster_name = $Using:cluster_name
            $params = $Using:params

            $machines = $null

            # Waiting for 'Refresh library server' job to complete due to VMM error:
            # https://support.microsoft.com/en-sg/help/2756886/error-801-missing-objecttype-and-vmmadminui-has-stopped-working-in-sys
            try {
                $machines = Get-SCVMHostCluster -Name $cluster_name | Get-SCVMHost | Get-SCVirtualMachine
            }
            catch {
                # if error try waiting for update library to complete
                $refreshJobRunning = $true
                $maxWait = 6
                $i = 0
                # Wait while other refresh job is running
                $jobs = Get-SCJob -Running
                while ($refreshJobRunning) {
                    $refreshJobRunning = $false
                    foreach ($job in  $jobs) {
                        # Check if a 'Refresh library share' job is in running state for the Node
                        if ($job.Name -eq 'Refresh library share' -and
                            -not $job.IsCompleted) {
                            $refreshJobRunning = $true
                            break
                        }
                    }
                    Start-Sleep -Seconds 10
                    $i = $i + 1
                    if ($i -gt $maxWait) {
                        # exit from the wait loop if we waited for more than $maxWait intervals
                        break
                    }
                }
                # try listing VMs ince again
                $machines = Get-SCVMHostCluster -Name $cluster_name | Get-SCVMHost | Get-SCVirtualMachine
            }

            # Get list of running jobs
            $jobs = Get-SCJob -Running

            foreach ($machine in $machines)
            {
                $found = $true
                $vmId = $machine.ID.Guid
                if($params -ne $null)
                {
                    $found = $false
                    foreach ($vm in $params) {
                        if ($vmId -eq $vm.vm_id) {
                            $found = $true
                            break
                        }
                    }
                }
                if ($found)
                {
                    # Run refresh for the virtual machines (only if we sent a list of VMs that we wan't to query).
                    # This means that only the when we sent VM list in params and a running VM is found and is without IP we will
                    # run update. Update VM is a job that should not be called unnecessary.

                    $refreshRunning = $false
                    $existingTaskId = $null
                    foreach ($job in $jobs) {
                        # Check if a 'Refresh virtual machine' job is in running state for the VM
                        if (($job.TargetObjectID.Guid -eq $vmId) -and
                            ($job.Name -eq 'Refresh virtual machine') -and
                            (-not $job.IsCompleted)) {
                            $refreshRunning = $true
                            $existingTaskId =  $job.ID.Guid
                            break
                        }
                    }
                    # Start another refresh if not running, otherwise return the ID of already running job
                    if (-not $refreshRunning) {
                        $machine | Read-SCVirtualMachine -RunAsynchronously -JobVariable "XRayRefreshVM" | Out-Null
                        $XRayRefreshVM.ID.Guid
                    } else {
                        $existingTaskId
                    }
                }
            }
        }
        $jobs = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $ret = @()
        if($jobs)
        {
            foreach($job in $jobs){
                $task = @{}
                $task["task_id"] = $job.ToString()
                $task["task_type"] = "vmm"
                $ret += $task
            }
        }

        $response=[HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error refreshing VMs. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error refreshing VMs. " + $msg, $params)
    }

    return $response
}

Function Set-VmmVMPossibleOwners($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {

        $sc = {
            $tasks = @{}

            $isSameNodelist = $false
            $oldPossibleOwners = $null
            foreach ($vm in $Using:params) {
                $nonPossibleOwners = @()
                #check if possible owners of the vm before is the same at the existing one
                if ($oldPossibleOwners -eq $null -or
                    $oldPossibleOwners -ne $vm.possible_owners) {
                    $isSameNodelist = $false
                }
                else {
                    $isSameNodelist = $true
                }

                if (-not $isSameNodelist) {
                    $clusterHosts = Get-SCVMHostCluster $Using:cluster_name | Get-SCVMHost

                    # create a list of non possible owners
                    foreach ($clusterHost in $clusterHosts) {
                        $isPossibleOwner = $false
                        foreach ($possibleOwner in $vm.possible_owners) {
                            if ($clusterHost.ID.Guid -eq $possibleOwner) {
                                $isPossibleOwner = $true
                                break
                            }
                        }
                        if (-not $isPossibleOwner) {
                            $nonPossibleOwners += $clusterHost.Name
                        }
                    }
                }

                $oldPossibleOwners = $vm.possible_owners

                $vmChange = Get-SCVirtualMachine -ID $vm.id |
                        Set-SCVirtualMachine -ClusterNonPossibleOwner $nonPossibleOwners -RunAsynchronously `
                                             -JobVariable "XRaySetPossibleOwners" -ErrorAction Stop

                $tasks[$vm.id] = $XRaySetPossibleOwners.ID.Guid
                $vmChange = $null
            }

            $tasks
        }
        $tasks = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $ret = @()
        foreach($task in $tasks.GetEnumerator())
        {
            $obj = @{}
            $obj['vm_id'] = $task.Name
            $obj['task_id'] = $task.Value
            $obj["task_type"] = "vmm"
            $ret += $obj
        }

        $response=[HypervRestApiResponse]::new(200, $ret)
    } catch {
        # rethrow exception
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Could not set possible owners. " + $msg
        throw [HypervRestApiException]::new(401, "BAD_REQUEST", "Could not set possible owners. " + $msg, $params)
    }

    return $response
}

Function New-VmmVM($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmTemplateName = $params.vm_template_name
        $vmHostMap = $params.vm_host_map
        $vmDatastorePath = $params.vm_datastore_path
        $dataDisks = $params.data_disks
        $differencingDisksPath = $params.differencing_disks_path

        $ret = @()

        # We try to get baseTemplateID multiple times
        $i = 0
        $maxIterations = 3
        do {
            $notReceived = $false
            $baseTemplateID = Invoke-Command -Session $session -ScriptBlock { `
                Get-SCVMTemplate -Name $Using:vmTemplateName | Select-Object -ExpandProperty ID }
            $i = $i + 1
            if ($baseTemplateID -eq $null) {
                $notReceived = $true
                Start-Sleep -Seconds 5
            }
        } while($notReceived -and $i -le $maxIterations)


        # If base template exists, we create VM from template
        if($baseTemplateID)
        {
            $baseTemplateID = $baseTemplateID.Guid.ToString()
            $sc = {
                foreach($vm in $Using:vmHostMap)
                {
                    $vmName = $vm.vm_name
                    $vmHostId = $vm.node_id
                    $jobguid = [guid]::NewGuid().ToString()

                    $description = "XRay Test VM"
                    $operatingSystem = Get-SCOperatingSystem | Where-Object {$_.Name -eq "Ubuntu Linux 16.04 (64 bit)"}
                    $vmTemplate = Get-SCVMTemplate -ID $Using:baseTemplateID
                    $dataDisks = $Using:dataDisks

                    $virtualMachineConfiguration = New-SCVMConfiguration -VMTemplate $vmTemplate -Name $vmName

                    # We need to check for object type returned by the Get-SCVMHost cmdlet since there seems to be a bug
                    # where we occasionally get some other type instead of VMM host
                    $i = 0
                    $maxIterations = 10
                    do {
                        $wrongObjType = $false
                        $vmHost = Get-SCVMHost -ID $vmHostId
                        $i = $i + 1
                        if ($vmHost.GetType().FullName -ne "Microsoft.SystemCenter.VirtualMachineManager.Host") {
                            $wrongObjType = $true
                            Start-Sleep -Seconds 5
                        }
                    } while($wrongObjType -and $i -le $maxIterations)

                    if ($wrongObjType) {
                        throw "Cannot get VMHost '$vmHostId'."
                    }

                    $virtualMachineConfiguration = Set-SCVMConfiguration -VMConfiguration $virtualMachineConfiguration -VMHost $vmHost -PinVMHost $true

                    $allNICConfigurations = Get-SCVirtualNetworkAdapterConfiguration -VMConfiguration $virtualMachineConfiguration
                    # Update-SCVMConfiguration -VMConfiguration $virtualMachineConfiguration | Out-Null

                    $virtualMachineConfiguration = Set-SCVMConfiguration -VMConfiguration $virtualMachineConfiguration -VMLocation $Using:vmDatastorePath -PinVMLocation $true

                    $VHDConfiguration = Get-SCVirtualHardDiskConfiguration -VMConfiguration $virtualMachineConfiguration
                    foreach($vhd in $VHDConfiguration)
                    {
                        $fileName = $vhd.SourceVirtualDiskDrive.VirtualHardDisk.Name
                        if ($fileName.IndexOf("_data") -ge 0) {
                            $fileName = $vmName + $fileName.SubString($fileName.IndexOf("_data"))
                        }
                        else {
                            $fileName = $vmName + "_boot"
                        }
                        if (-not $Using:differencingDisksPath) {
                            $fileName += ".vhdx"
                            Set-SCVirtualHardDiskConfiguration -VHDConfiguration $vhd -PinSourceLocation $false `
                                                               -DestinationLocation $Using:vmDatastorePath `
                                                               -StorageQoSPolicy $null -FileName $fileName | Out-Null
                        } else {
                            $fileName += "_diff.vhdx"
                            $parentDiffDiskPath = $Using:differencingDisksPath
                            Set-SCVirtualHardDiskConfiguration -VHDConfiguration $vhd -PinSourceLocation $false `
                                                               -DestinationLocation $Using:vmDatastorePath `
                                                               -FileName $fileName -PinDestinationLocation $false `
                                                               -ParentVirtualHardDiskDestinationPath $parentDiffDiskPath `
                                                               -StorageQoSPolicy $null -DeploymentOption "UseDifferencing" | Out-Null
                        }
                    }
                    # Update-SCVMConfiguration -VMConfiguration $virtualMachineConfiguration | Out-Null

                    if($dataDisks -eq $null -and $vmTemplate.Name.IndexOf("_goldimage") -ge 0)
                    {
                        $dataDisks = @()
                        $hardDiskIds = Get-SCVirtualMachine -Name $vmTemplate.Name | Select-Object -ExpandProperty VirtualDiskDrives |
                                        Where-Object VolumeType -EQ None | Select-Object -ExpandProperty VirtualHardDiskID
                        foreach($hardDiskId in $hardDiskIds)
                        {
                            $diskSize = Get-SCVirtualHardDisk -ID $hardDiskId | Select-Object -ExpandProperty MaximumSize
                            $dataDisks += $diskSize / 1024 / 1024 / 1024
                        }
                    }
                    $i = 0
                    if($dataDisks)
                    {
                        foreach($diskSize in $dataDisks)
                        {
                            $fileName = $vmName + "_data_$i.vhdx"
                            $hardDisk = New-SCVirtualDiskDrive -SCSI -Bus 0 -LUN $i -JobGroup $jobguid -VirtualHardDiskSizeMB ($diskSize * 1024) -CreateDiffDisk $false -Dynamic -Filename $fileName -VolumeType None
                            $i++
                        }
                    }
                    Update-SCVMConfiguration -VMConfiguration $virtualMachineConfiguration | Out-Null
                    if(-not $Using:differencingDisksPath) {
                        $vm = New-SCVirtualMachine -Name $vmName -VMConfiguration $virtualMachineConfiguration -Description $description `
                                                   -BlockDynamicOptimization $false -JobGroup $jobguid -RunAsynchronously `
                                                   -JobVariable XRayCreateVM -StartAction "NeverAutoTurnOnVM" -StopAction ShutdownGuestOS `
                                                   -OperatingSystem $operatingSystem
                    } else {
                        $vm = New-SCVirtualMachine -Name $vmName -VMConfiguration $virtualMachineConfiguration -Description $description `
                                                   -BlockDynamicOptimization $false -JobGroup $jobguid -RunAsynchronously `
                                                   -JobVariable XRayCreateVM -StartAction "NeverAutoTurnOnVM" -StopAction ShutdownGuestOS `
                                                   -OperatingSystem $operatingSystem -UseDiffDiskOptimization
                    }
                    $vm = $null
                    $XRayCreateVm.ID.Guid
                    $allNICConfigurations = $null
                    $hardDisk = $null
                }
            }
            $jobs = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

            if($jobs)
            {
                foreach($job in $jobs){
                    $task = @{}
                    $task["task_id"] = $job.ToString()
                    $task["task_type"] = "vmm"
                    $ret += $task
                }
            }
            else {
                Write-Log "ERROR"  "Cannot start PS task to create VM."
                throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Cannot start PS task to create VM.', $params)
            }
        }
        else {
            Write-Log "ERROR"  "Base template VM does not exist."
            throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Base template VM does not exist.', $params)
        }

        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error while creating VMs. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while creating VMs. " + $msg, $params)
    }

    return $response
}

Function Move-VmmVMDatastore($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmDatastoreMap = $params.vm_datastore_map

        $MoveVMStorage = {
            $jobguid = [guid]::NewGuid().ToString()

            $vm = Get-SCVirtualMachine -ID $Using:vmId -ErrorAction Stop
            $vmHost = $vm.VMHost

            Move-SCVirtualMachine -VM $vm -VMHost $vmHost -Path $Using:vmDatastorePath -UseLAN -RunAsynchronously `
                                  -JobGroup $jobguid -JobVariable XRayMoveVMDatastore | Out-Null
            $XRayMoveVMDatastore.ID.Guid
        }

        $ret = @()

        foreach($vm in $vmDatastoreMap)
        {
            $vmId = $vm.vm_id
            $vmDatastorePath = $vm.datastore_name

            $taskId = Invoke-Command -Session $session -ScriptBlock $MoveVMStorage -ErrorAction Stop

            if($taskId)
            {
                $task = @{}
                $task["task_id"] = $taskId.toString()
                $task["task_type"] = "vmm"
                $ret += $task
            }
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error while moving datastore. "+ $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while moving datastore. " + $msg, $params)
    }

    return $response
}

Function Move-VmmVM($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmHostMap = $params.vm_host_map
        $vmDatastorePath = $params.vm_datastore_path

        $MoveVM = {
            $jobguid = [guid]::NewGuid().ToString()
            $vmHostId = $Using:vmHostId
            $vmId = $Using:vmId
            $vmDatastorePath = $Using:vmDatastorePath

            $vm = Get-SCVirtualMachine -ID $vmId

            # We need to check for object type returned by the Get-SCVMHost cmdlet since there seems to be a bug
            # where we occasionally get some other type instead of VMM host
            $i = 0
            $maxIterations = 10
            do {
                $wrongObjType = $false
                $vmHost = Get-SCVMHost -ID $vmHostId
                $i = $i + 1
                if ($vmHost.GetType().FullName -ne "Microsoft.SystemCenter.VirtualMachineManager.Host") {
                    $wrongObjType = $true
                    Start-Sleep -Seconds 5
                }
            } while($wrongObjType -and $i -le $maxIterations)

            if ($wrongObjType) {
                throw "Cannot get VMHost '$vmHostId'."
            }

            Move-SCVirtualMachine -VM $vm -VMHost $vmHost -UseLAN -RunAsynchronously -JobGroup $jobguid `
                                  -Path $vmDatastorePath -JobVariable XRayMoveVM | Out-Null
            $XRayMoveVM.ID.Guid
        }

        $ret = @()

        foreach($vm in $vmHostMap)
        {
            $vmId = $vm.vm_id
            $vmHostId = $vm.node_id

            $taskId = Invoke-Command -Session $session -ScriptBlock $MoveVM -ErrorAction Stop

            if($taskId)
            {
                $task = @{}
                $task["task_id"] = $taskId.toString()
                $task["task_type"] = "vmm"
                $ret += $task
            }
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"   "Error while moving VMs. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while moving VMs. " + $msg, $params)
    }

    return $response
}

Function New-VmmVMClone($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $baseVmId = $params.base_vm_id
        $vmName = $params.vm_name
        $vmDatastorePath = $params.vm_datastore_path

        $CloneVm = {
            $jobguid = [guid]::NewGuid().ToString()
            $baseVmId = $Using:baseVmId
            $vmName = $Using:vmName
            $vmDatastorePath = $Using:vmDatastorePath

            $baseVm = Get-SCVirtualMachine -ID $baseVmId
            $vmHost = $baseVm.VMHost
            $operatingSystem = Get-SCOperatingSystem | Where-Object {$_.Name -eq "None"}

            $virtualNetworkAdapter = Get-SCVirtualNetworkAdapter -VM $baseVm
            $VMNetwork = $virtualNetworkAdapter.VMNetwork

            if($virtualNetworkAdapter.VLanEnabled)
            {
                $newNetworkAdapter = Set-SCVirtualNetworkAdapter -VirtualNetworkAdapter $VirtualNetworkAdapter -VMNetwork $VMNetwork `
                                                                 -VLanEnabled $true -VLanID $virtualNetworkAdapter.VLanID `
                                                                 -MACAddressType Dynamic -IPv4AddressType Dynamic -IPv6AddressType Dynamic `
                                                                 -NoPortClassification -EnableVMNetworkOptimization $false `
                                                                 -EnableMACAddressSpoofing $false -JobGroup $jobguid
            }
            else
            {
                $newNetworkAdapter = Set-SCVirtualNetworkAdapter -VirtualNetworkAdapter $VirtualNetworkAdapter -VMNetwork $VMNetwork `
                                                                 -MACAddressType Dynamic -IPv4AddressType Dynamic -IPv6AddressType Dynamic `
                                                                 -NoPortClassification -EnableVMNetworkOptimization $false `
                                                                 -EnableMACAddressSpoofing $false -JobGroup $jobguid
            }

            $newVm = New-SCVirtualMachine -Name $vmName -Path $vmDatastorePath -VM $baseVm -VMHost $vmHost -JobGroup $jobguid `
                                          -SkipInstallVirtualizationGuestServices -JobVariable XRayCreateVM -RunAsynchronously `
                                          -OperatingSystem $operatingSystem -StartAction NeverAutoTurnOnVM -StopAction ShutdownGuestOS

            $XRayCreateVm.ID.Guid

            $newNetworkAdapter = $null
            $newVm = $null
        }

        $ret = @()

        $taskId = Invoke-Command -Session $session -ScriptBlock $CloneVm -ErrorAction Stop
        if($taskId)
        {
            $obj = @{}
            $obj['vm_id'] = $vmName
            $obj['task_id'] = $taskId.ToString()
            $obj["task_type"] = "vmm"
            $ret += $obj
        }
        else {
            Write-Log "ERROR"  "Internal Server Error. Cannot start VMM job for VM cloning."
            throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Cannot start VMM job for VM cloning.', $params)
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error while cloning VMs. " +$msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while cloning VMs. " + $msg, $params)
    }

    return $response
}

Function ConvertTo-Template($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $targetDir = $params.target_dir
        $templateName = $params.template_name
        $vmmLibraryServerShare = $params.vmm_library_server_share

        $ConvertToTemplate = {
            $targetDir = $Using:targetDir
            $templateName = $Using:templateName
            $vmmLibraryServerShare = $Using:vmmLibraryServerShare
            $cluster_name = $Using:cluster_name

            $libraryServerShareObject = Get-SCLibraryShare | Where-Object Path -eq $vmmLibraryServerShare
            $libraryServer =  $libraryServerShareObject.LibraryServer

            $vm = Get-SCVirtualMachine -Name $templateName | Where-Object OperatingSystem -eq 'None'
            if($vm.Status -eq "Saved")
            {
                $vm | DiscardSavedState-VM | Out-Null
            }

            $description = "cluster_name=" + $cluster_name
            $sharePath = $vmmLibraryServerShare + "\" + $targetDir

            $template = New-SCVMTemplate -Name $templateName -Description $description -RunAsynchronously -VM $vm[0] `
                                         -LibraryServer $libraryServer -SharePath $sharePath -NoCustomization -JobVariable XRayCloneVM
            $template = $null

            $XRayCloneVM.ID.Guid
        }

        $ret = @()

        $taskId = Invoke-Command -Session $session -ScriptBlock $ConvertToTemplate -ErrorAction Stop
        if($taskId)
        {
            $obj = @{}
            $obj['vm_id'] = $templateName
            $obj['task_id'] = $taskId.ToString()
            $obj["task_type"] = "vmm"
            $ret += $obj
        }
        else {
            Write-Log "ERROR"   'Internal Server Error. Cannot start VMM job to create VM template.'
            throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", 'Internal Server Error. Cannot start VMM job to create VM template.', $params)
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error while converting VM to template. "+ $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while converting VM to template. " + $msg, $params)
    }

    return $response
}

# Removes VM from the cluster.
Function Remove-VmmVM($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $vmIds = $params.vm_ids
        $forceDelete = $params.force_delete

        $sc = {
            $tasks = @{}
            $jobs = Get-SCJob -Running -Name 'Create virtual machine'
            foreach($vmId in $Using:vmIds) {
                $jobs | Where-Object TargetObjectID -eq $vmId | Stop-SCJob | Out-Null
                $vm = Get-SCVirtualMachine -ID $vmId
                if($vm) {
                    if($Using:forceDelete) {
                        Remove-SCVirtualMachine -VM $vm -RunAsynchronously -JobVariable XRayDeleteVM -Force | Out-Null
                    } else {
                        try {
                            Remove-SCVirtualMachine -VM $vm -RunAsynchronously -JobVariable XRayDeleteVM | Out-Null
                        } catch {
                            # do nothing
                        }
                    }
                    if($XRayDeleteVM) {
                        $tasks[$vmId] = $XRayDeleteVM.ID.Guid
                    } else {
                        $tasks[$vmId] = [GUID]::Empty
                    }
                }
            }
            $tasks
        }
        $tasks = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $ret = @()
        foreach($task in $tasks.GetEnumerator()) {
            $obj = @{}
            $obj['vm_id'] = $task.Name
            $obj['task_id'] = $task.Value
            $obj["task_type"] = "vmm"
            $ret += $obj
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error while removing VMs. "+ $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while removing VMs. " + $msg, $params)
    }

    return $response
}

Function Set-VmmVMSnapshot($session, $cluster_name, $json_params="{}") {
    $params = ConvertFrom-Json $json_params
    $response = $null

    try {
        $sc = {
            $tasks = @{}
            foreach($param in $Using:params){
                $vm_id = $param.vm_id
                $vm = Get-SCVirtualMachine -ID $vm_id
                $name = $param.name
                $description = ""
                if($param.description -ne $null){
                    $description = $param.description
                }
                $result = New-SCVMCheckpoint -VM $vm -Name $name -Description $description -RunAsynchronously -jobvariable "XraySnapshotVM"
                $result = $null
                $tasks[$vm_id] = $XraySnapshotVM.ID.Guid
            }
            $tasks
        }
        $tasks = Invoke-Command -Session $session -ScriptBlock $sc -ErrorAction Stop

        $ret = @()
        foreach($task in $tasks.GetEnumerator())
        {
            $obj = @{}
            $obj['vm_id'] = $task.Name
            $obj['task_id'] = $task.Value
            $obj["task_type"] = "vmm"
            $ret += $obj
        }
        $response = [HypervRestApiResponse]::new(200, $ret)
    } catch [HypervRestApiException] {
        Write-Log "ERROR"  $_.Exception
        throw $_.Exception
    } catch {
        $msg = $_.Exception.Message
        Write-Log "ERROR"  "Error while creating VM snapshot. " + $msg
        throw [HypervRestApiException]::new(500, "INTERNAL_ERROR", "Error while creating VM snapshot. " + $msg, $params)
    }

    return $response
}
