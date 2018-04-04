/*	Mirroring Failover */
Get-EventLog -ComputerName TUL1CORPWIT1,TUL1CIPXIDB3,TUL1CIPXIDB2 -LogName Application | 
    Where-Object {$_.TimeGenerated -ge '3/19/2018 8:00:00 PM' -and ($_.Message -ilike '*The specified network name is no longer available*' -or $_.Message -ilike '*timed out*')} |
        Select-Object MachineName, TimeGenerated, Source, Message | ft -AutoSize -Wrap

/*	Shutdown/Reboot	*/
Get-EventLog -ComputerName tul1cipmbdb1, tul1cipmbdb2 -LogName System | 
    Where-Object { $_.TimeGenerated -ge '4/01/2018 4:00:00 AM' -and ($_.Message -ilike "*shutdown*" -or $_.Message -ilike "*reboot*")} |
        Format-Table MachineName, EventID,TimeGenerated, EntryType, Source, Message, UserName -AutoSize -Wrap

/*	Shutdown/Reboot	*/
Get-EventLog -ComputerName tul1cipmbdb1, tul1cipmbdb2 -LogName System -InstanceId 1074,41,6008,6005,7036 | 
    Where-Object { $_.TimeGenerated -ge '4/01/2018 4:00:00 AM' }