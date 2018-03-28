Get-EventLog -ComputerName TUL1CORPWIT1,TUL1CIPXIDB3,TUL1CIPXIDB2 -LogName Application | 
    Where-Object {$_.TimeGenerated -ge '3/19/2018 8:00:00 PM' -and ($_.Message -ilike '*The specified network name is no longer available*' -or $_.Message -ilike '*timed out*')} |
        Select-Object MachineName, TimeGenerated, Source, Message | ft -AutoSize -Wrap
