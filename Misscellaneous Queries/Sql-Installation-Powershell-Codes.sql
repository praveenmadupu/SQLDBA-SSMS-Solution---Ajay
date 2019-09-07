Install-WindowsFeature Net-Framework-Core -source D:\Sources\SxS\

netsh advfirewall firewall add rule name="Microsoft iSCSI Software Target Service-TCP-3260" dir=in action=allow protocol=TCP localport=3260
netsh advfirewall firewall add rule name="Microsoft iSCSI Software Target Service-TCP-135" dir=in action=allow protocol=TCP localport=135
netsh advfirewall firewall add rule name="Microsoft iSCSI Software Target Service-UDP-138" dir=in action=allow protocol=UDP localport=138
netsh advfirewall firewall add rule name="Microsoft iSCSI Software Target Service" dir=in action=allow program="%SystemRoot%\System32\WinTarget.exe" enable=yes
netsh advfirewall firewall add rule name="Microsoft iSCSI Software Target Service Status Proxy" dir=in action=allow program="%SystemRoot%\System32\WTStatusProxy.exe" enable=yes

Netsh interface ipv4 set address "Ethernet" static 10.10.10.21
Netsh interface ipv4 set dnsservers "Ethernet" static 10.10.10.10 primary
netdom renamecomputer %computername% /newname:TSQLPRD01
netdom join TSQLPRD01 /domain:Contso.com

$Servers = 'tsqlprd01','tsqlprd02','tsqlprd03'
$command = {
    netsh advfirewall firewall add rule name="SQL Server (MSSQLSERVER)" dir=in action=allow protocol=TCP localport=1433
}
Invoke-Command -ComputerName $Servers -ScriptBlock $command

$command = {
    netsh advfirewall firewall add rule name="SQL Server (SQL2017)" dir=in action=allow protocol=TCP localport=1432
}
Invoke-Command -ComputerName $Servers -ScriptBlock $command

$command = {
    netsh advfirewall firewall add rule name="SQL Server Browser" dir=in action=allow protocol=UDP localport=1434 
}
Invoke-Command -ComputerName $Servers -ScriptBlock $command


diskmgmt.msc
KB2919355
https://www.sqlnethub.com/blog/installing-sql-server-2016-on-windows-server-2012-r2/