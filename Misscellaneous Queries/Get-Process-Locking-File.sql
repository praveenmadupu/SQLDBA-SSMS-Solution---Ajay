PowerShell script to check an application that's locking a file?
https://docs.microsoft.com/en-us/sysinternals/downloads/handle

Get-LockingProcess -Path 'E:\Get-MSSQLLinkPasswords.ps1.txt'

handle 'E:\Get-MSSQLLinkPasswords.ps1.txt' -accepteula