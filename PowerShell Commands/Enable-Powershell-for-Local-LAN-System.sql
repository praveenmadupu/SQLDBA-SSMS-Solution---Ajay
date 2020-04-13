https://stackoverflow.com/questions/14952833/get-wmiobject-win32-process-computername-gets-error-access-denied-code-0x8
	-- https://stackoverflow.com/a/14953535/4449743
	-- https://stackoverflow.com/a/11338395/4449743
Launch "wmimgmt.msc"
Right-click on "WMI Control (Local)" then select Properties
Go to the "Security" tab and select "Security" then "Advanced" then "Add"
Select the user name(s) or group(s) you want to grant access to the WMI and click ok
Grant the required permissions, I recommend starting off by granting all permissions to ensure that access is given, then remove permissions later as necessary.
Ensure the "Apply to" option is set to "This namespace and subnamespaces"
Save and exit all prompts
Add the user(s) or group(s) to the Local "Distributed COM Users" group. Note: The "Authenticated Users" and "Everyone" groups cannot be added here, so you can alternatively use the "Domain Users" group.

netsh advfirewall firewall set rule group="Windows Management Instrumentation (WMI)" new enable=yes

--	=======================================================================================
#Install-Module CredentialManager -Force;
#Get-Command -Module CredentialManager
$computerName = 'msi';

Get-Credential -UserName "$computerName\Ajay" -Message "Password please" | New-StoredCredential -Target $computerName -Persist LocalMachine
$creds = Get-StoredCredential -Target $computerName;

Get-VolumeInfo -ComputerName $computerName
Get-DbaDiskSpace -ComputerName $computerName -Credential $creds