$SQLServers = @('tul1pub2008c1','tul1mdpdwds12','tul1mdpdwds13','tul1mdpdwmid01','tul1cosmo2008','tul1sub2008')
$Users = @('corporate\gsambasivam','corporate\anasingh','corporate\gramaprasad','corporate\narici','corporate\skaliyaperumal')


foreach($Srv in $SQLServers)
{
    foreach($Usr in $Users)
    {
        $command = {
            param($User)
            Write-Output "$($env:COMPUTERNAME) => $User";
            net Localgroup Administrators $User /add     
        }
        Invoke-Command -ComputerName $Srv -ScriptBlock $command -Args $Usr;
    }
}