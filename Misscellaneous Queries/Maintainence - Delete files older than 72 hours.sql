# Delete files older than 48 hours
$CleanUpTimeHours = 168
$a = Get-ChildItem C:\TEMP
try {
	foreach($x in $a)
    {
        $y = ((Get-Date) - $x.CreationTime);
        $y = ($y.Days * 24) + ($y.Hours);
        if ($y -gt $CleanUpTimeHours -and $x.PsISContainer -ne $True)
            {	$x.Delete()
                #Write-Host $x.Name
			}
    }
	return 0; #success
}
catch {
	return 1; #failure
}