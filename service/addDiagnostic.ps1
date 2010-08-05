clear-Host
$file = gci . *.java -recurse|where-object {!($_.psiscontainer)}

write-host "Re-adding diagnostic commands.`n`n"
foreach ($str in $file)
{
	$path = $str.FullName
	echo $path
	$cont = get-content -path $path
	$cont | foreach {$_ -replace "^(\s)*(//)(\s)*((if(.)*)?Diagnostic\.)", "`$1`$3`$4"} | set-content $path

}
write-host "`n`nDone."
