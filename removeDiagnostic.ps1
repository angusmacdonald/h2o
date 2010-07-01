clear-Host
$file = gci . *.java -recurse|where-object {!($_.psiscontainer)}

write-host "Removing diagnostic commands.`n`n"
if ($file){
	foreach ($str in $file)
	{

	$path = $str.FullName
	echo $path
	$cont = get-content -path $path
		
	$cont | foreach {$_ -replace "^((\s)*((if(.)*\((.)*\)(.)*)?Diagnostic\.)(.)*;(.)*)", "//`$1"} | set-content $path
	}
}
write-host "`n`nDone."
