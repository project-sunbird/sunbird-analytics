verbose=off
contentUrl=''
conceptUrl=''
range='1,10'
while [ $# -gt 0 ]
do
	case "$1" in
		-v) verbose=on;;
		-ct) conceptUrl="$2"; shift;;
		-cn) contentUrl="$2"; shift;;
		-r) range="$2"; shift;;
		*)  break;;	# terminate while loop
	esac
	shift
done
#Need help to get this part running
if ["$verbose" & "$contentUrl"!='']; then
	python downloadContent.py --range "'$range'" --url "$contentUrl" -- verbose
elif ["$verbose"]; then
	python downloadContent.py --range "'$range'" -- verbose
elif ["$contentUrl"!='']; then
	python downloadContent.py --range "'$range'" --url "$contentUrl"
else
	python downloadContent.py --range "'$range'"
fi
python handleMedia.py
python parseJson.py
if ["$contentUrl"!='']; then
	python getConcepts.py --url "$contentUrl"
python conceptFilter.py
python toEnrichedJson.py
