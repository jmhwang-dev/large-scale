git fetch -p
git branch -vv | awk '/: gone]/ {print $1}' | xargs -r git branch -d
