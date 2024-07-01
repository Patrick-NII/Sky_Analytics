
remotes=("myrepo" "collegerepo")

branch="algo/data-etl"

for remote in "${remotes[@]}";do
	echo "Pushing to $remote"
	git push $remote $branch
done
