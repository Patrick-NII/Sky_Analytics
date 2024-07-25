<<<<<<< HEAD

remotes=("myrepo" "collegerepo")

branch="algo/data-etl"

for remote in "${remotes[@]}";do
	echo "Pushing to $remote"
	git push $remote $branch
=======
remotes=("myrepo" "andrearepo")

branch="Patrick"

for remote in "${remotes[@]}"; do
    echo "Pushing to $remote"
    git push $remote $branch
>>>>>>> c696780c62bbe1d292208a62dd625bd238ace301
done
