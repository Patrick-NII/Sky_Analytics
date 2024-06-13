remotes=("myrepo" "andrearepo")

branch="Patrick"

for remote in "${remotes[@]}"; do
    echo "Pushing to $remote"
    git push $remote $branch
done
