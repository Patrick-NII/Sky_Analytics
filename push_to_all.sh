#!/bin/bash
remotes=("myrepo""collegerepo")

branch="Bugfix"

for remote in "${remotes[@]}";do
	echo "Pushing to $remote"
	git push $remote $branch
done
