#!/bin/bash

git filter-branch --env-filter 'if [ "$GIT_AUTHOR_NAME" = "Jingyuan Zhu" ]; then
     GIT_AUTHOR_EMAIL=jingyz@umich.edu;
     GIT_AUTHOR_NAME="JingyZhu";
     GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL;
     GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"; fi' -- --all