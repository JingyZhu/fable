#!/bin/bash

# --cap-add=SYS_ADMIN used to make chrome being able to run
sudo docker run -it --env= ROOT_USER=1 \
     --name fable fable
