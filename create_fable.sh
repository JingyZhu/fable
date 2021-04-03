#!/bin/bash

# --cap-add=SYS_ADMIN used to make chrome being able to run
sudo docker run -it --cap-add=SYS_ADMIN \
     --name fable fable
