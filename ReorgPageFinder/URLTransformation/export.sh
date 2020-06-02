#!/bin/bash
# Output program to binary executable. Dotnet is required to compile

dotnet clean
dotnet restore
dotnet publish -c release -r linux-x64 -o URLTransformation