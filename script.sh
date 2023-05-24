#!/bin/sh
date=$(date +"%A, %B %C %Y")
echo date
#cargo run -- --latest && cp ./target/debug/pricecatcher.zip pricecatcher.zip
git add ./ && git commit -m "daily update" #&& git push origin
