#!/bin/sh
date=$(date +"%A, %B %C %Y")
cargo run -- --latest && cp ./target/debug/pricecatcher.zip pricecatcher.zip
git add ./ && git commit -m "daily update: $date" && git push origin
